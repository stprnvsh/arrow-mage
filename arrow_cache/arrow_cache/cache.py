import time
import threading
import logging
import uuid
import os
from typing import Any, Dict, List, Optional, Union, Tuple, Callable, Set
import pyarrow as pa
import pandas as pd
import weakref
import psutil
import re
import json
import pyarrow.compute as pc

from .cache_entry import CacheEntry
from .metadata_store import MetadataStore
from .converters import to_arrow_table, from_arrow_table, estimate_size_bytes, _map_arrow_type_to_duckdb, _get_geometry_info
from .eviction_policy import create_eviction_policy, EvictionPolicy
from .config import ArrowCacheConfig, DEFAULT_CONFIG
from .memory import MemoryManager, apply_compression, zero_copy_slice, estimate_table_memory_usage
from .partitioning import TablePartition, PartitionedTable, partition_table
from .threading import ThreadPoolManager, AsyncTaskManager, BackgroundProcessingQueue
from .query_optimization import QueryOptimizer, optimize_duckdb_connection, explain_query, prepare_query_for_execution, extract_table_references
from .duckdb_ingest import import_from_duckdb

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def _parse_type_str(type_str):
    """Parse a string representation of an Arrow type into an actual type instance."""
    import pyarrow as pa
    
    # Clean up the type string
    type_str = type_str.strip().lower()
    
    # Map common type strings to Arrow type instances
    if 'string' in type_str:
        return pa.string()
    elif 'int64' in type_str or 'int32' in type_str:
        return pa.int64()
    elif 'int' in type_str:
        return pa.int32()
    elif 'float' in type_str or 'double' in type_str:
        return pa.float64()
    elif 'bool' in type_str:
        return pa.bool_()
    elif 'date' in type_str:
        return pa.date32()
    elif 'timestamp' in type_str:
        return pa.timestamp('ns')
    elif 'binary' in type_str:
        return pa.binary()
    else:
        # Default to string for unknown types
        return pa.string()

def _is_numeric_type(arrow_type):
    """Check if an Arrow type is numeric."""
    return (
        pa.types.is_integer(arrow_type) or
        pa.types.is_floating(arrow_type) or
        pa.types.is_decimal(arrow_type)
    )

class ArrowCache:
    """
    High-performance in-memory cache for data frames and tables using Apache Arrow.
    
    Uses DuckDB for metadata management and query capabilities.
    """
    
    def __init__(
        self,
        max_size_bytes: Optional[int] = None,
        eviction_policy: str = "lru",
        check_interval: float = 60.0,
        metadata_db_path: Optional[str] = None,
        policy_args: Optional[Dict[str, Any]] = None,
        config: Optional[Union[Dict[str, Any], ArrowCacheConfig]] = None
    ):
        """
        Initialize the Arrow cache.
        
        Args:
            max_size_bytes: Maximum cache size in bytes (None for unlimited)
            eviction_policy: Name of the eviction policy to use
            check_interval: Interval in seconds to check for expired entries
            metadata_db_path: Path to the DuckDB database file (None for in-memory)
            policy_args: Additional arguments for the eviction policy
            config: Additional configuration options (overrides individual parameters)
        """
        # Create configuration
        if config is None:
            # Use individual parameters
            config_dict = DEFAULT_CONFIG.copy()
            if max_size_bytes is not None:
                config_dict["memory_limit"] = max_size_bytes
            self.config = ArrowCacheConfig(**config_dict)
        elif isinstance(config, dict):
            # Convert dict to ArrowCacheConfig
            self.config = ArrowCacheConfig(**config)
        else:
            # Use provided ArrowCacheConfig
            self.config = config
        
        # Set instance parameters from config
        self.max_size_bytes = self.config["memory_limit"]
        self.current_size_bytes = 0
        
        # Initialize the memory manager
        self.memory_manager = MemoryManager(
            self.config,
            spill_callback=self._handle_memory_pressure
        )
        
        # Initialize the thread pool
        self.thread_pool = ThreadPoolManager(self.config)
        
        # Initialize async task manager for background operations
        self.async_tasks = AsyncTaskManager(self.thread_pool)
        
        # Initialize background processing queue
        self.bg_queue = BackgroundProcessingQueue(
            self.thread_pool,
            worker_count=self.config["background_threads"]
        )
        
        # Initialize the metadata store with lazy registration if enabled
        self.metadata_store = MetadataStore(
            metadata_db_path, 
            lazy_registration=self.config.get("lazy_duckdb_registration", True)
        )
        
        # Initialize the query optimizer
        self.query_optimizer = QueryOptimizer(self.config)
        
        # Initialize the eviction policy
        self.eviction_policy = create_eviction_policy(
            eviction_policy, **(policy_args or {})
        )
        
        # Cache entries dictionary (key -> CacheEntry or PartitionedTable)
        self.entries = {}
        self.partitioned_tables = {}  # key -> PartitionedTable
        
        # Cache statistics
        self.hits = 0
        self.misses = 0
        
        # Lock for thread safety
        self.lock = threading.RLock()
        
        # Expiration checker thread
        self.check_interval = check_interval
        self._stop_checker_event = threading.Event()
        self._checker_thread = None
        
        # Start the expiration checker if an interval is provided
        if check_interval > 0:
            self._start_checker()
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def put(
        self,
        key: str,
        data: Any,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        overwrite: bool = True,
        preserve_index: bool = True,
        auto_partition: Optional[bool] = None
    ) -> str:
        """
        Put data into the cache.
        
        Args:
            key: Key to store the data under
            data: Data to store (DataFrame, GeoDataFrame, Arrow table, etc.)
            ttl: Time-to-live in seconds (None for no expiration)
            metadata: Additional metadata to store with the entry
            overwrite: Whether to overwrite existing entry if key exists
            preserve_index: Whether to preserve DataFrame indices
            auto_partition: Whether to automatically partition large datasets
                (None uses the config default)
            
        Returns:
            The key under which the data is stored
        """
        with self.lock:
            # Type check key
            if not isinstance(key, str):
                raise TypeError("Cache key must be a string")
                
            # Check if entry exists and overwrite is allowed
            if key in self.entries and not overwrite:
                raise ValueError(f"Entry with key '{key}' already exists and overwrite is False")
            
            # Auto-partition setting
            if auto_partition is None:
                auto_partition = self.config["auto_partition"]
            
            # Prepare metadata
            if metadata is None:
                metadata = {}
                
            # Add creation timestamp to metadata
            combined_metadata = {
                "created_at": time.time(),
                **metadata
            }
            
            # Prepare table and extract metadata from conversion
            table, conversion_metadata = self._prepare_table_for_put(data, preserve_index, self.config)
            combined_metadata.update(conversion_metadata) # Merge conversion metadata
            
            try:
                # Calculate size in bytes using our accurate memory estimation
                size_bytes = estimate_table_memory_usage(table)
                
                # Add size info to logs for debugging
                logger.info(f"Table size for '{key}': {size_bytes/(1024*1024):.2f} MB, {table.num_rows} rows, {table.num_columns} columns")
                
                # Check if we have enough space or need to evict entries
                if self.max_size_bytes is not None:
                    space_needed = size_bytes
                    if key in self.entries:
                        # Fix for PartitionedTable which uses total_size_bytes instead of size_bytes
                        if key in self.partitioned_tables:
                            space_needed -= self.partitioned_tables[key].total_size_bytes
                        else:
                            space_needed -= self.entries[key].size_bytes
                        
                    if space_needed > 0 and self.current_size_bytes + space_needed > self.max_size_bytes:
                        # Need to evict entries
                        if not self._evict(space_needed):
                            # Could not evict enough
                            logger.error(f"Could not make space for new entry: {size_bytes} bytes needed")
                            raise MemoryError(f"Could not make space for new entry: {size_bytes} bytes needed")
                
                # Get row count for partition decision
                row_count = table.num_rows
                
                should_partition = (
                    auto_partition and 
                    row_count > self.config["partition_size_rows"]
                )
                
                try:
                    if should_partition:
                        # Create a partitioned table
                        return self._put_partitioned(key, table, ttl, combined_metadata)
                    else:
                        # Create a normal cache entry
                        entry = CacheEntry(
                            key=key,
                            table=table,
                            size_bytes=size_bytes,
                            ttl=ttl,
                            metadata=combined_metadata
                        )
                        
                        # Remove old entry if it exists
                        if key in self.entries:
                            self._remove_entry(key)
                        
                        # First attempt to update the metadata store and register the table
                        try:
                            # Update the metadata store first (before registering the table)
                            # to satisfy foreign key constraint
                            self.metadata_store.add_entry(
                                key=key,
                                created_at=entry.created_at,
                                last_accessed_at=entry.last_accessed_at,
                                access_count=entry.access_count,
                                expires_at=entry.expires_at,
                                size_bytes=size_bytes,
                                schema=table.schema,
                                num_rows=table.num_rows,
                                metadata=combined_metadata
                            )
                            
                            # Then register with DuckDB for querying
                            self.metadata_store.register_table(key, table)
                            
                            # Only after successful metadata updates, add to entries dict
                            self.entries[key] = entry
                            self.current_size_bytes += size_bytes
                            
                            # Update eviction policy after successful addition
                            self.eviction_policy.add_entry(key, size_bytes)
                            
                            return key
                            
                        except Exception as metadata_error:
                            # Log and re-raise the metadata error
                            logger.error(f"Failed to update metadata for cache entry: {metadata_error}")
                            raise
                except Exception as e:
                    logger.error(f"Failed to add data to cache: {e}")
                    # Re-raise the exception
                    raise
                    
            except Exception as e:
                logger.error(f"Failed to process data for caching: {e}")
                raise
    
    def _put_partitioned(
        self,
        key: str,
        table: pa.Table,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Add a partitioned table to the cache.
        
        Args:
            key: Key to store the table under
            table: Arrow table to store
            ttl: Time-to-live in seconds (None for no expiration)
            metadata: Additional metadata
            
        Returns:
            The key under which the table is stored
        """
        # First, remove any existing entry with this key
        if key in self.entries:
            self._remove_entry(key)
        
        # Create the partitioned table
        partitioned = PartitionedTable(key, table.schema, self.config, metadata)
        
        # Add partitions
        partitions = partition_table(
            table, 
            self.config,
            self.config["partition_size_rows"],
            self.config["partition_size_bytes"]
        )
        
        for part_table in partitions:
            partitioned.add_partition(part_table)
            
        # Recalculate the size to ensure accuracy and prevent memory over-counting
        partitioned.recalculate_total_size()
        
        # Store in our collections
        self.entries[key] = partitioned
        self.partitioned_tables[key] = partitioned
        self.current_size_bytes += partitioned.total_size_bytes
        
        # Add to metadata store
        if metadata is None:
            metadata = {}
        
        # Add partitioning information to metadata
        metadata["partitioned"] = True
        metadata["partition_count"] = len(partitions)
        metadata["total_rows"] = partitioned.total_rows
        
        self.metadata_store.add_entry(
            key=key,
            created_at=time.time(),
            last_accessed_at=time.time(),
            access_count=0,
            expires_at=(time.time() + ttl) if ttl is not None else None,
            size_bytes=partitioned.total_size_bytes,
            schema=table.schema,
            num_rows=partitioned.total_rows,
            metadata=metadata
        )
        
        # Register the table with DuckDB
        # Note: For partitioned tables, we register the concatenated view
        self.metadata_store.register_table(key, partitioned.get_table())
        
        return key
    
    def get(
        self,
        key: str,
        target_type: Optional[str] = None,
        default: Any = None,
        offset: int = 0,
        limit: Optional[int] = None
    ) -> Any:
        """
        Get data from the cache.
        
        Args:
            key: Key to retrieve
            target_type: Target data type to convert to (pandas, geopandas, etc.)
            default: Value to return if key is not found
            offset: Row offset for slicing (useful for partitioned tables)
            limit: Maximum number of rows to return (None for all)
            
        Returns:
            The cached data or default if not found
        """
        with self.lock:
            entry = self.entries.get(key)
            
            if entry is None:
                self.misses += 1
                return default
            
            # Check if partitioned table
            if key in self.partitioned_tables:
                return self._get_partitioned(
                    key, target_type, offset, limit
                )
            
            # Regular cache entry
            # Check if expired
            if entry.is_expired():
                self._remove_entry(key)
                self.misses += 1
                return default
            
            # Update access stats
            entry.access()
            self.eviction_policy.update_entry_accessed(key)
            self.metadata_store.update_access_stats(
                key, entry.last_accessed_at, entry.access_count
            )
            
            self.hits += 1
            
            # Get the Arrow table
            table = entry.get_table()
            
            # Apply slicing if requested
            if offset > 0 or limit is not None:
                if offset >= table.num_rows:
                    return default
                if limit is None:
                    limit = table.num_rows - offset
                table = zero_copy_slice(table, offset, limit)
            
            # Convert to target type if requested
            if target_type is not None:
                return from_arrow_table(table, target_type, entry.metadata)
            else:
                # By default, return the Arrow table directly
                return table
    
    def _get_partitioned(
        self,
        key: str,
        target_type: Optional[str] = None,
        offset: int = 0,
        limit: Optional[int] = None
    ) -> Any:
        """
        Get data from a partitioned table
        
        Args:
            key: Key to retrieve
            target_type: Target data type to convert to
            offset: Row offset for slicing
            limit: Maximum number of rows to return
            
        Returns:
            The cached data
        """
        partitioned = self.partitioned_tables[key]
        
        # Update access stats in metadata store
        now = time.time()
        self.metadata_store.update_access_stats(key, now, 0)  # Increment happens in partitions
        
        self.hits += 1
        
        # Determine which partitions to load based on offset and limit
        if offset == 0 and limit is None:
            # Get full table
            table = partitioned.get_table()
        else:
            # Get slice
            if limit is None:
                limit = partitioned.total_rows - offset
            table = partitioned.get_slice(offset, limit)
        
        # Convert to target type if requested
        if target_type is not None:
            return from_arrow_table(table, target_type, partitioned.metadata)
        else:
            # By default, return the Arrow table directly
            return table
    
    def remove(self, key: str) -> bool:
        """
        Remove an entry from the cache.
        
        Args:
            key: Key to remove
            
        Returns:
            True if the entry was removed, False if it didn't exist
        """
        with self.lock:
            return self._remove_entry(key)
    
    def clear(self) -> None:
        """Clear all entries from the cache."""
        with self.lock:
            # Clear entries one by one to ensure proper cleanup
            keys = list(self.entries.keys())
            for key in keys:
                self._remove_entry(key)
            
            # Make sure our tracking is reset
            self.entries = {}
            self.partitioned_tables = {}
            self.current_size_bytes = 0
            
            # Clear the metadata store
            self.metadata_store.clear_all_entries()
    
    def get_keys(self) -> List[str]:
        """Get a list of all keys in the cache."""
        with self.lock:
            return list(self.entries.keys())
    
    def contains(self, key: str) -> bool:
        """
        Check if a key exists in the cache.
        
        Args:
            key: Key to check
            
        Returns:
            True if the key exists and is not expired, False otherwise
        """
        with self.lock:
            entry = self.entries.get(key)
            if entry is None:
                return False
            
            # Partitioned tables don't expire
            if key in self.partitioned_tables:
                return True
                
            if entry.is_expired():
                self._remove_entry(key)
                return False
                
            return True
    
    def get_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific cache entry.
        
        Args:
            key: Cache entry key
            
        Returns:
            Entry metadata or None if the entry doesn't exist
        """
        with self.lock:
            if key not in self.entries:
                return None
                
            return self.metadata_store.get_entry_metadata(key)
    
    def update_ttl(self, key: str, ttl: Optional[float]) -> bool:
        """
        Update the TTL for a cache entry.
        
        Args:
            key: Cache entry key
            ttl: New TTL in seconds (None for no expiration)
            
        Returns:
            True if successful, False if entry doesn't exist
        """
        with self.lock:
            entry = self.entries.get(key)
            if entry is None:
                return False
                
            # Partitioned tables don't use TTL currently
            if key in self.partitioned_tables:
                # Update in metadata store only
                metadata = self.metadata_store.get_entry_metadata(key)
                if metadata:
                    self.metadata_store.add_entry(
                        key=key,
                        created_at=metadata.get("created_at", time.time()),
                        last_accessed_at=metadata.get("last_accessed_at", time.time()),
                        access_count=metadata.get("access_count", 0),
                        expires_at=(time.time() + ttl) if ttl is not None else None,
                        size_bytes=metadata.get("size_bytes", 0),
                        schema=self.partitioned_tables[key].schema,
                        num_rows=self.partitioned_tables[key].total_rows,
                        metadata=metadata
                    )
                return True
                
            # Regular cache entry
            entry.update_ttl(ttl)
            
            # Update metadata store
            self.metadata_store.add_entry(
                key=key,
                created_at=entry.created_at,
                last_accessed_at=entry.last_accessed_at,
                access_count=entry.access_count,
                expires_at=entry.expires_at,
                size_bytes=entry.size_bytes,
                schema=entry.table.schema,
                num_rows=entry.table.num_rows,
                metadata=entry.metadata
            )
            
            return True
    
    def _deregister_tables(self, table_keys: List[str]) -> None:
        """
        Deregister tables from DuckDB to free memory after query execution.
        
        Args:
            table_keys: List of table keys to deregister
        """
        if not table_keys:
            return
            
        deregistered_tables = []
        
        for table_key in table_keys:
            # Skip empty strings or None values
            if not table_key:
                continue
                
            # Handle keys both with and without _cache_ prefix
            key = table_key[7:] if table_key.startswith('_cache_') else table_key
            
            # Skip if already deregistered in this batch
            if key in deregistered_tables:
                continue
                
            try:
                # Only deregister if table exists in our entries
                if key in self.entries:
                    # Deregister from DuckDB
                    self.metadata_store.unregister_table(key)
                    logger.debug(f"Deregistered table '_cache_{key}' from DuckDB")
                    deregistered_tables.append(key)
            except Exception as e:
                logger.warning(f"Error deregistering table '_cache_{key}': {e}")
                
        return deregistered_tables
        
    def query(self, sql: str, optimize: bool = True, auto_deregister: bool = True) -> pa.Table:
        """
        Execute a SQL query against the cache.
        
        Tables in cache can be accessed using the naming pattern `_cache_<key>`.
        For example, to query a cached table with key "my_table", use:
        SELECT * FROM _cache_my_table WHERE column = 'value'.
        
        Args:
            sql: SQL query to execute
            optimize: Whether to optimize the query (default: True)
            auto_deregister: Whether to automatically deregister tables after query (default: True)
            
        Returns:
            Arrow Table with query results
        """
        # --- Diagnostic: Ensure spatial extension is loaded --- 
        try:
            # This is inefficient but helps diagnose if the initial load isn't persisting
            self.metadata_store.con.execute("LOAD spatial;")
        except Exception as load_err:
            logger.warning(f"Diagnostic LOAD spatial before query failed: {load_err}")
            # Proceed anyway, the original error might provide more info
        # -----------------------------------------------------
        
        with self.lock:
            # Ensure DuckDB connection is available
            if self.metadata_store.is_closed:
                raise ValueError("Cache is closed")
                
            # Ensure all referenced tables are registered with DuckDB
            if not optimize and self.config.get("lazy_duckdb_registration", True):
                # If not using the optimizer, try to extract and register tables
                referenced_tables = self._extract_table_references(sql)
                if referenced_tables:
                    self._ensure_tables_registered(referenced_tables)
            
            # Always use query optimization for better performance unless explicitly disabled
            if optimize:
                # Use the query optimizer with proper thread safety
                try:
                    # Track execution time for performance monitoring
                    start_time = time.time()
                    
                    # Import the optimization functions
                    from .query_optimization import prepare_query_for_execution, extract_table_references
                    
                    # Prepare the query (ensures tables are registered and properly quoted)
                    prepared_sql = prepare_query_for_execution(
                        self.metadata_store.con, 
                        sql,
                        table_registry_callback=self._ensure_tables_registered
                    )
                    
                    # Execute the prepared query
                    result = self.metadata_store.con.execute(prepared_sql).arrow()
                    execution_time = time.time() - start_time
                    
                    # Deregister tables if requested
                    if auto_deregister:
                        table_refs = extract_table_references(sql)
                        if table_refs:
                            self._deregister_tables(table_refs)
                    
                    # Log query performance info
                    logger.info(f"Query executed in {execution_time:.3f}s")
                    
                    # Return the Arrow table directly
                    return result
                except Exception as e:
                    logger.error(f"Query execution failed: {e}")
                    raise
            else:
                # Execute directly without optimization
                try:
                    start_time = time.time()
                    result_table = self.metadata_store.query(sql)
                    execution_time = time.time() - start_time
                    logger.info(f"Direct query executed in {execution_time:.3f}s")
                    
                    # Deregister tables if auto_deregister is enabled
                    if auto_deregister:
                        referenced_tables = self._extract_table_references(sql)
                        if referenced_tables:
                            self._deregister_tables(referenced_tables)
                    
                    # Return the Arrow table directly
                    return result_table
                except Exception as e:
                    logger.error(f"Direct query execution failed: {e}")
                    raise
    
    def _extract_table_references(self, sql: str) -> List[str]:
        """
        Extract table references from a SQL query
        
        Args:
            sql: SQL query
            
        Returns:
            List of table references
        """
        tables = []
        
        # Special case pattern for function-like syntax: table_(params)
        function_pattern = re.compile(r'(FROM|JOIN)\s+(_?cache_[a-zA-Z0-9_]+)_?\s*\(([^)]*)\)', re.IGNORECASE)
        for match in function_pattern.finditer(sql):
            # Reconstruct the full table name with parentheses
            table_base = match.group(2)
            params = match.group(3)
            full_table = f"{table_base}({params})"
            
            # Also handle trailing underscore before parentheses
            if table_base.endswith('_'):
                full_table = f"{table_base[:-1]}({params})"
            else:
                full_table = f"{table_base}({params})"
                
            logger.info(f"Extracted table with function-like syntax: {full_table}")
            tables.append(full_table)
        
        # Extract tables from FROM clauses - now also handle table patterns with parentheses
        # Pattern for FROM with parentheses tables - e.g. FROM _cache_nyc_yellow_taxi_(jan_2023) or FROM cache_nyc_yellow_taxi(jan_2023)
        from_paren_pattern = re.compile(r'FROM\s+((?:_)?cache_[a-zA-Z0-9_]+\s*\([^)]*\))', re.IGNORECASE)
        for match in from_paren_pattern.finditer(sql):
            table_with_params = match.group(1).strip()
            # Check if not already wrapped in quotes
            if not (table_with_params.startswith('"') and table_with_params.endswith('"')):
                tables.append(table_with_params)
        
        # Standard FROM pattern (no parentheses)
        from_pattern = re.compile(r'FROM\s+(?:"|`)?([\w.]+)(?:"|`)?', re.IGNORECASE)
        for match in from_pattern.finditer(sql):
            from_table = match.group(1).strip()
            tables.append(from_table)
            
        # Extract tables from JOIN clauses - also handle parentheses tables
        # Pattern for JOIN with parentheses tables
        join_paren_pattern = re.compile(r'JOIN\s+((?:_)?cache_[a-zA-Z0-9_]+\s*\([^)]*\))', re.IGNORECASE)
        for match in join_paren_pattern.finditer(sql):
            table_with_params = match.group(1).strip()
            # Check if not already wrapped in quotes
            if not (table_with_params.startswith('"') and table_with_params.endswith('"')):
                tables.append(table_with_params)
        
        # Standard JOIN pattern
        join_pattern = re.compile(r'JOIN\s+(?:"|`)?([\w.]+)(?:"|`)?(?:\s+AS\s+[\w.]+|\s+ON\s+|$)', re.IGNORECASE)
        for match in join_pattern.finditer(sql):
            join_table = match.group(1).strip()
            
            # Handle the case where there might be extra parts after the table name
            if ' ' in join_table:
                join_table_parts = join_table.split()
                if len(join_table_parts) > 1:
                    join_table = join_table_parts[0].strip()
            
            # Handle _cache_ prefix
            if join_table.startswith('_cache_'):
                tables.append(join_table[7:])  # Remove '_cache_' prefix
            else:
                tables.append(join_table)
        
        # Remove duplicates
        unique_tables = []
        seen = set()
        for table in tables:
            if table.lower() not in seen:
                seen.add(table.lower())
                unique_tables.append(table)
        
        # Filter out common SQL keywords that might be mistaken for tables
        sql_keywords = {'select', 'from', 'where', 'group', 'order', 'having', 'limit', 'offset',
                       'join', 'inner', 'outer', 'left', 'right', 'full', 'cross', 'natural',
                       'union', 'intersect', 'except', 'by', 'on', 'using', 'distinct', 'all'}
        filtered_tables = [t for t in unique_tables if t.lower() not in sql_keywords]
        
        return filtered_tables
    
    def _ensure_tables_registered(self, table_keys: List[str]) -> None:
        """
        Ensure that the specified tables are registered with DuckDB.
        
        Args:
            table_keys: List of table keys to ensure are registered
        """
        if not table_keys:
            return
            
        registered_tables = []
        
        # First pass: try to find exact table key matches
        for table_key in table_keys:
            # Skip empty strings or None values
            if not table_key:
                continue
                
            # Handle keys both with and without _cache_ prefix
            key = table_key[7:] if table_key.startswith('_cache_') else table_key
            
            # Handle special case for table with parentheses
            has_parentheses = '(' in key and ')' in key
            if has_parentheses:
                # Extract the base name before parentheses
                base_key = key.split('(')[0].rstrip('_')
                logger.info(f"Extracted base key '{base_key}' from '{key}' for table with parentheses")
                
                # Check if the base table exists
                if base_key in self.entries:
                    logger.info(f"Found base table '{base_key}' for request with parentheses '{key}'")
                    key = base_key
            
            # Skip if already registered in this batch
            if key in registered_tables:
                continue
                
            # Check if the table exists in the cache
            if key in self.entries:
                try:
                    if key in self.partitioned_tables:
                        # For partitioned tables
                        partitioned_table = self.partitioned_tables[key]
                        table = partitioned_table.get_table()  # This will load if needed
                        self.metadata_store.ensure_registered(key, table)
                        logger.info(f"Registered partitioned table '_cache_{key}' with DuckDB")
                    else:
                        # For regular cache entries
                        entry = self.entries[key]
                        table = entry.get_table()  # This will update access stats
                        self.metadata_store.ensure_registered(key, table)
                        logger.info(f"Registered table '_cache_{key}' with DuckDB")
                        
                    registered_tables.append(key)
                    # Skip further processing for this key
                    continue
                except Exception as e:
                    logger.error(f"Failed to register table '_cache_{key}': {e}")
            
            # If we get here, we couldn't find an exact match
            # Try to find table keys that are related to the requested key
            # For example, if "nyc_yellow_taxi_jan_2023" is requested,
            # we might find "nyc_yellow_taxi" in our entries
            found_match = False
            
            # Case 1: Handle keys with suffixes (when we have the base table)
            # For example, "nyc_yellow_taxi_jan_2023" might match "nyc_yellow_taxi"
            if '_' in key:
                # Try progressively shorter keys by removing suffix parts
                parts = key.split('_')
                for i in range(len(parts)-1, 0, -1):
                    base_key = '_'.join(parts[:i])
                    if base_key in self.entries and base_key not in registered_tables:
                        try:
                            entry = self.entries[base_key]
                            if base_key in self.partitioned_tables:
                                partitioned_table = self.partitioned_tables[base_key]
                                table = partitioned_table.get_table()
                                self.metadata_store.ensure_registered(base_key, table)
                            else:
                                table = entry.get_table()
                                self.metadata_store.ensure_registered(base_key, table)
                                
                            # Also register with the requested key as an alias
                            self.metadata_store.register_alias(base_key, key)
                            
                            logger.info(f"Registered table '_cache_{base_key}' for request '_cache_{key}' with DuckDB")
                            registered_tables.append(base_key)
                            found_match = True
                            break
                        except Exception as e:
                            logger.error(f"Failed to register base table '_cache_{base_key}' for '_cache_{key}': {e}")
            
            # Case 2: Handle special case for tables with trailing underscore
            # For example, "nyc_yellow_taxi_" might match "nyc_yellow_taxi"
            if not found_match and key.endswith('_'):
                base_key = key[:-1]  # Remove trailing underscore
                if base_key in self.entries and base_key not in registered_tables:
                    try:
                        entry = self.entries[base_key]
                        if base_key in self.partitioned_tables:
                            partitioned_table = self.partitioned_tables[base_key]
                            table = partitioned_table.get_table()
                            self.metadata_store.ensure_registered(base_key, table)
                        else:
                            table = entry.get_table()
                            self.metadata_store.ensure_registered(base_key, table)
                            
                        logger.info(f"Registered table '_cache_{base_key}' for request '_cache_{key}' with DuckDB")
                        registered_tables.append(base_key)
                        found_match = True
                    except Exception as e:
                        logger.error(f"Failed to register base table '_cache_{base_key}' for '_cache_{key}': {e}")
            
            if not found_match:
                logger.warning(f"Table '_cache_{key}' referenced in query not found in cache")
                
        return registered_tables
    
    def explain(self, sql: str) -> str:
        """
        Explain a SQL query's execution plan
        
        Args:
            sql: SQL query to explain
            
        Returns:
            Human-readable explanation of the query plan
        """
        with self.lock:
            # Validate input
            if not isinstance(sql, str):
                raise TypeError("SQL query must be a string")
                
            try:
                return explain_query(self.metadata_store.con, sql)
            except Exception as e:
                logger.error(f"Failed to explain query: {e}")
                raise
    
    def persist(self, key: str, storage_dir: Optional[str] = None) -> bool:
        """
        Persist a cached entry to disk for later retrieval
        
        Args:
            key: Cache entry key
            storage_dir: Directory to store the data (None uses config default)
            
        Returns:
            True if successful, False otherwise
        """
        with self.lock:
            if key not in self.entries:
                return False
                
            storage_path = storage_dir or self.config["storage_path"]
            if not storage_path:
                logger.error("No storage path specified")
                return False
                
            # Handle partitioned tables
            if key in self.partitioned_tables:
                partitioned_table = self.partitioned_tables[key]
                
                # Create directory if needed
                table_dir = os.path.join(storage_path, key)
                os.makedirs(table_dir, exist_ok=True)
                
                # Store metadata in DuckDB
                self.metadata_store.add_persistence_metadata(
                    key=key,
                    storage_path=table_dir,
                    is_partitioned=True,
                    partition_count=len(partitioned_table.partition_order),
                    metadata={
                        "key": key,
                        "total_rows": partitioned_table.total_rows,
                        "total_size_bytes": partitioned_table.total_size_bytes,
                        "partitions": list(partitioned_table.partition_order),
                        "schema": partitioned_table.schema.to_string(),
                        "created_at": time.time(),
                        "partition_metadata": partitioned_table.partition_metadata
                    }
                )
                
                return partitioned_table.persist_partitions(table_dir)
                
            # Handle regular entries
            entry = self.entries[key]
            table = entry.get_table()
            
            try:
                import pyarrow.parquet as pq
                import tempfile
                import shutil
                
                # Create directory if needed
                table_dir = os.path.join(storage_path, key)
                os.makedirs(table_dir, exist_ok=True)
                
                # Save table as Parquet to a temporary file first
                temp_file = tempfile.NamedTemporaryFile(delete=False, dir=table_dir, suffix='.parquet.tmp')
                temp_file.close()  # Close the file to allow writing to it
                
                # Write to the temporary file
                pq.write_table(
                    table, 
                    temp_file.name, 
                    compression=self.config["compression_type"]
                )
                
                # Rename temp file to final destination (atomic operation)
                file_path = os.path.join(table_dir, "data.parquet")
                shutil.move(temp_file.name, file_path)
                
                # Store metadata in DuckDB
                meta_dict = entry.to_dict()
                # Convert schema to string for storage
                meta_dict["schema"] = table.schema.to_string()
                
                self.metadata_store.add_persistence_metadata(
                    key=key,
                    storage_path=table_dir,
                    is_partitioned=False,
                    partition_count=0,
                    metadata=meta_dict
                )
                
                return True
            except IOError as e:
                logger.error(f"I/O error persisting cached entry: {e}")
                # Try to clean up any temporary files
                if 'temp_file' in locals() and os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
                return False
            except Exception as e:
                logger.error(f"Failed to persist cached entry: {e}")
                # Try to clean up any temporary files
                if 'temp_file' in locals() and os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
                return False
    
    def load(self, key: str, storage_dir: Optional[str] = None) -> bool:
        """
        Load a persisted entry into the cache
        
        Args:
            key: Cache entry key
            storage_dir: Directory to load from (None uses config default)
            
        Returns:
            True if successful, False otherwise
        """
        storage_path = storage_dir or self.config["storage_path"]
        if not storage_path:
            logger.error("No storage path specified")
            return False
        
        # Get persistence metadata from DuckDB
        persistence_meta = self.metadata_store.get_persistence_metadata(key)
        if not persistence_meta:
            logger.error(f"No persistence metadata found for key: {key}")
            return False
        
        # Use the stored path
        table_dir = persistence_meta["storage_path"]
        is_partitioned = persistence_meta["is_partitioned"]
        metadata = persistence_meta["metadata"]
        
        if is_partitioned:
            try:
                # Create partitioned table from metadata
                # Fix the schema parsing issue - parse schema directly from string
                schema_str = metadata["schema"]

                # Corrected regex patterns using raw strings and adjusted matching
                # Pattern 1: Field(name: type)
                # Pattern 2: field_name: name, type: type
                field_matches_1 = re.findall(r'Field\((\w+):', schema_str)
                field_matches_2 = re.findall(r'field_name:\s*(\w+)', schema_str)
                field_matches = field_matches_1 if field_matches_1 else field_matches_2

                type_matches = re.findall(r'type:\s*(\S+?)(\)|, nullable=)', schema_str)
                field_types = [match[0] for match in type_matches] # Extract the type part

                # Create fields from extracted information
                fields = []
                if len(field_matches) == len(field_types):
                    for i, name in enumerate(field_matches):
                        name = name.strip().strip('"\'')
                        field_type = _parse_type_str(field_types[i])
                        fields.append(pa.field(name, field_type))
                else:
                     logger.error(f"Schema parsing mismatch for key '{key}': Found {len(field_matches)} names and {len(field_types)} types.")
                     # Attempt fallback or raise error
                     raise ValueError(f"Cannot parse schema string for key {key}")

                # Create schema
                schema = pa.schema(fields)

                partitioned = PartitionedTable(
                    key=key,
                    schema=schema,
                    config=self.config,
                    metadata=metadata
                )
                
                # Set table properties from metadata
                partitioned.total_rows = metadata["total_rows"]
                partitioned.total_size_bytes = metadata["total_size_bytes"]
                partitioned.partition_order = metadata["partitions"]
                partitioned.partition_metadata = metadata["partition_metadata"]
                
                # Load partition metadata (but not the actual data)
                for partition_id in partitioned.partition_order:
                    partition_path = os.path.join(table_dir, f"{partition_id}.parquet")
                    partition_meta = partitioned.partition_metadata[partition_id]
                    
                    partitioned.partitions[partition_id] = TablePartition(
                        partition_id=partition_id,
                        size_bytes=partition_meta["size_bytes"],
                        row_count=partition_meta["row_count"],
                        metadata={"parent_key": key},
                        path=partition_path
                    )
                
                with self.lock:
                    # Add to our collections
                    if key in self.entries:
                        self._remove_entry(key)
                        
                    self.entries[key] = partitioned
                    self.partitioned_tables[key] = partitioned
                    self.current_size_bytes += partitioned.total_size_bytes
                    
                    # Register with metadata store and DuckDB
                    self.metadata_store.add_entry(
                        key=key,
                        created_at=time.time(),
                        last_accessed_at=time.time(),
                        access_count=0,
                        expires_at=None,
                        size_bytes=partitioned.total_size_bytes,
                        schema=partitioned.schema,
                        num_rows=partitioned.total_rows,
                        metadata=metadata
                    )
                    
                    # Register the table with DuckDB
                    self.metadata_store.register_table(key, partitioned.get_table())
                    
                    return True
            except Exception as e:
                logger.error(f"Failed to load partitioned table: {e}")
                return False
        
        # Regular cache entry
        try:
            import pyarrow.parquet as pq
            
            # Load table
            file_path = os.path.join(table_dir, "data.parquet")
            table = pq.read_table(file_path)
            
            # Create cache entry from metadata
            entry = CacheEntry(
                key=key,
                table=table,
                size_bytes=metadata.get("size_bytes", 0),
                ttl=None,  # No TTL for loaded entries
                metadata=metadata.get("metadata", {})
            )
            
            with self.lock:
                # Add to our collections
                if key in self.entries:
                    self._remove_entry(key)
                    
                self.entries[key] = entry
                self.current_size_bytes += entry.size_bytes
                
                # Register with metadata store and DuckDB
                self.metadata_store.add_entry(
                    key=key,
                    created_at=entry.created_at,
                    last_accessed_at=entry.last_accessed_at,
                    access_count=entry.access_count,
                    expires_at=None,
                    size_bytes=entry.size_bytes,
                    schema=table.schema,
                    num_rows=table.num_rows,
                    metadata=entry.metadata
                )
                
                # Register the table with DuckDB
                self.metadata_store.register_table(key, table)
                
                return True
        except Exception as e:
            logger.error(f"Failed to load cached entry: {e}")
            return False
            
    def cleanup_persisted_files(self) -> None:
        """
        Clean up all persisted files
        """
        import shutil
        
        # Get all persistence metadata
        persisted_entries = self.metadata_store.get_all_persistence_metadata()
        
        for entry in persisted_entries:
            storage_path = entry["storage_path"]
            key = entry["key"]
            
            # Remove the storage directory
            try:
                if os.path.exists(storage_path):
                    shutil.rmtree(storage_path)
                    logger.info(f"Removed persisted data for key: {key}")
            except Exception as e:
                logger.error(f"Failed to remove persisted data for key {key}: {e}")
                
        # Clear all persistence metadata
        self.metadata_store.clear_all_persistence_metadata()
        
    def cleanup_spill_files(self) -> None:
        """
        Clean up all spilled files
        """
        spill_dir = self.config["spill_directory"]
        if spill_dir and os.path.exists(spill_dir):
            try:
                import shutil
                shutil.rmtree(spill_dir)
                logger.info(f"Removed spill directory: {spill_dir}")
            except Exception as e:
                logger.error(f"Failed to remove spill directory: {e}")
    
    def close(self) -> None:
        """Close the cache and release resources."""
        logger.info("Closing Arrow Cache and cleaning up resources")
        
        # Stop the checker thread
        self._stop_checker_event.set()
        if self._checker_thread and self._checker_thread.is_alive():
            try:
                self._checker_thread.join(timeout=1.0)
            except Exception as e:
                logger.warning(f"Error joining checker thread: {e}")
            self._checker_thread = None
        
        # Clean up all persisted files
        if self.config.get("delete_files_on_close", True):
            # Get persistence metadata before closing the database connection
            try:
                # Get all persistence metadata
                persisted_entries = self.metadata_store.get_all_persistence_metadata()
                
                # Clean up the files
                import shutil
                for entry in persisted_entries:
                    storage_path = entry["storage_path"]
                    key = entry["key"]
                    
                    # Remove the storage directory
                    try:
                        if os.path.exists(storage_path):
                            shutil.rmtree(storage_path)
                            logger.info(f"Removed persisted data for key: {key}")
                    except Exception as e:
                        logger.error(f"Failed to remove persisted data for key {key}: {e}")
                
                # Clear all persistence metadata
                self.metadata_store.clear_all_persistence_metadata()
            except Exception as e:
                logger.error(f"Error during persistence cleanup: {e}")
            
            # Clean up spill files
            self.cleanup_spill_files()
        
        # Clear the cache - catch any exceptions
        try:
            self.clear()
        except Exception as e:
            logger.error(f"Error during cache clear: {e}")
        
        # Close the metadata store
        try:
            self.metadata_store.close()
        except Exception as e:
            logger.error(f"Error closing metadata store: {e}")
        
        # Shutdown thread pools - handle RuntimeError
        try:
            self.thread_pool.shutdown()
        except RuntimeError as e:
            # This can happen if the event loop is already closed
            logger.warning(f"RuntimeError during thread pool shutdown, likely event loop is closed: {e}")
        except Exception as e:
            logger.error(f"Error shutting down thread pool: {e}")
        
        # Stop background processing - handle RuntimeError
        try:
            self.bg_queue.stop()
        except RuntimeError as e:
            # This can happen if the event loop is already closed
            logger.warning(f"RuntimeError during background queue stop, likely event loop is closed: {e}")
        except Exception as e:
            logger.error(f"Error stopping background queue: {e}")
        
        # Stop memory manager
        try:
            self.memory_manager.close()
        except Exception as e:
            logger.error(f"Error closing memory manager: {e}")
        
        logger.info("Arrow Cache closed and resources cleaned up")
    
    def __del__(self) -> None:
        """Ensure resources are released when the object is deleted."""
        try:
            # Only call close if metadata_store still exists and is not already closed
            if hasattr(self, 'metadata_store') and hasattr(self.metadata_store, 'con') and not self.metadata_store.con.is_closed:
                self.close()
        except:
            pass

    def _handle_memory_pressure(self, needed_bytes: int) -> int:
        """
        Handle memory pressure by spilling to disk or evicting entries
        
        Args:
            needed_bytes: Number of bytes to free
            
        Returns:
            Number of bytes freed
        """
        logger.info(f"Cache memory pressure detected: Need to free {needed_bytes} bytes")
        freed_bytes = 0
        
        # Check if we're in a reasonable situation before proceeding
        # If the amount to free is unreasonably large compared to our cache limit,
        # this might indicate a memory tracking issue
        if needed_bytes > self.max_size_bytes * 0.8:
            logger.warning(f"Unreasonably large memory free request: {needed_bytes/(1024*1024):.2f} MB " 
                          f"(cache limit: {self.max_size_bytes/(1024*1024):.2f} MB)")
            # Return a small non-zero value to prevent continuous retries
            return 1024*1024  # Pretend we freed 1MB
        
        with self.lock:
            # First, check if we have partitioned tables to spill
            if not self.config["spill_to_disk"] or not self.partitioned_tables:
                logger.info("No partitioned tables available for spilling")
                # Fall back to entry eviction
                additional_freed = self._evict(needed_bytes)
                logger.info(f"Freed additional {additional_freed} bytes through eviction")
                return additional_freed
                
            # Get partitioned tables sorted by last access time (oldest first)
            partitioned_tables = sorted(
                self.partitioned_tables.items(),
                key=lambda item: self.metadata_store.get_entry_metadata(item[0]).get("last_accessed_at", 0)
            )
            
            # Count total spillable bytes to see if we have enough to fulfill the request
            total_spillable = 0
            spillable_tables = []
            
            for key, partitioned_table in partitioned_tables:
                try:
                    # Skip already persisted tables
                    if hasattr(partitioned_table, 'is_persisted') and partitioned_table.is_persisted:
                        continue
                        
                    # Count spillable partitions
                    spillable = [
                        partition for partition in partitioned_table.get_partitions()
                        if not partition.is_pinned and partition.is_loaded
                    ]
                    
                    spillable_bytes = sum(p.size_bytes for p in spillable)
                    if spillable_bytes > 0:
                        total_spillable += spillable_bytes
                        spillable_tables.append((key, partitioned_table, spillable_bytes))
                except Exception as e:
                    logger.error(f"Error checking spillable bytes for table '_cache_{key}': {e}")
            
            logger.info(f"Found {len(spillable_tables)} tables with {total_spillable/(1024*1024):.2f} MB spillable data")
            
            # If we don't have enough spillable data, evict entries as well
            if total_spillable < needed_bytes * 0.9:  # If we can free less than 90% of what's needed
                logger.info(f"Not enough spillable data ({total_spillable/(1024*1024):.2f} MB) to free {needed_bytes/(1024*1024):.2f} MB")
                # Try to evict entries for the remaining amount
                evict_needed = needed_bytes - total_spillable
                additional_freed = self._evict(evict_needed)
                freed_bytes += additional_freed
                logger.info(f"Freed additional {additional_freed} bytes through eviction")
            
            # Try to spill partitions from each table
            for key, partitioned_table, _ in spillable_tables:
                try:
                    logger.debug(f"Attempting to spill partitions from table '_cache_{key}'")
                    freed = partitioned_table.spill_partitions(spill_dir, needed_bytes - freed_bytes)
                    freed_bytes += freed
                    
                    logger.debug(f"Spilled {freed} bytes from table '_cache_{key}'")
                    
                    if freed_bytes >= needed_bytes:
                        logger.info(f"Successfully freed {freed_bytes} bytes through spilling")
                        return freed_bytes
                except Exception as e:
                    logger.error(f"Error spilling partitions from table '_cache_{key}': {e}")
                    # Continue with next table
            
            # If we get here, we tried everything but couldn't free enough
            logger.info(f"Freed {freed_bytes} bytes through spilling")
            return freed_bytes
    
    def _evict(self, needed_bytes: int) -> int:
        """
        Evict entries to free up space.
        
        Args:
            needed_bytes: Number of bytes to free
            
        Returns:
            Number of bytes freed
        """
        if not self.entries:
            return 0
            
        # Get candidates from the eviction policy
        candidates = self.eviction_policy.get_eviction_candidates(
            self.entries, needed_bytes
        )
        
        # Remove the candidates
        freed_bytes = 0
        for key in candidates:
            if key in self.entries:
                if key in self.partitioned_tables:
                    freed_bytes += self.partitioned_tables[key].total_size_bytes
                else:
                    freed_bytes += self.entries[key].size_bytes
                self._remove_entry(key)
                
                # If we've freed enough space, stop
                if freed_bytes >= needed_bytes:
                    break
        
        return freed_bytes
        
    def _remove_entry(self, key: str) -> bool:
        """
        Remove an entry from the cache (internal method).
        
        Args:
            key: Key to remove
            
        Returns:
            True if the entry was removed, False if it didn't exist
        """
        entry = self.entries.pop(key, None)
        if entry is None:
            return False
        
        # Update size tracking
        if key in self.partitioned_tables:
            partitioned = self.partitioned_tables.pop(key)
            self.current_size_bytes -= partitioned.total_size_bytes
        else:
            self.current_size_bytes -= entry.size_bytes
        
        # Unregister from DuckDB
        self.metadata_store.unregister_table(key)
        
        # Remove from metadata store
        self.metadata_store.remove_entry(key)
        
        # Update eviction policy
        self.eviction_policy.remove_entry(key)
        
        return True
        
    def _check_expired(self) -> int:
        """
        Check for and remove expired entries.
        
        Returns:
            Number of expired entries removed
        """
        with self.lock:
            now = time.time()
            expired_keys = []
            
            # Find expired entries (partitioned tables don't expire)
            for key, entry in list(self.entries.items()):
                if key not in self.partitioned_tables and entry.is_expired():
                    expired_keys.append(key)
            
            # Remove expired entries
            for key in expired_keys:
                self._remove_entry(key)
            
            return len(expired_keys)
    
    def _checker_loop(self) -> None:
        """Background thread to check for expired entries."""
        while not self._stop_checker_event.is_set():
            try:
                num_expired = self._check_expired()
                if num_expired > 0:
                    logger.debug(f"Removed {num_expired} expired cache entries")
                    
                # Also clean up completed async tasks
                self.async_tasks.cleanup()
            except Exception as e:
                logger.exception(f"Error checking expired entries: {e}")
            
            # Wait for the next interval or until stopped
            self._stop_checker_event.wait(self.check_interval)
    
    def _start_checker(self) -> None:
        """Start the expiration checker thread."""
        if self._checker_thread is not None:
            return
            
        self._stop_checker_event.clear()
        self._checker_thread = threading.Thread(
            target=self._checker_loop,
            daemon=True
        )
        self._checker_thread.start()
    
    def stop_checker(self) -> None:
        """Stop the expiration checker thread."""
        if self._checker_thread is None:
            return
            
        self._stop_checker_event.set()
        self._checker_thread.join(timeout=1.0)
        self._checker_thread = None
        
    def status(self) -> Dict[str, Any]:
        """
        Get status information about the cache.
        
        Returns:
            Dictionary with status information
        """
        with self.lock:
            total_queries = self.hits + self.misses
            hit_ratio = self.hits / total_queries if total_queries > 0 else 0.0
            
            # Get memory information
            memory_info = self.memory_manager.get_memory_info()
            
            # Sanity check the current_size_bytes by comparing with system memory
            system_memory = psutil.virtual_memory().total
            if self.current_size_bytes > system_memory * 2:
                # This is clearly an error in tracking - log and reset
                logger.error(f"Memory tracking error detected: tracked memory ({self.current_size_bytes/(1024**3):.2f} GB) "
                           f"exceeds total system memory ({system_memory/(1024**3):.2f} GB)")
                
                # Recalculate sizes for partitioned tables which are likely the issue
                total_recalculated = 0
                for key, partitioned in self.partitioned_tables.items():
                    old_size = partitioned.total_size_bytes
                    partitioned.recalculate_total_size()
                    new_size = partitioned.total_size_bytes
                    total_recalculated += (old_size - new_size)
                    
                # Adjust the current size
                self.current_size_bytes -= total_recalculated
                logger.info(f"Adjusted memory tracking by {total_recalculated/(1024**3):.2f} GB")
                
                # If still unreasonable, force a reset to process RSS
                if self.current_size_bytes > system_memory:
                    new_estimate = memory_info.get("process_rss", 0)
                    logger.warning(f"Memory tracking still unreasonable, resetting to process RSS: {new_estimate/(1024**3):.2f} GB")
                    self.current_size_bytes = new_estimate
            
            # Get query cache statistics
            query_cache_stats = self.query_optimizer.get_cache_stats()
            
            return {
                "entry_count": len(self.entries),
                "partitioned_tables": len(self.partitioned_tables),
                "current_size_bytes": self.current_size_bytes,
                "max_size_bytes": self.max_size_bytes,
                "hits": self.hits,
                "misses": self.misses,
                "hit_ratio": hit_ratio,
                "metadata_db_size_bytes": self.metadata_store.get_database_size(),
                "memory": memory_info,
                "query_cache": query_cache_stats,
                "active_background_tasks": self.async_tasks.get_active_count(),
                "bg_queue_size": self.bg_queue.get_queue_size(),
                "system_memory": {
                    "total": psutil.virtual_memory().total,
                    "available": psutil.virtual_memory().available,
                    "percent_used": psutil.virtual_memory().percent
                }
            }

    def _prepare_table_for_put(self, data: Any, preserve_index: bool, config: ArrowCacheConfig) -> Tuple[pa.Table, Dict[str, Any]]:
        """
        Prepare data by converting to Arrow table, applying compression, and extracting metadata.
        
        Args:
            data: Data to prepare
            preserve_index: Whether to preserve DataFrame indices
            config: Cache configuration
            
        Returns:
            Tuple of (prepared Arrow Table, extracted metadata dictionary)
        """
        extracted_metadata = {}
        
        # Extract basic metadata from schema if present
        if hasattr(data, 'schema') and data.schema.metadata:
            extracted_metadata = {
                k.decode('utf-8') if isinstance(k, bytes) else k: 
                v.decode('utf-8') if isinstance(v, bytes) else v 
                for k, v in data.schema.metadata.items()
            }
            
        # Convert to Arrow table using the updated converters
        table = to_arrow_table(data, preserve_index)
        # The metadata from the conversion is now part of the table's schema metadata
        if table.schema.metadata:
            # Decode metadata if needed
            extracted_metadata = {
                k.decode('utf-8') if isinstance(k, bytes) else k: 
                v.decode('utf-8') if isinstance(v, bytes) else v 
                for k, v in table.schema.metadata.items()
            }
            
        # Apply compression if configured
        if config["enable_compression"]:
            try:
                table = apply_compression(
                    table,
                    compression_type=config["compression_type"],
                    compression_level=config["compression_level"],
                    use_dictionary=config["dictionary_encoding"]
                )
            except Exception as e:
                logger.error(f"Failed to apply compression: {e}")
                
        # --- Extract Auto-Metadata using PyArrow and Helpers --- 
        try:
            col_types = {}
            col_duckdb_types = {} # Store DuckDB types
            col_nulls = {}
            col_chunks = {}
            col_dict = {}
            col_minmax = {}

            for col_name in table.column_names:
                column = table.column(col_name)
                arrow_type = column.type
                col_types[col_name] = str(arrow_type)
                col_duckdb_types[col_name] = _map_arrow_type_to_duckdb(arrow_type) # Use the new helper
                col_nulls[col_name] = column.null_count
                col_chunks[col_name] = column.num_chunks
                col_dict[col_name] = pa.types.is_dictionary(arrow_type)

                # Calculate min/max only for suitable types
                if _is_numeric_type(arrow_type) or pa.types.is_temporal(arrow_type):
                    if column.null_count < len(column): # Avoid error on all-null columns
                        min_max_val = pc.min_max(column)
                        # Convert scalar values to standard Python types for JSON serialization
                        col_minmax[col_name] = {
                            "min": min_max_val['min'].as_py() if min_max_val['min'].is_valid else None,
                            "max": min_max_val['max'].as_py() if min_max_val['max'].is_valid else None
                        }
                    else:
                        col_minmax[col_name] = {"min": None, "max": None}

            # Add to extracted_metadata as JSON strings
            extracted_metadata['column_types_json'] = json.dumps(col_types)
            extracted_metadata['column_duckdb_types_json'] = json.dumps(col_duckdb_types) # Add DuckDB types
            extracted_metadata['column_null_counts_json'] = json.dumps(col_nulls)
            extracted_metadata['column_chunk_counts_json'] = json.dumps(col_chunks)
            extracted_metadata['column_dictionary_encoded_json'] = json.dumps(col_dict)
            extracted_metadata['column_min_max_json'] = json.dumps(col_minmax, default=str) # Use default=str for temporal types

            # Get geometry information
            geom_col, geom_format = _get_geometry_info(table)
            if geom_col:
                extracted_metadata['geometry_column'] = geom_col
                extracted_metadata['storage_format'] = geom_format
                logger.info(f"Detected geometry column '{geom_col}' with format '{geom_format}'")
            else:
                logger.info("No specific geometry column detected.")

        except Exception as meta_extract_error:
            logger.warning(f"Error extracting auto-metadata: {meta_extract_error}")
        # ----------------------------------------
                
        return table, extracted_metadata

    def _put_streaming(
        self,
        key: str,
        data: Any,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        preserve_index: bool = True
    ) -> str:
        """
        Stream large data into partitioned storage and handle metadata.
        
        Args:
            key: Key to store the data under
            data: Data to stream (file path, DataFrame, etc.)
            ttl: Time-to-live in seconds
            metadata: Initial metadata (will be updated with extracted info)
            preserve_index: Whether to preserve DataFrame indices
            
        Returns:
            The cache key
        """
        logger.info(f"Streaming large data into cache with key: {key}")
        
        with self.lock:
            # Remove any existing entry with this key
            if key in self.entries:
                self._remove_entry(key)
            
            # Create partitioned table
            partitioned = PartitionedTable(key, None, self.config, metadata or {})
            
            # Dictionary to hold metadata extracted during streaming/conversion
            streaming_metadata = {}
            
            try:
                # Handle file paths
                if isinstance(data, str) and os.path.exists(data):
                    ext = os.path.splitext(data)[1].lower()
                    
                    if ext == '.parquet':
                        import pyarrow.parquet as pq
                        
                        # Initialize with schema
                        partitioned.schema = pq.read_schema(data)
                        
                        # Extract potential Parquet metadata (including geo if saved with GeoPandas)
                        parquet_metadata = pq.read_metadata(data).metadata
                        if parquet_metadata:
                            streaming_metadata.update({
                                k.decode(): v.decode() for k, v in parquet_metadata.items() if k.decode().startswith('geo')
                            })
                            if 'crs_wkt' in streaming_metadata:
                                streaming_metadata['crs_info'] = streaming_metadata.pop('crs_wkt')
                            if 'primary_column' in streaming_metadata:
                                streaming_metadata['geometry_column'] = streaming_metadata.pop('primary_column')
                            streaming_metadata['storage_format'] = "WKB" # Assume WKB if geo metadata present
                            
                        # Stream in batches using PyArrow
                        with pq.ParquetFile(data) as pf:
                            for i, batch in enumerate(pf.iter_batches(batch_size=self.config["streaming_chunk_size"])):
                                table_batch = pa.Table.from_batches([batch])
                                partitioned.add_partition(table_batch)
                                
                                # Log progress periodically
                                if (i + 1) % 10 == 0:
                                    logger.debug(f"Added {i + 1} batches, {partitioned.total_rows} rows so far")
                    
                    elif ext == '.csv':
                        import pyarrow.csv as csv
                        
                        # Use PyArrow's CSV reader
                        parse_options = csv.ParseOptions(delimiter=',')
                        convert_options = csv.ConvertOptions(
                            include_columns=None,
                            include_missing_columns=True,
                            auto_dict_encode=self.config.get("dictionary_encoding", False)
                        )
                        read_options = csv.ReadOptions(
                            use_threads=True,
                            block_size=self.config["streaming_chunk_size"]
                        )
                        
                        # Stream in batches
                        reader = csv.open_csv(
                            data,
                            read_options=read_options,
                            parse_options=parse_options,
                            convert_options=convert_options
                        )
                        
                        # Read the first batch to get the schema
                        first_batch = reader.read_next_batch()
                        first_table = pa.Table.from_batches([first_batch])
                        partitioned.schema = first_table.schema
                        partitioned.add_partition(first_table)
                        
                        # Read and add remaining batches
                        batch_counter = 1
                        while True:
                            try:
                                batch = reader.read_next_batch()
                                if batch.num_rows == 0:
                                    break
                                
                                table = pa.Table.from_batches([batch])
                                partitioned.add_partition(table)
                                batch_counter += 1
                                
                                # Log progress periodically
                                if batch_counter % 10 == 0:
                                    logger.debug(f"Added {batch_counter} batches, {partitioned.total_rows} rows so far")
                            except StopIteration:
                                break
                    
                    else:
                        # For other file types, read with PyArrow if possible
                        try:
                            # Try using pyarrow.dataset which handles many file formats
                            import pyarrow.dataset as ds
                            dataset = ds.dataset(data)
                            partitioned.schema = dataset.schema
                            
                            for i, batch in enumerate(dataset.scanner(batch_size=self.config["streaming_chunk_size"]).scan_batches()):
                                table_batch = batch.to_table()
                                partitioned.add_partition(table_batch)
                                
                                # Log progress periodically
                                if (i + 1) % 10 == 0:
                                    logger.debug(f"Added {i + 1} batches, {partitioned.total_rows} rows so far")
                        except Exception as e:
                            logger.error(f"Error streaming file with pyarrow.dataset: {e}")
                            raise ValueError(f"Unsupported file format: {ext}")
                
                # Handle pandas DataFrames
                elif hasattr(data, 'to_arrow'):
                    # Use pandas' to_arrow method if available
                    table = data.to_arrow()
                    partitioned.schema = table.schema
                    
                    # Split into partitions
                    partitions = partition_table(
                        table, 
                        self.config,
                        self.config["partition_size_rows"],
                        self.config["partition_size_bytes"]
                    )
                    
                    for part_table in partitions:
                        partitioned.add_partition(part_table)
                
                # Handle dicts or other objects by converting to PyArrow directly
                else:
                    # Convert to Arrow table
                    table = to_arrow_table(data, preserve_index)
                    partitioned.schema = table.schema
                    
                    # Apply compression if configured
                    if self.config["enable_compression"]:
                        try:
                            table = apply_compression(
                                table,
                                compression_type=self.config["compression_type"],
                                compression_level=self.config["compression_level"],
                                use_dictionary=self.config["dictionary_encoding"]
                            )
                        except Exception as e:
                            logger.error(f"Failed to apply compression: {e}")
                    
                    # Split into partitions
                    partitions = partition_table(
                        table, 
                        self.config,
                        self.config["partition_size_rows"],
                        self.config["partition_size_bytes"]
                    )
                    
                    for part_table in partitions:
                        partitioned.add_partition(part_table)
                
                # Recalculate the size to ensure accuracy
                partitioned.recalculate_total_size()
                
                # Store in our collections
                self.entries[key] = partitioned
                self.partitioned_tables[key] = partitioned
                self.current_size_bytes += partitioned.total_size_bytes
                
                # Add to metadata store
                self._add_partitioned_metadata(key, partitioned, ttl, metadata or {})
                
                return key
                
            except Exception as e:
                logger.error(f"Error in streaming data to cache: {e}")
                raise

    def _add_partitioned_metadata(self, key: str, partitioned: PartitionedTable, ttl: Optional[float], metadata: Dict[str, Any]) -> None:
        """Add partitioned table metadata to metadata store"""
        metadata = metadata or {}
        
        # Add partitioning information to metadata
        metadata["partitioned"] = True
        metadata["partition_count"] = len(partitioned.partition_order)
        metadata["total_rows"] = partitioned.total_rows
        
        self.metadata_store.add_entry(
            key=key,
            created_at=time.time(),
            last_accessed_at=time.time(),
            access_count=0,
            expires_at=(time.time() + ttl) if ttl is not None else None,
            size_bytes=partitioned.total_size_bytes,
            schema=partitioned.schema,
            num_rows=partitioned.total_rows,
            metadata=metadata
        )
        
        # Register the table with DuckDB
        # Note: For partitioned tables, we register the concatenated view
        try:
            concatenated_table = partitioned.get_table()
            self.metadata_store.register_table(key, concatenated_table)
        except Exception as e:
            logger.error(f"Error registering table: {e}")
            # Try to recover by forcing a consistent schema across partitions
            try:
                partitioned.standardize_partition_schemas()
                concatenated_table = partitioned.get_table()
                self.metadata_store.register_table(key, concatenated_table)
            except Exception as recovery_error:
                logger.error(f"Failed to recover from schema inconsistency: {recovery_error}")
                raise

    def import_data(
        self,
        key: str,
        source: str,
        source_type: str = "auto",
        connection_options: Optional[Dict[str, Any]] = None,
        query: Optional[str] = None,
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        auto_partition: Optional[bool] = None
    ) -> str:
        """
        Import data from a source directly into the cache using DuckDB's connectors.
        
        This method leverages DuckDB's native connectors to import data from various
        sources like S3, PostgreSQL, Iceberg, etc., directly into the cache.
        
        Args:
            key: Key to store the data under
            source: Source URI or path (e.g., 's3://bucket/file.parquet', 'postgres://user:pass@host/db')
            source_type: Source type ('auto', 'parquet', 'csv', 'json', 'postgres', 'iceberg', etc.)
            connection_options: Connection options for database connectors
            query: SQL query to execute (for database sources)
            table_name: Table name to load (for database sources)
            schema: Schema name for database tables
            ttl: Time-to-live for the cached data
            metadata: Additional metadata to store with the cached data
            options: Additional options for the specific connector
            auto_partition: Whether to automatically partition large tables
            
        Returns:
            Key of the cached data
            
        Examples:
            # Import from S3
            cache.import_data(
                "nyc_taxi", 
                "s3://bucket/yellow_tripdata_2021-01.parquet",
                options={"s3_options": {"region": "us-east-1"}}
            )
            
            # Import from PostgreSQL
            cache.import_data(
                "customers",
                "postgresql://user:pass@localhost:5432/mydb",
                table_name="customers",
                schema="public"
            )
            
            # Import from Iceberg
            cache.import_data(
                "sales",
                "iceberg://catalog",
                table_name="sales"
            )
        """
        # Import the data using DuckDB's connectors
        arrow_table = import_from_duckdb(
            source=source,
            source_type=source_type,
            connection_options=connection_options,
            query=query,
            table_name=table_name,
            schema=schema,
            options=options
        )
        
        # Store metadata about the import
        if metadata is None:
            metadata = {}
        
        # Add source information to metadata
        metadata.update({
            "source": source,
            "source_type": source_type,
            "import_method": "duckdb",
        })
        
        if table_name:
            metadata["table_name"] = table_name
        if schema:
            metadata["schema"] = schema
        
        # Use the general put method to store the data
        # It handles auto-partitioning based on settings
        return self.put(
            key=key,
            data=arrow_table,
            ttl=ttl,
            metadata=metadata,
            overwrite=True,
            auto_partition=auto_partition
        )
    
    def import_query(
        self,
        key: str,
        sql: str,
        connection: str,
        connection_type: str = "postgres",
        connection_options: Optional[Dict[str, Any]] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        options: Optional[Dict[str, Any]] = None,
        auto_partition: Optional[bool] = None
    ) -> str:
        """
        Import data from a SQL query on a remote database directly into the cache.
        
        A convenience method for importing data using a SQL query.
        
        Args:
            key: Key to store the data under
            sql: SQL query to execute
            connection: Connection string or identifier
            connection_type: Connection type ('postgres', 'mysql', 'sqlserver', etc.)
            connection_options: Additional connection options
            ttl: Time-to-live for the cached data
            metadata: Additional metadata to store with the cached data
            options: Additional options for the connector
            auto_partition: Whether to automatically partition large tables
            
        Returns:
            Key of the cached data
            
        Examples:
            # Import from PostgreSQL using a query
            cache.import_query(
                "active_customers",
                "SELECT * FROM customers WHERE active = TRUE",
                "postgresql://user:pass@localhost:5432/mydb"
            )
        """
        return self.import_data(
            key=key,
            source=connection,
            source_type=connection_type,
            connection_options=connection_options,
            query=sql,
            ttl=ttl,
            metadata=metadata,
            options=options,
            auto_partition=auto_partition
        )
