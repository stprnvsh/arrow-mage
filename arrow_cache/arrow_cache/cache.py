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

from .cache_entry import CacheEntry
from .metadata_store import MetadataStore
from .converters import to_arrow_table, from_arrow_table, estimate_size_bytes
from .eviction_policy import create_eviction_policy, EvictionPolicy
from .config import ArrowCacheConfig, DEFAULT_CONFIG
from .memory import MemoryManager, apply_compression, zero_copy_slice, estimate_table_memory_usage
from .partitioning import TablePartition, PartitionedTable, partition_table
from .threading import ThreadPoolManager, AsyncTaskManager, BackgroundProcessingQueue
from .query_optimization import QueryOptimizer, optimize_duckdb_connection, explain_query

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
            preserve_index: Whether to preserve DataFrame indices as columns
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
            
            try:
                # Check if data is already an Arrow table
                if isinstance(data, pa.Table):
                    table = data
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
                else:
                    # For large files, use streaming conversion if enabled
                    file_size_hint = getattr(data, 'memory_usage', lambda **kwargs: 0)().sum() if hasattr(data, 'memory_usage') else 0
                    use_streaming = self.config["enable_streaming"] and (
                        file_size_hint > self.config["max_conversion_memory"] or
                        isinstance(data, str) and os.path.exists(data) and 
                        os.path.getsize(data) > self.config["max_conversion_memory"]
                    )
                    
                    if use_streaming:
                        logger.info(f"Using streaming conversion for large data with key: {key}")
                        # For very large files, convert and partition directly in chunks
                        return self._put_streaming(key, data, ttl, combined_metadata, preserve_index)
                    else:
                        # Convert to Arrow table using normal method
                        table = to_arrow_table(data, preserve_index)
                        
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
                    
                    # Execute with callbacks for registration and optional deregistration
                    deregister_callback = self._deregister_tables if auto_deregister else None
                    result, hints, info = self.query_optimizer.optimize_and_execute(
                        self.metadata_store.con, 
                        sql,
                        ensure_tables_callback=self._ensure_tables_registered,
                        deregister_tables_callback=deregister_callback
                    )
                    execution_time = time.time() - start_time
                    
                    # Log query performance info
                    logger.info(f"Query executed in {execution_time:.3f}s")
                    if logger.isEnabledFor(logging.DEBUG):
                        logger.debug(f"Query execution info: {info}")
                        if hints:
                            logger.debug(f"Query optimization hints: {hints}")
                    
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
        Extract table references from a SQL query.
        
        Args:
            sql: SQL query
            
        Returns:
            List of table keys referenced in the query
        """
        # This is an improved implementation that extracts table names from FROM and JOIN clauses
        # with special handling for table names with parentheses that might be mistaken for function calls
        tables = []
        
        # Normalize the query for easier parsing
        sql = ' '.join(sql.split()).lower()
        
        # First, check for table references that look like function calls: table_name(param)
        func_pattern = re.compile(r'(FROM|JOIN)\s+(?:_cache_)?([a-zA-Z0-9_]+)(\s*\([^)]*\))', re.IGNORECASE)
        func_matches = func_pattern.findall(sql)
        
        for clause, table, params in func_matches:
            # Sanitize the table name
            clean_table = table.strip()
            
            # Extract parameter without parentheses and clean it
            param_content = params.strip()[1:-1].strip()
            if param_content:
                # Create possible variations of the table name
                tables.append(clean_table)  # Base table name
                
                # With parameter appended
                clean_param = re.sub(r'[^a-zA-Z0-9_]', '_', param_content)
                tables.append(f"{clean_table}_{clean_param}")  # Table with parameter as suffix
            else:
                tables.append(clean_table)
        
        # Look for standard FROM clause tables
        from_parts = sql.split(' from ')
        if len(from_parts) > 1:
            # Get the part after FROM
            from_part = from_parts[1]
            # Cut at the next keyword (WHERE, JOIN, GROUP, ORDER, etc.)
            for keyword in ['where', 'join', 'group', 'order', 'having', 'limit', 'union', 'intersect', 'except']:
                keyword_parts = from_part.split(f' {keyword} ')
                if len(keyword_parts) > 1:
                    from_part = keyword_parts[0]
            
            # Extract table names from the FROM clause
            table_names = [t.strip() for t in from_part.split(',')]
            for table in table_names:
                # Handle aliasing (e.g., "table as t" or "table t")
                table_parts = table.split(' as ')
                if len(table_parts) > 1:
                    table = table_parts[0].strip()
                else:
                    table_parts = table.split()
                    if len(table_parts) > 1:
                        table = table_parts[0].strip()
                
                # Handle _cache_ prefix
                if table.startswith('_cache_'):
                    tables.append(table[7:])  # Remove '_cache_' prefix
                else:
                    tables.append(table)
        
        # Look for JOIN clauses
        join_parts = sql.split(' join ')
        for i in range(1, len(join_parts)):
            join_part = join_parts[i]
            # Cut at the next ON or USING keyword to isolate table name
            for keyword in [' on ', ' using ']:
                keyword_parts = join_part.split(keyword)
                if len(keyword_parts) > 1:
                    join_part = keyword_parts[0]
            
            # Get the table name
            join_table = join_part.split()[0].strip()
            
            # Handle aliasing
            join_table_parts = join_table.split(' as ')
            if len(join_table_parts) > 1:
                join_table = join_table_parts[0].strip()
            else:
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
        
        # Handle special case for tables with trailing underscore followed by parentheses
        # For example, "nyc_yellow_taxi_(jan_2023)" or "nyc_yellow_taxi_"
        for i, table in enumerate(filtered_tables[:]):
            # Check if the table has a trailing underscore
            if table.endswith('_'):
                # Add version without trailing underscore
                base_table = table[:-1]
                if base_table not in filtered_tables:
                    filtered_tables.append(base_table)
        
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
            
            # Skip if already registered in this batch
            if key in registered_tables:
                continue
                
            # Check if the table exists in the cache
            if key in self.entries:
                try:
                    if key in self.partitioned_tables:
                        # For partitioned tables
                        table = entry.get_table()  # This will load if needed
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
                                table = entry.get_table()
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
                            table = entry.get_table()
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
                
                # Extract field definitions using regex - safe alternative to Schema.from_string
                field_matches = re.findall(r'Field\((.*?):', schema_str) or re.findall(r'field_name: (.*?)[,\)]', schema_str)
                field_types = re.findall(r'type: (.*?)[\),]', schema_str)
                
                # Create fields from extracted information
                fields = []
                for i, name in enumerate(field_matches):
                    name = name.strip().strip("'\"")
                    # Default to string type if type extraction fails
                    field_type = pa.string() if i >= len(field_types) else _parse_type_str(field_types[i])
                    fields.append(pa.field(name, field_type))
                
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
            self._checker_thread.join(timeout=1.0)
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
        
        # Clear the cache
        self.clear()
        
        # Close the metadata store
        self.metadata_store.close()
        
        # Shutdown thread pools
        self.thread_pool.shutdown()
        
        # Stop background processing
        self.bg_queue.stop()
        
        # Stop memory manager
        self.memory_manager.close()
        
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
        logger.info(f"Memory pressure detected: Need to free {needed_bytes} bytes")
        freed_bytes = 0
        
        with self.lock:
            # First, try to spill partitioned tables to disk
            if self.config["spill_to_disk"] and self.partitioned_tables:
                # Ensure spill directory exists
                spill_dir = self.config["spill_directory"]
                try:
                    os.makedirs(spill_dir, exist_ok=True)
                except OSError as e:
                    logger.error(f"Failed to create spill directory {spill_dir}: {e}")
                    # Continue with eviction since spilling failed
                
                # Get partitioned tables sorted by last access time (oldest first)
                partitioned_tables = sorted(
                    self.partitioned_tables.items(),
                    key=lambda item: self.metadata_store.get_entry_metadata(item[0]).get("last_accessed_at", 0)
                )
                
                # Try to spill partitions from each table
                for key, partitioned_table in partitioned_tables:
                    try:
                        # Skip already persisted tables if we have that information
                        if hasattr(partitioned_table, 'is_persisted') and partitioned_table.is_persisted:
                            continue
                            
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
        
            # If we still need more space or spilling failed, evict entries
            if freed_bytes < needed_bytes:
                try:
                    additional_freed = self._evict(needed_bytes - freed_bytes)
                    freed_bytes += additional_freed
                    logger.info(f"Freed additional {additional_freed} bytes through eviction")
                except Exception as e:
                    logger.error(f"Error evicting entries: {e}")
            
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

    def _put_streaming(
        self,
        key: str,
        data: Any,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
        preserve_index: bool = True
    ) -> str:
        """
        Stream large data into partitioned storage to minimize memory usage
        
        Args:
            key: Key to store the data under
            data: Data to stream (file path, DataFrame, etc.)
            ttl: Time-to-live in seconds
            metadata: Additional metadata
            preserve_index: Whether to preserve DataFrame indices
            
        Returns:
            The cache key
        """
        logger.info(f"Streaming large data into cache with key: {key}")
        
        # First, determine the schema by reading a small sample
        if isinstance(data, str) and os.path.exists(data):
            # It's a file path - get file extension
            ext = os.path.splitext(data)[1].lower()
            
            if ext == '.parquet':
                import pyarrow.parquet as pq
                # Read just the schema and a small sample
                schema = pq.read_schema(data)
                with pq.ParquetFile(data) as pf:
                    sample_table = next(pf.iter_batches(batch_size=1000)).to_table()
            elif ext == '.csv':
                # Stream from CSV file using pyarrow's CSV reader (schema consistent)
                try:
                    chunk_size = self.config["streaming_chunk_size"]
                    import pyarrow.csv as csv
                    
                    # Create partitioned table first - will be populated in chunks
                    partitioned = PartitionedTable(key, None, self.config, metadata)
                    
                    # Special handling for nonstandard formats
                    # Read the first few lines to detect format
                    with open(data, 'r') as f:
                        first_lines = [f.readline().strip() for _ in range(10)]
                        first_lines = [line for line in first_lines if line]  # Remove empty lines
                    
                    # [... existing format detection code ...]
                    
                    # Read the file directly with Arrow CSV reader in batches
                    with self.lock:
                        # Remove any existing entry with this key
                        if key in self.entries:
                            self._remove_entry(key)
                        
                        # Add the first batch to get the schema
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
                        
                        # Store in our collections
                        self.entries[key] = partitioned
                        self.partitioned_tables[key] = partitioned
                        self.current_size_bytes += partitioned.total_size_bytes
                        
                        # Add to metadata store
                        self._add_partitioned_metadata(key, partitioned, ttl, metadata)
                        
                        return key
                except Exception as e:
                    logger.error(f"Error using Arrow CSV reader: {e}, falling back to alternative")
                    # Fall back to other methods
            # [... continue with existing code ...]
        
        # [... existing code ...]
    
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
