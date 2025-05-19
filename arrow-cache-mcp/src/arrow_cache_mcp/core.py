"""
Core functionality for the Arrow Cache MCP.

This module provides the main cache management functions for the MCP system.
"""

import os
import sys
import gc
import logging
import shutil
import time
from typing import Dict, List, Optional, Tuple, Any, Union

from arrow_cache import ArrowCache, ArrowCacheConfig
from .duckdb_ingest import import_from_duckdb, import_to_cache

# Configure logging
logger = logging.getLogger(__name__)

# Default configuration
SPILL_DIRECTORY = ".arrow_cache_spill_mcp"
DEFAULT_CONFIG = {
    "memory_limit": 20 * 1024 * 1024 * 1024,  # 20 GB limit
    "spill_to_disk": True,                   # Allow spilling if memory is exceeded
    "spill_directory": SPILL_DIRECTORY,
    "auto_partition": True,                  # Enable auto-partitioning for large datasets
    "compression_type": "lz4",               # Use LZ4 compression
    "enable_compression": True,              # Enable compression
    "dictionary_encoding": True,             # Use dictionary encoding for strings
    "cache_query_plans": True,
    "thread_count": 10,
    "background_threads": 5,
    "partition_size_rows": 10000000,         # 10M rows per partition
    "partition_size_bytes": 1 * 1024 * 1024 * 1024,  # 1GB per partition
    "streaming_chunk_size": 500000,          # Larger streaming chunks
    "lazy_duckdb_registration": True,        # Only register tables when needed for queries
}

_cache_instance = None

class CacheNotInitializedError(Exception):
    """Custom exception for when the cache is not initialized."""
    pass

def get_arrow_cache(config: Optional[Dict[str, Any]] = None) -> ArrowCache:
    """
    Get or create the ArrowCache instance.
    
    Args:
        config: Optional configuration dictionary to override defaults
        
    Returns:
        ArrowCache instance
    """
    global _cache_instance
    
    if _cache_instance is not None:
        return _cache_instance
        
    # Merge default config with provided config
    merged_config = DEFAULT_CONFIG.copy()
    if config:
        merged_config.update(config)
    
    logger.info("Initializing ArrowCache...")
    
    # Clear any existing cache files
    clear_cache_files()
    
    # Ensure spill directory exists
    os.makedirs(merged_config["spill_directory"], exist_ok=True)
    
    # Create ArrowCache config
    arrow_config = ArrowCacheConfig(**merged_config)
    
    # Initialize the cache
    _cache_instance = ArrowCache(config=arrow_config)
    logger.info(f"ArrowCache Initialized. Using spill dir: {merged_config['spill_directory']}")
    
    # Register atexit handler
    import atexit
    atexit.register(close_cache)
    
    # Apply patches and enhancements
    patch_partitioned_table()
    enhance_memory_management()
    
    return _cache_instance

def clear_cache_files() -> None:
    """
    Clear all cache files including spill directory and any persisted files.
    """
    # Clear spill directory
    if os.path.exists(SPILL_DIRECTORY):
        try:
            shutil.rmtree(SPILL_DIRECTORY)
            logger.info(f"Removed spill directory: {SPILL_DIRECTORY}")
        except Exception as e:
            logger.error(f"Error clearing spill directory: {e}")
    
    # Create empty spill directory
    os.makedirs(SPILL_DIRECTORY, exist_ok=True)
    
    # Clear any DuckDB-related files that might be persisting
    duckdb_files = [f for f in os.listdir('.') if f.endswith('.duckdb') or f.endswith('.duckdb.wal')]
    for file in duckdb_files:
        try:
            os.remove(file)
            logger.info(f"Removed DuckDB file: {file}")
        except Exception as e:
            logger.error(f"Error removing DuckDB file {file}: {e}")
    
    logger.info("Cache files cleared")

def close_cache() -> None:
    """
    Close the cache and ensure all resources are released.
    """
    global _cache_instance
    
    if _cache_instance is None:
        return
        
    logger.info("Closing ArrowCache and cleaning up resources...")
    
    try:
        cache = _cache_instance
        
        # Check if connection is still alive
        if not hasattr(cache, 'metadata_store') or not hasattr(cache.metadata_store, 'con'):
            logger.info("DuckDB connection or metadata store not available, skipping some cleanup operations")
            # Still clean up the files
            try:
                clear_cache_files()
            except Exception as e:
                logger.error(f"Error clearing cache files: {e}")
            
            _cache_instance = None
            return
        
        # First check if tables exist before trying to use them
        try:
            # Check if registered_tables exists
            tables_exist = cache.metadata_store.con.execute("""
                SELECT count(*) FROM sqlite_master 
                WHERE type='table' AND name='registered_tables'
            """).fetchone()[0] > 0
            
            persistence_exists = cache.metadata_store.con.execute("""
                SELECT count(*) FROM sqlite_master 
                WHERE type='table' AND name='persistence_metadata'
            """).fetchone()[0] > 0
        except Exception as e:
            logger.warning(f"Error checking table existence: {e}")
            tables_exist = False
            persistence_exists = False
        
        # First try to get keys
        keys = []
        if tables_exist:
            try:
                keys = cache.get_keys()
            except Exception as e:
                logger.error(f"Error getting cache keys: {e}")
        
        # Manually unregister and remove all tables first
        for key in keys:
            try:
                # Try to remove the dataset
                cache.remove(key)
            except Exception as e:
                logger.error(f"Error removing dataset {key}: {e}")
                # Fallback: try to unregister the table directly
                if tables_exist:
                    try:
                        cache.metadata_store.unregister_table(key)
                    except Exception as inner_e:
                        logger.error(f"Error unregistering table {key}: {inner_e}")
        
        # Reset DuckDB connection if the metadata store exists
        try:
            if hasattr(cache, 'metadata_store') and hasattr(cache.metadata_store, 'con'):
                # Close and reopen the connection to reset its state
                original_path = cache.metadata_store.db_path
                cache.metadata_store.con.close()
                import duckdb
                cache.metadata_store.con = duckdb.connect(original_path)
        except Exception as e:
            logger.error(f"Error resetting DuckDB connection: {e}")
        
        # Force cache to clear all entries
        try:
            cache.clear()
        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
        
        # Close the cache
        try:
            cache.close()
        except Exception as e:
            logger.error(f"Error closing cache: {e}")
        
        # Clear any remaining files
        try:
            clear_cache_files()
        except Exception as e:
            logger.error(f"Error clearing cache files: {e}")
        
        # Run Python garbage collection to release memory
        for _ in range(3):  # Multiple collection passes can help
            gc.collect()
        
        logger.info("ArrowCache successfully closed and cleaned up")
    except Exception as e:
        logger.error(f"Error during cache cleanup: {e}")
        import traceback
        logger.error(traceback.format_exc())
    
    _cache_instance = None

def import_data_directly(
    key: str,
    source: str,
    source_type: str = "auto",
    connection_options: Optional[Dict[str, Any]] = None,
    query: Optional[str] = None,
    table_name: Optional[str] = None,
    schema: Optional[str] = None,
    ttl: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
    options: Optional[Dict[str, Any]] = None
) -> Tuple[bool, Union[Dict[str, Any], str]]:
    """
    Import data directly into the cache using DuckDB's connectors.
    
    This is a simplified wrapper around the import_to_cache function that handles
    error reporting and metadata formatting for the MCP.
    
    Args:
        key: Key to store the data under
        source: Source URI or path
        source_type: Source type
        connection_options: Connection options
        query: SQL query to execute
        table_name: Table name to load
        schema: Schema name
        ttl: Time-to-live for the cached data
        metadata: Additional metadata to store with the cached data
        options: Additional options for the specific connector
        
    Returns:
        Tuple of (success, result_or_error_message)
    """
    cache = get_arrow_cache()
    
    # Check if dataset already exists
    if cache.contains(key):
        return False, f"Dataset '{key}' already exists. Please use a different name or remove it first."
    
    try:
        start_time = time.time()
        
        # Initialize metadata if not provided
        if metadata is None:
            metadata = {}
        
        # Add basic metadata
        metadata.update({
            'source': source,
            'source_type': source_type,
            'loaded_at': time.time(),
        })
        
        # Add query or table info if provided
        if query:
            metadata['query'] = query
        if table_name:
            metadata['table_name'] = table_name
        if schema:
            metadata['schema'] = schema
        
        # Import directly to cache using DuckDB's connectors
        # This will also calculate and include size in the metadata
        cache_key = import_to_cache(
            cache_instance=cache,
            key=key,
            source=source,
            source_type=source_type,
            connection_options=connection_options,
            query=query,
            table_name=table_name,
            schema=schema,
            ttl=ttl,
            metadata=metadata,
            options=options
        )
        
        # Get imported table metadata
        result_metadata = cache.get_metadata(cache_key) or {}
        
        # Create final metadata
        final_metadata = {}
        
        # First, add our original metadata
        final_metadata.update(metadata)
        
        # Then add key fields from the result_metadata, especially size info
        if result_metadata:
            for k, v in result_metadata.items():
                # Always keep these fields from import_to_cache
                if k in ['size_bytes', 'size_mb', 'row_count', 'column_count', 
                         'columns', 'column_types', 'dtypes']:
                    final_metadata[k] = v
                # For other fields, only add if they're not already set
                elif k not in final_metadata:
                    final_metadata[k] = v
        
        # Check if size_bytes is missing or zero - if so, calculate it
        if 'size_bytes' not in final_metadata or final_metadata.get('size_bytes', 0) <= 0:
            logger.info(f"Size information missing or zero for {key}, calculating manually")
            
            # Calculate memory usage using Arrow's methods
            try:
                # Get a sample to estimate size
                table = cache.get(cache_key, limit=1000)  # Get a sample of rows
                
                # Method 1: Try using table.nbytes if available (best method)
                try:
                    if hasattr(table, 'nbytes'):
                        size_bytes = table.nbytes
                        logger.info(f"Used table.nbytes for size calculation: {size_bytes} bytes")
                    else:
                        raise AttributeError("table.nbytes not available")
                except Exception as e:
                    logger.debug(f"nbytes method failed: {e}, trying buffer tracking")
                    
                    # Method 2: Buffer tracking approach
                    try:
                        size_bytes = 0
                        # Track unique buffers to avoid double counting
                        seen_buffers = set()
                        
                        for col_name in table.column_names:
                            col = table.column(col_name)
                            
                            # Add size of each buffer in the column
                            for chunk in col.chunks:
                                for buf in chunk.buffers():
                                    if buf is not None:
                                        # Use buffer address as identifier to avoid duplicates
                                        buf_id = id(buf)
                                        if buf_id not in seen_buffers:
                                            seen_buffers.add(buf_id)
                                            size_bytes += buf.size
                        
                        # Add schema overhead
                        size_bytes += len(table.schema.to_string()) * 2
                        
                        # Scale up for full table if we have row count
                        if 'row_count' in final_metadata and final_metadata['row_count'] > 0:
                            sample_rows = len(table)
                            if sample_rows > 0:  # Avoid division by zero
                                scaling_factor = final_metadata['row_count'] / sample_rows
                                size_bytes = int(size_bytes * scaling_factor)
                                
                        logger.info(f"Used buffer tracking for size calculation: {size_bytes} bytes")
                    except Exception as e:
                        logger.debug(f"Buffer tracking failed: {e}, trying estimation by type")
                        
                        # Method 3: Estimate based on data types
                        size_bytes = 0
                        row_count = final_metadata.get('row_count', len(table))
                        
                        for col_name in table.column_names:
                            col = table.column(col_name)
                            col_type = str(col.type)
                            
                            # Estimate bytes per value based on type
                            bytes_per_value = 8  # Default 
                            
                            if 'string' in col_type.lower():
                                # Sample string lengths
                                sample = col.slice(0, min(100, len(col)))
                                try:
                                    sample_values = [str(x) if x is not None else '' for x in sample.to_pylist()]
                                    if sample_values:
                                        avg_len = sum(len(s) for s in sample_values) / len(sample_values)
                                        bytes_per_value = max(8, avg_len)
                                except:
                                    bytes_per_value = 32  # Default for strings
                            elif 'int8' in col_type or 'bool' in col_type:
                                bytes_per_value = 1
                            elif 'int16' in col_type or 'uint16' in col_type:
                                bytes_per_value = 2
                            elif 'int32' in col_type or 'uint32' in col_type or 'float' in col_type:
                                bytes_per_value = 4
                            elif 'int64' in col_type or 'uint64' in col_type or 'double' in col_type:
                                bytes_per_value = 8
                            elif 'dictionary' in col_type:
                                bytes_per_value = 12  # Higher for dictionary encoding
                            
                            # Calculate column size
                            col_size = bytes_per_value * row_count
                            size_bytes += col_size
                        
                        # Add overhead for structure
                        size_bytes = int(size_bytes * 1.05)  # 5% overhead
                        logger.info(f"Used type-based estimation for size: {size_bytes} bytes")
                
                # Update metadata with calculated size
                final_metadata['size_bytes'] = size_bytes
                final_metadata['size_mb'] = size_bytes / (1024 * 1024)
                
            except Exception as size_err:
                logger.warning(f"All size calculation methods failed: {size_err}")
                # Last resort - very basic estimation
                try:
                    row_count = final_metadata.get('row_count', 0)
                    col_count = len(final_metadata.get('columns', []))
                    if row_count > 0 and col_count > 0:
                        size_bytes = row_count * col_count * 16  # 16 bytes per cell
                        final_metadata['size_bytes'] = size_bytes
                        final_metadata['size_mb'] = size_bytes / (1024 * 1024)
                        logger.info(f"Used last-resort estimation: {size_bytes} bytes")
                except Exception as e:
                    logger.error(f"Final size estimation failed: {e}")
        
        # Add timing information
        load_time = time.time() - start_time
        final_metadata['load_time_seconds'] = load_time
        final_metadata['total_time_seconds'] = load_time
        final_metadata['name'] = key
        
        # Log size information for debugging
        if 'size_bytes' in final_metadata:
            logger.info(f"Imported table size: {final_metadata['size_bytes']} bytes ({final_metadata.get('size_mb', 0):.2f} MB)")
        else:
            logger.warning("Size information missing from imported table metadata")
        
        return True, final_metadata
    
    except Exception as e:
        import traceback
        logger.error(f"Error importing data: {e}")
        logger.debug(traceback.format_exc())
        return False, f"Error importing data: {str(e)}"

def patch_partitioned_table() -> None:
    """
    Add compatibility patch for PartitionedTable to work with the eviction policy.
    
    The eviction policy expects a size_bytes attribute, but PartitionedTable uses total_size_bytes.
    This patch adds a size_bytes property to make it compatible.
    """
    try:
        from arrow_cache.partitioning import PartitionedTable
        
        # Add a size_bytes property if it doesn't already exist
        if not hasattr(PartitionedTable, 'size_bytes'):
            # Using the descriptor protocol to create a property
            PartitionedTable.size_bytes = property(
                lambda self: self.total_size_bytes,
                lambda self, value: setattr(self, 'total_size_bytes', value)
            )
            logger.info("Applied PartitionedTable compatibility patch")
    except Exception as e:
        logger.error(f"Failed to apply PartitionedTable patch: {e}")

def enhance_memory_management() -> None:
    """
    Enhance memory management for ArrowCache by adding custom spill handlers
    and fixing any issues with partitioned tables.
    """
    cache = get_arrow_cache()
    
    def custom_spill_handler(needed_bytes):
        """Custom spill handler that properly handles partitioned tables"""
        logger.info(f"Custom spill handler called, trying to free {needed_bytes/1024/1024:.2f} MB")
        
        # Get all partitioned tables
        freed_bytes = 0
        if hasattr(cache, 'partitioned_tables'):
            # Create a list and sort by last accessed time (oldest first)
            candidates = sorted(
                [(key, table) for key, table in cache.partitioned_tables.items() 
                 if hasattr(table, 'spill_partitions')],
                key=lambda x: cache.metadata_store.get_entry_metadata(x[0]).get("last_accessed_at", 0) 
                              if cache.metadata_store.get_entry_metadata(x[0]) else 0
            )
            
            spill_dir = cache.config["spill_directory"]
            
            # Spill partitions from each table until we've freed enough
            for key, table in candidates:
                if freed_bytes >= needed_bytes:
                    break
                    
                # Use the table's spill_partitions method
                try:
                    freed = table.spill_partitions(spill_dir, needed_bytes - freed_bytes)
                    logger.info(f"Freed {freed/1024/1024:.2f} MB from {key}")
                    freed_bytes += freed
                except Exception as e:
                    logger.error(f"Error spilling partitions from {key}: {e}")
                    # Try the slow but safer approach - unload individual partitions
                    try:
                        # Try to unload partitions manually
                        if hasattr(table, 'partitions'):
                            for part_id, partition in table.partitions.items():
                                if partition.is_loaded and not partition.is_pinned:
                                    estimated_size = getattr(partition, 'size_bytes', 0)
                                    partition.free()  # Release memory
                                    freed_bytes += estimated_size
                                    logger.info(f"Manually freed partition {part_id} ({estimated_size/1024/1024:.2f} MB)")
                                    if freed_bytes >= needed_bytes:
                                        break
                    except Exception as inner_e:
                        logger.error(f"Error in manual partition unloading: {inner_e}")
        
        # Log the result
        logger.info(f"Custom spill handler freed {freed_bytes/1024/1024:.2f} MB")
        return freed_bytes
    
    # Register our custom spill handler with the memory manager
    if hasattr(cache, 'memory_manager') and hasattr(cache.memory_manager, 'spill_callback'):
        # Store the original spill callback as a fallback
        original_spill_callback = cache.memory_manager.spill_callback
        
        # Create a wrapper function to use both our handler and the original
        def combined_spill_handler(needed_bytes):
            # First try our custom handler
            freed_bytes = custom_spill_handler(needed_bytes)
            
            # If we need more space, call the original handler
            if freed_bytes < needed_bytes and original_spill_callback:
                try:
                    more_freed = original_spill_callback(needed_bytes - freed_bytes)
                    freed_bytes += more_freed
                except Exception as e:
                    logger.error(f"Error in original spill callback: {e}")
            
            return freed_bytes
        
        # Replace the original spill callback with our combined one
        cache.memory_manager.spill_callback = combined_spill_handler
        logger.info("Enhanced memory management installed")

def remove_dataset(dataset_name: str) -> Tuple[bool, str]:
    """
    Remove a dataset from the cache.
    
    Args:
        dataset_name: Name of the dataset to remove
        
    Returns:
        Tuple of (success, message)
    """
    cache = get_arrow_cache()
    
    if not cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' not found."
    
    try:
        cache.remove(dataset_name)
        return True, f"Dataset '{dataset_name}' removed successfully."
    except Exception as e:
        logger.error(f"Error removing dataset {dataset_name}: {e}")
        return False, f"Error removing dataset: {str(e)}"

def get_datasets_list() -> List[Dict[str, Any]]:
    """
    Get a list of all datasets in the cache with their metadata.
    
    Returns:
        List of dictionaries containing dataset information
    """
    cache = get_arrow_cache()
    
    try:
        # Get datasets directly from metadata store - this is the most reliable source
        metadata_df = cache.metadata_store.get_all_entries()
        
        if metadata_df.empty:
            logger.info("No entries found in metadata store")
            return []
        
        datasets = []
        for _, row in metadata_df.iterrows():
            # Extract key fields from metadata DataFrame
            key = row['key']
            size_bytes = row['size_bytes']
            num_rows = row['num_rows']
            created_at = row['created_at']
            last_accessed_at = row['last_accessed_at']
            
            # Parse schema from schema_json
            try:
                schema_json = row['schema_json']
                
                # Parse schema directly from metadata_store instead of trying to reconstruct it
                import pyarrow as pa
                
                # Try to extract column names directly from the schema_json string
                import re
                column_matches = re.findall(r'field_name: (.*?)[,\)]', schema_json)
                if not column_matches:
                    # Alternative pattern
                    column_matches = re.findall(r'Field\((.*?):', schema_json)
                
                column_names = [name.strip().strip("'\"") for name in column_matches]
                column_count = len(column_names)
                
                # If we couldn't extract column names, try accessing metadata_json for column info
                if not column_names and row['metadata_json']:
                    import json
                    metadata = json.loads(row['metadata_json'])
                    if 'columns' in metadata:
                        column_names = metadata['columns']
                        column_count = len(column_names)
                
            except Exception as e:
                logger.error(f"Error parsing schema for {key}: {e}")
                column_names = []
                column_count = 0
            
            # Get geospatial metadata if present
            geometry_column = row.get('geometry_column', None)
            geometry_type = row.get('geometry_type', None)
            crs_info = row.get('crs_info', None)
            storage_format = row.get('storage_format', None)
            
            # Parse metadata from metadata_json
            try:
                if row['metadata_json']:
                    import json
                    metadata = json.loads(row['metadata_json'])
                else:
                    metadata = {}
            except Exception as e:
                logger.error(f"Error parsing metadata for {key}: {e}")
                metadata = {}
            
            # Create dataset info dict with all important information
            dataset_info = {
                'name': key,
                'size_bytes': size_bytes,
                'size_mb': size_bytes / (1024*1024),
                'row_count': num_rows,
                'columns': column_names,
                'column_count': column_count,
                'created_at': created_at,
                'last_accessed': last_accessed_at,
            }
            
            # Add geospatial info if available
            if geometry_column:
                dataset_info['geometry_column'] = geometry_column
                dataset_info['geometry_type'] = geometry_type
                dataset_info['crs'] = crs_info
                dataset_info['geometry_storage'] = storage_format
            
            # Add auto-extracted column stats if available
            for json_col in [
                'column_types_json',
                'column_duckdb_types_json',
                'column_null_counts_json',
                'column_chunk_counts_json',
                'column_dictionary_encoded_json',
                'column_min_max_json'
            ]:
                if row.get(json_col):
                    try:
                        # Parse JSON and add under a simplified key
                        key_name = json_col.replace('_json', '')
                        dataset_info[key_name] = json.loads(row[json_col])
                    except json.JSONDecodeError as json_err:
                        logger.error(f"Error parsing {json_col} for {key}: {json_err}")
            
            # Add metadata fields directly to dataset_info for easy access
            if metadata:
                dataset_info.update(metadata)
            
            # Store the full metadata separately
            dataset_info['metadata'] = metadata
            
            datasets.append(dataset_info)
        
        logger.info(f"Found {len(datasets)} datasets from metadata store")
        return datasets
    except Exception as e:
        logger.error(f"Error getting datasets list: {e}")
        import traceback
        logger.error(traceback.format_exc())
        
        # Fallback if metadata store fails: try direct cache status
        try:
            logger.info("Attempting fallback using cache.status()")
            status = cache.status()
            if 'entries' not in status:
                logger.info("No entries in cache status")
                return []
                
            # Get keys
            keys = cache.get_keys()
            logger.info(f"Found {len(keys)} keys")
            
            datasets = []
            for key in keys:
                try:
                    # Get sample to extract basic info
                    sample = cache.get(key, limit=5)
                    
                    # Try to get metadata
                    metadata = cache.get_metadata(key) or {}
                    
                    # Create dataset info with basic properties
                    dataset_info = {
                        'name': key,
                        'size_bytes': status.get('current_size_bytes', 0) // len(keys),  # Estimate
                        'size_mb': status.get('current_size_bytes', 0) / (1024*1024) / len(keys),
                        'row_count': 0,  # Will try to get accurate count below
                        'column_count': len(sample.columns) if hasattr(sample, 'columns') else 0,
                        'columns': list(sample.columns) if hasattr(sample, 'columns') else [],
                    }
                    
                    # Try to get accurate row count
                    try:
                        count_df = cache.query(f"SELECT COUNT(*) as row_count FROM _cache_{key}")
                        dataset_info['row_count'] = int(count_df['row_count'][0])
                    except Exception as count_e:
                        logger.error(f"Error getting row count for {key}: {count_e}")
                    
                    # Add metadata
                    if metadata:
                        # Add metadata to dataset_info
                        if 'metadata' in metadata and isinstance(metadata['metadata'], dict):
                            dataset_info.update(metadata['metadata'])
                        
                        # Add size info from metadata if available
                        if 'size_bytes' in metadata:
                            dataset_info['size_bytes'] = metadata['size_bytes']
                            dataset_info['size_mb'] = metadata['size_bytes'] / (1024*1024)
                    
                    datasets.append(dataset_info)
                except Exception as e:
                    logger.error(f"Error getting info for key {key}: {e}")
            
            return datasets
        except Exception as fallback_e:
            logger.error(f"Fallback also failed: {fallback_e}")
            return []

def get_memory_usage() -> Dict[str, Any]:
    """
    Get current memory usage statistics directly from cache and memory manager.
    
    Returns:
        Dictionary with memory usage statistics
    """
    cache = get_arrow_cache()
    
    # Get complete cache status which contains all the information we need
    status = cache.status()
    
    # The memory_manager in ArrowCache has detailed memory tracking
    memory_info = status.get('memory', {})
    
    # Get memory limit from config or status
    memory_limit = status.get('max_size_bytes', cache.config["memory_limit"])
    if memory_limit is None:
        # If still None, use system memory as reference
        import psutil
        memory_limit = int(psutil.virtual_memory().total * 0.8)  # 80% of system memory
    
    # Get current cache size 
    cache_size_bytes = status.get('current_size_bytes', 0)
    
    # Get process memory metrics
    process_rss = memory_info.get('process_rss', 0)
    
    # Estimate of application overhead (Python interpreter, etc.)
    app_overhead = min(process_rss, 200 * 1024 * 1024)  # Cap at process_rss or 200MB
    
    # Return comprehensive memory stats
    memory_usage = {
        # Cache-specific metrics
        'cache_size_bytes': cache_size_bytes,
        'cache_size_mb': cache_size_bytes / (1024*1024),
        'memory_limit_bytes': memory_limit,
        'memory_limit_mb': memory_limit / (1024*1024),
        'cache_utilization_percent': (cache_size_bytes / memory_limit) * 100 if memory_limit > 0 else 0,
        'entry_count': status.get('entry_count', 0),
        
        # Process metrics
        'process_rss_bytes': process_rss,
        'process_rss_mb': process_rss / (1024*1024),
        'app_overhead_mb': app_overhead / (1024*1024),
        
        # System memory metrics from ArrowCache
        'system_memory_total': memory_info.get('system_memory_total', 0),
        'system_memory_available': memory_info.get('system_memory_available', 0),
        'system_memory_used_percent': memory_info.get('system_memory_percent', 0),
        
        # Arrow memory pool metrics if available
        'pool_bytes_allocated': memory_info.get('pool_bytes_allocated', 0),
        'pool_max_memory': memory_info.get('pool_max_memory', 0),
        
        # For backward compatibility
        'current_size_bytes': cache_size_bytes,
        'current_size_mb': cache_size_bytes / (1024*1024),
        'utilization_percent': (cache_size_bytes / memory_limit) * 100 if memory_limit > 0 else 0,
    }
    
    return memory_usage

def remove_and_update(dataset_name: str) -> Tuple[bool, str]:
    """
    Remove a dataset and return a success message.
    
    Args:
        dataset_name: Name of the dataset to remove
        
    Returns:
        Tuple of (success, message)
    """
    return remove_dataset(dataset_name)

def get_dataset_metadata(dataset_name: str) -> Optional[Dict[str, Any]]:
    """Placeholder function to get dataset metadata."""
    cache = get_arrow_cache()
    if cache.contains(dataset_name):
        # In a real implementation, this would return more detailed metadata
        metadata = cache.get_metadata(dataset_name)
        if metadata:
            return metadata
        return {"name": dataset_name, "status": "available", "placeholder": True}
    return None 