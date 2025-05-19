"""
Arrow Cache - High-performance caching system using Apache Arrow and DuckDB
"""

__version__ = "0.2.0"

from .cache import ArrowCache
from .cache_entry import CacheEntry
from .converters import to_arrow_table, from_arrow_table, estimate_size_bytes
from .config import ArrowCacheConfig, DEFAULT_CONFIG
from .memory import (
    MemoryManager, MemoryTracker, apply_compression, 
    zero_copy_slice, zero_copy_filter, 
    efficient_table_chunking, optimize_string_columns,
    get_table_memory_footprint
)
from .partitioning import TablePartition, PartitionedTable, partition_table
from .threading import (
    ThreadPoolManager, 
    AsyncTaskManager, 
    parallel_map, 
    process_arrow_batches_parallel,
    apply_arrow_compute_parallel
)
from .query_optimization import QueryOptimizer, optimize_duckdb_connection, explain_query
from .utils import clear_arrow_cache
from .duckdb_ingest import import_from_duckdb, import_to_cache

__all__ = [
    # Main cache class
    "ArrowCache",
    
    # Configuration
    "ArrowCacheConfig",
    "DEFAULT_CONFIG",
    
    # Core components
    "CacheEntry",
    "to_arrow_table",
    "from_arrow_table",
    "estimate_size_bytes",
    
    # Memory management
    "MemoryManager",
    "MemoryTracker",
    "apply_compression",
    "zero_copy_slice",
    "zero_copy_filter",
    "efficient_table_chunking",
    "optimize_string_columns",
    "get_table_memory_footprint",
    
    # Partitioning
    "TablePartition",
    "PartitionedTable",
    "partition_table",
    
    # Threading and parallel execution
    "ThreadPoolManager",
    "AsyncTaskManager",
    "parallel_map",
    "process_arrow_batches_parallel",
    "apply_arrow_compute_parallel",
    
    # Query optimization
    "QueryOptimizer",
    "optimize_duckdb_connection",
    "explain_query",
    
    # DuckDB data ingestion
    "import_from_duckdb",
    "import_to_cache",
    
    # Utilities
    "clear_arrow_cache",
]
