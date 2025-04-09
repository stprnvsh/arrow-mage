"""
Arrow Cache - High-performance caching system using Apache Arrow and DuckDB
"""

__version__ = "0.2.0"

from .cache import ArrowCache
from .cache_entry import CacheEntry
from .converters import to_arrow_table, from_arrow_table, estimate_size_bytes
from .config import ArrowCacheConfig, DEFAULT_CONFIG
from .memory import MemoryManager, MemoryTracker, apply_compression, zero_copy_slice
from .partitioning import TablePartition, PartitionedTable, partition_table
from .threading import ThreadPoolManager, AsyncTaskManager, parallel_map
from .query_optimization import QueryOptimizer, optimize_duckdb_connection, explain_query

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
    
    # Partitioning
    "TablePartition",
    "PartitionedTable",
    "partition_table",
    
    # Threading and parallel execution
    "ThreadPoolManager",
    "AsyncTaskManager",
    "parallel_map",
    
    # Query optimization
    "QueryOptimizer",
    "optimize_duckdb_connection",
    "explain_query",
]
