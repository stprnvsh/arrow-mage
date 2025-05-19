"""
Configuration settings for Arrow Cache system.
"""
from typing import Optional, Dict, Any, List
import os

# Default settings
DEFAULT_CONFIG = {
    # Memory settings
    "memory_limit": None,  # None means use system available memory
    "memory_pool_type": "system",  # Options: system, jemalloc, mimalloc
    "memory_spill_threshold": 0.8,  # Percentage of memory limit that triggers spilling
    "enable_leak_detection": False,  # Enable memory leak detection (may impact performance)
    
    # Partitioning settings
    "partition_size_rows": 100_000,  # Default number of rows per partition
    "partition_size_bytes": 100 * 1024 * 1024,  # 100MB default partition size
    "auto_partition": True,  # Automatically partition large datasets
    
    # Streaming settings for large files
    "enable_streaming": True,  # Enable streaming for large files
    "streaming_chunk_size": 50_000,  # Rows per chunk when streaming
    "parallel_conversion": True,  # Use parallel processing for conversion
    "max_conversion_memory": 2 * 1024 * 1024 * 1024,  # 2GB max memory for conversion
    
    # Threading settings
    "thread_count": 0,  # 0 means use all available cores
    "background_threads": 2,  # Number of background threads for async operations
    
    # Compression settings
    "enable_compression": True,
    "compression_type": "zstd",  # Options: none, lz4, zstd, snappy
    "compression_level": 3,  # Compression level (algorithm-specific)
    "dictionary_encoding": True,  # Enable dictionary encoding for string columns
    
    # Query optimization
    "cache_query_plans": True,
    "query_plan_cache_size": 100,  # Number of query plans to cache
    "use_statistics": True,  # Use statistics for query optimization
    "lazy_duckdb_registration": False,  # Only register tables with DuckDB when queried
    
    # Storage settings
    "spill_to_disk": True,
    "spill_directory": os.path.join(os.getcwd(), ".arrow_cache_spill"),
    "persistent_storage": False,
    "storage_path": os.path.join(os.getcwd(), ".arrow_cache_storage"),
    
    # Pandas integration
    "pandas_use_extension_arrays": True,
    "pandas_preserve_index": True,
    "pandas_use_categories": True,
    
    # Cache behavior
    "prefetch_partitions": True,
    "prefetch_limit": 3,  # Number of partitions to prefetch
    
    # Monitoring
    "collect_metrics": True,
    "metrics_log_interval": 60,  # seconds
}

class ArrowCacheConfig:
    """Configuration manager for Arrow Cache settings"""
    
    def __init__(self, **kwargs):
        """
        Initialize configuration with default values, overridden by any provided kwargs.
        
        Args:
            **kwargs: Configuration overrides
        """
        self._config = DEFAULT_CONFIG.copy()
        
        # Override defaults with any provided values
        for key, value in kwargs.items():
            if key in self._config:
                self._config[key] = value
            else:
                raise ValueError(f"Unknown configuration parameter: {key}")
                
        # Setup derived values
        if self._config["thread_count"] == 0:
            import multiprocessing
            self._config["thread_count"] = multiprocessing.cpu_count()
            
        # Create spill directory if needed
        if self._config["spill_to_disk"] and not os.path.exists(self._config["spill_directory"]):
            os.makedirs(self._config["spill_directory"], exist_ok=True)
            
        # Create storage directory if needed
        if self._config["persistent_storage"] and not os.path.exists(self._config["storage_path"]):
            os.makedirs(self._config["storage_path"], exist_ok=True)
    
    def __getitem__(self, key):
        """Get a configuration value"""
        return self._config.get(key)
    
    def __setitem__(self, key, value):
        """Set a configuration value"""
        if key in self._config:
            self._config[key] = value
        else:
            raise ValueError(f"Unknown configuration parameter: {key}")
    
    def get(self, key, default=None):
        """Get a configuration value with a default fallback"""
        return self._config.get(key, default)
    
    def to_dict(self):
        """Return the configuration as a dictionary"""
        return self._config.copy()
    
    @classmethod
    def from_dict(cls, config_dict):
        """Create a configuration from a dictionary"""
        return cls(**config_dict)
    
    @classmethod
    def from_file(cls, filename):
        """Load configuration from a JSON file"""
        import json
        with open(filename, 'r') as f:
            config_dict = json.load(f)
        return cls(**config_dict)
    
    def save_to_file(self, filename):
        """Save configuration to a JSON file"""
        import json
        with open(filename, 'w') as f:
            json.dump(self._config, f, indent=2) 