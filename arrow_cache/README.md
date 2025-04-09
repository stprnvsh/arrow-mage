# Arrow Cache

A high-performance caching system for data frames and tables using Apache Arrow and DuckDB with efficient memory management and persistence capabilities.

## Features

- **High-performance storage** - Store pandas DataFrames, GeoPandas GeoDataFrames, Parquet files, and Arrow tables with minimal overhead
- **Zero-copy data access** - Fast data access using Arrow's shared memory model
- **Memory efficiency** - Intelligent partitioning of large datasets to manage memory usage
- **SQL query capabilities** - DuckDB-powered SQL queries against cached tables
- **Persistence** - Store and load data to/from disk with atomic operations for data safety
- **Automatic cache eviction** - LRU, LFU, and other eviction policies to manage memory pressure
- **Memory-aware spilling** - Automatically spill partitions to disk when memory is low
- **Thread-safe operations** - Proper locking for concurrent access to the cache
- **Metadata management** - All metadata stored efficiently in DuckDB

## Installation

```bash
pip install arrow_cache
```

With GeoPandas support:
```bash
pip install arrow_cache[geo]
```

## Quick Start

```python
from arrow_cache import ArrowCache

# Create a cache with default settings
cache = ArrowCache()

# Cache a pandas DataFrame
import pandas as pd
df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
cache.put('my_dataframe', df)

# Query the cached data using SQL
result_df = cache.query('SELECT * FROM _cache_my_dataframe WHERE a > 1')

# Retrieve the data
cached_df = cache.get('my_dataframe')  # Returns a pandas DataFrame

# When finished, close the cache to clean up resources
cache.close()
```

## Configuration

Arrow Cache offers extensive configuration options:

```python
from arrow_cache import ArrowCache, ArrowCacheConfig

config = ArrowCacheConfig(
    # Memory management
    memory_limit=1024 * 1024 * 1024,  # 1GB cache limit
    memory_spill_threshold=0.8,       # Start spilling when 80% full
    
    # Partitioning for large datasets
    auto_partition=True,
    partition_size_rows=100_000,      # 100k rows per partition
    partition_size_bytes=50 * 1024 * 1024,  # 50MB per partition
    
    # Compression settings
    enable_compression=True,
    compression_type="lz4",           # Fast compression
    dictionary_encoding=True,         # Dictionary encoding for strings
    
    # Performance tuning
    thread_count=4,                   # Worker threads
    cache_query_plans=True,           # Cache query plans for repeated queries
    
    # Storage settings
    spill_to_disk=True,               # Allow spilling to disk
    spill_directory=".arrow_cache_spill",
    persistent_storage=True,
    storage_path=".arrow_cache_storage",
    delete_files_on_close=True,       # Clean up files when closing
)

cache = ArrowCache(config=config)
```

## Core Functionality

### Storing and Retrieving Data

```python
# Store data with optional TTL (time-to-live)
cache.put('my_dataframe', df, ttl=3600)  # Expire after 1 hour

# Add metadata
cache.put('my_dataframe', df, metadata={'source': 'database', 'version': 2})

# Retrieve data
df = cache.get('my_dataframe')

# Get a slice of data (efficient for large datasets)
df_slice = cache.get('my_dataframe', offset=1000, limit=100)

# Get as a specific type
arrow_table = cache.get('my_dataframe', target_type='arrow')
```

### SQL Queries

SQL queries are powered by DuckDB and can be run directly against cached tables:

```python
# Tables are registered with the prefix '_cache_'
result = cache.query('''
    SELECT 
        a, 
        COUNT(*) as count 
    FROM _cache_my_dataframe 
    GROUP BY a
''')

# Explain the query plan
explain = cache.explain('SELECT * FROM _cache_my_dataframe WHERE a > 10')
print(explain)
```

### Memory Management

Arrow Cache intelligently manages memory:

```python
# Check current cache status
status = cache.status()
print(f"Cache size: {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
print(f"Entries: {status['entry_count']}")
print(f"Memory usage: {status['memory']['allocated_bytes'] / 1024 / 1024:.2f} MB")

# Manually clear cache if needed
cache.clear()
```

### Data Persistence

Store and load data to/from disk:

```python
# Persist a dataset to disk
cache.persist('my_dataframe', storage_dir='/path/to/storage')

# Load a persisted dataset
cache.load('my_dataframe', storage_dir='/path/to/storage')
```

## Advanced Features

### Working with Large Datasets

Arrow Cache automatically partitions large datasets for efficient memory management:

```python
# Large datasets are automatically partitioned
large_df = pd.read_csv('large_dataset.csv')  # Millions of rows
cache.put('large_data', large_df)

# Get just a slice without loading everything into memory
slice_df = cache.get('large_data', offset=1000000, limit=1000)
```

### Working with GeoPandas

```python
import geopandas as gpd
from arrow_cache import ArrowCache

cache = ArrowCache()
gdf = gpd.read_file("some_geo_data.geojson")
cache.put('geo_data', gdf)

# Run spatial queries through DuckDB
result = cache.query('''
    SELECT * FROM _cache_geo_data 
    WHERE ST_Contains(geometry, ST_Point(0, 0))
''')
```

### Thread Safety

All cache operations are thread-safe:

```python
import concurrent.futures

def process_batch(batch_id):
    # Each thread can safely access the cache
    data = cache.get('large_data', offset=batch_id*1000, limit=1000)
    processed = process_function(data)
    return processed

with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    results = list(executor.map(process_batch, range(10)))
```

### Resource Management

Arrow Cache efficiently manages resources:

```python
# Use as a context manager
with ArrowCache() as cache:
    cache.put('temp_data', df)
    # Cache and resources are automatically cleaned up when exiting

# Or explicitly close when done
cache = ArrowCache()
try:
    # Use cache
    cache.put('my_data', df)
    result = cache.query('SELECT * FROM _cache_my_data')
finally:
    # This will clean up all resources, including any persisted files
    # if delete_files_on_close=True (default)
    cache.close()
```

## How It Works

Arrow Cache uses several advanced techniques:

1. **Apache Arrow** - For zero-copy, columnar data storage
2. **DuckDB** - For metadata storage and high-performance SQL queries
3. **Partitioning** - Breaking large datasets into manageable chunks
4. **Memory Tracking** - Monitoring memory usage and triggering spilling/eviction
5. **Atomic Operations** - Ensuring data integrity during failures
6. **Shared Memory Model** - Reducing memory overhead through shared buffers

## License

MIT License
