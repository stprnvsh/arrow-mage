#!/usr/bin/env python
"""
Basic usage example for ArrowCache

This example demonstrates:
1. Creating a cache
2. Adding different types of data
3. Retrieving data
4. Running SQL queries via DuckDB
5. Cache eviction
"""

import os
import sys
import pandas as pd
import numpy as np
import time

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from arrow_cache import ArrowCache

def main():
    # Create a cache with 10MB max size using LRU eviction policy
    print("Creating Arrow Cache...")
    cache = ArrowCache(
        max_size_bytes=10 * 1024 * 1024,  # 10MB (reduced from 100MB)
        eviction_policy="lru",
        check_interval=5.0  # Check for expired entries every 5 seconds
    )

    # Example 1: Caching a pandas DataFrame
    print("\n=== Example 1: Caching a pandas DataFrame ===")
    df = pd.DataFrame({
        'id': range(1000),
        'value': np.random.randn(1000),
        'category': np.random.choice(['A', 'B', 'C', 'D'], 1000)
    })
    
    # Store in cache with 30 second TTL
    cache.put('example_df', df, ttl=30)
    print(f"Added DataFrame to cache with key 'example_df'")
    
    # Retrieve from cache
    cached_df = cache.get('example_df')
    print(f"Retrieved DataFrame with shape: {cached_df.shape}")
    print(cached_df.head(3))

    # Example 2: Running SQL queries
    print("\n=== Example 2: Running SQL queries ===")
    result = cache.query("SELECT category, COUNT(*) as count, AVG(value) as avg_value FROM _cache_example_df GROUP BY category")
    print("SQL Query Result:")
    print(result)

    # Example 3: Adding multiple datasets
    print("\n=== Example 3: Adding multiple datasets ===")
    # Create a time series DataFrame
    dates = pd.date_range('2023-01-01', periods=1000)
    timeseries_df = pd.DataFrame({
        'date': dates,
        'value': np.cumsum(np.random.randn(1000)),
        'volume': np.random.randint(100, 10000, 1000)
    })
    
    cache.put('timeseries', timeseries_df)
    print(f"Added time series data with key 'timeseries'")
    
    # Create a geographical DataFrame
    cities_df = pd.DataFrame({
        'city': ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix'],
        'lat': [40.7128, 34.0522, 41.8781, 29.7604, 33.4484],
        'lng': [-74.0060, -118.2437, -87.6298, -95.3698, -112.0740],
        'population': [8804190, 3898747, 2746388, 2304580, 1608139]
    })
    
    cache.put('cities', cities_df)
    print(f"Added cities data with key 'cities'")
    
    # Check cache status
    status = cache.status()
    print("\nCache Status:")
    print(f"- Entry count: {status['entry_count']}")
    print(f"- Current size: {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
    print(f"- Hit ratio: {status['hit_ratio']:.2f}")

    # Example 4: Complex SQL query joining multiple cached tables
    print("\n=== Example 4: Complex SQL query across tables ===")
    result = cache.query("""
        WITH ranked_cities AS (
            SELECT 
                city, 
                lat, 
                lng, 
                population,
                RANK() OVER (ORDER BY population DESC) as population_rank
            FROM _cache_cities
        ),
        recent_data AS (
            SELECT 
                date,
                value,
                volume
            FROM _cache_timeseries
            ORDER BY date DESC
            LIMIT 10
        )
        SELECT 
            c.city,
            c.population,
            c.population_rank,
            t.date,
            t.value as latest_value
        FROM ranked_cities c
        CROSS JOIN recent_data t
        WHERE c.population_rank <= 3
        ORDER BY c.population_rank, t.date DESC
    """)
    print("Complex SQL Query Result:")
    print(result.head(10))

    # Example 5: Cache eviction
    print("\n=== Example 5: Cache eviction ===")
    
    # First, access the entries in a specific order to establish LRU order
    print("Setting up LRU order by accessing cache entries...")
    _ = cache.get('cities')      # Least recently used
    _ = cache.get('timeseries')  # Second least recently used
    _ = cache.get('example_df')  # Most recently used
    
    print("LRU order (from least to most recent):")
    print("  'cities' -> 'timeseries' -> 'example_df'")
    
    # Get cache status before adding new data
    status = cache.status()
    print("\nCache Status before eviction:")
    print(f"- Entry count: {status['entry_count']}")
    print(f"- Current size: {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
    print(f"- Cache keys: {cache.get_keys()}")
    
    # Create multiple smaller DataFrames to add to the cache
    print("\nAdding multiple items to trigger eviction...")
    
    # Create and add several frames of increasing size
    for i in range(1, 6):
        size = 150_000 * i
        df = pd.DataFrame({
            'id': range(size),
            'value': np.random.randn(size),
        })
        key = f'data_{i}'
        print(f"Adding '{key}' with {size} rows...")
        cache.put(key, df)
        
        # Display cache status after each addition
        status = cache.status()
        print(f"  Cache now has {status['entry_count']} entries, {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
        print(f"  Keys: {cache.get_keys()}")
    
    # Verify final cache state
    print("\nFinal cache status after all additions:")
    status = cache.status()
    print(f"- Entry count: {status['entry_count']}")
    print(f"- Current size: {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
    print(f"- Cache keys: {cache.get_keys()}")
    
    # Check which items remain in the cache
    all_keys = ['cities', 'timeseries', 'example_df', 'data_1', 'data_2', 'data_3', 'data_4', 'data_5']
    for key in all_keys:
        print(f"Is '{key}' in cache? {cache.contains(key)}")

    # Example 6: Cache context manager
    print("\n=== Example 6: Cache context manager ===")
    with ArrowCache(max_size_bytes=10 * 1024 * 1024) as temp_cache:
        temp_cache.put('temp_data', pd.DataFrame({'a': [1, 2, 3]}))
        print(f"Added temporary data to cache")
        print(f"Retrieved data: {temp_cache.get('temp_data')}")
    
    print("Cache was automatically closed after context exit")

    # Clean up resources
    print("\nCleaning up...")
    cache.close()
    print("Done!")

if __name__ == "__main__":
    main()
