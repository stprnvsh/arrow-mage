#!/usr/bin/env python
"""
Advanced features example for ArrowCache

This example demonstrates:
1. Configuration with custom settings
2. Automatic partitioning for large datasets
3. Memory management and spilling to disk
4. Optimized SQL queries with query plan caching
5. Parallel operations
6. Persistence to disk
"""

import os
import sys
import pandas as pd
import numpy as np
import time
import psutil
import concurrent.futures
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

# Add the parent directory to the path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from arrow_cache import (
    ArrowCache, 
    ArrowCacheConfig, 
    DEFAULT_CONFIG,
    parallel_map,
    explain_query
)

def print_section(title):
    """Print a section title"""
    print(f"\n{'=' * 80}")
    print(f"  {title}")
    print(f"{'=' * 80}")


def create_timeseries_data(rows=1_000_000, columns=5):
    """Create a large time series DataFrame"""
    print(f"Creating time series data with {rows:,} rows and {columns} columns...")
    
    # Create date range
    start_date = datetime(2020, 1, 1)
    dates = [start_date + timedelta(minutes=i) for i in range(rows)]
    
    # Create values
    data = {
        'timestamp': dates,
        'value': np.cumsum(np.random.normal(0, 1, rows)),
    }
    
    # Add additional columns
    for i in range(columns - 2):
        data[f'metric_{i}'] = np.random.normal(100, 15, rows)
    
    # Create DataFrame
    return pd.DataFrame(data)


def create_customer_data(customers=100_000, transactions_per_customer=10):
    """Create customer transaction data"""
    print(f"Creating customer data with {customers:,} customers...")
    
    # Create customer base data
    customer_data = []
    for i in range(customers):
        customer_data.append({
            'customer_id': i,
            'name': f"Customer {i}",
            'segment': np.random.choice(['A', 'B', 'C', 'D']),
            'region': np.random.choice(['North', 'South', 'East', 'West']),
            'signup_date': datetime(2020, 1, 1) + timedelta(days=np.random.randint(0, 365*2))
        })
    
    # Create transaction data
    transaction_data = []
    for customer in customer_data:
        for _ in range(np.random.randint(1, transactions_per_customer * 2)):
            days_since_signup = (datetime.now() - customer['signup_date']).days
            if days_since_signup <= 0:
                days_since_signup = 1
                
            transaction_data.append({
                'transaction_id': len(transaction_data),
                'customer_id': customer['customer_id'],
                'amount': np.random.lognormal(4, 1),  # Mean around $50
                'date': customer['signup_date'] + timedelta(days=np.random.randint(0, days_since_signup)),
                'product_category': np.random.choice(['Electronics', 'Clothing', 'Food', 'Books', 'Services']),
                'store_id': np.random.randint(1, 50)
            })
    
    return pd.DataFrame(customer_data), pd.DataFrame(transaction_data)


def monitor_memory_usage(interval=1.0, duration=60, plot=False):
    """Monitor memory usage over time"""
    memory_usage = []
    times = []
    
    start_time = time.time()
    process = psutil.Process(os.getpid())
    
    while time.time() - start_time < duration:
        memory_info = process.memory_info()
        memory_usage.append(memory_info.rss / (1024 * 1024))  # MB
        times.append(time.time() - start_time)
        time.sleep(interval)
    
    if plot:
        plt.figure(figsize=(10, 6))
        plt.plot(times, memory_usage)
        plt.xlabel('Time (seconds)')
        plt.ylabel('Memory Usage (MB)')
        plt.title('Memory Usage Over Time')
        plt.grid(True)
        plt.savefig('memory_usage.png')
        plt.close()
    
    return times, memory_usage


def parallel_query_test(cache, query, iterations=10):
    """Test parallel query execution"""
    # Function to run a query
    def run_query(i):
        start = time.time()
        result = cache.query(query)
        end = time.time()
        return {
            'iteration': i,
            'rows': len(result),
            'execution_time': end - start
        }
    
    # Run queries in parallel
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        results = list(executor.map(run_query, range(iterations)))
    
    return results


def main():
    # Create a custom configuration
    print("Creating custom configuration...")
    config = ArrowCacheConfig(
        memory_limit=500 * 1024 * 1024,  # 500MB
        partition_size_rows=100_000,      # 100K rows per partition
        partition_size_bytes=50 * 1024 * 1024,  # 50MB per partition
        spill_to_disk=True,
        thread_count=4,
        compression_type="lz4",
        enable_compression=True,
        dictionary_encoding=True,
        cache_query_plans=True
    )
    
    # Create cache with configuration
    print("Creating Arrow Cache with custom configuration...")
    cache = ArrowCache(config=config)
    
    # Create temporary directory for spilled partitions if it doesn't exist
    if not os.path.exists(config["spill_directory"]):
        os.makedirs(config["spill_directory"])
    
    # Example 1: Automatic partitioning of large datasets
    print_section("Example 1: Automatic Partitioning")
    
    # Create a large time series dataset
    timeseries_df = create_timeseries_data(rows=500_000, columns=5)
    
    # Add to cache with auto-partitioning
    start = time.time()
    cache.put('timeseries', timeseries_df)
    end = time.time()
    
    print(f"Added time series data to cache in {end-start:.2f} seconds")
    
    # Check cache status
    status = cache.status()
    print("\nCache Status:")
    print(f"- Entry count: {status['entry_count']}")
    print(f"- Partitioned tables: {status['partitioned_tables']}")
    print(f"- Current size: {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
    
    # Get metadata about the partitioned table
    metadata = cache.get_metadata('timeseries')
    print("\nTime Series Metadata:")
    print(f"- Partitioned: {metadata.get('metadata', {}).get('partitioned', False)}")
    print(f"- Partition count: {metadata.get('metadata', {}).get('partition_count', 0)}")
    print(f"- Total rows: {metadata.get('metadata', {}).get('total_rows', 0)}")
    
    # Example 2: Memory-efficient access with slicing
    print_section("Example 2: Memory-Efficient Access with Slicing")
    
    # Retrieve a slice of the data
    start = time.time()
    slice_df = cache.get('timeseries', offset=200_000, limit=1000)
    end = time.time()
    
    print(f"Retrieved 1,000 rows (starting at offset 200,000) in {end-start:.2f} seconds")
    print(f"Slice shape: {slice_df.shape}")
    print(slice_df.head(3))
    
    # Example 3: Query optimization
    print_section("Example 3: Query Optimization with Plan Caching")
    
    # Run an initial query
    query = """
    SELECT 
        DATE_TRUNC('day', timestamp) as day,
        AVG(value) as avg_value,
        MIN(value) as min_value,
        MAX(value) as max_value,
        COUNT(*) as count
    FROM _cache_timeseries
    GROUP BY day
    ORDER BY day
    """
    
    print("Running initial query...")
    start = time.time()
    result = cache.query(query)
    first_query_time = time.time() - start
    
    print(f"Initial query completed in {first_query_time:.2f} seconds")
    print(f"Result shape: {result.shape}")
    print(result.head(3))
    
    # Explain the query
    explanation = cache.explain(query)
    print("\nQuery Explanation:")
    print(explanation)
    
    # Run the query again to use the cached plan
    print("\nRunning the same query again (should use cached plan)...")
    start = time.time()
    result = cache.query(query)
    second_query_time = time.time() - start
    
    print(f"Second query completed in {second_query_time:.2f} seconds")
    print(f"Speed improvement: {first_query_time / second_query_time:.1f}x faster")
    
    # Check query cache stats
    status = cache.status()
    print("\nQuery Cache Statistics:")
    print(f"- Cache size: {status['query_cache']['size']}")
    print(f"- Hit count: {status['query_cache']['hit_count']}")
    print(f"- Avg execution time: {status['query_cache']['avg_execution_time']:.4f} seconds")
    
    # Example 4: Multiple large datasets and memory management
    print_section("Example 4: Multiple Large Datasets and Memory Management")
    
    # Create customer and transaction data
    customers_df, transactions_df = create_customer_data(customers=50_000, transactions_per_customer=5)
    
    print(f"Created customers data with shape: {customers_df.shape}")
    print(f"Created transactions data with shape: {transactions_df.shape}")
    
    # Start memory monitoring in a separate thread
    print("\nMonitoring memory usage during data loading...")
    memory_thread = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    memory_future = memory_thread.submit(monitor_memory_usage, 0.5, 30)
    
    # Add customer data to cache
    cache.put('customers', customers_df)
    print(f"Added customers data to cache")
    
    # Add transaction data to cache (this might trigger eviction or spilling)
    cache.put('transactions', transactions_df)
    print(f"Added transactions data to cache")
    
    # Check cache status
    status = cache.status()
    print("\nCache Status after adding all data:")
    print(f"- Entry count: {status['entry_count']}")
    print(f"- Partitioned tables: {status['partitioned_tables']}")
    print(f"- Current size: {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
    
    # Get memory information
    memory_info = status['memory']
    print("\nMemory Information:")
    print(f"- Allocated bytes: {memory_info['allocated_bytes'] / 1024 / 1024:.2f} MB")
    print(f"- System memory used: {memory_info['system_memory_used'] / 1024 / 1024:.2f} MB")
    print(f"- System memory available: {memory_info['system_memory_available'] / 1024 / 1024 / 1024:.2f} GB")
    
    # Example 5: Complex queries and parallel execution
    print_section("Example 5: Complex Queries and Parallel Execution")
    
    # Define a complex query joining multiple tables
    complex_query = """
    SELECT 
        c.segment,
        c.region,
        DATE_TRUNC('month', t.date) as month,
        t.product_category,
        COUNT(*) as transaction_count,
        SUM(t.amount) as total_amount,
        AVG(t.amount) as avg_amount
    FROM _cache_customers c
    JOIN _cache_transactions t ON c.customer_id = t.customer_id
    GROUP BY c.segment, c.region, month, t.product_category
    ORDER BY total_amount DESC
    LIMIT 20
    """
    
    print("Running complex query...")
    start = time.time()
    result = cache.query(complex_query)
    end = time.time()
    
    print(f"Complex query completed in {end-start:.2f} seconds")
    print(f"Result shape: {result.shape}")
    print(result.head(5))
    
    # Run parallel queries to test concurrency
    print("\nRunning parallel queries...")
    parallel_results = parallel_query_test(cache, complex_query, iterations=5)
    
    execution_times = [r['execution_time'] for r in parallel_results]
    print(f"Parallel query execution times: {[f'{t:.2f}s' for t in execution_times]}")
    print(f"Average execution time: {sum(execution_times) / len(execution_times):.2f} seconds")
    
    # Example 6: Persistence to disk and reload
    print_section("Example 6: Persistence to Disk and Reload")
    
    # Create storage directory if it doesn't exist
    storage_dir = os.path.join(os.getcwd(), ".arrow_cache_storage")
    if not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    
    # Persist the customers table to disk
    print(f"Persisting customers table to disk at {storage_dir}...")
    start = time.time()
    cache.persist('customers', storage_dir)
    end = time.time()
    
    print(f"Persistence completed in {end-start:.2f} seconds")
    
    # Clear the cache to simulate restarting the application
    print("\nClearing cache to simulate restart...")
    cache.clear()
    
    # Check cache status
    status = cache.status()
    print(f"Cache after clearing: {status['entry_count']} entries, {status['current_size_bytes'] / 1024 / 1024:.2f} MB")
    
    # Load the persisted table
    print("\nLoading persisted data from disk...")
    start = time.time()
    success = cache.load('customers', storage_dir)
    end = time.time()
    
    print(f"Load {'succeeded' if success else 'failed'} in {end-start:.2f} seconds")
    
    # Check that the data is available
    if success:
        df = cache.get('customers')
        print(f"Loaded customers data with shape: {df.shape}")
        print(df.head(3))
        
        # Run a query on the loaded data
        result = cache.query("SELECT segment, COUNT(*) as count FROM _cache_customers GROUP BY segment")
        print("\nQuery on loaded data:")
        print(result)
    
    # Wait for memory monitoring to complete
    times, memory_usage = memory_future.result()
    
    print("\nMemory monitoring results:")
    print(f"- Min memory: {min(memory_usage):.2f} MB")
    print(f"- Max memory: {max(memory_usage):.2f} MB")
    print(f"- Memory growth: {max(memory_usage) - min(memory_usage):.2f} MB")
    
    # Clean up resources
    print("\nCleaning up...")
    cache.close()
    print("Done!")


if __name__ == "__main__":
    main() 