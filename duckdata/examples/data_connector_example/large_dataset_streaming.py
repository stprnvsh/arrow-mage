#!/usr/bin/env python3
"""
Example demonstrating efficient data transfer and disk spilling for large datasets

This example shows how to use the optimized CrossLink functionality to:
1. Create and handle large datasets that exceed memory
2. Stream data in chunks to avoid memory issues
3. Monitor resource usage during operations
"""
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pipelink.crosslink import CrossLink

def generate_large_dataset(rows=10_000_000, columns=20):
    """Generate a large synthetic dataset"""
    print(f"Generating dataset with {rows:,} rows and {columns} columns...")
    
    # Start with an ID column and timestamp
    df = pd.DataFrame({
        'id': range(rows),
        'timestamp': pd.date_range(start='2023-01-01', periods=rows, freq='S')
    })
    
    # Add numeric columns
    for i in range(columns - 2):
        col_name = f'value_{i}'
        # Generate different patterns of data
        if i % 3 == 0:
            # Normal distribution
            df[col_name] = np.random.normal(0, 1, rows)
        elif i % 3 == 1:
            # Exponential distribution
            df[col_name] = np.random.exponential(1, rows)
        else:
            # Uniform distribution
            df[col_name] = np.random.uniform(-1, 1, rows)
    
    print(f"Dataset generated. Memory usage: {df.memory_usage(deep=True).sum() / (1024**3):.2f} GB")
    return df

def main():
    """Demonstrate large dataset handling with CrossLink"""
    # Create a CrossLink instance with a persistent database
    db_path = os.path.join(os.path.dirname(__file__), "large_data_example.duckdb")
    cl = CrossLink(db_path)
    
    try:
        # Enable resource monitoring
        cl.monitor_resources(True)
        print("Resource monitoring enabled. Press Ctrl+C to stop.")
        
        # Step 1: Generate a large dataset
        # For testing, we'll use a smaller dataset by default
        # For a true large dataset test, increase the number of rows
        rows = 500_000  # Increase to 10M+ for a real large dataset test
        cols = 20
        
        large_df = generate_large_dataset(rows=rows, columns=cols)
        
        print("\nDataset Info:")
        print(f"Shape: {large_df.shape}")
        print(f"Memory usage: {large_df.memory_usage(deep=True).sum() / (1024**3):.2f} GB")
        print(f"First few rows:")
        print(large_df.head())
        
        # Step 2: Push the large dataset to CrossLink with chunking
        print("\nPushing large dataset to CrossLink with chunking...")
        dataset_id = cl.push(
            large_df,
            name="large_dataset",
            description="Large synthetic dataset for testing",
            use_arrow=True,
            chunk_size=100_000,  # Process in chunks of 100k rows
            use_disk_spilling=True  # Allow DuckDB to spill to disk if needed
        )
        print(f"Dataset stored with ID: {dataset_id}")
        
        # Step 3: Retrieve the dataset with streaming to process in chunks
        print("\nRetrieving dataset with streaming...")
        chunk_count = 0
        row_count = 0
        
        # Process the data in chunks without loading it all into memory
        for chunk in cl.pull("large_dataset", stream=True, chunk_size=50_000):
            chunk_count += 1
            row_count += len(chunk)
            
            # Do some processing on each chunk
            chunk_mean = chunk.select_dtypes(include=['number']).mean()
            
            # Just print progress every few chunks
            if chunk_count % 5 == 0:
                print(f"Processed chunk {chunk_count}, total rows: {row_count:,}")
                print(f"Chunk mean for numeric columns: min={chunk_mean.min():.4f}, max={chunk_mean.max():.4f}")
        
        print(f"\nFinished processing {chunk_count} chunks, total rows: {row_count:,}")
        
        # Step 4: Get resource usage statistics
        stats = cl.get_resource_stats()
        if stats:
            time_series = stats['time_series']
            summary = stats['summary']
            
            print("\nResource usage summary:")
            print(summary)
            
            # Plot memory usage if matplotlib is available
            try:
                plt.figure(figsize=(10, 6))
                plt.plot(time_series['timestamp'], time_series['memory_mb'], label='Memory (MB)')
                plt.title('Memory Usage During CrossLink Operations')
                plt.xlabel('Time')
                plt.ylabel('Memory (MB)')
                plt.legend()
                plt.grid(True)
                
                plot_file = "memory_usage.png"
                plt.savefig(plot_file)
                print(f"Memory usage plot saved to {plot_file}")
                plt.close()
            except Exception as e:
                print(f"Could not create plot: {e}")
            
            # Also check DuckDB stats
            if 'duckdb_stats' in stats and stats['duckdb_stats'] is not None:
                print("\nDuckDB database stats:")
                print(stats['duckdb_stats'])
        
        # Step 5: Execute a query that processes the large dataset 
        # but outputs a small result using DuckDB's SQL capabilities
        print("\nRunning aggregation query on large dataset...")
        query = """
        SELECT 
            date_trunc('hour', timestamp) as hour,
            COUNT(*) as count,
            AVG(value_0) as avg_value_0,
            AVG(value_1) as avg_value_1,
            AVG(value_2) as avg_value_2
        FROM large_dataset
        GROUP BY hour
        ORDER BY hour
        """
        
        # Use DuckDB to execute the query directly
        result = cl.conn.execute(query).fetchdf()
        print(f"Query result shape: {result.shape}")
        print(result.head())
        
    finally:
        # Disable resource monitoring
        cl.monitor_resources(False)
        cl.close()
        print("\nCrossLink connection closed.")

if __name__ == "__main__":
    main() 