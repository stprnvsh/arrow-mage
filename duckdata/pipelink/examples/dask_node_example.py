#!/usr/bin/env python3
"""
Example node script using DaskNodeContext for parallel processing with Dask.
This example demonstrates how to use Dask with PipeLink for efficient data processing.
"""

import numpy as np
import pandas as pd

try:
    from pipelink.python.dask_context import DaskNodeContext
except ImportError:
    # Add parent directory to path for development
    import os
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from pipelink.python.dask_context import DaskNodeContext

# Define a processing function that will be applied to each partition in parallel
def process_partition(df):
    """
    Example processing function that will be applied to each Dask partition
    
    In a real-world scenario, this could be a complex data transformation,
    model prediction, or any other CPU-intensive operation.
    """
    # Simulate CPU-intensive work
    result = df.copy()
    
    # Add some new columns with computations
    result['squared'] = df['value'] ** 2
    result['sqrt'] = np.sqrt(df['value'])
    result['log'] = np.log1p(df['value'])
    
    # Add a column based on a complex calculation
    result['complex_calc'] = np.sin(df['value']) + np.cos(df['value'] * 2) + np.tan(df['value'] / 3)
    
    # Simulate more work with a sleep (uncomment for testing)
    # import time
    # time.sleep(0.1)  # Simulate 100ms of processing per chunk
    
    return result

def main():
    """Main function to demonstrate Dask integration with PipeLink"""
    # Initialize DaskNodeContext - will automatically set up a Dask cluster
    with DaskNodeContext() as ctx:
        # Get parameters from metadata
        input_name = ctx.get_param("input_dataset", "large_dataset")
        output_name = ctx.get_param("output_dataset", "processed_dataset")
        
        # Get input as a Dask DataFrame - enables parallel processing
        ddf = ctx.get_dask_input(input_name)
        
        # Print information about the Dask DataFrame
        print(f"Processing Dask DataFrame with {ddf.npartitions} partitions")
        print(f"Column names: {ddf.columns.tolist()}")
        print(f"Types: {ddf.dtypes}")
        
        # Apply our processing function to each partition in parallel
        result_ddf = ddf.map_partitions(process_partition, meta=ddf.dtypes)
        
        # Example of other common Dask operations:
        
        # 1. Filter rows (lazy evaluation)
        filtered_ddf = result_ddf[result_ddf['value'] > 50]
        
        # 2. Group and aggregate
        agg_df = filtered_ddf.groupby('category').agg({
            'value': ['mean', 'min', 'max', 'count'],
            'squared': 'mean', 
            'sqrt': 'mean',
            'complex_calc': 'mean'
        }).compute()  # This triggers computation
        
        # Print summary statistics
        print("\nSummary statistics by category:")
        print(agg_df)
        
        # 3. Continue with parallel processing
        final_ddf = filtered_ddf.assign(
            normalized=(filtered_ddf['value'] - filtered_ddf['value'].mean()) / filtered_ddf['value'].std()
        )
        
        # Save output using the DaskNodeContext helper
        # This will trigger computation and efficiently save results
        ctx.save_dask_output(output_name, final_ddf, 
                           description="Parallel processed data with Dask")
        
        print(f"\nProcessing complete. Results saved to '{output_name}'")
        print(f"Check the Dask dashboard for performance visualization.")

if __name__ == "__main__":
    # If this node is run manually, create a test dataset for it
    if len(sys.argv) <= 1:
        print("No metadata file provided, creating test data...")
        
        # Create a large test dataset (5 million rows)
        num_rows = 5_000_000
        categories = ['A', 'B', 'C', 'D', 'E']
        
        # Generate test data in chunks to avoid memory issues
        chunks = []
        chunk_size = 500_000
        
        for i in range(0, num_rows, chunk_size):
            chunk = pd.DataFrame({
                'id': range(i, min(i + chunk_size, num_rows)),
                'value': np.random.rand(min(chunk_size, num_rows - i)) * 100,
                'category': np.random.choice(categories, min(chunk_size, num_rows - i))
            })
            chunks.append(chunk)
        
        test_df = pd.concat(chunks)
        
        # Save test data to DuckDB
        import duckdb
        
        conn = duckdb.connect("test_pipeline.duckdb")
        conn.execute("CREATE OR REPLACE TABLE large_dataset AS SELECT * FROM test_df")
        conn.close()
        
        # Create a simple metadata file for testing
        import yaml
        
        metadata = {
            "node_id": "dask_processing",
            "inputs": ["large_dataset"],
            "outputs": ["processed_dataset"],
            "params": {
                "input_dataset": "large_dataset",
                "output_dataset": "processed_dataset"
            },
            "db_path": "test_pipeline.duckdb"
        }
        
        with open("test_meta.yaml", "w") as f:
            yaml.dump(metadata, f)
        
        print("Test data created. Running with test metadata...")
        sys.argv.append("test_meta.yaml")
    
    # Run the main function
    main() 