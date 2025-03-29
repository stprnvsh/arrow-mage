"""
Benchmark Pipeline - Complete Simple Pipeline in Python

This script replicates the entire multi-language pipeline (Python -> R -> Julia -> Python)
in a single Python script to benchmark performance between cross-language zero-copy
approach vs. single-language approach.
"""
import pandas as pd
import numpy as np
import time
import os
import sys
import yaml
import re
import duckdb
import statistics
import uuid
import json
from pipelink.crosslink import CrossLink
import hashlib

def generate_data(cl, rows=1000):
    """
    Replicates the generate_data.py script functionality
    """
    print("\n==== GENERATE DATA (Python) ====")
    start_time = time.time()
    
    # Generate random data
    df = pd.DataFrame({
        'id': range(rows),
        'value': np.random.normal(0, 1, rows),
        'category': np.random.choice(['A', 'B', 'C'], size=rows)
    })
    
    # Use CrossLink's direct zero-copy push
    dataset_id = cl.push(
        df, 
        name='raw_data_python',
        description='Random data for demonstration with zero-copy capability',
        enable_zero_copy=True
    )
    
    end_time = time.time()
    print(f"Generated {rows} rows of data with ID {dataset_id}")
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    
    return dataset_id

def transform_data(cl, raw_data_id):
    """
    Replicates the transform_data.R script functionality in Python
    """
    print("\n==== TRANSFORM DATA (Python version of R script) ====")
    start_time = time.time()
    
    # Get data with zero-copy
    data = cl.pull(raw_data_id, zero_copy=True)
    print(f"Data received - dimensions: {len(data)} rows x {len(data.columns)} columns")
    print(f"Column names: {', '.join(data.columns)}")
    
    # Perform transformations (same as R script)
    transformed_data = data.copy()
    
    # Add new features
    transformed_data['abs_value'] = abs(transformed_data['value'])
    transformed_data['log_value'] = np.log(abs(transformed_data['value']) + 1)
    transformed_data['scaled_value'] = transformed_data['value'] / max(abs(transformed_data['value']))
    transformed_data['category_code'] = pd.Categorical(transformed_data['category']).codes
    
    # Save the transformed data
    dataset_id = cl.push(
        transformed_data, 
        name='transformed_data_python',
        description='Transformed data with additional features (Python version)',
        enable_zero_copy=True
    )
    
    end_time = time.time()
    print(f"Transformed data: added {len(transformed_data.columns) - len(data.columns)} new features")
    print(f"Dataset registered with ID: {dataset_id}")
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    
    return dataset_id

def analyze_data(cl, transformed_data_id):
    """
    Replicates the analyze_data.jl script functionality in Python
    """
    print("\n==== ANALYZE DATA (Python version of Julia script) ====")
    start_time = time.time()
    
    # Get transformed data using true zero-copy
    transformed_data = cl.pull(transformed_data_id, zero_copy=True)
    print(f"Loaded data via zero-copy: {len(transformed_data)} rows, columns: {', '.join(transformed_data.columns)}")
    
    # Basic statistics
    stats = {}
    
    # Summary statistics for numeric columns
    numeric_cols = transformed_data.select_dtypes(include=np.number).columns.tolist()
    
    for col in numeric_cols:
        if col != 'id':  # Skip id column
            col_stats = {
                "mean": transformed_data[col].mean(),
                "median": transformed_data[col].median(),
                "std": transformed_data[col].std(),
                "min": transformed_data[col].min(),
                "max": transformed_data[col].max()
            }
            
            stats[str(col)] = col_stats
    
    # Convert to a flat DataFrame for storage
    result_rows = []
    
    for key, value in stats.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                result_rows.append({"metric": f"{key}_{subkey}", "value": subvalue})
        else:
            result_rows.append({"metric": key, "value": value})
    
    result = pd.DataFrame(result_rows)
    print(f"Analysis complete with {len(result)} statistics calculated")
    
    # Save results 
    dataset_id = cl.push(
        result,
        name='analysis_results_python',
        description='Statistical analysis of the transformed data (Python version)',
        enable_zero_copy=True
    )
    
    end_time = time.time()
    print(f"Analysis results shared with ID: {dataset_id}")
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    
    return dataset_id

def create_report(cl, transformed_data_id, analysis_results_id):
    """
    Replicates the create_report.py script functionality
    """
    print("\n==== CREATE REPORT (Python) ====")
    start_time = time.time()
    
    # Get data with zero-copy
    transformed_data = cl.pull(transformed_data_id, zero_copy=True)
    analysis_results = cl.pull(analysis_results_id, zero_copy=True)
    
    print(f"Loaded transformed_data with {len(transformed_data)} rows and {len(transformed_data.columns)} columns")
    print(f"Loaded analysis_results with {len(analysis_results)} rows")
    
    # Create report
    report = pd.DataFrame({
        'metric': ['mean', 'std'],
        'value': [
            transformed_data['value'].mean(),
            transformed_data['value'].std(),
        ]
    })
    
    # Add some analysis results if available
    for _, row in analysis_results.iterrows():
        if 'value_mean' in row['metric']:
            report = pd.concat([report, pd.DataFrame({
                'metric': ['analysis_mean'],
                'value': [float(row['value'])]
            })], ignore_index=True)
    
    # Save result
    dataset_id = cl.push(
        report,
        name='final_report_python',
        description='Final report with summary metrics (Python version)',
        enable_zero_copy=True
    )
    
    end_time = time.time()
    print(f"Report created successfully with ID {dataset_id}")
    print(f"Time taken: {end_time - start_time:.4f} seconds")
    
    return dataset_id

def run_benchmark(db_path, rows=1000):
    """
    Run the complete pipeline benchmark
    """
    print(f"===== STARTING PYTHON-ONLY PIPELINE BENCHMARK =====")
    print(f"Using database: {db_path}")
    print(f"Data size: {rows} rows")
    
    total_start_time = time.time()
    
    # Initialize CrossLink
    cl = CrossLink(db_path)
    
    # Run pipeline steps
    raw_data_id = generate_data(cl, rows)
    transformed_data_id = transform_data(cl, raw_data_id)
    analysis_results_id = analyze_data(cl, transformed_data_id)
    report_id = create_report(cl, transformed_data_id, analysis_results_id)
    
    total_end_time = time.time()
    total_time = total_end_time - total_start_time
    
    print("\n===== PYTHON-ONLY PIPELINE COMPLETE =====")
    print(f"Total time: {total_time:.4f} seconds")
    
    return {
        "raw_data_id": raw_data_id,
        "transformed_data_id": transformed_data_id,
        "analysis_results_id": analysis_results_id,
        "report_id": report_id,
        "total_time": total_time
    }

def main():
    # Get arguments
    if len(sys.argv) < 2:
        print("Usage: python benchmark_pipeline.py <db_path> [rows]")
        print("Example: python benchmark_pipeline.py ../data/benchmark.duckdb 1000")
        sys.exit(1)
    
    db_path = sys.argv[1]
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 1000
    
    # Run benchmark
    results = run_benchmark(db_path, rows)
    
    # Save results to a file
    results_file = os.path.join(os.path.dirname(db_path), "benchmark_results.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    
    print(f"Benchmark results saved to {results_file}")

if __name__ == "__main__":
    main() 