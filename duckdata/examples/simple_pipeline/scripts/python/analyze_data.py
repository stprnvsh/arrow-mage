"""
Analyze data for the PipeLink example pipeline (Python version).

This is the Python equivalent of analyze_data.jl for performance comparison.
It analyzes the transformed data using CrossLink for zero-copy data sharing.
"""

import os
import sys
import json
import time
import numpy as np
import pandas as pd
import re
from datetime import datetime

# Import CrossLink (make sure it's in the PYTHONPATH)
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), 
                            "../../../../pipelink"))
from crosslink.python.crosslink import CrossLink

print("Starting analyze_data.py")
start_time = time.time()

def analyze_data(data, detailed=False):
    """
    Perform statistical analysis on the input data.
    
    Args:
        data: Pandas DataFrame to analyze
        detailed: Whether to perform detailed analysis
        
    Returns:
        DataFrame with analysis results
    """
    # Basic statistics
    stats = {}
    
    # Get numeric columns
    numeric_cols = data.select_dtypes(include=['number']).columns.tolist()
    
    for col in numeric_cols:
        if col != 'id':  # Skip id column
            col_stats = {
                'mean': data[col].mean(),
                'median': data[col].median(),
                'std': data[col].std(),
                'min': data[col].min(),
                'max': data[col].max()
            }
            
            stats[str(col)] = col_stats
    
    # Convert to a flat DataFrame for storage
    result_rows = []
    
    for key, value in stats.items():
        if isinstance(value, dict):
            for subkey, subvalue in value.items():
                result_rows.append({
                    'metric': f"{key}_{subkey}", 
                    'value': subvalue
                })
        else:
            result_rows.append({
                'metric': key, 
                'value': value
            })
    
    return pd.DataFrame(result_rows)

def main():
    # Check command line arguments
    if len(sys.argv) < 2:
        raise ValueError("No metadata file provided. This script should be run by PipeLink.")
    
    # Load metadata from file
    meta_path = sys.argv[1]
    print(f"Using metadata file: {meta_path}")
    
    # Parse the metadata file to extract db_path
    with open(meta_path, 'r') as f:
        meta_content = f.read()
    
    db_path_match = re.search(r'db_path:\s*(.+)', meta_content)
    if not db_path_match:
        raise ValueError("Could not find db_path in metadata file")
    
    db_path = db_path_match.group(1).strip()
    print(f"Using db_path: {db_path}")
    
    # Initialize CrossLink with the database
    print("Initializing CrossLink with database...")
    crosslink_init_start = time.time()
    cl = CrossLink(db_path)
    crosslink_init_time = time.time() - crosslink_init_start
    print(f"CrossLink initialized in {crosslink_init_time:.6f} seconds")
    
    # Get transformed data using zero-copy
    print("Getting transformed data via CrossLink (zero-copy)...")
    pull_start = time.time()
    transformed_data = cl.pull("transformed_data", zero_copy=True)
    pull_time = time.time() - pull_start
    print(f"Loaded data via zero-copy in {pull_time:.6f} seconds: {len(transformed_data)} rows, columns: {', '.join(transformed_data.columns)}")
    
    # Analyze the data
    print("Analyzing data...")
    analysis_start = time.time()
    result = analyze_data(transformed_data)
    analysis_time = time.time() - analysis_start
    print(f"Analysis complete with {len(result)} statistics calculated in {analysis_time:.6f} seconds")
    
    # Save results using CrossLink's push method
    print("Saving analysis results with CrossLink...")
    push_start = time.time()
    dataset_id = cl.push(
        result, 
        "analysis_results",
        description="Statistical analysis of the transformed data (Python version)",
        use_arrow=True
    )
    push_time = time.time() - push_start
    print(f"Analysis results shared via CrossLink with ID: {dataset_id} in {push_time:.6f} seconds")
    
    # Total time
    total_time = time.time() - start_time
    print(f"Total execution time: {total_time:.6f} seconds")
    
    # Write timing information to a file
    timing_info = {
        "language": "python",
        "crosslink_init_time": crosslink_init_time,
        "data_pull_time": pull_time,
        "analysis_time": analysis_time,
        "data_push_time": push_time,
        "total_time": total_time,
        "rows_processed": len(transformed_data),
        "columns_processed": len(transformed_data.columns),
        "timestamp": datetime.now().isoformat()
    }
    
    with open("python_timing.json", "w") as f:
        json.dump(timing_info, f, indent=2)

if __name__ == "__main__":
    main()
    print("analyze_data.py completed") 