"""
Generate data for the PipeLink example pipeline.

This is the first node in the pipeline that generates random data using zero-copy data sharing.
"""
from pipelink.crosslink import CrossLink
import pandas as pd
import numpy as np
import os
import yaml

def main():
    # Get the metadata file path from command line args
    import sys
    if len(sys.argv) < 2:
        raise ValueError("No metadata file provided. This script should be run by PipeLink.")
    
    # Read metadata file
    meta_path = sys.argv[1]
    print(f"Using metadata file: {meta_path}")
    
    # Extract database path from metadata
    with open(meta_path, 'r') as f:
        meta_content = f.read()
    
    # Parse the db_path directly
    import re
    db_path_match = re.search(r'db_path:\s*(.+)', meta_content)
    db_path = db_path_match.group(1).strip()
    print(f"Using db_path: {db_path}")
    
    # Initialize CrossLink with direct database access
    cl = CrossLink(db_path)
    
    # Generate random data
    rows = 100  # Could be parametrized
    df = pd.DataFrame({
        'id': range(rows),
        'value': np.random.normal(0, 1, rows),
        'category': np.random.choice(['A', 'B', 'C'], size=rows)
    })
    
    # Use CrossLink's push (the C++ bindings handle zero-copy automatically)
    dataset_id = cl.push(
        df, 
        name='raw_data',
        description='Random data for demonstration with zero-copy capability'
    )
    
    print(f"Generated {rows} rows of data with ID {dataset_id} using zero-copy")

if __name__ == "__main__":
    main() 