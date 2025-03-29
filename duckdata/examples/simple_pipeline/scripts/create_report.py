"""
Create final report for the PipeLink example pipeline.

This is the last node in the pipeline that creates a report using data from previous nodes
with zero-copy data access.
"""
from pipelink.crosslink import CrossLink
import pandas as pd
import numpy as np
import re
import duckdb

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
    db_path_match = re.search(r'db_path:\s*(.+)', meta_content)
    db_path = db_path_match.group(1).strip()
    print(f"Using db_path: {db_path}")
    
    # Initialize CrossLink with direct database access
    cl = CrossLink(db_path)
    
    # Get direct table references for true zero-copy
    transformed_table_ref = cl.get_table_reference("transformed_data")
    analysis_table_ref = cl.get_table_reference("analysis_results")
    
    print(f"Got direct reference to transformed_data table: {transformed_table_ref['table_name']}")
    print(f"Got direct reference to analysis_results table: {analysis_table_ref['table_name']}")
    
    # Connect to the database directly for zero-copy access
    conn = duckdb.connect(db_path)
    
    # Access data with direct SQL queries (zero-copy)
    transformed_data = conn.execute(f"SELECT * FROM {transformed_table_ref['table_name']}").fetchdf()
    analysis_results = conn.execute(f"SELECT * FROM {analysis_table_ref['table_name']}").fetchdf()
    
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
        if 'value_mean' in row['statistic']:
            report = pd.concat([report, pd.DataFrame({
                'metric': ['analysis_mean'],
                'value': [float(row['value'])]
            })], ignore_index=True)
    
    # Save output using zero-copy directly 
    result_table_name = "final_report_table"
    
    # Register the DataFrame with DuckDB
    conn.register("temp_report", report)
    conn.execute(f"CREATE OR REPLACE TABLE {result_table_name} AS SELECT * FROM temp_report")
    
    # Register the table with CrossLink for zero-copy
    dataset_id = cl.register_external_table(
        table_name=result_table_name,
        data=None,  # Not needed since table already exists in DuckDB
        name="final_report",
        description="Final report with summary metrics (zero-copy)"
    )
    
    print(f"Report created successfully with ID {dataset_id} using zero-copy access")

if __name__ == "__main__":
    main() 