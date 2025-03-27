#!/usr/bin/env python3
"""
Generate test data for DuckData tests.

This script creates various test datasets in different formats:
- CSV
- Parquet
- JSON
- DuckDB database
"""
import os
import sys
import pandas as pd
import numpy as np
import duckdb
from pathlib import Path

def generate_basic_dataset(output_dir, rows=1000):
    """Generate a basic dataset with simple data types."""
    np.random.seed(42)  # For reproducibility
    
    df = pd.DataFrame({
        'id': range(1, rows + 1),
        'value': np.random.normal(0, 1, rows),
        'integer': np.random.randint(-100, 100, rows),
        'category': np.random.choice(['A', 'B', 'C'], size=rows),
        'date': pd.date_range('2022-01-01', periods=rows)
    })
    
    # Save in different formats
    df.to_csv(os.path.join(output_dir, 'basic_dataset.csv'), index=False)
    df.to_parquet(os.path.join(output_dir, 'basic_dataset.parquet'), index=False)
    df.to_json(os.path.join(output_dir, 'basic_dataset.json'), orient='records')
    
    # Save to DuckDB
    conn = duckdb.connect(os.path.join(output_dir, 'basic_dataset.duckdb'))
    conn.execute("CREATE TABLE basic_data AS SELECT * FROM df")
    conn.close()
    
    return df

def generate_complex_dataset(output_dir, rows=500):
    """Generate a more complex dataset with various data types."""
    np.random.seed(43)  # For reproducibility
    
    # Create a complex dataframe with various data types
    df = pd.DataFrame({
        'id': range(1, rows + 1),
        'float_col': np.random.normal(0, 1, rows),
        'int_col': np.random.randint(-1000, 1000, rows),
        'bool_col': np.random.choice([True, False], rows),
        'cat_col': pd.Categorical(np.random.choice(['X', 'Y', 'Z'], rows)),
        'date_col': pd.date_range('2022-01-01', periods=rows),
        'timestamp_col': pd.date_range('2022-01-01', periods=rows, freq='H'),
        'str_col': [f"str_{i}" for i in range(rows)],
        'null_col': [None if i % 5 == 0 else i for i in range(rows)]
    })
    
    # Add some nested data structures
    df['list_col'] = [list(range(i % 5 + 1)) for i in range(rows)]
    df['dict_col'] = [{f'key_{j}': j for j in range(i % 3 + 1)} for i in range(rows)]
    
    # Save in different formats
    df.to_csv(os.path.join(output_dir, 'complex_dataset.csv'), index=False)
    
    # For Parquet, we need to remove the nested columns that aren't supported
    df_parquet = df.drop(columns=['list_col', 'dict_col'])
    df_parquet.to_parquet(os.path.join(output_dir, 'complex_dataset.parquet'), index=False)
    
    # For JSON, we can keep everything
    df.to_json(os.path.join(output_dir, 'complex_dataset.json'), orient='records')
    
    # Save to DuckDB (without nested columns)
    conn = duckdb.connect(os.path.join(output_dir, 'complex_dataset.duckdb'))
    conn.execute("CREATE TABLE complex_data AS SELECT * FROM df_parquet")
    conn.close()
    
    return df

def generate_time_series_dataset(output_dir, days=30, measurements_per_day=24):
    """Generate a time series dataset."""
    np.random.seed(44)  # For reproducibility
    
    total_rows = days * measurements_per_day
    
    # Create timestamps
    timestamps = pd.date_range('2022-01-01', periods=total_rows, freq='H')
    
    # Create multiple time series
    df = pd.DataFrame({
        'timestamp': timestamps,
        'series_a': np.sin(np.linspace(0, 15 * np.pi, total_rows)) + np.random.normal(0, 0.1, total_rows),
        'series_b': np.cos(np.linspace(0, 15 * np.pi, total_rows)) + np.random.normal(0, 0.1, total_rows),
        'series_c': np.random.normal(0, 1, total_rows).cumsum(),  # Random walk
        'event': np.random.choice(['normal', 'alert', 'critical'], total_rows, p=[0.8, 0.15, 0.05])
    })
    
    # Save in different formats
    df.to_csv(os.path.join(output_dir, 'time_series.csv'), index=False)
    df.to_parquet(os.path.join(output_dir, 'time_series.parquet'), index=False)
    df.to_json(os.path.join(output_dir, 'time_series.json'), orient='records')
    
    # Save to DuckDB
    conn = duckdb.connect(os.path.join(output_dir, 'time_series.duckdb'))
    conn.execute("CREATE TABLE time_series AS SELECT * FROM df")
    conn.close()
    
    return df

def main():
    """Generate all test datasets."""
    if len(sys.argv) > 1:
        output_dir = sys.argv[1]
    else:
        # Use default output directory
        output_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    print(f"Generating test datasets in {output_dir}...")
    
    # Generate datasets
    basic_df = generate_basic_dataset(output_dir)
    complex_df = generate_complex_dataset(output_dir)
    time_series_df = generate_time_series_dataset(output_dir)
    
    print(f"Generated {len(basic_df)} rows in basic dataset")
    print(f"Generated {len(complex_df)} rows in complex dataset")
    print(f"Generated {len(time_series_df)} rows in time series dataset")
    print("Done!")

if __name__ == "__main__":
    main() 