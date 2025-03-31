#!/usr/bin/env python3
import time
import pandas as pd
import numpy as np
import os
import tempfile
import subprocess
import pyarrow as pa
import pyarrow.parquet as pq
from duckdata.crosslink.python.crosslink import CrossLink

# Configuration
DATA_SIZE = 100_000_000  # Increased data size (ten million rows)
NUM_RUNS = 1  # Number of benchmark runs

def create_synthetic_data(size):
    """Create a synthetic dataset with random values"""
    print(f"Creating synthetic dataset with {size} rows...")
    data = {
        'id': np.arange(size),
        'value1': np.random.normal(0, 1, size),
        'value2': np.random.uniform(-100, 100, size),
        'category': np.random.choice(['A', 'B', 'C', 'D'], size),
        'timestamp': pd.date_range(start='2023-01-01', periods=size, freq='s')
    }
    return pd.DataFrame(data)

def process_data_python(df):
    """Perform some processing in Python"""
    print("Processing data in Python...")
    start_time = time.time()
    
    # Perform some calculations
    result_df = df.copy()
    result_df['value1_squared'] = df['value1'] ** 2
    result_df['value2_normalized'] = (df['value2'] - df['value2'].min()) / (df['value2'].max() - df['value2'].min())
    
    # Group and aggregate
    agg_result = result_df.groupby('category').agg({
        'value1': ['mean', 'std'],
        'value2': ['min', 'max', 'mean'],
        'value1_squared': 'sum'
    }).reset_index()
    
    duration = time.time() - start_time
    print(f"Python processing completed in {duration:.4f} seconds")
    
    return result_df, agg_result, duration

def process_data_r(dataset_id, output_dataset_id):
    """Call R script to process data via CrossLink"""
    print("Processing data in R...")
    start_time = time.time()

    # Create temporary R script with corrected CrossLink calls
    with tempfile.NamedTemporaryFile(suffix='.R', mode='w', delete=False) as r_script:
        r_script.write(f"""
        # Load required libraries
        library(CrossLink) # Correct package name
        if (!requireNamespace("dplyr", quietly = TRUE)) {{
          stop("Error: 'dplyr' package not installed.") # Benchmark requires dplyr
        }}
        library(dplyr)

        # Get CrossLink instance
        # Make sure the db_path matches the one used in Python
        cl <- CrossLink::crosslink_connect(db_path = "crosslink_benchmark.duckdb") 

        # Get data from CrossLink
        # Use tryCatch for better error handling if pull fails
        df_arrow <- tryCatch({{
             CrossLink::crosslink_pull(cl, "{dataset_id}") 
        }}, error = function(e) {{
             stop(paste("Failed to pull dataset '{dataset_id}':", e$message))
        }})
        
        # Convert Arrow Table to data.frame for dplyr (if necessary, depends on dplyr version)
        # For newer dplyr/arrow, direct operations might work, but converting is safer for compatibility
        if (!inherits(df_arrow, "data.frame")) {{
           if (requireNamespace("arrow", quietly = TRUE)) {{
               df <- as.data.frame(df_arrow)
           }} else {{
               stop("Arrow package needed to convert pulled object to data.frame")
           }}
        }} else {{
           df <- df_arrow # If pull already returned data.frame (less likely)
        }}

        # Process data
        result <- df %>%
          mutate(
            value1_cubed = value1^3,
            value2_log = log(abs(value2) + 1),
            flag = ifelse(value1 > 0 & value2 > 0, "positive", "other")
          )

        # Calculate statistics
        stats <- result %>%
          group_by(flag) %>%
          summarize(
            count = n(),
            avg_value1 = mean(value1),
            avg_value2 = mean(value2),
            sum_cubed = sum(value1_cubed)
          )

        # Convert result back to Arrow Table for pushing
        if (requireNamespace("arrow", quietly = TRUE)) {{
            result_arrow <- arrow::arrow_table(result)
        }} else {{
            stop("Arrow package needed to convert result back to Arrow Table")
        }}
        
        # Push results back to CrossLink
        CrossLink::crosslink_push(cl, result_arrow, name = "{output_dataset_id}", description = "R processing results") 

        # Cleanup connection (optional, depends if Python side does it)
        # CrossLink::crosslink_cleanup(cl) 

        # Return success
        cat("R processing completed via CrossLink\n")
        """)
        r_script_path = r_script.name

    # Run R script
    cmd = ["Rscript", r_script_path]

    try:
        # Increased timeout in case processing takes time
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=300) 
        print(result.stdout) # Print R script output
        duration = time.time() - start_time
        print(f"R processing completed in {duration:.4f} seconds")
        return duration
    except subprocess.TimeoutExpired:
        print("Error: R script timed out.")
        return None
    except subprocess.CalledProcessError as e:
        print(f"Error in R processing STDOUT: {e.stdout}")
        print(f"Error in R processing STDERR: {e.stderr}")
        return None
    finally:
        if os.path.exists(r_script_path):
           os.unlink(r_script_path)

def run_sos_pipeline_crosslink():
    """Run a Script of Scripts (SoS) pipeline, using CrossLink ONLY for Python->R transfer."""
    cl = None
    try:
        # Create a CrossLink instance
        cl = CrossLink(db_path="crosslink_benchmark.duckdb")
        
        # --- Python Part (In-Memory) ---
        # Create input data directly in memory
        df = create_synthetic_data(DATA_SIZE)
        
        # Process in Python (in memory)
        python_result, agg_result, python_time = process_data_python(df)
        # --- End Python Part ---
        
        # --- Python -> R Transfer via CrossLink ---
        python_output_id = "python_output_for_r"
        r_output_id = "r_final_output"
        
        print("Pushing Python results to CrossLink for R...")
        py_push_start = time.time()
        py_table = pa.Table.from_pandas(python_result)
        # Ensure this push doesn't keep the connection locked unnecessarily
        # Depending on CrossLink implementation, might need explicit close/reopen or context manager
        cl.push(py_table, name=python_output_id, description="Python results for R")
        py_push_time = time.time() - py_push_start
        print(f"Python results pushed to CrossLink in {py_push_time:.4f} seconds")
        # --- End Python -> R Transfer ---

        # --- Explicitly cleanup Python CrossLink connection BEFORE calling R ---
        print("Cleaning up Python CrossLink connection before calling R...")
        if hasattr(cl, 'cleanup'):
            cl.cleanup()
        else:
            print("Warning: Python CrossLink object has no 'cleanup' method.")
        print("Python CrossLink connection cleaned up.")
        cl = None
        # --- End cleanup ---

        # --- R Processing Part (Called Externally) ---
        # R script will connect, pull python_output_id, process, push r_output_id
        r_time = process_data_r(python_output_id, r_output_id)
        # --- End R Processing Part ---

        # --- R -> Python Transfer via CrossLink ---
        print("Re-establishing Python CrossLink connection to pull R results...")
        cl = CrossLink(db_path="crosslink_benchmark.duckdb")
        
        print("Pulling final R results from CrossLink...")
        r_pull_start = time.time()
        r_result = None
        try:
            pulled_r_object = cl.pull(r_output_id)
            if pulled_r_object is None:
                 print(f"Error: cl.pull('{r_output_id}') returned None")
            else:
                 # We don't necessarily need to convert/use it, just measure the pull time
                 r_result = pulled_r_object 
                 print(f"Pulled final R result (type: {type(r_result)})")
        except Exception as e:
            print(f"Error during final pull for '{r_output_id}': {e}")
            import traceback
            traceback.print_exc() # Print full traceback
            # Allow benchmark to continue
            
        r_pull_time = time.time() - r_pull_start
        print(f"Final R results pulled (or attempt finished) from CrossLink in {r_pull_time:.4f} seconds")
        # --- End R -> Python Transfer ---
        
        # --- Calculate Times ---
        data_transfer_time = py_push_time + r_pull_time # Time spent by Python pushing/pulling
        # Note: r_time includes R's internal pull/push + processing
        total_time = python_time + (r_time if r_time else 0) + data_transfer_time
        
        return {
            'python_time': python_time,
            'r_time': r_time if r_time else 0,
            'data_transfer_time': data_transfer_time,
            'total_time': total_time
        }
    finally:
        # Final cleanup if cl instance still exists
        if cl is not None and hasattr(cl, 'cleanup'):
            print("Performing final cleanup of Python CrossLink instance...")
            cl.cleanup()

def main():
    print("============================================")
    print("Running SoS Pipeline Benchmark (CrossLink)")
    print("============================================")
    
    results = []
    for i in range(NUM_RUNS):
        print(f"\nRun {i+1}/{NUM_RUNS}")
        result = run_sos_pipeline_crosslink()
        results.append(result)
    
    # Calculate average times
    avg_python = sum(r['python_time'] for r in results) / NUM_RUNS
    avg_r = sum(r['r_time'] for r in results if r['r_time']) / NUM_RUNS
    avg_transfer = sum(r['data_transfer_time'] for r in results) / NUM_RUNS
    avg_total = sum(r['total_time'] for r in results) / NUM_RUNS
    
    print("\n============================================")
    print("Benchmark Results (CrossLink)")
    print("============================================")
    print(f"Average Python processing time: {avg_python:.4f} seconds")
    print(f"Average R processing time: {avg_r:.4f} seconds")
    print(f"Average data transfer time: {avg_transfer:.4f} seconds")
    print(f"Average total pipeline time: {avg_total:.4f} seconds")
    print("============================================")

if __name__ == "__main__":
    main() 