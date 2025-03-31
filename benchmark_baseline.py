#!/usr/bin/env python3
import time
import pandas as pd
import numpy as np
import os
import tempfile
import subprocess

# Configuration
DATA_SIZE = 100_000_000  # Keep the large size
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

def process_data_r(input_file, output_file):
    """Call R script to process data passed via CSV"""
    print("Processing data in R (reading/writing CSV)...")
    start_time = time.time()

    # Create temporary R script
    with tempfile.NamedTemporaryFile(suffix='.R', mode='w', delete=False) as r_script:
        r_script.write(f"""
        # Check if dplyr is installed
        if (!requireNamespace("dplyr", quietly = TRUE)) {{
          stop("Error: 'dplyr' package not installed.") # Benchmark requires dplyr
        }}
        library(dplyr)

        # Get input and output files
        input_file <- "{input_file}"
        output_file <- "{output_file}"

        # Read data using base R read.csv
        cat(paste("Reading CSV:", input_file, "\n"))
        tryCatch({{ 
            df <- read.csv(input_file)
        }}, error = function(e) {{
             stop(paste("Failed to read CSV '{input_file}':", e$message))
        }})
        cat("CSV read successfully.\n")

        # Process data using dplyr
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

        # Write results using base R write.csv
        cat(paste("Writing CSV:", output_file, "\n"))
        tryCatch({{ 
             write.csv(result, output_file, row.names = FALSE)
        }}, error = function(e) {{
             # Warning instead of stop, as this write isn't timed
             warning(paste("Failed to write CSV '{output_file}':", e$message))
        }})
        
        # Return success
        cat("R processing completed\n")
        """)
        r_script_path = r_script.name

    # Run R script
    cmd = ["Rscript", r_script_path]

    try:
        # Increased timeout for potentially slower CSV I/O
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=600) 
        print(result.stdout)
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

def run_sos_pipeline():
    """Run a complete Script of Scripts (SoS) pipeline using CSV for intermediate file"""
    temp_dir = tempfile.mkdtemp()
    try:
        # Keep initial data generation quick (still using Pandas in memory)
        python_output_file = os.path.join(temp_dir, "python_output.csv") # Intermediate is CSV
        r_output_file = os.path.join(temp_dir, "r_output.csv") # Final output is CSV

        # Create input data (remains in memory for Python)
        df = create_synthetic_data(DATA_SIZE)

        # Process in Python
        python_result, agg_result, python_time = process_data_python(df)

        # Save Python results as CSV for R
        print(f"Writing intermediate CSV: {python_output_file}")
        write_start = time.time()
        python_result.to_csv(python_output_file, index=False)
        write_time = time.time() - write_start
        print(f"Intermediate CSV written in {write_time:.4f} seconds")
        
        # Process in R (will read the CSV)
        r_time = process_data_r(python_output_file, r_output_file)

        # Calculate total pipeline time (including Python write + R read time)
        # Note: r_time includes the R read.csv time.
        # We add the Python write_time separately.
        total_time = python_time + write_time + (r_time if r_time else 0)

        return {
            'python_time': python_time,
            'r_time': r_time if r_time else 0,
            'total_time': total_time
        }
    finally:
        # Clean up temp files
        print(f"Cleaning up temp directory: {temp_dir}")
        for file in [python_output_file, r_output_file]:
            if os.path.exists(file):
                try:
                    os.unlink(file)
                except OSError as e:
                    print(f"Error removing file {file}: {e}")
        try:
            os.rmdir(temp_dir)
        except OSError as e:
            print(f"Error removing directory {temp_dir}: {e}")

def main():
    print("============================================")
    print("Running SoS Pipeline Benchmark (Baseline)")
    print("============================================")
    
    results = []
    for i in range(NUM_RUNS):
        print(f"\nRun {i+1}/{NUM_RUNS}")
        result = run_sos_pipeline()
        results.append(result)
    
    # Calculate average times
    avg_python = sum(r['python_time'] for r in results) / NUM_RUNS
    avg_r = sum(r['r_time'] for r in results if r['r_time']) / NUM_RUNS
    avg_total = sum(r['total_time'] for r in results) / NUM_RUNS
    
    print("\n============================================")
    print("Benchmark Results (Baseline)")
    print("============================================")
    print(f"Average Python processing time: {avg_python:.4f} seconds")
    print(f"Average R processing time: {avg_r:.4f} seconds")
    print(f"Average total pipeline time: {avg_total:.4f} seconds")
    print("============================================")

if __name__ == "__main__":
    main() 