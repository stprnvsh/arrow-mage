"""
Benchmark Runner for Simple Pipeline

This script runs both the cross-language pipeline and the Python-only version
to benchmark performance and compare execution times.
"""
import subprocess
import time
import os
import json
import sys
import datetime
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

def ensure_dir(directory):
    """Make sure directory exists"""
    Path(directory).mkdir(parents=True, exist_ok=True)

def run_cross_language_pipeline(pipeline_script, db_path, rows=100):
    """Run the cross-language pipeline"""
    print("\n======== RUNNING CROSS-LANGUAGE PIPELINE ========")
    start_time = time.time()
    
    # Create metadata file
    metadata_file = os.path.join(os.path.dirname(db_path), "metadata.yaml")
    with open(metadata_file, "w") as f:
        f.write(f"db_path: {db_path}\nrows: {rows}\n")
    
    # Run pipeline script
    result = subprocess.run(
        [sys.executable, pipeline_script, metadata_file],
        capture_output=True,
        text=True
    )
    
    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"Cross-language pipeline completed in {total_time:.4f} seconds")
    
    # Check for errors
    if result.returncode != 0:
        print(f"ERROR: Pipeline failed with code {result.returncode}")
        print(f"Error output: {result.stderr}")
    
    return {
        "success": result.returncode == 0,
        "output": result.stdout,
        "error": result.stderr,
        "total_time": total_time
    }

def run_python_pipeline(benchmark_script, db_path, rows=100):
    """Run the Python-only pipeline"""
    print("\n======== RUNNING PYTHON-ONLY PIPELINE ========")
    
    # Run benchmark script
    result = subprocess.run(
        [sys.executable, benchmark_script, db_path, str(rows)],
        capture_output=True,
        text=True
    )
    
    # Get the results from JSON file
    results_file = os.path.join(os.path.dirname(db_path), "benchmark_results.json")
    if os.path.exists(results_file):
        with open(results_file, "r") as f:
            benchmark_results = json.load(f)
    else:
        benchmark_results = {"total_time": -1}
    
    print(f"Python-only pipeline completed in {benchmark_results.get('total_time', -1):.4f} seconds")
    
    # Check for errors
    if result.returncode != 0:
        print(f"ERROR: Benchmark failed with code {result.returncode}")
        print(f"Error output: {result.stderr}")
    
    return {
        "success": result.returncode == 0,
        "output": result.stdout,
        "error": result.stderr,
        "results": benchmark_results
    }

def save_results(results, output_dir):
    """Save benchmark results"""
    ensure_dir(output_dir)
    
    # Save detailed results
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    results_file = os.path.join(output_dir, f"benchmark_results_{timestamp}.json")
    with open(results_file, "w") as f:
        json.dump(results, f, indent=2)
    
    # Create a simple comparison DataFrame
    comparison = pd.DataFrame([
        {
            "Pipeline": "Cross-language (Py+R+Julia)",
            "Time (sec)": results["cross_language"]["total_time"],
            "Success": results["cross_language"]["success"]
        },
        {
            "Pipeline": "Python-only",
            "Time (sec)": results["python_only"]["results"]["total_time"],
            "Success": results["python_only"]["success"]
        }
    ])
    
    # Save comparison as CSV
    comparison_file = os.path.join(output_dir, f"comparison_{timestamp}.csv")
    comparison.to_csv(comparison_file, index=False)
    
    # Create a bar chart
    plt.figure(figsize=(10, 6))
    plt.bar(
        comparison["Pipeline"], 
        comparison["Time (sec)"],
        color=["blue", "orange"]
    )
    plt.title(f"Pipeline Performance Comparison ({results['rows']} rows)")
    plt.ylabel("Execution Time (seconds)")
    plt.grid(axis="y", linestyle="--", alpha=0.7)
    
    # Add time values on top of bars
    for i, time_value in enumerate(comparison["Time (sec)"]):
        plt.text(
            i, time_value + 0.1,
            f"{time_value:.2f}s",
            ha="center"
        )
    
    # Compute speedup
    cross_time = results["cross_language"]["total_time"]
    python_time = results["python_only"]["results"]["total_time"]
    if cross_time > 0 and python_time > 0:
        speedup = cross_time / python_time
        plt.figtext(
            0.5, 0.01, 
            f"Python-only is {speedup:.2f}x {'faster' if speedup > 1 else 'slower'} than cross-language",
            ha="center", fontsize=12
        )
    
    # Save the chart
    chart_file = os.path.join(output_dir, f"comparison_chart_{timestamp}.png")
    plt.savefig(chart_file, bbox_inches="tight")
    
    print(f"\nResults saved to {output_dir}")
    print(f"Summary: {comparison_file}")
    print(f"Chart: {chart_file}")
    print(f"Detailed results: {results_file}")
    
    return comparison

def main():
    """Main benchmark runner"""
    # Parse arguments
    if len(sys.argv) < 2:
        print("Usage: python run_benchmark.py <db_path> [rows]")
        print("Example: python run_benchmark.py ../data/benchmark.duckdb 1000")
        sys.exit(1)
    
    db_path = os.path.abspath(sys.argv[1])
    rows = int(sys.argv[2]) if len(sys.argv) > 2 else 100
    
    # Define paths
    benchmark_dir = os.path.dirname(os.path.abspath(__file__))
    pipeline_dir = os.path.dirname(benchmark_dir)
    output_dir = os.path.join(os.path.dirname(db_path), "benchmark_results")
    
    # Ensure output directory exists
    ensure_dir(output_dir)
    
    # Define script paths
    cross_language_script = os.path.join(pipeline_dir, "run_pipeline.py")
    python_benchmark_script = os.path.join(benchmark_dir, "benchmark_pipeline.py")
    
    print(f"======== STARTING PIPELINE BENCHMARKS ========")
    print(f"Database: {db_path}")
    print(f"Data size: {rows} rows")
    print(f"Output directory: {output_dir}")
    
    # Run the cross-language pipeline
    cross_language_results = run_cross_language_pipeline(cross_language_script, db_path, rows)
    
    # Run the Python-only pipeline
    python_results = run_python_pipeline(python_benchmark_script, db_path, rows)
    
    # Combine results
    all_results = {
        "timestamp": datetime.datetime.now().isoformat(),
        "db_path": db_path,
        "rows": rows,
        "cross_language": cross_language_results,
        "python_only": python_results
    }
    
    # Save and compare results
    comparison = save_results(all_results, output_dir)
    
    # Print summary
    print("\n======== BENCHMARK SUMMARY ========")
    print(comparison.to_string(index=False))
    
    # Determine winner
    if all_results["cross_language"]["total_time"] < all_results["python_only"]["results"]["total_time"]:
        print("\nCross-language pipeline was faster!")
        speedup = all_results["python_only"]["results"]["total_time"] / all_results["cross_language"]["total_time"]
        print(f"Speedup: {speedup:.2f}x")
    else:
        print("\nPython-only pipeline was faster!")
        speedup = all_results["cross_language"]["total_time"] / all_results["python_only"]["results"]["total_time"]
        print(f"Speedup: {speedup:.2f}x")

if __name__ == "__main__":
    main() 