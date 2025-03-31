#!/usr/bin/env python3
import subprocess
import time
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import re
import os

def run_benchmark(script):
    """Run benchmark script and capture output"""
    print(f"Running {script}...")
    result = subprocess.run(["python", script], capture_output=True, text=True)
    print(result.stdout)  # Print output for debugging
    return result.stdout

def parse_benchmark_results(output, benchmark_type):
    """Parse the benchmark output to extract timing information"""
    if benchmark_type == "baseline":
        python_time_match = re.search(r"Average Python processing time: (\d+\.\d+) seconds", output)
        r_time_match = re.search(r"Average R processing time: (\d+\.\d+) seconds", output)
        total_time_match = re.search(r"Average total pipeline time: (\d+\.\d+) seconds", output)
        
        results = {
            'type': benchmark_type,
            'python_time': float(python_time_match.group(1)) if python_time_match else 0.0,
            'r_time': float(r_time_match.group(1)) if r_time_match else 0.0,
            'data_transfer_time': 0.0,  # No explicit data transfer in baseline
            'total_time': float(total_time_match.group(1)) if total_time_match else 0.0
        }
    else:  # crosslink
        python_time_match = re.search(r"Average Python processing time: (\d+\.\d+) seconds", output)
        r_time_match = re.search(r"Average R processing time: (\d+\.\d+) seconds", output)
        transfer_time_match = re.search(r"Average data transfer time: (\d+\.\d+) seconds", output)
        total_time_match = re.search(r"Average total pipeline time: (\d+\.\d+) seconds", output)
        
        results = {
            'type': benchmark_type,
            'python_time': float(python_time_match.group(1)) if python_time_match else 0.0,
            'r_time': float(r_time_match.group(1)) if r_time_match else 0.0,
            'data_transfer_time': float(transfer_time_match.group(1)) if transfer_time_match else 0.0,
            'total_time': float(total_time_match.group(1)) if total_time_match else 0.0
        }
    
    return results

def create_comparison_chart(baseline_results, crosslink_results):
    """Create comparison chart and save as PNG"""
    results_df = pd.DataFrame([baseline_results, crosslink_results])
    
    # Extract data for plotting
    categories = ['python_time', 'r_time', 'data_transfer_time', 'total_time']
    category_labels = ['Python', 'R', 'Data Transfer', 'Total']
    
    baseline_values = [baseline_results[cat] for cat in categories]
    crosslink_values = [crosslink_results[cat] for cat in categories]
    
    # Calculate improvements
    speedups = []
    for i, cat in enumerate(categories):
        if crosslink_results[cat] > 0 and baseline_results[cat] > 0:
            speedup = (baseline_results[cat] - crosslink_results[cat]) / baseline_results[cat] * 100
            speedups.append(f"{speedup:.1f}%")
        else:
            speedups.append("N/A")
    
    # Create bar chart
    fig, ax = plt.subplots(figsize=(12, 6))
    
    x = np.arange(len(categories))
    width = 0.35
    
    baseline_bars = ax.bar(x - width/2, baseline_values, width, label='Baseline')
    crosslink_bars = ax.bar(x + width/2, crosslink_values, width, label='CrossLink')
    
    ax.set_title('SoS Pipeline Benchmark: Baseline vs CrossLink')
    ax.set_ylabel('Time (seconds)')
    ax.set_xticks(x)
    ax.set_xticklabels(category_labels)
    ax.legend()
    
    # Add value labels on top of bars
    def add_labels(bars):
        for bar in bars:
            height = bar.get_height()
            ax.annotate(f'{height:.2f}s',
                       xy=(bar.get_x() + bar.get_width() / 2, height),
                       xytext=(0, 3),  # 3 points vertical offset
                       textcoords="offset points",
                       ha='center', va='bottom')
    
    add_labels(baseline_bars)
    add_labels(crosslink_bars)
    
    # Add improvement percentage between bars
    for i, speedup in enumerate(speedups):
        if speedup != "N/A":
            ax.annotate(speedup,
                       xy=(x[i], min(baseline_values[i], crosslink_values[i]) / 2),
                       ha='center', va='center',
                       fontweight='bold', color='green' if "%" in speedup and float(speedup[:-1]) > 0 else 'red')
    
    plt.tight_layout()
    plt.savefig('benchmark_comparison.png')
    print(f"Comparison chart saved as benchmark_comparison.png")
    
    # Return the figure for display
    return fig

def main():
    print("============================================")
    print("Running SoS Pipeline Benchmark Comparison")
    print("============================================")
    
    # Make benchmark scripts executable
    os.chmod('benchmark_baseline.py', 0o755)
    os.chmod('benchmark_crosslink.py', 0o755)
    
    # Run baseline benchmark
    baseline_output = run_benchmark('benchmark_baseline.py')
    baseline_results = parse_benchmark_results(baseline_output, "baseline")
    
    print("\n============================================\n")
    
    # Run CrossLink benchmark
    crosslink_output = run_benchmark('benchmark_crosslink.py')
    crosslink_results = parse_benchmark_results(crosslink_output, "crosslink")
    
    # Calculate improvements (safely)
    if baseline_results['total_time'] > 0 and crosslink_results['total_time'] > 0:
        improvement = (baseline_results['total_time'] - crosslink_results['total_time']) / baseline_results['total_time'] * 100
        improvement_str = f"{improvement:.2f}%"
    else:
        improvement_str = "N/A (missing data)"
    
    print("\n============================================")
    print("Benchmark Comparison Results")
    print("============================================")
    print(f"Baseline total time: {baseline_results['total_time']:.4f} seconds")
    print(f"CrossLink total time: {crosslink_results['total_time']:.4f} seconds")
    print(f"Speed improvement: {improvement_str}")
    print("============================================")
    
    # Create comparison chart
    create_comparison_chart(baseline_results, crosslink_results)
    
    # Save detailed results to CSV
    results_df = pd.DataFrame([baseline_results, crosslink_results])
    results_df.to_csv('benchmark_results.csv', index=False)
    print("Detailed results saved to benchmark_results.csv")

if __name__ == "__main__":
    main() 