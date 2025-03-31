#!/bin/bash

echo "==================================================="
echo "CrossLink SoS Pipeline Benchmark Suite"
echo "==================================================="

# Make scripts executable
chmod +x benchmark_baseline.py benchmark_crosslink.py run_comparison.py

# Set up environment
echo "Setting up environment..."
export PYTHONPATH=$PYTHONPATH:$(pwd)
export DYLD_LIBRARY_PATH=$(pwd)/duckdata/crosslink/cpp/build:$DYLD_LIBRARY_PATH

# Run the comparison script
echo "Starting benchmark comparison..."
python run_comparison.py

echo "==================================================="
echo "Benchmark completed!"
echo "Results are available in benchmark_results.csv"
echo "Visual comparison is available in benchmark_comparison.png"
echo "===================================================" 