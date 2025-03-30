#!/bin/bash

# Make scripts executable
chmod +x benchmark_baseline_py.py benchmark_baseline_r.R benchmark_crosslink_py.py benchmark_crosslink_r.R

echo "Starting Benchmark Suite..."
echo "============================="

# Run baseline
# echo "Running Baseline Benchmark..."
# ./benchmark_baseline_py.py
# ./benchmark_baseline_r.R

# Run CrossLink
echo "Running CrossLink Benchmark..."
# Use 'python' command to ensure correct environment is used
python benchmark_crosslink_py.py

# Set library path and run R script
# Note: Use DYLD_LIBRARY_PATH for macOS, LD_LIBRARY_PATH for Linux
export DYLD_LIBRARY_PATH=$(pwd)/duckdata/crosslink/cpp/build:$DYLD_LIBRARY_PATH
echo "[Shell] Set DYLD_LIBRARY_PATH to: $DYLD_LIBRARY_PATH"
./benchmark_crosslink_r.R

echo "============================="
echo "Benchmark Suite Complete" 