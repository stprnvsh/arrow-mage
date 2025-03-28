#!/bin/bash
set -e

echo "========================================================"
echo "       Multi-Language Pipeline Example                   "
echo "========================================================"
echo "This script demonstrates a pipeline that uses Python, R, and Julia"
echo "to process and analyze data using DuckDB as a shared database."
echo ""

# Make scripts executable
chmod +x nodes/generate_data.py
chmod +x nodes/process_data.R
chmod +x nodes/analyze_data.jl

# Create data directory if it doesn't exist
mkdir -p data

# Add project root to PYTHONPATH
export PYTHONPATH=$PYTHONPATH:/Users/pranavsateesh/arrow-mage/

# Install R package if needed
echo "Setting up R environment..."
Rscript -e "if(!require('pipeduck', quietly=TRUE)) { install.packages('/Users/pranavsateesh/arrow-mage/duckdata/pipeduck/r', repos=NULL, type='source') }"

# Install Julia package if needed
echo "Setting up Julia environment..."
julia -e "using Pkg; Pkg.add(path=\"/Users/pranavsateesh/arrow-mage/duckdata/pipeduck/julia\")"

echo ""
echo "Running the pipeline with PipeDuck..."
echo "-------------------------------------------------------"
# Run the pipeline using pipeduck
pipeduck run pipeline.yml

echo ""
echo "========================================================"
echo "       Pipeline Execution Complete!                     "
echo "========================================================"
echo ""
echo "Results are available in the data/ directory:"
echo "- DuckDB database: data/pipeline.duckdb"
echo "- Analysis report: data/analysis_report.txt"
echo "" 