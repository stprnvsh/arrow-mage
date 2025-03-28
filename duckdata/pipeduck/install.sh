#!/bin/bash

# PipeDuck installation script
# This script installs PipeDuck and all its dependencies

echo "Installing PipeDuck..."

# Get the directory of this script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
cd "$SCRIPT_DIR"

# Install main package in development mode
echo "Installing main package..."
pip install -e .

# Make sure Python dependencies are installed
echo "Installing Python dependencies..."
pip install pandas pyarrow duckdb pyyaml

# Verify installation
echo "Verifying installation..."
python -c "import pipeduck; print(f'PipeDuck installed successfully: {pipeduck.__version__}')"

echo "Installation complete! You can now use PipeDuck."
echo "Run 'pipeduck --help' to see available commands." 