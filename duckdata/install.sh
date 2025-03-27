#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Installing PipeLink packages for Python, R, Julia, and C++..."

# Check for required tools
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed. Aborting."; exit 1; }
R_INSTALLED=true
JULIA_INSTALLED=true
CPP_INSTALLED=true
command -v R >/dev/null 2>&1 || { echo "Warning: R is not installed. Skipping R package installation."; R_INSTALLED=false; }
command -v julia >/dev/null 2>&1 || { echo "Warning: Julia is not installed. Skipping Julia package installation."; JULIA_INSTALLED=false; }
command -v cmake >/dev/null 2>&1 || { echo "Warning: CMake is not installed. Skipping C++ package installation."; CPP_INSTALLED=false; }

# Install Python package
echo "Installing Python package..."
pip install -e .

# Install R package if R is available
if [ "$R_INSTALLED" != "false" ]; then
    if [ -d "$SCRIPT_DIR/r-pipelink" ]; then
        echo "Installing R package..."
        R -e "install.packages('$SCRIPT_DIR/r-pipelink', repos = NULL, type = 'source')"
    else
        echo "Warning: r-pipelink directory not found. Skipping R package installation."
    fi
fi

# Install Julia package if Julia is available
if [ "$JULIA_INSTALLED" != "false" ]; then
    if [ -d "$SCRIPT_DIR/jl-pipelink" ]; then
        echo "Installing Julia package..."
        julia -e "using Pkg; Pkg.develop(path=\"$SCRIPT_DIR/jl-pipelink\")"
    else
        echo "Warning: jl-pipelink directory not found. Skipping Julia package installation."
    fi
fi

# Build and install C++ library if CMake is available
if [ "$CPP_INSTALLED" != "false" ]; then
    if [ -d "$SCRIPT_DIR/cpp-pipelink" ]; then
        echo "Building C++ library..."
        cd "$SCRIPT_DIR/cpp-pipelink"
        
        # Run the build script if it exists
        if [ -f "./build.sh" ]; then
            chmod +x ./build.sh
            ./build.sh
        else
            # Otherwise build manually
            mkdir -p build
            cd build
            cmake ..
            make -j4
        fi
        
        echo "C++ library built successfully."
    else
        echo "Warning: cpp-pipelink directory not found. Skipping C++ library installation."
    fi
fi

echo "Installation complete!" 