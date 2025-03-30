#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

echo "Installing CrossLink packages for Python, R, Julia, and C++..."

# Check for required tools
command -v python3 >/dev/null 2>&1 || { echo "Python 3 is required but not installed. Aborting."; exit 1; }
R_INSTALLED=true
JULIA_INSTALLED=true
CPP_INSTALLED=true
command -v R >/dev/null 2>&1 || { echo "Warning: R is not installed. Skipping R package installation."; R_INSTALLED=false; }
command -v julia >/dev/null 2>&1 || { echo "Warning: Julia is not installed. Skipping Julia package installation."; JULIA_INSTALLED=false; }
command -v cmake >/dev/null 2>&1 || { echo "Warning: CMake is not installed. Skipping C++ package installation."; CPP_INSTALLED=false; }
command -v pip >/dev/null 2>&1 || { echo "pip is required but not installed. Aborting."; exit 1; }


# Install Python package
if [ -d "$SCRIPT_DIR/crosslink/python" ]; then
    echo "Installing Python package..."
    cd "$SCRIPT_DIR/crosslink/python"
    pip install -e .
    cd "$SCRIPT_DIR" # Go back to the script directory
else
    echo "Warning: crosslink/python directory not found. Skipping Python package installation."
fi


# Install R package if R is available
if [ "$R_INSTALLED" != "false" ]; then
    if [ -d "$SCRIPT_DIR/crosslink/r" ]; then
        echo "Installing R package..."
        R -e "install.packages('$SCRIPT_DIR/crosslink/r', repos = NULL, type = 'source')"
    else
        echo "Warning: crosslink/r directory not found. Skipping R package installation."
    fi
fi

# Install Julia package if Julia is available
if [ "$JULIA_INSTALLED" != "false" ]; then
    if [ -d "$SCRIPT_DIR/crosslink/julia" ]; then
        echo "Installing Julia package..."
        
        # Direct development of the package
        julia -e 'using Pkg; Pkg.develop(path="'$SCRIPT_DIR/crosslink/julia'"); println("CrossLink package installation completed!")'
        
        # Install Julia dependencies for C++ bindings
        echo "Installing Julia dependencies for C++ bindings..."

        # Set up environment variables for JlCxx
        if [ "$(uname)" == "Darwin" ]; then  # macOS
            export CXXWRAP_PREFIX_PATH=$(julia -e 'using CxxWrap; print(CxxWrap.prefix_path())')
            echo "Set CXXWRAP_PREFIX_PATH=$CXXWRAP_PREFIX_PATH"
        elif [ "$(uname)" == "Linux" ]; then  # Linux
            export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(julia -e 'using CxxWrap; print(CxxWrap.prefix_path())')/lib"
            echo "Updated LD_LIBRARY_PATH with JlCxx library path"
        fi

        # Run the diagnostic script if it exists
        if [ -f "$SCRIPT_DIR/crosslink/julia/setup_julia_bindings.jl" ]; then
            echo "Running Julia binding setup script..."
            julia --project="$SCRIPT_DIR/crosslink/julia" "$SCRIPT_DIR/crosslink/julia/setup_julia_bindings.jl"
        fi
    else
        echo "Warning: crosslink/julia directory not found. Skipping Julia package installation."
    fi
fi

# Build and install C++ library if CMake is available
if [ "$CPP_INSTALLED" != "false" ]; then
    if [ -d "$SCRIPT_DIR/crosslink/cpp" ]; then
        echo "Building C++ library..."
        cd "$SCRIPT_DIR/crosslink/cpp"

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
            # Consider adding installation step? cmake --install build
        fi
        cd "$SCRIPT_DIR" # Go back to the script directory

        echo "C++ library built successfully."
    else
        echo "Warning: crosslink/cpp directory not found. Skipping C++ library installation."
    fi
fi

echo "Installation complete!" 