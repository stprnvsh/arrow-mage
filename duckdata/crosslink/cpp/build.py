#!/usr/bin/env python3
"""
Build script for CrossLink C++ library and language bindings
"""
import os
import sys
import shutil
import subprocess
import platform
from pathlib import Path

def run_command(cmd, cwd=None):
    """Run a command and return its output"""
    print(f"Running: {' '.join(cmd)}")
    try:
        process = subprocess.run(cmd, cwd=cwd, check=True, 
                            capture_output=True, text=True)
        return process.stdout
    except subprocess.CalledProcessError as e:
        print(f"Command failed with exit code {e.returncode}")
        print(f"Error output:\n{e.stderr}")
        raise

def build_cpp_library():
    """Build the C++ library and bindings"""
    # Determine script directory
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent
    
    # Create necessary directories
    lib_dir = project_root / "lib"
    include_dir = project_root / "include" / "crosslink"
    python_dir = project_root / "python" / "crosslink"
    r_dir = project_root / "r" / "libs"
    julia_dir = project_root / "julia" / "deps"
    
    os.makedirs(lib_dir, exist_ok=True)
    os.makedirs(include_dir, exist_ok=True)
    os.makedirs(python_dir, exist_ok=True)
    os.makedirs(r_dir, exist_ok=True)
    os.makedirs(julia_dir, exist_ok=True)
    
    # Create build directory
    build_dir = script_dir / "build"
    os.makedirs(build_dir, exist_ok=True)
    
    # Configure with CMake
    cmake_args = [
        "cmake",
        "..",
        f"-DCMAKE_PREFIX_PATH=/opt/homebrew",
        "-DCMAKE_FIND_FRAMEWORK=NEVER",
        "-DCMAKE_FIND_APPBUNDLE=NEVER",
        "-DCMAKE_BUILD_TYPE=Release",
        "-DBUILD_PYTHON_BINDINGS=ON",
        "-DBUILD_R_BINDINGS=ON",
        "-DBUILD_JULIA_BINDINGS=ON",
        "-DArrow_DIR=/opt/homebrew/lib/cmake/Arrow",
    ]
    
    # Add platform-specific flags
    if platform.system() == "Darwin":  # macOS
        cmake_args.extend([
            "-DCMAKE_OSX_DEPLOYMENT_TARGET=10.15",
            "-DCMAKE_INSTALL_RPATH=@loader_path/../lib"
        ])
    
    # Run CMake configure
    run_command(cmake_args, cwd=build_dir)
    
    # Build the library
    run_command(["cmake", "--build", ".", "--config", "Release"], cwd=build_dir)
    
    # Install bindings to language directories
    run_command(["cmake", "--install", "."], cwd=build_dir)
    
    print("CrossLink C++ library and bindings built successfully!")
    return 0

if __name__ == "__main__":
    sys.exit(build_cpp_library()) 