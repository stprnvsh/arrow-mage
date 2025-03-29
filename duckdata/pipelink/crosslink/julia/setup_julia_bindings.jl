#!/usr/bin/env julia

"""
Helper script to diagnose and fix Julia binding issues for CrossLink

This script helps to:
1. Verify Julia package dependencies are installed
2. Set up necessary environment variables
3. Diagnose common JlCxx linking issues
4. Attempt to resolve them automatically
"""

function print_header(msg)
    println("\n", "="^50)
    println(" ", msg)
    println("="^50)
end

function check_package_installed(pkg_name)
    try
        @eval using $(Symbol(pkg_name))
        return true
    catch e
        return false
    end
end

function install_package(pkg_name)
    println("Installing package: $pkg_name...")
    try
        @eval using Pkg
        @eval Pkg.add("$pkg_name")
        return true
    catch e
        println("Failed to install $pkg_name: $e")
        return false
    end
end

# Main diagnostics
print_header("CrossLink Julia Bindings Setup")
println("This script will help set up and troubleshoot Julia bindings for CrossLink")

# Check for required packages
print_header("Checking Required Packages")

packages = ["CxxWrap", "libcxxwrap_julia", "Arrow", "DuckDB", "DataFrames"]
missing_packages = String[]

for pkg in packages
    print("Checking for $pkg... ")
    if check_package_installed(pkg)
        println("✓ Found")
    else
        println("✗ Missing")
        push!(missing_packages, pkg)
    end
end

# Install missing packages
if !isempty(missing_packages)
    print_header("Installing Missing Packages")
    for pkg in missing_packages
        if install_package(pkg)
            println("✓ Successfully installed $pkg")
        else
            println("✗ Failed to install $pkg")
        end
    end
end

# Get CxxWrap info
print_header("CxxWrap Configuration")
try
    using CxxWrap
    prefix_path = CxxWrap.prefix_path()
    println("CxxWrap prefix path: $prefix_path")
    
    # Check if directory exists
    if isdir(prefix_path)
        println("✓ Prefix path directory exists")
    else
        println("✗ Prefix path directory does not exist: $prefix_path")
    end
    
    # Check for lib directory
    lib_dir = joinpath(prefix_path, "lib")
    if isdir(lib_dir)
        println("✓ Library directory exists: $lib_dir")
    else
        println("✗ Library directory does not exist: $lib_dir")
    end
    
    # Check for include directory
    include_dir = joinpath(prefix_path, "include")
    if isdir(include_dir)
        println("✓ Include directory exists: $include_dir")
    else
        println("✗ Include directory does not exist: $include_dir")
    end
    
    # Check for JlCxx header
    jlcxx_header = joinpath(include_dir, "jlcxx", "jlcxx.hpp")
    if isfile(jlcxx_header)
        println("✓ JlCxx header found: $jlcxx_header")
    else
        println("✗ JlCxx header not found: $jlcxx_header")
    end
    
catch e
    println("Error getting CxxWrap information: $e")
end

# Set up environment variables
print_header("Setting Up Environment Variables")

using CxxWrap
prefix_path = CxxWrap.prefix_path()

if Sys.isapple()
    println("Detected macOS system")
    set_env_cmd = "export CXXWRAP_PREFIX_PATH=\"$prefix_path\""
    println("Recommended environment setup:")
    println(set_env_cmd)
    println("export CMAKE_PREFIX_PATH=\"\$CMAKE_PREFIX_PATH:$prefix_path\"")
elseif Sys.islinux()
    println("Detected Linux system")
    lib_path = joinpath(prefix_path, "lib")
    set_env_cmd = "export LD_LIBRARY_PATH=\"\$LD_LIBRARY_PATH:$lib_path\""
    println("Recommended environment setup:")
    println(set_env_cmd)
    println("export CMAKE_PREFIX_PATH=\"\$CMAKE_PREFIX_PATH:$prefix_path\"")
elseif Sys.iswindows()
    println("Detected Windows system")
    lib_path = joinpath(prefix_path, "lib")
    set_env_cmd = "\$env:PATH += \";$lib_path\""
    println("Recommended environment setup (PowerShell):")
    println(set_env_cmd)
    println("\$env:CMAKE_PREFIX_PATH += \";$prefix_path\"")
end

# Try to set environment variable for current session
try
    if Sys.isapple() || Sys.islinux()
        ENV["CXXWRAP_PREFIX_PATH"] = prefix_path
        println("✓ Set CXXWRAP_PREFIX_PATH for current session")
    end
catch e
    println("✗ Could not set environment variable for current session: $e")
end

# Test library loading
print_header("Testing Library Loading")

try
    # Check if the shared library exists
    project_dir = dirname(dirname(@__DIR__))
    
    # Different platforms have different library extensions
    lib_name = Sys.iswindows() ? "libcrosslink_jl.dll" : 
               Sys.isapple() ? "libcrosslink_jl.dylib" : "libcrosslink_jl.so"
    
    # Look in multiple possible locations
    possible_paths = [
        joinpath(project_dir, "julia", "deps", lib_name),
        joinpath(project_dir, "cpp", "build", lib_name),
        joinpath(project_dir, "lib", lib_name)
    ]
    
    lib_found = false
    for path in possible_paths
        if isfile(path)
            println("✓ Found CrossLink library: $path")
            lib_found = true
            
            # Try loading it
            try
                using CxxWrap
                println("Attempting to load library...")
                lib = CxxWrap.load_library(path)
                println("✓ Successfully loaded library!")
            catch e
                println("✗ Failed to load library: $e")
                println("This may indicate linking issues with JlCxx")
            end
            
            break
        end
    end
    
    if !lib_found
        println("✗ Could not find CrossLink library")
        println("You may need to build the C++ library first with:")
        println("cd <project_root>/cpp && python build.py")
    end
catch e
    println("Error testing library loading: $e")
end

# Final instructions
print_header("Next Steps")
println("""
To resolve JlCxx linking issues:

1. Make sure environment variables are set correctly:
   $set_env_cmd

2. Rebuild the C++ library:
   cd <project_root>/cpp && python build.py

3. If problems persist, try reinstalling CxxWrap.jl:
   julia -e 'using Pkg; Pkg.add("CxxWrap"); Pkg.build("CxxWrap")'

4. For detailed debugging, run:
   julia -e 'using CxxWrap; println(CxxWrap.prefix_path())'
   julia -e 'using libcxxwrap_julia; println(dirname(dirname(pathof(libcxxwrap_julia))))'
""")

print_header("Setup Complete") 