#!/usr/bin/env julia

# This script installs required Julia packages and creates a symlink to the CrossLink module

# Install required packages if not already installed
using Pkg

# List of required packages
required_packages = [
    "DuckDB",
    "DataFrames",
    "Arrow",
    "JSON",
    "CSV",
    "Dates",
    "UUIDs",
    "Statistics",
    "SHA"
]

# Install packages
for pkg in required_packages
    println("Checking for package: $pkg")
    try
        Pkg.status(pkg)
        println("✅ Package $pkg is already installed")
    catch
        println("📦 Installing package $pkg...")
        Pkg.add(pkg)
    end
end

# Create symlink for CrossLink module
crosslink_src = joinpath(@__DIR__, "..", "..", "pipelink", "crosslink", "julia")
crosslink_dest = joinpath(dirname(Base.find_package("DuckDB")), "..", "..", "crosslink")

if !isdir(crosslink_dest)
    # Create parent directory if needed
    mkpath(dirname(crosslink_dest))
    
    # Create symlink
    println("Creating symlink from $crosslink_src to $crosslink_dest")
    symlink(crosslink_src, crosslink_dest)
    println("✅ CrossLink module linked successfully")
else
    println("✅ CrossLink module directory already exists at $crosslink_dest")
end

# Test importing CrossLink
println("Testing CrossLink import...")
try
    include(joinpath(crosslink_dest, "crosslink.jl"))
    println("✅ CrossLink module imported successfully")
catch e
    println("❌ Failed to import CrossLink: $e")
end

println("Setup complete!") 