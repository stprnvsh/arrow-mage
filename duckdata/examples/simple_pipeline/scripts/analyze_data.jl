"""
Analyze data for the PipeLink example pipeline.

This is the third node in the pipeline that analyzes the transformed data
using CrossLink for zero-copy data sharing.
"""

using Statistics
using DataFrames
using JSON
using Dates
using CSV
using UUIDs  # Add UUID for consistent dataset IDs

# Import CrossLink with a more robust path resolution
function import_crosslink()
    try
        # First try standard path from setup_julia.jl
        crosslink_path = joinpath(dirname(Base.find_package("DuckDB")), "..", "..", "crosslink", "crosslink.jl")
        if isfile(crosslink_path)
            include(crosslink_path)
            return true
        end
    catch
        # Fallback options
    end
    
    try
        # Try direct path relative to script location
        crosslink_path = joinpath(dirname(@__DIR__), "..", "..", "pipelink", "crosslink", "julia", "crosslink.jl")
        if isfile(crosslink_path)
            include(crosslink_path)
            return true
        end
    catch
        # Fallback options
    end
    
    try
        # Try original path
        crosslink_path = joinpath(dirname(Base.find_package("DuckDB")), "..", "..", "..", "pipelink", "crosslink", "julia", "crosslink.jl")
        if isfile(crosslink_path)
            include(crosslink_path)
            return true
        end
    catch
        # Fallback options
    end
    
    return false
end

# Import CrossLink
if !import_crosslink()
    error("Failed to import CrossLink module. Please run setup_julia.jl first.")
end
using .CrossLink

println("Starting analyze_data.jl")

"""
    analyze_data(data, detailed=false)

Perform statistical analysis on the input data.
"""
function analyze_data(data, detailed=false)
    # Basic statistics
    stats = Dict{String, Any}()
    
    # Summary statistics for numeric columns
    numeric_cols = filter(col -> eltype(data[!, col]) <: Number, names(data))
    
    for col in numeric_cols
        if col != "id"  # Skip id column
            col_stats = Dict{String, Any}(
                "mean" => mean(data[!, col]),
                "median" => median(data[!, col]),
                "std" => std(data[!, col]),
                "min" => minimum(data[!, col]),
                "max" => maximum(data[!, col])
            )
            
            stats[string(col)] = col_stats
        end
    end
    
    # Convert to a flat DataFrame for storage
    result_rows = []
    
    for (key, value) in stats
        if value isa Dict
            for (subkey, subvalue) in value
                push!(result_rows, (statistic = "$(key)_$(subkey)", value = subvalue))
            end
        else
            push!(result_rows, (statistic = key, value = value))
        end
    end
    
    return DataFrame(result_rows)
end

# Main function to process node
function main()
    # Check command line arguments
    if length(ARGS) < 1
        error("No metadata file provided. This script should be run by PipeLink.")
    end
    
    # Load metadata from file
    meta_path = ARGS[1]
    println("Using metadata file: $meta_path")
    
    # Parse the metadata file to extract db_path
    meta_content = read(meta_path, String)
    db_path_line = match(r"db_path:\s*(.+)", meta_content)
    db_path = String(strip(db_path_line.captures[1]))
    println("Using db_path: $db_path")
    
    # Initialize CrossLink manager with the database
    println("Initializing CrossLink with database: $db_path")
    manager = CrossLinkManager(db_path, true)  # Enable debug mode
    
    # Get transformed data using CrossLink's pull_data
    println("Getting transformed data via CrossLink...")
    
    # Try with error handling
    try
        transformed_data = pull_data(manager, "transformed_data", zero_copy=true)
        
        println("Loaded data: $(nrow(transformed_data)) rows, columns: $(join(names(transformed_data), ", "))")
        
        # Analyze the data
        println("Analyzing data...")
        result = analyze_data(transformed_data)
        println("Analysis complete with $(nrow(result)) statistics calculated")
        
        # Save analysis results using CrossLink's push_data
        println("Saving analysis results with CrossLink...")
        try
            # Use push_data with memory mapping for zero-copy
            dataset_id = push_data(
                manager,
                result,
                "analysis_results",
                description="Statistical analysis of the transformed data",
                enable_zero_copy=true,
                memory_mapped=true,
                shared_memory=false,
                access_languages=["python", "r", "julia", "cpp"]
            )
            
            println("Analysis results saved with dataset ID: $dataset_id")
        catch e
            println("Error saving results: $e")
            # Print stack trace for debugging
            Base.showerror(stderr, e, catch_backtrace())
            rethrow(e)
        end
    catch e
        println("Error retrieving or processing data: $e")
        # Print stack trace for debugging
        Base.showerror(stderr, e, catch_backtrace())
        rethrow(e)
    end
end

# Run the main function
try
    main()
    println("analyze_data.jl completed")
catch e
    println("Error in analyze_data.jl: $e")
    Base.showerror(stderr, e, catch_backtrace())
    # Re-throw to ensure the pipeline knows there was an error
    rethrow(e)
end 