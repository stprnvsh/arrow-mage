"""
Analyze data for the PipeLink example pipeline.

This is the third node in the pipeline that analyzes the transformed data
using CrossLink for zero-copy data sharing.
"""

using Statistics
using DataFrames
using JSON
using Dates

# Import CrossLink
include(joinpath(dirname(Base.find_package("DuckDB")), "..", "..", "..", "pipelink", "crosslink", "julia", "crosslink.jl"))
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
                push!(result_rows, (metric = "$(key)_$(subkey)", value = subvalue))
            end
        else
            push!(result_rows, (metric = key, value = value))
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
    manager = CrossLinkManager(db_path)
    
    # Get transformed data using true zero-copy
    println("Getting transformed data via CrossLink (zero-copy)...")
    transformed_data = pull_data(manager, "transformed_data", zero_copy=true)
    println("Loaded data via zero-copy: $(nrow(transformed_data)) rows, columns: $(join(names(transformed_data), ", "))")
    
    # Analyze the data
    println("Analyzing data...")
    result = analyze_data(transformed_data)
    println("Analysis complete with $(nrow(result)) statistics calculated")
    
    # Use push_data with our result
    println("Saving analysis results with CrossLink...")
    dataset_id = push_data(
        manager, 
        result, 
        "analysis_results",
        description="Statistical analysis of the transformed data",
        arrow_data=true
    )
    
    println("Analysis results shared via CrossLink with ID: $dataset_id")
end

# Run the main function
main()
println("analyze_data.jl completed") 