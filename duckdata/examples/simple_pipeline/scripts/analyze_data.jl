"""
Analyze data for the PipeLink example pipeline.

This is the fourth node in the pipeline that analyzes the enhanced data from the Rust node.
"""

# Try to import the PipeLink package, or load modules directly if not installed
try
    # Try to import the installed package
    using PipeLink
catch e
    println("Failed to import PipeLink package, trying to load modules directly...")
    
    # Try different paths to find the modules
    script_dir = dirname(@__FILE__)
    paths = [
        joinpath(script_dir, "..", "..", "..", "jl-pipelink", "src", "CrossLink.jl"),
        joinpath(script_dir, "..", "..", "..", "jl-pipelink", "src", "PipeLinkNode.jl"),
        joinpath(script_dir, "..", "..", "..", "pipelink", "julia", "crosslink.jl"),
        joinpath(script_dir, "..", "..", "..", "pipelink", "julia", "pipelink_node.jl")
    ]
    
    module_loaded = false
    for path in paths
        if isfile(path)
            println("Including file: $path")
            include(path)
            module_loaded = true
            
            # If we find CrossLink.jl, we need to make it available to PipeLinkNode
            if occursin("CrossLink.jl", path)
                # Create global CrossLink module
                global CrossLink = Main.CrossLink
            elseif occursin("PipeLinkNode.jl", path) || occursin("pipelink_node.jl", path)
                # Import from PipeLinkNode
                global NodeContext = Main.PipeLinkNode.NodeContext
                global get_input = Main.PipeLinkNode.get_input
                global save_output = Main.PipeLinkNode.save_output
                global get_param = Main.PipeLinkNode.get_param
            end
        end
    end
    
    if !module_loaded
        error("Could not find PipeLink modules to include directly. Please install the PipeLink Julia package.")
    end
end

using Statistics
using DataFrames

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
        col_stats = Dict{String, Any}(
            "mean" => mean(data[!, col]),
            "median" => median(data[!, col]),
            "std" => std(data[!, col]),
            "min" => minimum(data[!, col]),
            "max" => maximum(data[!, col])
        )
        
        if detailed
            col_stats["quantiles"] = quantile(data[!, col], [0.25, 0.75])
        end
        
        stats[string(col)] = col_stats
    end
    
    # Categorical analysis
    categorical_cols = ["category"]
    if "classification" in names(data)
        push!(categorical_cols, "classification")
    end
    
    for cat_col in categorical_cols
        if cat_col in names(data)
            category_counts = combine(groupby(data, Symbol(cat_col)), nrow => :count)
            stats["$(cat_col)_counts"] = category_counts
        end
    end
    
    # Correlation matrix for numeric columns
    if detailed && length(numeric_cols) > 1
        corr_matrix = cor(Matrix(data[!, numeric_cols]))
        stats["correlation_matrix"] = Dict(
            "columns" => numeric_cols,
            "values" => corr_matrix
        )
    end
    
    # Special analysis for Rust-added columns if they exist
    rust_cols = ["squared_value", "cube_value"]
    existing_rust_cols = filter(col -> col in names(data), rust_cols)
    
    if !isempty(existing_rust_cols)
        stats["rust_columns_present"] = join(existing_rust_cols, ", ")
        
        # Compare original values with transformed ones
        if "value" in names(data) && "squared_value" in names(data)
            # Check how many values are > 1 where squaring increases the magnitude
            increased_by_square = sum(abs.(data.squared_value) .> abs.(data.value))
            stats["values_increased_by_squaring"] = increased_by_square
        end
    end
    
    # Convert to a flat DataFrame for storage
    result_rows = []
    
    for (key, value) in stats
        if value isa AbstractDataFrame
            push!(result_rows, (statistic = "$(key)_dataframe", value = string(value)))
        elseif value isa Dict
            for (subkey, subvalue) in value
                push!(result_rows, (statistic = "$(key)_$(subkey)", value = string(subvalue)))
            end
        else
            push!(result_rows, (statistic = key, value = string(value)))
        end
    end
    
    return DataFrame(result_rows)
end

function main()
    ctx = NodeContext()
    try
        # Get input data
        data = get_input(ctx, "transformed_data", true, false)
        
        # Get parameters
        detailed = get_param(ctx, "detailed", true)
        
        # Analyze data
        result = analyze_data(data, detailed)
        
        # Save output
        save_output(ctx, "analysis_results", result, "Statistical analysis of the enhanced data from the Rust node")
        
        println("Analysis complete with $(nrow(result)) statistics calculated")
    finally
        close(ctx)
    end
end

# Run the main function
main() 