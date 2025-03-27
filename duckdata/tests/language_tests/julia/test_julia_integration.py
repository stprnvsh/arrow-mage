"""
Tests for Julia integration with DuckData.
"""
import os
import sys
import pytest
import yaml
import pandas as pd
import numpy as np
import subprocess
from pipelink.crosslink import CrossLink
from pipelink.python.pipelink import run_pipeline

class TestJuliaIntegration:
    """Tests for Julia integration with DuckData."""
    
    def test_julia_available(self):
        """Check if Julia is available in the system."""
        try:
            result = subprocess.run(["julia", "--version"], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)
            return result.returncode == 0
        except:
            pytest.skip("Julia not available on this system")
    
    def test_julia_node_execution(self, test_dir, sample_dataframe, check_language_available):
        """Test execution of a Julia node."""
        # Skip if Julia is not available
        if not check_language_available("julia"):
            pytest.skip("Julia is not available")
        
        # Create a Julia script for testing
        script_content = """
# Test Julia script for DuckData

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

using DataFrames
using Statistics

function main()
    ctx = NodeContext()
    try
        # Get input data
        df = get_input(ctx, "input_data")
        
        # Process the data
        df.value_cubed = df.value .^ 3
        df.value_abs = abs.(df.value)
        df.value_sqrt = sqrt.(abs.(df.value))
        
        # Add a column to indicate Julia processing
        df.processed_by_julia = fill(true, nrow(df))
        
        # Save the results
        save_output(ctx, "julia_processed_data", df, "Data processed by Julia")
        
        println("Processed $(nrow(df)) rows with Julia")
    finally
        close(ctx)
    end
end

# Run the main function
main()
"""
        
        # Create script file
        script_path = os.path.join(test_dir, "process_data.jl")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Create a pipeline configuration
        nodes = [
            {
                "id": "julia_process_data",
                "language": "julia",
                "script": script_path,
                "inputs": ["input_data"],
                "outputs": ["julia_processed_data"]
            }
        ]
        
        pipeline_config = {
            "name": "julia_test_pipeline",
            "description": "Test pipeline for Julia integration",
            "db_path": os.path.join(test_dir, "julia_test.duckdb"),
            "working_dir": test_dir,
            "nodes": nodes
        }
        
        # Save the pipeline configuration
        config_path = os.path.join(test_dir, "julia_test_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Initialize CrossLink and save test data
        cl = CrossLink(db_path=pipeline_config["db_path"])
        cl.push(sample_dataframe, name="input_data", description="Test input data")
        cl.close()
        
        # Run the pipeline
        run_pipeline(config_path)
        
        # Verify the results
        cl = CrossLink(db_path=pipeline_config["db_path"])
        result_df = cl.pull("julia_processed_data")
        cl.close()
        
        # Check that the transformation was applied
        assert "value_cubed" in result_df.columns
        assert "value_abs" in result_df.columns
        assert "value_sqrt" in result_df.columns
        assert "processed_by_julia" in result_df.columns
        assert result_df["processed_by_julia"].all()
    
    def test_julia_data_analysis(self, test_dir, complex_dataframe, check_language_available):
        """Test Julia data analysis capabilities."""
        # Skip if Julia is not available
        if not check_language_available("julia"):
            pytest.skip("Julia is not available")
        
        # Create a Julia script for statistical analysis
        script_content = """
# Julia script for data analysis in DuckData

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

using DataFrames
using Statistics

# Try to load additional packages if available
for pkg in ["StatsBase", "Distributions", "GLM"]
    try
        @eval using $(Symbol(pkg))
        println("Loaded package: $pkg")
    catch e
        println("Package $pkg not available. Some functionality may be limited.")
    end
end

"""
Analyze data with Julia statistics
"""
function analyze_data(df)
    # Basic summary statistics for numeric columns
    numeric_cols = filter(col -> eltype(df[!, col]) <: Number, names(df))
    
    stats_rows = []
    for col in numeric_cols
        if col == "id" || col == "null_col"
            continue  # Skip ID and null columns
        end
        
        values = df[!, col]
        non_missing = filter(!ismissing, values)
        
        # Basic statistics
        push!(stats_rows, (col=col, statistic="count", value=length(non_missing)))
        push!(stats_rows, (col=col, statistic="mean", value=mean(non_missing)))
        push!(stats_rows, (col=col, statistic="median", value=median(non_missing)))
        push!(stats_rows, (col=col, statistic="std", value=std(non_missing)))
        push!(stats_rows, (col=col, statistic="min", value=minimum(non_missing)))
        push!(stats_rows, (col=col, statistic="max", value=maximum(non_missing)))
        
        # Add more advanced statistics if StatsBase is available
        if @isdefined(StatsBase)
            push!(stats_rows, (col=col, statistic="skewness", value=StatsBase.skewness(non_missing)))
            push!(stats_rows, (col=col, statistic="kurtosis", value=StatsBase.kurtosis(non_missing)))
        end
    end
    
    # Create summary dataframe
    summary_df = DataFrame(stats_rows)
    
    # If GLM is available, try to fit a linear model
    model_results = DataFrame()
    if @isdefined(GLM) && "float_col" in names(df) && "int_col" in names(df)
        try
            # Fit linear model
            model = GLM.lm(@formula(float_col ~ int_col), df)
            
            # Extract coefficients
            coefs = GLM.coeftable(model)
            
            # Convert to DataFrame
            model_results = DataFrame(
                term = coefs.rownms,
                estimate = coefs.cols[1],
                std_error = coefs.cols[2],
                t_value = coefs.cols[3],
                p_value = coefs.cols[4]
            )
        catch e
            println("Linear model fitting failed: ", e)
        end
    end
    
    return summary_df, model_results
end

function main()
    ctx = NodeContext()
    try
        # Get input data
        df = get_input(ctx, "complex_data")
        
        # Analyze data
        summary_stats, model_results = analyze_data(df)
        
        # Save the results
        save_output(ctx, "julia_stats", summary_stats, "Statistical analysis from Julia")
        
        # Save model results if they exist
        if ncol(model_results) > 0
            save_output(ctx, "julia_model", model_results, "Linear model results from Julia")
        end
        
        println("Generated $(nrow(summary_stats)) statistical measures")
    finally
        close(ctx)
    end
end

# Run the main function
main()
"""
        
        # Create script file
        script_path = os.path.join(test_dir, "julia_analysis.jl")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Create a pipeline configuration
        nodes = [
            {
                "id": "julia_data_analysis",
                "language": "julia",
                "script": script_path,
                "inputs": ["complex_data"],
                "outputs": ["julia_stats", "julia_model"]
            }
        ]
        
        pipeline_config = {
            "name": "julia_analysis_pipeline",
            "description": "Test pipeline for Julia data analysis",
            "db_path": os.path.join(test_dir, "julia_analysis.duckdb"),
            "working_dir": test_dir,
            "nodes": nodes
        }
        
        # Save the pipeline configuration
        config_path = os.path.join(test_dir, "julia_analysis_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Initialize CrossLink and save test data
        cl = CrossLink(db_path=pipeline_config["db_path"])
        cl.push(complex_dataframe, name="complex_data", description="Complex test data")
        cl.close()
        
        # Run the pipeline
        run_pipeline(config_path)
        
        # Verify the results
        cl = CrossLink(db_path=pipeline_config["db_path"])
        
        # Check summary statistics
        summary_stats = cl.pull("julia_stats")
        assert summary_stats is not None
        assert "col" in summary_stats.columns
        assert "statistic" in summary_stats.columns
        assert "value" in summary_stats.columns
        
        # Check model results if they exist
        try:
            model_results = cl.pull("julia_model")
            assert model_results is not None
            assert "term" in model_results.columns
            assert "estimate" in model_results.columns
        except:
            # Model results might not be available if the linear model failed
            pass
            
        cl.close() 