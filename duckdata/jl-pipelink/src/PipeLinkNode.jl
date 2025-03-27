"""
PipeLink node utilities for Julia

Helper functions for working with PipeLink in Julia node scripts
"""
module PipeLinkNode

using YAML
using Dates
using JSON
using DataFrames

# Import from CrossLink module
using ..CrossLink: CrossLinkManager, push_data, pull_data, list_dataset_versions, get_schema_history
using ..CrossLink: get_lineage, check_compatibility

export NodeContext, get_input, save_output, get_param, get_node_id, get_pipeline_name
export get_versions, get_dataset_schema_history, get_dataset_lineage, check_dataset_compatibility

"""
    load_node_metadata()

Load node metadata from command-line argument.

Returns:
    Dict containing node metadata
"""
function load_node_metadata()
    # Get metadata file path from command line
    if length(ARGS) == 0
        error("No metadata file provided. This script should be run by PipeLink.")
    end
    
    meta_path = ARGS[1]
    
    if !isfile(meta_path)
        error("Metadata file not found: $meta_path")
    end
    
    # Load metadata from YAML
    return YAML.load_file(meta_path)
end

"""
    NodeContext

Context manager to simplify working with node inputs and outputs.

Example:
```julia
ctx = NodeContext()
try
    # Get input datasets
    input_df = get_input(ctx, "input_name")
    
    # Get parameters
    param = get_param(ctx, "param_name", default_value)
    
    # Process data...
    result_df = process_data(input_df, param)
    
    # Save output
    save_output(ctx, "output_name", result_df)
finally
    close(ctx)
end
```
"""
mutable struct NodeContext
    meta::Dict{String, Any}
    cl::CrossLinkManager
    input_cache::Dict{String, Any}
    
    function NodeContext(meta_path::Union{String, Nothing}=nothing)
        # Load metadata
        if meta_path === nothing
            meta = load_node_metadata()
        else
            if !isfile(meta_path)
                error("Metadata file not found: $meta_path")
            end
            meta = YAML.load_file(meta_path)
        end
        
        # Initialize CrossLink with db_path from metadata
        db_path = get(meta, "db_path", "crosslink.duckdb")
        cl = CrossLinkManager(db_path)
        
        new(meta, cl, Dict{String, Any}())
    end
end

"""
    get_input(ctx::NodeContext, name::String, required::Bool=true, use_arrow::Bool=true, version=nothing)

Get an input dataset by name.

Arguments:
- `ctx`: NodeContext instance
- `name`: Name of the input dataset
- `required`: Whether this input is required
- `use_arrow`: Whether to use Arrow for data retrieval
- `version`: Specific version to retrieve (default: latest version)

Returns:
    DataFrame with the requested data
"""
function get_input(ctx::NodeContext, name::String, required::Bool=true, use_arrow::Bool=true, version=nothing)
    # Check if this input is defined for this node
    inputs = get(ctx.meta, "inputs", String[])
    if !(name in inputs)
        if required
            error("Input '$name' is not defined for this node")
        end
        return nothing
    end
    
    # Check cache
    if haskey(ctx.input_cache, name)
        return ctx.input_cache[name]
    end
    
    # Pull from CrossLink
    df = pull_data(ctx.cl, name, as_arrow=use_arrow, version=version)
    ctx.input_cache[name] = df
    return df
end

"""
    save_output(ctx::NodeContext, name::String, df, description::Union{String, Nothing}=nothing;
                use_arrow::Bool=true, sources=nothing, transformation=nothing, force_new_version::Bool=false)

Save an output dataset.

Arguments:
- `ctx`: NodeContext instance
- `name`: Name of the output dataset
- `df`: DataFrame to save
- `description`: Optional description for the dataset
- `use_arrow`: Whether to use Arrow for data storage (default: true)
- `sources`: List of source dataset IDs or names used to create this dataset
- `transformation`: Description of the transformation applied
- `force_new_version`: Whether to create a new version even if schema hasn't changed

Returns:
    Dataset ID
"""
function save_output(ctx::NodeContext, name::String, df, description::Union{String, Nothing}=nothing;
                    use_arrow::Bool=true, sources=nothing, transformation=nothing, force_new_version::Bool=false)
    # Check if this output is defined for this node
    outputs = get(ctx.meta, "outputs", String[])
    if !(name in outputs)
        error("Output '$name' is not defined for this node")
    end
    
    # Generate a description if not provided
    if isnothing(description)
        node_id = get(ctx.meta, "node_id", "unknown")
        description = "Output '$name' from node '$node_id'"
    end
    
    # Add node info to lineage if not provided
    if isnothing(transformation)
        node_id = get(ctx.meta, "node_id", "unknown")
        pipeline_name = get(ctx.meta, "pipeline_name", "unknown")
        transformation = "Processed by node '$node_id' in pipeline '$pipeline_name'"
    end
    
    # Collect input datasets as sources if not provided
    if isnothing(sources) && haskey(ctx.meta, "inputs")
        sources = ctx.meta["inputs"]
    end
    
    # Push to CrossLink
    return push_data(ctx.cl, df, 
                    name=name, 
                    description=description, 
                    use_arrow=use_arrow,
                    sources=sources,
                    transformation=transformation,
                    force_new_version=force_new_version)
end

"""
    get_param(ctx::NodeContext, name::String, default=nothing)

Get a parameter value.

Arguments:
- `ctx`: NodeContext instance
- `name`: Parameter name
- `default`: Default value if parameter is not found

Returns:
    Parameter value or default
"""
function get_param(ctx::NodeContext, name::String, default=nothing)
    params = get(ctx.meta, "params", Dict{String, Any}())
    return get(params, name, default)
end

"""
    get_node_id(ctx::NodeContext)

Get the node ID.
"""
function get_node_id(ctx::NodeContext)
    return get(ctx.meta, "node_id", "unknown")
end

"""
    get_pipeline_name(ctx::NodeContext)

Get the pipeline name.
"""
function get_pipeline_name(ctx::NodeContext)
    return get(ctx.meta, "pipeline_name", "unknown")
end

"""
    get_versions(ctx::NodeContext, identifier::String)
    
List all versions of a dataset.

Arguments:
- `ctx`: NodeContext instance
- `identifier`: Dataset ID or name

Returns:
    DataFrame containing version information
"""
function get_versions(ctx::NodeContext, identifier::String)
    return list_dataset_versions(ctx.cl, identifier)
end

"""
    get_dataset_schema_history(ctx::NodeContext, identifier::String)
    
Get schema evolution history for a dataset.

Arguments:
- `ctx`: NodeContext instance
- `identifier`: Dataset ID or name

Returns:
    DataFrame containing schema history
"""
function get_dataset_schema_history(ctx::NodeContext, identifier::String)
    return get_schema_history(ctx.cl, identifier)
end

"""
    get_dataset_lineage(ctx::NodeContext, identifier::String)
    
Get data lineage information for a dataset.

Arguments:
- `ctx`: NodeContext instance
- `identifier`: Dataset ID or name

Returns:
    DataFrame containing lineage information
"""
function get_dataset_lineage(ctx::NodeContext, identifier::String)
    return get_lineage(ctx.cl, identifier)
end

"""
    check_dataset_compatibility(ctx::NodeContext, source_id::String, target_id::String)

Check if two datasets have compatible schemas.

Arguments:
- `ctx`: NodeContext instance
- `source_id`: Source dataset ID or name
- `target_id`: Target dataset ID or name

Returns:
    Dict containing compatibility information
"""
function check_dataset_compatibility(ctx::NodeContext, source_id::String, target_id::String)
    return check_compatibility(ctx.cl, source_id, target_id)
end

"""
    Base.close(ctx::NodeContext)

Close the CrossLink connection.
"""
function Base.close(ctx::NodeContext)
    Base.close(ctx.cl)
end

end # module 