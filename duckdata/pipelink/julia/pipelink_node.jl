module PipeLinkNode

using YAML
using DataFrames
using Dates
using JSON

# Import CrossLink module if already defined
const CrossLink = if isdefined(Main, :CrossLink)
    Main.CrossLink
elseif @isdefined CrossLink
    CrossLink
else
    nothing
end

export NodeContext, get_input, save_output, get_param, get_connection, close

"""
    NodeContext

Context manager for working with PipeLink nodes in Julia.
"""
mutable struct NodeContext
    meta::Dict{String, Any}
    cl::Any
    input_cache::Dict{String, Any}
    
    """
        NodeContext(meta_path::Union{String, Nothing}=nothing)
    
    Initialize NodeContext with metadata from specified path or command line argument.
    """
    function NodeContext(meta_path::Union{String, Nothing}=nothing)
        # Load metadata
        meta = _load_metadata(meta_path)
        
        # Initialize CrossLink
        cl = nothing
        try
            if CrossLink !== nothing
                cl = CrossLink.CrossLinkManager(get(meta, "db_path", "crosslink.duckdb"))
                @info "Initialized CrossLink connection to $(get(meta, "db_path", "crosslink.duckdb"))"
            else
                @warn "CrossLink module not available. Some functionality will be limited."
            end
        catch e
            @warn "Failed to initialize CrossLink: $e"
        end
        
        # Create context
        new(meta, cl, Dict{String, Any}())
    end
end

"""
    _load_metadata(meta_path::Union{String, Nothing})

Load metadata from YAML file.
"""
function _load_metadata(meta_path::Union{String, Nothing})
    # Get metadata path from command line if not provided
    if meta_path === nothing
        args = ARGS
        if length(args) < 1
            error("No metadata file provided. This script should be run by PipeLink.")
        end
        meta_path = args[1]
    end
    
    # Check if file exists
    if !isfile(meta_path)
        error("Metadata file not found: $meta_path")
    end
    
    # Load YAML
    return YAML.load_file(meta_path)
end

"""
    get_input(ctx::NodeContext, name::String, required::Bool=true, zero_copy::Bool=true)

Get input dataset by name.
"""
function get_input(ctx::NodeContext, name::String, required::Bool=true, zero_copy::Bool=true)
    # Check if this input is defined
    inputs = get(ctx.meta, "inputs", [])
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
    
    # Get from CrossLink
    if ctx.cl !== nothing
        try
            # Try to get data with zero-copy if requested
            if zero_copy && isdefined(Main, :CrossLink)
                # Try to get reference first for true zero-copy
                try
                    table_ref = CrossLink.get_table_reference(ctx.cl, name)
                    @info "Got table reference for $name: $(table_ref["table_name"])"
                    # We could use this reference directly in queries if needed
                end catch e
                    @warn "Could not get direct table reference: $e"
                end
            end
            
            # Pull data
            df = CrossLink.pull_data(ctx.cl, name)
            
            # Cache and return
            ctx.input_cache[name] = df
            return df
        catch e
            @warn "Failed to get input '$name' from CrossLink: $e"
            if required
                error("Required input '$name' could not be accessed")
            end
            return nothing
        end
    else
        @warn "CrossLink not available, cannot get input '$name'"
        if required
            error("Required input '$name' could not be accessed - CrossLink not available")
        end
        return nothing
    end
end

"""
    save_output(ctx::NodeContext, name::String, df::DataFrame, description::Union{String, Nothing}=nothing, enable_zero_copy::Bool=true)

Save output dataset.
"""
function save_output(ctx::NodeContext, name::String, df::DataFrame, description::Union{String, Nothing}=nothing, enable_zero_copy::Bool=true)
    # Check if output is defined
    outputs = get(ctx.meta, "outputs", [])
    if !(name in outputs)
        error("Output '$name' is not defined for this node")
    end
    
    # Generate description if not provided
    if description === nothing
        node_id = get(ctx.meta, "node_id", "unknown")
        description = "Output '$name' from node '$node_id'"
    end
    
    # Push to CrossLink
    if ctx.cl !== nothing
        try
            # Use CrossLink for efficient data sharing
            return CrossLink.push_data(ctx.cl, df, name, description=description, arrow_data=enable_zero_copy)
        catch e
            @warn "Failed to save output to CrossLink: $e"
            error("Could not save output '$name'")
        end
    else
        @warn "CrossLink not available, cannot save output '$name'"
        error("Could not save output '$name' - CrossLink not available")
    end
end

"""
    get_param(ctx::NodeContext, name::String, default::Any=nothing)

Get parameter value with optional default.
"""
function get_param(ctx::NodeContext, name::String, default::Any=nothing)
    params = get(ctx.meta, "params", Dict{String, Any}())
    return get(params, name, default)
end

"""
    get_connection(ctx::NodeContext)

Get the CrossLink connection for direct access.
"""
function get_connection(ctx::NodeContext)
    return ctx.cl
end

"""
    close(ctx::NodeContext)

Close the NodeContext and its resources.
"""
function close(ctx::NodeContext)
    if ctx.cl !== nothing
        try
            # Close CrossLink connection
            CrossLink.close(ctx.cl)
        catch e
            @warn "Error closing CrossLink: $e"
        end
    end
end

end # module 