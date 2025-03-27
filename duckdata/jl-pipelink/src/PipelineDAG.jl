"""
PipelineDAG: Directed Acyclic Graph functionality for PipeLink

This module provides functionality for building, analyzing, and executing pipelines
as Directed Acyclic Graphs (DAGs).
"""
module PipelineDAG

using YAML
using Dates
using DataFrames
using LightGraphs
using MetaGraphs
using Printf

export load_pipeline, validate_pipeline, build_dependency_graph, get_execution_order
export run_pipeline, run_node, resolve_path

"""
    load_pipeline(pipeline_file::String)

Load pipeline configuration from YAML file.

Arguments:
- `pipeline_file`: Path to pipeline YAML file

Returns:
    Dict containing the pipeline configuration
"""
function load_pipeline(pipeline_file::String)
    if !isfile(pipeline_file)
        error("Pipeline file not found: $pipeline_file")
    end
    
    return YAML.load_file(pipeline_file)
end

"""
    is_path_absolute(path::String)

Check if a file path is absolute.

Arguments:
- `path`: Path to check

Returns:
    Boolean indicating whether the path is absolute
"""
function is_path_absolute(path::String)
    if Sys.iswindows()
        # Windows: check for drive letter or UNC path
        return occursin(r"^[A-Za-z]:|^\\\\", path)
    else
        # Unix-like: check for leading slash
        return startswith(path, "/")
    end
end

"""
    resolve_path(base_dir::String, path::String)

Resolve a path relative to a base directory.

Arguments:
- `base_dir`: Base directory
- `path`: Path to resolve

Returns:
    Absolute path
"""
function resolve_path(base_dir::String, path::String)
    if is_path_absolute(path)
        return path
    else
        return normpath(joinpath(base_dir, path))
    end
end

"""
    validate_pipeline(pipeline::Dict)

Validate pipeline configuration.

Arguments:
- `pipeline`: Pipeline configuration dict

Throws:
    Error if validation fails
"""
function validate_pipeline(pipeline::Dict)
    # Check required fields
    if !haskey(pipeline, "name")
        error("Pipeline configuration missing required field: name")
    end
    
    if !haskey(pipeline, "nodes") || length(pipeline["nodes"]) == 0
        error("Pipeline configuration missing required field: nodes")
    end
    
    # Validate each node
    node_ids = String[]
    
    for node in pipeline["nodes"]
        # Check required fields
        if !haskey(node, "id")
            error("Node missing required field: id")
        end
        
        # Check for duplicate IDs
        if node["id"] in node_ids
            error("Duplicate node ID: $(node["id"])")
        end
        push!(node_ids, node["id"])
        
        if !haskey(node, "language")
            error("Node '$(node["id"])' missing required field: language")
        end
        
        # Validate script field for code nodes
        if node["language"] in ["python", "r", "julia"]
            if !haskey(node, "script")
                error("Node '$(node["id"])' missing required field: script")
            end
        end
        
        # Validate data nodes
        if node["language"] == "data"
            if !haskey(node, "connection")
                error("Data node '$(node["id"])' missing required field: connection")
            end
            if !haskey(node["connection"], "type")
                error("Data node '$(node["id"])' connection missing required field: type")
            end
        end
        
        # Validate depends_on if present
        if haskey(node, "depends_on")
            depends_on = node["depends_on"]
            if !(typeof(depends_on) <: String) && !(typeof(depends_on) <: Vector{String})
                error("Node '$(node["id"])' has invalid depends_on format. Must be string or array of strings")
            end
        end
    end
    
    return nothing
end

"""
    build_dependency_graph(pipeline::Dict)

Build a directed acyclic graph from pipeline configuration.

Arguments:
- `pipeline`: Pipeline configuration dict

Returns:
    MetaDiGraph object representing the pipeline DAG
"""
function build_dependency_graph(pipeline::Dict)
    nodes = pipeline["nodes"]
    num_nodes = length(nodes)
    
    # Create lookup from node ID to index
    node_id_to_idx = Dict{String, Int}()
    for (i, node) in enumerate(nodes)
        node_id_to_idx[node["id"]] = i
    end
    
    # Create a directed graph
    g = MetaDiGraph(SimpleDigraph(num_nodes))
    
    # Add metadata to nodes
    for (i, node) in enumerate(nodes)
        set_prop!(g, i, :id, node["id"])
        
        # Add all node properties as metadata
        for (key, value) in node
            if key != "id"  # id is already set as a property
                set_prop!(g, i, Symbol(key), value)
            end
        end
    end
    
    # Add edges for dependencies
    for node in nodes
        node_idx = node_id_to_idx[node["id"]]
        
        # Check for explicit dependencies first
        if haskey(node, "depends_on")
            # Handle both single string and array formats
            depends_on = node["depends_on"]
            deps = (typeof(depends_on) <: String) ? [depends_on] : depends_on
            
            for dependency in deps
                if !haskey(node_id_to_idx, dependency)
                    error("Node '$(node["id"])' depends on non-existent node '$dependency'")
                end
                dep_idx = node_id_to_idx[dependency]
                add_edge!(g, dep_idx, node_idx)
            end
        end
        
        # Get inputs (for backward compatibility and implicit dependencies)
        inputs = get(node, "inputs", String[])
        
        # Find nodes that produce these inputs
        for input_name in inputs
            # Find the node that produces this output
            producers = String[]
            for producer in nodes
                outputs = get(producer, "outputs", String[])
                if input_name in outputs
                    push!(producers, producer["id"])
                    # Add edge from producer to consumer
                    producer_idx = node_id_to_idx[producer["id"]]
                    add_edge!(g, producer_idx, node_idx)
                end
            end
            
            if isempty(producers) && !isempty(inputs)
                @warn "Input '$input_name' for node '$(node["id"])' has no producer."
            end
        end
    end
    
    # Check for cycles
    if !is_acyclic(g)
        error("Pipeline has circular dependencies.")
    end
    
    return g
end

"""
    get_execution_order(g::MetaDiGraph; only_nodes=nothing, start_from=nothing)

Get execution order for nodes based on topological sort.

Arguments:
- `g`: MetaDiGraph representing the pipeline
- `only_nodes`: Only execute these nodes (and their dependencies)
- `start_from`: Start execution from this node

Returns:
    Vector of node IDs in execution order
"""
function get_execution_order(g::MetaDiGraph; only_nodes=nothing, start_from=nothing)
    # Get topological sort (execution order)
    topo_indices = topological_sort_by_dfs(g)
    execution_order = [get_prop(g, idx, :id) for idx in topo_indices]
    
    # Filter nodes if specified
    if only_nodes !== nothing
        # Find all dependencies for the specified nodes
        required_nodes = Set(only_nodes)
        
        for node in only_nodes
            # Find node index
            node_idx = 0
            for i in vertices(g)
                if get_prop(g, i, :id) == node
                    node_idx = i
                    break
                end
            end
            
            if node_idx == 0
                error("Node not found: $node")
            end
            
            # Get all ancestors (dependencies) using BFS
            ancestors = Int[]
            for v in vertices(g)
                if has_path(g, v, node_idx)
                    push!(ancestors, v)
                end
            end
            
            # Add ancestor node IDs to required_nodes
            for ancestor_idx in ancestors
                push!(required_nodes, get_prop(g, ancestor_idx, :id))
            end
        end
        
        # Filter execution order
        execution_order = filter(n -> n in required_nodes, execution_order)
    end
    
    # Start from a specific node if specified
    if start_from !== nothing
        if !(start_from in execution_order)
            error("Start node not found: $start_from")
        end
        
        # Get index of start node
        start_idx = findfirst(n -> n == start_from, execution_order)
        
        # Skip nodes before start node
        execution_order = execution_order[start_idx:end]
    end
    
    return execution_order
end

"""
    run_pipeline(pipeline_file::String; only_nodes=nothing, start_from=nothing, verbose=false)

Run a pipeline from a YAML file.

Arguments:
- `pipeline_file`: Path to pipeline YAML file
- `only_nodes`: Only execute these nodes (and their dependencies)
- `start_from`: Start execution from this node
- `verbose`: Enable verbose output

Returns:
    Boolean indicating success or failure
"""
function run_pipeline(pipeline_file::String; only_nodes=nothing, start_from=nothing, verbose=false)
    if verbose
        @info "Loading pipeline from $pipeline_file"
    end
    
    # Load pipeline configuration
    pipeline = load_pipeline(pipeline_file)
    
    # Set up working directory
    working_dir = dirname(abspath(pipeline_file))
    if haskey(pipeline, "working_dir")
        if is_path_absolute(pipeline["working_dir"])
            working_dir = pipeline["working_dir"]
        else
            working_dir = normpath(joinpath(working_dir, pipeline["working_dir"]))
        end
    end
    
    # Validate pipeline configuration
    validate_pipeline(pipeline)
    
    # Build dependency graph
    g = build_dependency_graph(pipeline)
    
    # Get execution order
    execution_order = get_execution_order(g; only_nodes=only_nodes, start_from=start_from)
    
    if verbose
        @info "Execution order: $(join(execution_order, ", "))"
    end
    
    # Determine pipeline database path
    db_path = get(pipeline, "db_path", "pipeline.duckdb")
    db_path = resolve_path(working_dir, db_path)
    
    if verbose
        @info "Using database at $db_path"
    end
    
    # Generate a unique pipeline ID
    pipeline_id = string(uuid4())
    
    # Run nodes in order
    for node_id in execution_order
        # Find node config
        node_config = nothing
        for node in pipeline["nodes"]
            if node["id"] == node_id
                node_config = node
                break
            end
        end
        
        if node_config === nothing
            error("Node not found in config: $node_id")
        end
        
        # Run the node
        success = run_node(node_config, pipeline, working_dir, pipeline_id, verbose)
        
        if !success
            error("Pipeline execution failed at node: $node_id")
            return false
        end
    end
    
    if verbose
        @info "Pipeline execution completed successfully."
    end
    
    return true
end

"""
    run_node(node_config::Dict, pipeline_config::Dict, working_dir::String, pipeline_id::String, verbose::Bool=false)

Run a single pipeline node.

Arguments:
- `node_config`: Node configuration
- `pipeline_config`: Full pipeline configuration
- `working_dir`: Working directory
- `pipeline_id`: Pipeline monitoring ID
- `verbose`: Enable verbose output

Returns:
    Boolean indicating success or failure
"""
function run_node(node_config::Dict, pipeline_config::Dict, working_dir::String, pipeline_id::String, verbose::Bool=false)
    node_id = node_config["id"]
    language = node_config["language"]
    
    if verbose
        @info "Running node '$node_id' ($language)"
    end
    
    # Create metadata file
    meta_file = tempname() * ".yml"
    
    # Prepare metadata
    metadata = Dict(
        "node_id" => node_id,
        "pipeline_name" => pipeline_config["name"],
        "inputs" => get(node_config, "inputs", String[]),
        "outputs" => get(node_config, "outputs", String[]),
        "params" => get(node_config, "params", Dict()),
        "db_path" => get(pipeline_config, "db_path", "crosslink.duckdb"),
        "language" => language
    )
    
    # Add connection details for data nodes
    if language == "data"
        metadata["connection"] = get(node_config, "connection", Dict())
    end
    
    # Write metadata
    open(meta_file, "w") do io
        YAML.write(io, metadata)
    end
    
    success = true
    try
        # Run script based on language
        if language == "python"
            script_path = resolve_path(working_dir, node_config["script"])
            cmd = `python $script_path $meta_file`
        elseif language == "r"
            script_path = resolve_path(working_dir, node_config["script"])
            cmd = `Rscript $script_path $meta_file`
        elseif language == "julia"
            script_path = resolve_path(working_dir, node_config["script"])
            cmd = `julia $script_path $meta_file`
        elseif language == "data"
            # For data nodes, we would need to implement a data connector
            @warn "Data nodes are not yet implemented in Julia version"
            return false
        else
            error("Unsupported language: $language")
        end
        
        # Run command
        if verbose
            result = run(cmd)
            success = result.exitcode == 0
        else
            # Redirect output if not verbose
            output = IOBuffer()
            result = run(pipeline(cmd, stdout=output, stderr=output))
            success = result.exitcode == 0
            
            # If failed, show the output
            if !success
                @error "Node '$node_id' failed with exit code $(result.exitcode)"
                @error String(take!(output))
            end
        end
        
    catch e
        success = false
        @error "Error running node '$node_id': $(sprint(showerror, e))"
    finally
        # Clean up metadata file
        rm(meta_file, force=true)
    end
    
    return success
end

end # module 