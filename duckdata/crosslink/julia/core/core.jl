using Arrow
import Arrow: Schema
using DuckDB
using DataFrames
using Dates
using UUIDs

# Store instances for connection pooling
const _INSTANCES = Dict{String, Any}()

"""
    CrossLinkManager

Manager for cross-language data sharing with zero-copy optimization.
"""
mutable struct CrossLinkManager
    conn::DuckDB.DB
    db_path::String
    debug::Bool
    metadata_cache::Dict{String, Any}
    tables_initialized::Bool
    arrow_available::Bool
    arrow_files::Vector{String}  # Track created arrow files for cleanup
    shared_memory_files::Vector{String}  # Track created shared memory files for cleanup
    cpp_available::Bool  # Whether C++ bindings are available
    cpp_instance::Any    # C++ instance if available
    
    """
        CrossLinkManager(db_path::String, debug::Bool=false)
    
    Create a CrossLink manager with connection to specified DuckDB database.
    """
    function CrossLinkManager(db_path::String, debug::Bool=false)
        # Check for cached instance
        abs_path = abspath(db_path)
        if haskey(_INSTANCES, abs_path)
            return _INSTANCES[abs_path]
        end
        
        # Try to use C++ implementation if available
        cpp_available = false
        cpp_instance = nothing
        
        # Load the C++ wrapper module
        try
            # Check if the CPP wrapper module already exists in this namespace
            if !@isdefined(CrossLinkCppWrapper)
                # Include the cpp_wrapper.jl file
                include(joinpath(dirname(@__DIR__), "shared_memory", "cpp_wrapper.jl"))
            end
            
            # Check if C++ is available
            cpp_available = CrossLinkCppWrapper.is_cpp_available()
            
            if cpp_available
                # Create C++ instance
                cpp_instance = CrossLinkCppWrapper.CrossLinkCpp(db_path, debug)
                if debug
                    println("Using C++ implementation for improved performance")
                end
            else
                if debug
                    println("C++ implementation not available, using pure Julia implementation")
                end
            end
        catch e
            if debug
                println("Failed to load C++ bindings: $e")
            end
            cpp_available = false
        end
        
        # Create directory if it doesn't exist
        db_dir = dirname(db_path)
        if !isempty(db_dir) && !isdir(db_dir)
            mkpath(db_dir)
        end
        
        # Connect to database
        conn = DuckDB.DB(db_path)
        
        # Try to optimize DuckDB settings for performance
        try
            # Enable parallel query execution
            DuckDB.execute(conn, "PRAGMA force_parallelism")
            
            # Enable SIMD optimization
            DuckDB.execute(conn, "PRAGMA enable_optimizer")
            
            # Set memory limit if needed
            # DuckDB.execute(conn, "PRAGMA memory_limit='8GB'")
            
            if debug
                println("Configured DuckDB with optimized settings")
            end
        catch e
            if debug
                @warn "Failed to configure DuckDB performance settings: $e"
            end
        end
        
        # Check if Arrow is available - now a dependency so should always be true
        arrow_available = true
        
        # Create instance
        instance = new(
            conn, 
            db_path, 
            debug, 
            Dict{String, Any}(), 
            false, 
            arrow_available, 
            String[], 
            String[],
            cpp_available,
            cpp_instance
        )
        
        # Cache for reuse
        _INSTANCES[abs_path] = instance
        
        # Initialize metadata tables
        setup_metadata_tables(instance)
        
        return instance
    end
end

# Simple dummy implementation of setup_metadata_tables to enable initialization
# This will be replaced by the actual implementation in metadata.jl
function setup_metadata_tables(cl::CrossLinkManager)
    if cl.tables_initialized
        return
    end
    
    # This will be properly implemented in metadata.jl
    cl.tables_initialized = true
end 

"""
    push_stream(cl::CrossLinkManager, schema::Arrow.Schema, name::String="")

Start a new stream with the given schema.
Returns a tuple of (stream_id, writer) where writer can be used to write batches of data.

This method requires C++ implementation to be available.
"""
function push_stream(cl::CrossLinkManager, schema::Arrow.Schema, name::String="")
    # Check if C++ implementation is available
    if !cl.cpp_available || cl.cpp_instance === nothing
        error("Streaming requires C++ implementation to be available")
    end
    
    # Use the C++ instance's push_stream method
    return CrossLinkCppWrapper.push_stream(cl.cpp_instance, schema, name)
end

"""
    pull_stream(cl::CrossLinkManager, stream_id::String)

Connect to an existing stream with the given ID.
Returns a reader (`Arrow.RecordBatchStream`) that can be used to read batches of data.

This method requires C++ implementation to be available.
"""
function pull_stream(cl::CrossLinkManager, stream_id::String)
    # Check if C++ implementation is available
    if !cl.cpp_available || cl.cpp_instance === nothing
        error("Streaming requires C++ implementation to be available")
    end
    
    # Use the C++ instance's pull_stream method - returns uintptr_t (UInt in Julia)
    stream_ptr_uint = CrossLinkCppWrapper.pull_stream(cl.cpp_instance, stream_id)
    stream_ptr = Ptr{Arrow.ArrowArrayStream}(stream_ptr_uint)

    # Check for null pointer
    if stream_ptr == C_NULL
        error("C++ pull_stream returned a NULL pointer for stream_id: $stream_id")
    end

    cl.debug && println("Received ArrowArrayStream pointer from C++ pull_stream: $stream_ptr")

    # Create an Arrow.RecordBatchStream from the pointer.
    # Arrow.jl will manage the stream lifecycle and call the release callback.
    # The caller of this Julia `pull_stream` function will be responsible for iterating
    # this stream and closing it when done (or letting it be garbage collected).
    reader = Arrow.RecordBatchStream(stream_ptr)

    return reader
end

# Re-export helper functions for StreamWriter (remains the same)
write_batch(writer, batch) = CrossLinkCppWrapper.write_batch(writer, batch)
close(stream::CrossLinkCppWrapper.StreamWriter) = CrossLinkCppWrapper.close(stream) # Only for writer now
get_stream_id(stream::CrossLinkCppWrapper.StreamWriter) = CrossLinkCppWrapper.get_stream_id(stream) # Only for writer now
get_schema(stream::CrossLinkCppWrapper.StreamWriter) = CrossLinkCppWrapper.get_schema(stream) # Only for writer now

# Remove direct re-exports for StreamReader methods, as interaction now happens via Arrow.RecordBatchStream
# read_next_batch(reader) = CrossLinkCppWrapper.read_next_batch(reader)
# close(stream::CrossLinkCppWrapper.StreamReader}) = CrossLinkCppWrapper.close(stream)
# get_stream_id(stream::CrossLinkCppWrapper.StreamReader) = CrossLinkCppWrapper.get_stream_id(stream)
# get_schema(stream::CrossLinkCppWrapper.StreamReader) = CrossLinkCppWrapper.get_schema(stream) 