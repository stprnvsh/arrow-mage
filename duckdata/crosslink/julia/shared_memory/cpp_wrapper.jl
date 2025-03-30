"""
C++ bindings wrapper for CrossLink in Julia

This module loads and provides an interface to the C++ implementation of CrossLink
for better cross-language interoperability and performance.
"""
module CrossLinkCppWrapper

using Arrow
using DuckDB
using DataFrames
export CrossLinkCpp, is_cpp_available
export StreamWriter, StreamReader, push_stream, pull_stream

# Flag for C++ binding availability
global _cpp_available = false
global _cpp_lib = nothing

"""
    try_load_cpp()

Try to load the C++ library from various possible locations.
Returns true if successful, false otherwise.
"""
function try_load_cpp()
    global _cpp_available, _cpp_lib
    
    # Return early if already loaded
    if _cpp_available
        return true
    end
    
    try
        # Get potential paths for the library
        project_dir = joinpath(dirname(@__DIR__), "..")
        
        # Different platforms have different library extensions
        lib_name = Sys.iswindows() ? "libcrosslink_jl.dll" : 
                  Sys.isapple() ? "libcrosslink_jl.dylib" : "libcrosslink_jl.so"
        
        # Look in multiple possible locations
        possible_paths = [
            joinpath(@__DIR__, lib_name),
            joinpath(project_dir, "julia", "deps", lib_name),
            joinpath(project_dir, "cpp", "build", lib_name),
            joinpath(project_dir, "deps", lib_name)
        ]
        
        # Try to load the library from each path
        for path in possible_paths
            if isfile(path)
                # Use CxxWrap if available
                try
                    # Check if CxxWrap is available
                    using CxxWrap
                    
                    # Load the library
                    @info "Loading C++ library from $path"
                    _cpp_lib = CxxWrap.load_library(path)
                    
                    # Set flag to true
                    _cpp_available = true
                    return true
                catch e
                    # Try loading with ccall if CxxWrap fails
                    try
                        # Direct ccall loading as fallback
                        _cpp_lib = path
                        
                        # Try a test function to verify the library works
                        # For example, get the version number
                        test_ptr = dlsym(dlopen(_cpp_lib), :crosslink_get_version)
                        if test_ptr != C_NULL
                            _cpp_available = true
                            return true
                        end
                    catch inner_e
                        @warn "Failed to load C++ library with ccall: $inner_e"
                    end
                end
            end
        end
        
        # If we get here, we failed to load the library
        return false
    catch e
        @warn "Error trying to load C++ bindings: $e"
        return false
    end
end

"""
    is_cpp_available()

Check if C++ bindings are available.
"""
function is_cpp_available()
    global _cpp_available
    return _cpp_available
end

"""
    has_shared_memory()

Check if the C++ implementation has shared memory support.
"""
function has_shared_memory()
    if !is_cpp_available()
        return false
    end
    
    # In a real implementation, we would check if shared memory
    # is supported in the C++ implementation
    return true
end

"""
    init_cpp()

Initialize the C++ wrapper.
"""
function init_cpp()
    global _cpp_available
    
    # Try to load C++ bindings
    _cpp_available = try_load_cpp()
    
    return _cpp_available
end

# Initialize on module load
init_cpp()

"""
    CrossLinkCpp

Julia wrapper for the C++ CrossLink implementation.
"""
mutable struct CrossLinkCpp
    db_path::String
    debug::Bool
    cpp_instance::Any  # Either a CxxWrap object or ccall-based handle
    
    """
        CrossLinkCpp(db_path::String="crosslink.duckdb", debug::Bool=false)
        
    Create a new CrossLink instance using C++ bindings.
    """
    function CrossLinkCpp(db_path::String="crosslink.duckdb", debug::Bool=false)
        # Try to load C++ bindings if not already loaded
        if !try_load_cpp()
            error("C++ bindings are not available")
        end
        
        global _cpp_lib
        
        # Create the C++ object based on what loading method worked
        if typeof(_cpp_lib) <: String  # ccall path
            # Create C++ object via ccall
            instance_ptr = ccall(
                dlsym(dlopen(_cpp_lib), :crosslink_create), 
                Ptr{Cvoid}, 
                (Cstring, Bool), 
                db_path, 
                debug
            )
            
            # Create the Julia wrapper
            instance = new(db_path, debug, instance_ptr)
            
            # Set finalizer to clean up C++ resources
            finalizer(x -> ccall(
                dlsym(dlopen(_cpp_lib), :crosslink_destroy),
                Cvoid,
                (Ptr{Cvoid},),
                x.cpp_instance
            ), instance)
            
            return instance
        else  # CxxWrap path
            # Create C++ object via CxxWrap
            cpp_obj = _cpp_lib.CrossLinkCpp(db_path, debug)
            
            # Create the Julia wrapper
            instance = new(db_path, debug, cpp_obj)
            
            return instance
        end
    end
end

"""
    push(cl::CrossLinkCpp, table::Arrow.Table, name::String="", description::String="")

Push an Arrow table to CrossLink via C++ bindings.
"""
function push(cl::CrossLinkCpp, table::Arrow.Table, name::String="", description::String="")
    global _cpp_lib
    
    # Use the appropriate method based on how the library was loaded
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        # Serialize Arrow table to bytes
        io = IOBuffer()
        Arrow.write(io, table)
        arrow_bytes = take!(io)
        
        # Call C++ function
        result_ptr = ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_push),
            Cstring,
            (Ptr{Cvoid}, Ptr{UInt8}, Csize_t, Cstring, Cstring),
            cl.cpp_instance,
            arrow_bytes,
            length(arrow_bytes),
            name,
            description
        )
        
        # Convert result to Julia string
        result = unsafe_string(result_ptr)
        
        # Free the C string
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_free_string),
            Cvoid,
            (Cstring,),
            result_ptr
        )
        
        return result
    else  # CxxWrap path
        # Convert Arrow table to format expected by binding
        io = IOBuffer()
        Arrow.write(io, table)
        arrow_bytes = take!(io)
        
        # Call the C++ method
        return cl.cpp_instance.push(arrow_bytes, name, description)
    end
end

"""
    pull(cl::CrossLinkCpp, identifier::String)

Pull a dataset from CrossLink as an Arrow table via C++ bindings.
"""
function pull(cl::CrossLinkCpp, identifier::String)
    global _cpp_lib
    
    # Use the appropriate method based on how the library was loaded
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        # Call C++ function to get Arrow data
        result_ptr = ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_pull),
            Ptr{UInt8},
            (Ptr{Cvoid}, Cstring, Ptr{Csize_t}),
            cl.cpp_instance,
            identifier,
            Ref{Csize_t}(0)  # Will be filled with data size
        )
        
        # Get the size
        size_ptr = Ref{Csize_t}(0)
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_get_last_data_size),
            Cvoid,
            (Ptr{Cvoid}, Ptr{Csize_t}),
            cl.cpp_instance,
            size_ptr
        )
        
        # Copy and convert data to Arrow table
        data_size = size_ptr[]
        data = unsafe_wrap(Array, result_ptr, data_size)
        io = IOBuffer(copy(data))
        
        # Free the C++ memory
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_free_data),
            Cvoid,
            (Ptr{UInt8},),
            result_ptr
        )
        
        # Read arrow table from memory
        return Arrow.Table(io)
    else  # CxxWrap path
        # Call the C++ method
        result_bytes = cl.cpp_instance.pull(identifier)
        
        # Convert bytes to Arrow table
        io = IOBuffer(result_bytes)
        return Arrow.Table(io)
    end
end

"""
    query(cl::CrossLinkCpp, sql::String)

Execute SQL query via C++ bindings and return results as Arrow table.
"""
function query(cl::CrossLinkCpp, sql::String)
    global _cpp_lib
    
    # Similar implementation as pull but with query method
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        # Call C++ function to get Arrow data
        result_ptr = ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_query),
            Ptr{UInt8},
            (Ptr{Cvoid}, Cstring, Ptr{Csize_t}),
            cl.cpp_instance,
            sql,
            Ref{Csize_t}(0)  # Will be filled with data size
        )
        
        # Get the size
        size_ptr = Ref{Csize_t}(0)
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_get_last_data_size),
            Cvoid,
            (Ptr{Cvoid}, Ptr{Csize_t}),
            cl.cpp_instance,
            size_ptr
        )
        
        # Copy and convert data to Arrow table
        data_size = size_ptr[]
        data = unsafe_wrap(Array, result_ptr, data_size)
        io = IOBuffer(copy(data))
        
        # Free the C++ memory
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_free_data),
            Cvoid,
            (Ptr{UInt8},),
            result_ptr
        )
        
        # Read arrow table from memory
        return Arrow.Table(io)
    else  # CxxWrap path
        # Call the C++ method
        result_bytes = cl.cpp_instance.query(sql)
        
        # Convert bytes to Arrow table
        io = IOBuffer(result_bytes)
        return Arrow.Table(io)
    end
end

"""
    list_datasets(cl::CrossLinkCpp)

List available datasets via C++ bindings.
"""
function list_datasets(cl::CrossLinkCpp)
    global _cpp_lib
    
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        # Call C++ function
        result_ptr = ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_list_datasets),
            Ptr{Ptr{UInt8}},
            (Ptr{Cvoid}, Ptr{Csize_t}),
            cl.cpp_instance,
            Ref{Csize_t}(0)  # Will be filled with count
        )
        
        # Get the count
        count_ptr = Ref{Csize_t}(0)
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_get_list_count),
            Cvoid,
            (Ptr{Cvoid}, Ptr{Csize_t}),
            cl.cpp_instance,
            count_ptr
        )
        
        count = count_ptr[]
        
        # Extract each string
        datasets = String[]
        for i in 1:count
            str_ptr = unsafe_load(result_ptr, i)
            push!(datasets, unsafe_string(str_ptr))
        end
        
        # Free the C++ memory
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_free_string_array),
            Cvoid,
            (Ptr{Ptr{UInt8}}, Csize_t),
            result_ptr,
            count
        )
        
        return datasets
    else  # CxxWrap path
        # Call the C++ method
        return cl.cpp_instance.list_datasets()
    end
end

"""
    register_notification(cl::CrossLinkCpp, callback::Function)

Register a callback for dataset notifications via C++ bindings.
"""
function register_notification(cl::CrossLinkCpp, callback::Function)
    global _cpp_lib
    
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        # Not easily implemented with ccall, would need custom C wrapper
        error("Notification callbacks not supported with ccall interface")
    else  # CxxWrap path
        # Call the C++ method
        return cl.cpp_instance.register_notification(callback)
    end
end

"""
    unregister_notification(cl::CrossLinkCpp, registration_id::String)

Unregister a notification callback via C++ bindings.
"""
function unregister_notification(cl::CrossLinkCpp, registration_id::String)
    global _cpp_lib
    
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_unregister_notification),
            Cvoid,
            (Ptr{Cvoid}, Cstring),
            cl.cpp_instance,
            registration_id
        )
    else  # CxxWrap path
        # Call the C++ method
        cl.cpp_instance.unregister_notification(registration_id)
    end
    
    return nothing
end

"""
    cleanup(cl::CrossLinkCpp)

Clean up resources via C++ bindings.
"""
function cleanup(cl::CrossLinkCpp)
    global _cpp_lib
    
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        ccall(
            dlsym(dlopen(_cpp_lib), :crosslink_cleanup),
            Cvoid,
            (Ptr{Cvoid},),
            cl.cpp_instance
        )
    else  # CxxWrap path
        # Call the C++ method
        cl.cpp_instance.cleanup()
    end
    
    return nothing
end

# StreamWriter class to handle writing batches to a stream
mutable struct StreamWriter
    cpp_writer::Any
    schema::Arrow.Schema
    stream_id::String
    
    function StreamWriter(cpp_writer, schema, stream_id)
        instance = new(cpp_writer, schema, stream_id)
        
        # Set finalizer to clean up C++ resources if needed
        if has_shared_memory()
            finalizer(x -> try_close(x), instance)
        end
        
        return instance
    end
end

# StreamReader class to handle reading batches from a stream
mutable struct StreamReader
    cpp_reader::Any
    schema::Union{Arrow.Schema, Nothing}
    stream_id::String
    
    function StreamReader(cpp_reader, schema, stream_id)
        instance = new(cpp_reader, schema, stream_id)
        
        # Set finalizer to clean up C++ resources if needed
        if has_shared_memory()
            finalizer(x -> try_close(x), instance)
        end
        
        return instance
    end
end

# Helper functions for StreamWriter
function write_batch(writer::StreamWriter, batch::Arrow.Table)
    if typeof(writer.cpp_writer) <: Ptr{Cvoid}  # ccall path
        error("Not yet implemented for direct ccall")
    else  # CxxWrap path
        # Serialize the batch to send to C++
        io = IOBuffer()
        Arrow.write(io, batch)
        batch_bytes = take!(io)
        
        # Call the C++ method
        writer.cpp_writer.write_batch(batch_bytes)
        return nothing
    end
end

function close(writer::StreamWriter)
    if typeof(writer.cpp_writer) <: Ptr{Cvoid}  # ccall path
        error("Not yet implemented for direct ccall")
    else  # CxxWrap path
        writer.cpp_writer.close()
        return nothing
    end
end

function try_close(writer::StreamWriter)
    try
        close(writer)
    catch e
        # Ignore errors during finalization
    end
end

function get_stream_id(writer::StreamWriter)
    return writer.stream_id
end

function get_schema(writer::StreamWriter)
    return writer.schema
end

# Helper functions for StreamReader
function read_next_batch(reader::StreamReader)
    if typeof(reader.cpp_reader) <: Ptr{Cvoid}  # ccall path
        error("Not yet implemented for direct ccall")
    else  # CxxWrap path
        # Get the batch bytes from C++
        batch_bytes = reader.cpp_reader.read_next_batch()
        
        # If we got an empty array, there are no more batches
        if length(batch_bytes) == 0
            return nothing
        end
        
        # Deserialize the batch
        io = IOBuffer(batch_bytes)
        return Arrow.Table(io)
    end
end

function close(reader::StreamReader)
    if typeof(reader.cpp_reader) <: Ptr{Cvoid}  # ccall path
        error("Not yet implemented for direct ccall")
    else  # CxxWrap path
        reader.cpp_reader.close()
        return nothing
    end
end

function try_close(reader::StreamReader)
    try
        close(reader)
    catch e
        # Ignore errors during finalization
    end
end

function get_stream_id(reader::StreamReader)
    return reader.stream_id
end

function get_schema(reader::StreamReader)
    if reader.schema === nothing
        if typeof(reader.cpp_reader) <: Ptr{Cvoid}  # ccall path
            error("Not yet implemented for direct ccall")
        else  # CxxWrap path
            # Get the schema bytes from C++
            schema_bytes = reader.cpp_reader.schema()
            
            # Deserialize the schema
            io = IOBuffer(schema_bytes)
            reader.schema = Arrow.Table(io).schema
        end
    end
    
    return reader.schema
end

# Define iterator interface for StreamReader
function Base.iterate(reader::StreamReader, state=nothing)
    batch = read_next_batch(reader)
    if batch === nothing
        return nothing
    else
        return (batch, nothing)
    end
end

function Base.IteratorSize(::Type{StreamReader})
    return Base.SizeUnknown()
end

function Base.eltype(::Type{StreamReader})
    return Arrow.Table
end

"""
    push_stream(cl::CrossLinkCpp, schema::Arrow.Schema, name::String="")

Start a new stream with the given schema using C++ bindings.
Returns a tuple of (stream_id, StreamWriter).
"""
function push_stream(cl::CrossLinkCpp, schema::Arrow.Schema, name::String="")
    if !has_shared_memory()
        error("C++ shared memory implementation is not available")
    end
    
    global _cpp_lib
    
    # Use the appropriate method based on how the library was loaded
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        error("Not yet implemented for direct ccall")
    else  # CxxWrap path
        # Serialize the schema to send to C++
        io = IOBuffer()
        Arrow.write(io, Arrow.Table(; a=Int64[])) # Empty table with schema
        schema_bytes = take!(io)
        
        # Call the C++ method
        result = cl.cpp_instance.push_stream(schema_bytes, name)
        
        # Extract stream_id and writer
        stream_id = result[1]
        cpp_writer = result[2]
        
        # Create Julia StreamWriter wrapper
        writer = StreamWriter(cpp_writer, schema, stream_id)
        
        return (stream_id, writer)
    end
end

"""
    pull_stream(cl::CrossLinkCpp, stream_id::String)

Connect to an existing stream with the given ID using C++ bindings.
Returns a StreamReader.
"""
function pull_stream(cl::CrossLinkCpp, stream_id::String)
    if !has_shared_memory()
        error("C++ shared memory implementation is not available")
    end
    
    global _cpp_lib
    
    # Use the appropriate method based on how the library was loaded
    if typeof(cl.cpp_instance) <: Ptr{Cvoid}  # ccall path
        error("Not yet implemented for direct ccall")
    else  # CxxWrap path
        # Call the C++ method
        cpp_reader = cl.cpp_instance.pull_stream(stream_id)
        
        # Create Julia StreamReader wrapper
        # We'll get the schema later when needed
        reader = StreamReader(cpp_reader, nothing, stream_id)
        
        return reader
    end
end

# Try to load C++ bindings when module is loaded
try_load_cpp()

end # module CrossLinkCppWrapper 