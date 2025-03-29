# Import necessary modules at top level
using SharedArrays

# Flag to indicate whether shared memory is available
global _SHARED_MEMORY_AVAILABLE = false

# Preferred shared memory module to use
global _SHARED_MEMORY_MODULE = :None

# Dictionary of known shared memory regions
global _SHARED_MEMORY_REGIONS = Dict{String, String}()

# Initialize shared memory capabilities
function init_shared_memory()
    global _SHARED_MEMORY_AVAILABLE = false
    global _SHARED_MEMORY_MODULE = :None
    
    # Try to detect available shared memory modules
    try
        # If C++ wrapper is available, use it
        if @isdefined(CrossLinkCppWrapper) && CrossLinkCppWrapper.has_shared_memory()
            _SHARED_MEMORY_MODULE = :CppWrapper
            _SHARED_MEMORY_AVAILABLE = true
            return true
        end
    catch e
        # Continue to other options
        @warn "C++ wrapper not available for shared memory: $e"
    end
    
    # Try to use SharedArrays
    try
        _SHARED_MEMORY_MODULE = :SharedArrays
        _SHARED_MEMORY_AVAILABLE = true
        return true
    catch e
        # Continue to other options
        @warn "SharedArrays not available for shared memory: $e"
    end
    
    # If all else fails, use memory mapping
    try
        _SHARED_MEMORY_MODULE = :Mmap
        _SHARED_MEMORY_AVAILABLE = true
        return true
    catch e
        # Unable to initialize shared memory
        _SHARED_MEMORY_AVAILABLE = false
        @warn "Memory mapping not available for shared memory: $e"
        return false
    end
end

# Initialize on module load
init_shared_memory()

"""
    check_shared_memory_available()

Check if shared memory is available in the Julia environment.
"""
function check_shared_memory_available()
    try
        # In a production environment, you'd check for proper shared memory support
        # For this implementation, we'll just check for Arrow.jl which is required
        # and use memory-mapped files as a shared memory simulation
        isdefined(Main, :Arrow)
        return true
    catch
        return false
    end
end

# Global variable to track additional shared memory features
# Renamed to avoid duplicate declaration
global _ADDITIONAL_SHARED_MEMORY_FEATURES = check_shared_memory_available()

"""
    setup_shared_memory(cl::CrossLinkManager, data::Union{DataFrame, Arrow.Table}, identifier::String)

Set up shared memory for data and return info about the shared memory region.
"""
function setup_shared_memory(cl::CrossLinkManager, data, identifier::String)
    # Check if shared memory is available
    if !_SHARED_MEMORY_AVAILABLE
        cl.debug && @info "Shared memory not available"
        return nothing
    end
    
    try
        # Different methods to set up shared memory
        if _SHARED_MEMORY_MODULE == :CppWrapper && cl.cpp_available && cl.cpp_instance !== nothing
            try
                # Convert to Arrow Table if needed
                arrow_table = data isa Arrow.Table ? data : Arrow.Table(data)
                
                # Use C++ implementation
                shared_key = CrossLinkCppWrapper.setup_shared_memory(cl.cpp_instance, arrow_table, identifier)
                
                cl.debug && @info "Set up shared memory using C++ wrapper with key: $(shared_key)"
                return Dict("shared_memory_key" => shared_key, "module" => "cpp")
            catch e
                cl.debug && @warn "Failed to set up shared memory via C++: $e"
                # Continue to other methods instead of failing
            end
        elseif _SHARED_MEMORY_MODULE == :SharedArrays
            # Not implemented
            cl.debug && @info "SharedArrays shared memory setup not implemented"
        elseif _SHARED_MEMORY_MODULE == :Mmap
            # Not implemented
            cl.debug && @info "Mmap shared memory setup not implemented"
        end
        
        # If we got here, no method succeeded
        return nothing
    catch e
        cl.debug && @warn "Error setting up shared memory: $e"
        return nothing
    end
end

"""
    get_from_shared_memory(cl::CrossLinkManager, shared_memory_key::Union{String, Missing, Nothing})

Get an Arrow table from shared memory. If shared memory fails or key is missing, 
returns nothing so the caller can fall back to other methods.
"""
function get_from_shared_memory(cl::CrossLinkManager, shared_memory_key::Union{String, Missing, Nothing})
    # Handle missing or nothing case
    if shared_memory_key === missing || shared_memory_key === nothing
        cl.debug && @info "Shared memory key is missing or nothing, will fall back to standard queries"
        return nothing
    end
    
    # Check if shared memory is available
    if !_SHARED_MEMORY_AVAILABLE
        cl.debug && @info "Shared memory not available, will fall back to standard queries"
        return nothing
    end
    
    try
        # This is just a placeholder - actual shared memory implementation
        # would depend on OS and available libraries
        if _SHARED_MEMORY_MODULE == :SharedArrays
            # Use SharedArrays.jl approach
            cl.debug && @info "Using SharedArrays for shared memory access"
            # Not implemented
        elseif _SHARED_MEMORY_MODULE == :Mmap
            # Use memory mapping approach
            cl.debug && @info "Using Mmap for shared memory access"
            # Not implemented
        elseif _SHARED_MEMORY_MODULE == :CppWrapper
            # Use C++ wrapper
            cl.debug && @info "Using C++ wrapper for shared memory access"
            if cl.cpp_available && cl.cpp_instance !== nothing
                try
                    # Get the shared memory via C++ implementation
                    result = CrossLinkCppWrapper.get_shared_memory(cl.cpp_instance, shared_memory_key)
                    if result !== nothing
                        return result
                    else
                        cl.debug && @info "Shared memory key not found: $shared_memory_key, falling back to standard queries"
                    end
                catch e
                    cl.debug && @warn "Failed to get shared memory via C++: $e, falling back to standard queries"
                end
            else
                cl.debug && @info "C++ not available for shared memory access, falling back to standard queries"
            end
        end
        
        # If we got here, no method succeeded
        cl.debug && @info "No shared memory method succeeded for key: $shared_memory_key, falling back to standard queries"
        return nothing
    catch e
        cl.debug && @warn "Error accessing shared memory: $e, falling back to standard queries"
        return nothing
    end
end

"""
    fallback_to_duckdb(cl::CrossLinkManager, identifier::String)

Fallback to using DuckDB when shared memory access fails.
"""
function fallback_to_duckdb(cl::CrossLinkManager, identifier::String)
    cl.debug && @info "Falling back to DuckDB for identifier: $identifier"
    
    try
        # Connect to DuckDB
        con = DuckDB.connect(cl.db_path)
        
        # Query the data
        result = DBInterface.execute(con, "SELECT * FROM $identifier")
        
        # Convert to Arrow table
        return Arrow.Table(result)
    catch e
        cl.debug && @warn "Error in DuckDB fallback: $e"
        return nothing
    end
end

"""
    cleanup_all_shared_memory(db_path::String="crosslink.duckdb", force::Bool=false)

Clean up all shared memory files in the crosslink_shared directory.

This function can be called explicitly to clean up shared memory resources from previous runs
that might not have been properly deleted.

# Arguments
- `db_path`: Path to the DuckDB database file to determine the shared memory directory
- `force`: Force deletion even if files appear to be in use

# Returns
- Number of files removed
"""
function cleanup_all_shared_memory(db_path::String="crosslink.duckdb", force::Bool=false)
    try
        # Get the shared directory
        shared_dir = joinpath(dirname(abspath(db_path)), "crosslink_shared")
        
        # Check if directory exists
        if !isdir(shared_dir)
            return 0
        end
        
        # Count of removed files
        removed_count = 0
        
        # Remove all .shared files
        for filename in readdir(shared_dir)
            if endswith(filename, ".shared")
                file_path = joinpath(shared_dir, filename)
                try
                    # Check if safe to delete if force=false
                    if force || is_file_safe_to_delete(file_path)
                        rm(file_path)
                        removed_count += 1
                    end
                catch e
                    @warn "Failed to remove shared memory file $file_path: $e"
                end
            end
        end
        
        # Try to remove the directory if it's empty
        try
            if isempty(readdir(shared_dir))
                rm(shared_dir)
            end
        catch
            # Ignore errors when removing directory
        end
        
        return removed_count
    catch e
        @warn "Error cleaning up shared memory files: $e"
        return 0
    end
end

"""
    cleanup_shared_memory(cl::CrossLinkManager, shared_memory_key::String)

Clean up a shared memory region.
"""
function cleanup_shared_memory(cl::CrossLinkManager, shared_memory_key::Union{String, Missing, Nothing})
    # Handle missing or nothing case
    if shared_memory_key === missing || shared_memory_key === nothing
        return
    end
    
    # Check if shared memory is available
    if !_SHARED_MEMORY_AVAILABLE
        return
    end
    
    try
        # Different methods to clean up shared memory
        if _SHARED_MEMORY_MODULE == :CppWrapper && cl.cpp_available && cl.cpp_instance !== nothing
            try
                # Use C++ implementation
                CrossLinkCppWrapper.cleanup_shared_memory(cl.cpp_instance, shared_memory_key)
                cl.debug && @info "Cleaned up shared memory using C++ wrapper: $(shared_memory_key)"
            catch e
                cl.debug && @warn "Failed to clean up shared memory via C++: $e"
            end
        elseif _SHARED_MEMORY_MODULE == :SharedArrays
            # Not implemented
        elseif _SHARED_MEMORY_MODULE == :Mmap
            # Not implemented
        end
    catch e
        cl.debug && @warn "Error cleaning up shared memory: $e"
    end
end

"""
    safe_shared_memory_get(cl::CrossLinkManager, identifier::String)

Safely get data from shared memory with proper fallback to DuckDB.
"""
function safe_shared_memory_get(cl::CrossLinkManager, shared_memory_key::Union{String, Missing, Nothing}, identifier::String)
    # Try shared memory first
    result = get_from_shared_memory(cl, shared_memory_key)
    
    # If shared memory fails, fall back to DuckDB
    if result === nothing
        cl.debug && @info "Shared memory access failed for $identifier, falling back to DuckDB"
        result = fallback_to_duckdb(cl, identifier)
    end
    
    return result
end 