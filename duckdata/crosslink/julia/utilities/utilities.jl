"""
    is_file_safe_to_delete(file_path::String)

Check if a file is safe to delete (not being used by other processes).
"""
function is_file_safe_to_delete(file_path::String)
    if !isfile(file_path)
        return true
    end
    
    # Try to open the file in write mode to check if it's locked
    try
        open(file_path, "r+") do io
            # If we can open it for writing, it's probably not in use
            return true
        end
    catch e
        # If we can't open the file, it's probably in use
        return false
    end
    
    # If we get here, the file is likely safe to delete
    return true
end

"""
    close(cl::CrossLinkManager)

Close the database connection and clean up resources.
"""
function close(cl::CrossLinkManager)
    # Remove from instance cache
    if haskey(_INSTANCES, abspath(cl.db_path))
        delete!(_INSTANCES, abspath(cl.db_path))
    end
    
    # Clear memory-mapped table cache
    empty!(_MMAPPED_TABLES)
    
    # Clean up shared memory regions
    for shared_file in cl.shared_memory_files
        try
            if isfile(shared_file)
                if is_file_safe_to_delete(shared_file)
                    rm(shared_file)
                    cl.debug && @info "Removed shared memory file: $shared_file"
                else
                    cl.debug && @info "Skipping deletion of shared memory file as it's in use: $shared_file"
                end
            end
        catch e
            @warn "Failed to remove shared memory file $shared_file: $e"
        end
    end
    
    # Clear the shared memory tracking array
    empty!(cl.shared_memory_files)
    
    # Clean up arrow files created by this instance
    for arrow_file in cl.arrow_files
        try
            if isfile(arrow_file)
                if is_file_safe_to_delete(arrow_file)
                    rm(arrow_file)
                    cl.debug && @info "Removed temporary Arrow file: $arrow_file"
                else
                    cl.debug && @info "Skipping deletion of Arrow file as it's in use: $arrow_file"
                end
            end
        catch e
            @warn "Failed to remove temporary Arrow file $arrow_file: $e"
        end
    end
    
    # Clear the tracking array
    empty!(cl.arrow_files)
    
    # Clean up the crosslink_mmaps directory
    try
        mmaps_dir = joinpath(dirname(cl.db_path), "crosslink_mmaps")
        if isdir(mmaps_dir)
            # Try to remove all .arrow files that are safe to delete
            for filename in readdir(mmaps_dir)
                if endswith(filename, ".arrow")
                    file_path = joinpath(mmaps_dir, filename)
                    try
                        if is_file_safe_to_delete(file_path)
                            rm(file_path)
                            cl.debug && @info "Removed mmapped Arrow file: $file_path"
                        end
                    catch e
                        @warn "Failed to remove mmapped Arrow file $file_path: $e"
                    end
                end
            end
            
            # Try to remove the directory if it's empty
            try
                if isempty(readdir(mmaps_dir))
                    rm(mmaps_dir)
                end
            catch
                # Ignore errors when removing directory
            end
        end
    catch e
        @warn "Error cleaning up crosslink_mmaps directory: $e"
    end
    
    # Clean up the crosslink_shared directory
    try
        shared_dir = joinpath(dirname(cl.db_path), "crosslink_shared")
        if isdir(shared_dir)
            # Try to remove all .shared files that are safe to delete
            for filename in readdir(shared_dir)
                if endswith(filename, ".shared")
                    file_path = joinpath(shared_dir, filename)
                    try
                        if is_file_safe_to_delete(file_path)
                            rm(file_path)
                            cl.debug && @info "Removed shared memory file: $file_path"
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
        end
    catch e
        @warn "Error cleaning up crosslink_shared directory: $e"
    end
    
    # Clear metadata cache
    empty!(cl.metadata_cache)
    
    # Close connection
    cl.conn = nothing
end

"""
    cleanup_all_arrow_files(db_path::String="crosslink.duckdb", force::Bool=false)

Clean up all Arrow files in the crosslink_mmaps directory.

This function can be called explicitly to clean up files from previous runs
that might not have been deleted properly.

# Arguments
- `db_path`: Path to the DuckDB database file to determine the mmaps directory
- `force`: Force deletion even if files appear to be in use

# Returns
- Number of files removed
"""
function cleanup_all_arrow_files(db_path::String="crosslink.duckdb", force::Bool=false)
    try
        # Get the mmaps directory
        mmaps_dir = joinpath(dirname(abspath(db_path)), "crosslink_mmaps")
        
        # Check if directory exists
        if !isdir(mmaps_dir)
            return 0
        end
        
        # Count of removed files
        removed_count = 0
        
        # Remove all .arrow files
        for filename in readdir(mmaps_dir)
            if endswith(filename, ".arrow")
                file_path = joinpath(mmaps_dir, filename)
                try
                    # Check if safe to delete if force=false
                    if force || is_file_safe_to_delete(file_path)
                        rm(file_path)
                        removed_count += 1
                    end
                catch e
                    @warn "Failed to remove Arrow file $file_path: $e"
                end
            end
        end
        
        # Try to remove the directory if it's empty
        try
            if isempty(readdir(mmaps_dir))
                rm(mmaps_dir)
            end
        catch
            # Ignore errors when removing directory
        end
        
        return removed_count
    catch e
        @warn "Error cleaning up Arrow files: $e"
        return 0
    end
end 