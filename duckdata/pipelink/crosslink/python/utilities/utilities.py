"""
Utility functions for CrossLink
"""
import os
import warnings
import shutil

def is_file_safe_to_delete(file_path):
    """Check if a file is safe to delete (not being used by other processes)
    
    Args:
        file_path: Path to the file to check
        
    Returns:
        bool: True if the file is safe to delete
    """
    if not os.path.exists(file_path):
        return True
        
    # Try to open the file in exclusive mode
    try:
        with open(file_path, 'r+b') as f:
            # On Unix systems, try to get an exclusive lock
            try:
                import fcntl
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                # If we get here, the file is not locked
                fcntl.flock(f, fcntl.LOCK_UN)
                return True
            except (ImportError, OSError):
                # fcntl not available or file is locked
                return True  # Assume it's safe on Windows or other platforms
    except PermissionError:
        # If we can't open the file, it's probably in use
        return False
    except Exception:
        # For any other error, assume it's safe to avoid leaving files behind
        return True

def close(cl):
    """Close the database connection and clean up resources
    
    Args:
        cl: CrossLink instance
    """
    # Remove from instance cache
    normalized_path = os.path.abspath(cl.db_path)
    if normalized_path in cl._instances:
        del cl._instances[normalized_path]
    
    # Clean up shared memory regions
    for shm in cl._shared_memory_regions:
        try:
            shm.close()
            try:
                shm.unlink()  # Remove from system
            except Exception:
                # Shared memory might already be unlinked
                pass
            cl._log(f"Closed and unlinked shared memory region: {shm.name}")
        except Exception as e:
            cl._log(f"Error cleaning shared memory: {e}", level="warning")
    
    # Clear the tracking list
    cl._shared_memory_regions = []
    
    # Clean up arrow files created by this instance
    for arrow_file in cl._arrow_files:
        try:
            if os.path.exists(arrow_file):
                # Check if this file is safe to delete
                if is_file_safe_to_delete(arrow_file):
                    os.unlink(arrow_file)
                    cl._log(f"Removed temporary Arrow file: {arrow_file}")
                else:
                    cl._log(f"Skipping deletion of Arrow file as it's in use: {arrow_file}")
        except Exception as e:
            warnings.warn(f"Failed to remove temporary Arrow file {arrow_file}: {e}")
    
    # Clear the tracking set
    cl._arrow_files.clear()
    
    # Clean up the crosslink_mmaps directory
    try:
        mmaps_dir = os.path.join(os.path.dirname(cl.db_path), "crosslink_mmaps")
        if os.path.exists(mmaps_dir):
            # Try to remove all .arrow files that are safe to delete
            for filename in os.listdir(mmaps_dir):
                if filename.endswith(".arrow"):
                    file_path = os.path.join(mmaps_dir, filename)
                    try:
                        if is_file_safe_to_delete(file_path):
                            os.unlink(file_path)
                            cl._log(f"Removed mmapped Arrow file: {file_path}")
                    except Exception as e:
                        warnings.warn(f"Failed to remove mmapped Arrow file {file_path}: {e}")
            
            # Try to remove the directory if it's empty
            try:
                if len(os.listdir(mmaps_dir)) == 0:
                    os.rmdir(mmaps_dir)
            except Exception:
                pass
    except Exception as e:
        warnings.warn(f"Error cleaning up crosslink_mmaps directory: {e}")
    
    # Clear metadata cache to free memory
    cl._metadata_cache.clear()
    
    # Close connection
    if cl.conn:
        try:
            cl.conn.close()
            cl.conn = None
        except Exception as e:
            warnings.warn(f"Error closing database connection: {e}")

def cleanup_all_arrow_files(db_path="crosslink.duckdb", force=False):
    """Clean up all Arrow files in the crosslink_mmaps directory
    
    This function can be called explicitly to clean up files from previous runs
    that might not have been deleted properly.
    
    Args:
        db_path: Path to the DuckDB database file to determine the mmaps directory
        force: Force deletion even if files appear to be in use
        
    Returns:
        int: Number of files removed
    """
    try:
        # Get the mmaps directory
        mmaps_dir = os.path.join(os.path.dirname(os.path.abspath(db_path)), "crosslink_mmaps")
        
        # Check if directory exists
        if not os.path.exists(mmaps_dir):
            return 0
        
        # Count of removed files
        removed_count = 0
        
        # Remove all .arrow files
        for filename in os.listdir(mmaps_dir):
            if filename.endswith(".arrow"):
                file_path = os.path.join(mmaps_dir, filename)
                try:
                    # Check if safe to delete if force=false
                    if force or is_file_safe_to_delete(file_path):
                        os.unlink(file_path)
                        removed_count += 1
                except Exception as e:
                    warnings.warn(f"Failed to remove Arrow file {file_path}: {e}")
        
        # Try to remove the directory if it's empty
        try:
            if len(os.listdir(mmaps_dir)) == 0:
                os.rmdir(mmaps_dir)
        except Exception:
            pass
        
        return removed_count
    except Exception as e:
        warnings.warn(f"Error cleaning up Arrow files: {e}")
        return 0 