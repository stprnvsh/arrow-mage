"""
Utility functions for arrow_cache.

This module contains utility functions for managing and cleaning up the arrow_cache resources.
"""

import os
import shutil
import glob
import gc
import subprocess
import sys

def clear_arrow_cache(spill_dir=None, verbose=True):
    """
    Clear all Arrow Cache files and resources
    
    Args:
        spill_dir: Custom spill directory path (if None, look for default locations)
        verbose: Whether to print status messages
    """
    # First run Python garbage collection to release any unreferenced memory
    if verbose:
        print("Running garbage collection...")
    gc.collect()
    
    # Default spill directory locations to check
    default_dirs = [
        os.path.join(os.getcwd(), ".arrow_cache_spill"),
        os.path.join(os.getcwd(), "arrow_cache_spill"),
        os.path.join(os.getcwd(), ".arrow_cache_spill_streamlit"),
    ]
    
    if spill_dir:
        # Use provided spill directory
        spill_dirs = [spill_dir]
    else:
        # Check all default locations
        spill_dirs = default_dirs
    
    # Add any environment-specified spill directory
    if 'ARROW_CACHE_SPILL_DIR' in os.environ:
        spill_dirs.append(os.environ['ARROW_CACHE_SPILL_DIR'])
    
    # Clear all potential spill directories
    for directory in spill_dirs:
        if os.path.exists(directory):
            try:
                if verbose:
                    print(f"Removing spill directory: {directory}")
                shutil.rmtree(directory)
                # Recreate empty directory
                os.makedirs(directory, exist_ok=True)
            except Exception as e:
                print(f"Error clearing spill directory {directory}: {e}")
    
    # Clear DuckDB files that might contain cached data - add more patterns
    duckdb_patterns = [
        '*.duckdb', '*.duckdb.wal', '*.duckdb-journal', 
        '*.db', '*.db-wal', '*.db-journal',
        'tmp-*.duckdb*', '*-shm', '*-wal'
    ]
    
    for pattern in duckdb_patterns:
        for file_path in glob.glob(pattern):
            try:
                if verbose:
                    print(f"Removing DuckDB file: {file_path}")
                os.remove(file_path)
            except Exception as e:
                print(f"Error removing file {file_path}: {e}")
    
    # Also look for any in-memory temporary files in /tmp that might be related
    tmp_patterns = [
        '/tmp/duckdb_*', 
        '/tmp/pyarrow_*', 
        '/tmp/arrow_*',
        '/tmp/tmp-*duckdb*'
    ]
    
    for pattern in tmp_patterns:
        for file_path in glob.glob(pattern):
            try:
                if verbose:
                    print(f"Removing temporary file: {file_path}")
                if os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                else:
                    os.remove(file_path)
            except Exception as e:
                print(f"Error removing temporary file {file_path}: {e}")
    
    # Clear any potential storage directories
    storage_dirs = [
        os.path.join(os.getcwd(), ".arrow_cache_storage"),
        os.path.join(os.getcwd(), "arrow_cache_storage"),
    ]
    
    for directory in storage_dirs:
        if os.path.exists(directory):
            try:
                if verbose:
                    print(f"Removing storage directory: {directory}")
                shutil.rmtree(directory)
            except Exception as e:
                print(f"Error clearing storage directory {directory}: {e}")
    
    # Try to clear memory by forcing a flush of the file system cache
    # Only on UNIX-like systems
    if sys.platform != 'win32':
        try:
            # This command drops cached pages from the kernel page cache
            subprocess.run("sync", shell=True)
            
            # On Linux, we could drop caches more aggressively, but this requires sudo
            # and isn't suitable for most user environments
            # subprocess.run("echo 1 | sudo tee /proc/sys/vm/drop_caches", shell=True)
        except Exception as e:
            if verbose:
                print(f"Failed to flush file system cache: {e}")
    
    # Run garbage collection again
    gc.collect()
    
    if verbose:
        print("Arrow Cache cleanup completed")

def cleanup_command():
    """
    Entry point for the arrow-cache-cleanup command.
    This function is used as a console script entry point.
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Clear Arrow Cache files and resources")
    parser.add_argument("--spill-dir", help="Custom spill directory path")
    parser.add_argument("--quiet", action="store_true", help="Suppress status messages")
    
    args = parser.parse_args()
    clear_arrow_cache(spill_dir=args.spill_dir, verbose=not args.quiet) 