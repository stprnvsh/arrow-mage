"""
Shared memory operations for CrossLink
"""
import warnings

# Try to import C++ bindings
try:
    from .cpp_wrapper import is_cpp_available, CrossLinkCpp
    _cpp_available = is_cpp_available
except ImportError:
    _cpp_available = False
    
# Try to import DuckDB for fallback
try:
    import duckdb
    _duckdb_available = True
except ImportError:
    _duckdb_available = False

# Global cache for shared memory regions
_shared_memory_regions = {}

# Check if shared memory is available
def _check_shared_memory_available():
    # First check for C++ bindings which provide shared memory
    if _cpp_available:
        return True
    
    # Fall back to Python shared memory
    try:
        from multiprocessing import shared_memory
        return True
    except ImportError:
        return False

# Global variable to track if shared memory is available
_shared_memory_available = _check_shared_memory_available()

def setup_shared_memory(cl, arrow_table, dataset_id):
    """Set up shared memory for an Arrow table
    
    Args:
        cl: CrossLink instance
        arrow_table: PyArrow Table to share
        dataset_id: Dataset ID for naming
        
    Returns:
        Dict with shared memory information or None if not supported
    """
    # If C++ bindings are available, use them for optimal performance
    if _cpp_available and hasattr(cl, '_cpp_instance'):
        try:
            # Already using C++ implementation
            return cl._cpp_instance.setup_shared_memory(arrow_table, dataset_id)
        except Exception as e:
            cl._log(f"Failed to setup shared memory via C++: {e}", level="warning")
            # Continue to Python implementation as fallback
    
    # Python implementation (existing code)
    if not _shared_memory_available:
        cl._log("Shared memory requires Python 3.8+ with multiprocessing.shared_memory", level="warning")
        return None
        
    try:
        # Lazy import
        import pyarrow as pa
        from multiprocessing import shared_memory
        
        # Serialize the Arrow table to bytes
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, arrow_table.schema) as writer:
            writer.write_table(arrow_table)
        buffer = sink.getvalue()
        buffer_size = len(buffer.to_pybytes())
        
        # Create shared memory region
        shm_name = f"crosslink_{dataset_id}"
        shm = shared_memory.SharedMemory(name=shm_name, create=True, size=buffer_size)
        
        # Copy Arrow data to shared memory
        shm.buf[:buffer_size] = buffer.to_pybytes()
        
        # Track for cleanup
        cl._shared_memory_regions.append(shm)
        
        # Also store in global cache for cross-instance access
        global _shared_memory_regions
        _shared_memory_regions[shm_name] = shm
        
        return {
            "shared_memory_key": shm_name,
            "shared_memory_size": buffer_size,
            "arrow_schema": arrow_table.schema.serialize().hex()
        }
    except ImportError as e:
        cl._log(f"SharedMemory import failed: {e}", level="warning")
        return None
    except Exception as e:
        cl._log(f"Failed to setup shared memory: {e}", level="error")
        return None

def get_from_shared_memory(cl, shared_memory_key, metadata=None):
    """Get Arrow table from shared memory
    
    Args:
        cl: CrossLink instance
        shared_memory_key: Shared memory region name
        metadata: Dataset metadata with schema information
        
    Returns:
        Arrow table or None if failed
    """
    # Handle None or empty shared memory key
    if shared_memory_key is None or shared_memory_key == "":
        cl._log("Shared memory key is missing or empty, will fall back to standard queries", level="info")
        return None
        
    # If C++ bindings are available, use them for optimal performance
    if _cpp_available and hasattr(cl, '_cpp_instance'):
        try:
            # Already using C++ implementation
            result = cl._cpp_instance.get_from_shared_memory(shared_memory_key, metadata)
            if result is not None:
                return result
            cl._log(f"C++ shared memory lookup failed for key: {shared_memory_key}, will fall back to standard queries", level="info")
        except Exception as e:
            cl._log(f"Failed to access shared memory via C++: {e}, will fall back to standard queries", level="warning")
    
    # Python implementation (existing code)
    if not _shared_memory_available:
        cl._log("Shared memory not available, will fall back to standard queries", level="info")
        return None
        
    try:
        # Lazy import
        import pyarrow as pa
        from multiprocessing import shared_memory
        
        # Try to attach to shared memory region
        if shared_memory_key in _shared_memory_regions:
            # Use cached region
            shm = _shared_memory_regions[shared_memory_key]
        else:
            try:
                # Attach to existing region
                shm = shared_memory.SharedMemory(name=shared_memory_key, create=False)
                _shared_memory_regions[shared_memory_key] = shm
            except FileNotFoundError:
                cl._log(f"Shared memory region not found: {shared_memory_key}, will fall back to standard queries", level="info")
                return None
        
        # Create buffer from shared memory
        buffer = pa.py_buffer(shm.buf)
        
        # Try to extract schema from metadata
        if metadata and metadata.get('arrow_schema'):
            schema_hex = metadata['arrow_schema']
            if isinstance(schema_hex, dict) and 'serialized' in schema_hex:
                schema_hex = schema_hex['serialized']
                
            if isinstance(schema_hex, str):
                # Deserialize schema
                schema_bytes = bytes.fromhex(schema_hex)
                schema = pa.ipc.read_schema(pa.py_buffer(schema_bytes))
                
                # Read with schema
                reader = pa.ipc.RecordBatchStreamReader(buffer, schema)
                return reader.read_all()
        
        # Fallback to reading without schema
        reader = pa.ipc.open_stream(buffer)
        return reader.read_all()
        
    except Exception as e:
        cl._log(f"Failed to access shared memory: {e}, will fall back to standard queries", level="warning")
        return None

def fallback_to_duckdb(cl, identifier):
    """Fallback to using DuckDB when shared memory access fails
    
    Args:
        cl: CrossLink instance
        identifier: Dataset identifier
        
    Returns:
        Arrow table or None if failed
    """
    if not _duckdb_available:
        cl._log("DuckDB not available for fallback query", level="warning")
        return None
        
    try:
        import pyarrow as pa
        
        # Connect to DuckDB
        conn = duckdb.connect(cl.db_path)
        
        # Safety: clean identifier to prevent SQL injection
        safe_identifier = ''.join(c for c in identifier if c.isalnum() or c == '_')
        
        # Query the data
        query = f"SELECT * FROM {safe_identifier}"
        arrow_result = conn.execute(query).fetch_arrow_table()
        
        # Close connection
        conn.close()
        
        return arrow_result
    except Exception as e:
        cl._log(f"Failed to execute DuckDB fallback query: {e}", level="error")
        return None

def safe_shared_memory_get(cl, shared_memory_key, identifier, metadata=None):
    """Safely get data from shared memory with fallback to DuckDB
    
    Args:
        cl: CrossLink instance
        shared_memory_key: Shared memory key
        identifier: Dataset identifier for fallback
        metadata: Dataset metadata
        
    Returns:
        Arrow table or None if all methods fail
    """
    # Try shared memory first
    result = get_from_shared_memory(cl, shared_memory_key, metadata)
    
    # If shared memory access fails, fall back to DuckDB
    if result is None:
        cl._log(f"Shared memory access failed for {identifier}, falling back to DuckDB", level="info")
        result = fallback_to_duckdb(cl, identifier)
    
    return result

def cleanup_all_shared_memory():
    """Clean up all shared memory regions used by CrossLink
    
    This is a static method that can be called to clean up shared memory resources
    even if you don't have a reference to the CrossLink instance.
    
    Returns:
        Number of shared memory regions cleaned up
    """
    # If C++ bindings are available, use them for optimal performance
    if _cpp_available:
        try:
            # Create a temporary C++ instance to call cleanup
            cpp_instance = CrossLinkCpp()
            count = cpp_instance.cleanup_shared_memory()
            return count
        except Exception as e:
            warnings.warn(f"Failed to clean up shared memory via C++: {e}")
            # Fall back to Python implementation
    
    # Python implementation (existing code)
    if not _shared_memory_available:
        return 0
        
    from multiprocessing import shared_memory
    import re
    
    count = 0
    pattern = re.compile(r'^crosslink_[0-9a-f\-]+$')
    
    # Get all shared memory regions from the system
    try:
        # Find all shared memory segments that match our pattern
        for shm_name in [name for name in shared_memory.ShareableList.get_shared_memory_tracker().get_names() 
                       if pattern.match(name)]:
            try:
                # Open and unlink shared memory
                shm = shared_memory.SharedMemory(name=shm_name, create=False)
                shm.close()
                shm.unlink()
                count += 1
            except Exception:
                pass
    except Exception:
        # Fallback to cleaning only known regions
        global _shared_memory_regions
        for name, shm in list(_shared_memory_regions.items()):
            try:
                shm.close()
                shm.unlink()
                del _shared_memory_regions[name]
                count += 1
            except Exception:
                pass
    
    # Clear the global cache
    _shared_memory_regions.clear()
    
    return count

# Factory function to create appropriate implementation based on availability
def create_shared_memory_manager(cl):
    """Create a shared memory manager based on available implementations
    
    Args:
        cl: CrossLink instance
        
    Returns:
        A shared memory manager instance
    """
    if _cpp_available:
        try:
            from .cpp_wrapper import CrossLinkCpp
            return CrossLinkCpp(cl.db_path, cl._debug)
        except Exception as e:
            warnings.warn(f"Failed to create C++ shared memory manager: {e}")
    
    # Fall back to Python implementation
    return None 