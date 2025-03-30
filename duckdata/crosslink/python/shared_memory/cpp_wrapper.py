"""
C++ binding wrapper for CrossLink Python

This module loads the C++ bindings for CrossLink and provides an interface
to the core C++ implementation for better cross-language zero-copy performance.
"""
import os
import sys
import importlib.util
from pathlib import Path
import warnings
# Add pyarrow import for type hinting if needed
try:
    import pyarrow as pa
except ImportError:
    pa = None

# Define variables for C++ binding availability
_cpp_available = False
_cpp_module = None

# Try to import the C++ extension module
def _try_import_cpp():
    """Try to import the C++ module from various locations"""
    global _cpp_available, _cpp_module
    
    # If already loaded, don't try again
    if _cpp_available:
        return True
    
    try:
        # First method: try direct import if installed in Python path
        import crosslink.crosslink_cpp as cpp_mod
        _cpp_module = cpp_mod
        _cpp_available = True
        return True
    except ImportError:
        pass
    
    # Second method: try to load from local path
    try:
        # Get the potential paths
        current_dir = Path(__file__).parent.resolve()
        project_dir = current_dir.parent.parent
        
        # Find the platform-specific extension suffix (e.g., .so, .pyd)
        try:
            import sysconfig
            so_suffix = sysconfig.get_config_var('EXT_SUFFIX') # e.g., .cpython-310-darwin.so
            if not so_suffix:
                so_suffix = '.so' # Fallback for Linux/macOS generic
        except:
            so_suffix = '.so' # Default fallback
        pyd_suffix = '.pyd' # Windows specific

        possible_paths = [
            # Check with platform specific suffix first
            current_dir / f"crosslink_cpp{so_suffix}",
            current_dir / f"crosslink_cpp{pyd_suffix}",
            project_dir / "cpp" / "build" / f"crosslink_cpp{so_suffix}",
            project_dir / "cpp" / "build" / f"crosslink_cpp{pyd_suffix}",
            # Fallback to generic names
            current_dir / "crosslink_cpp.so",  # Linux/macOS
            current_dir / "crosslink_cpp.pyd",  # Windows
            project_dir / "cpp" / "build" / "crosslink_cpp.so",
            project_dir / "cpp" / "build" / "crosslink_cpp.pyd",
            project_dir / "cpp" / "crosslink_cpp.so",
            project_dir / "cpp" / "crosslink_cpp.pyd"
        ]
        
        # --- Debugging path checking --- (Removed)
        # print("--- [cpp_wrapper.py] Debug: Checking paths ---")
        # for p_debug in possible_paths:
        #     print(f"  Checking: {p_debug} | Exists: {p_debug.exists()}")
        # print("--- [cpp_wrapper.py] Debug: End checking paths ---")
        # # --- End Debugging ---

        for path in possible_paths:
            if path.exists():
                # Load using importlib
                try:
                    # print(f"--- [cpp_wrapper.py] Debug: Attempting to load {path} ---") # Removed
                    spec = importlib.util.spec_from_file_location("crosslink_cpp", path)
                    if spec is None:
                        # print(f"--- [cpp_wrapper.py] Debug: spec_from_file_location returned None for {path} ---") # Removed
                        continue # Try next path

                    if spec.loader is None:
                        # print(f"--- [cpp_wrapper.py] Debug: spec.loader is None for {path} ---") # Removed
                        continue # Try next path
                        
                    cpp_mod = importlib.util.module_from_spec(spec)
                    if cpp_mod is None:
                        # print(f"--- [cpp_wrapper.py] Debug: module_from_spec returned None for {path} ---") # Removed
                        continue # Try next path
                        
                    spec.loader.exec_module(cpp_mod)
                    # print(f"--- [cpp_wrapper.py] Debug: Successfully loaded {path} ---") # Removed
                    _cpp_module = cpp_mod
                    _cpp_available = True
                    return True
                except Exception as e_load:
                    # Print the detailed loading error if needed, but keep it minimal for production
                    warnings.warn(f"Warning: Failed during importlib loading of '{path}'. Error: {e_load}") 
                    continue # Try next path
    
    # Handle outer exceptions during path setup if necessary
    except Exception as e:
        warnings.warn(f"Warning: Outer exception in _try_import_cpp. Error: {e}")
        pass # Let it fall through to the final warning if all else fails
    
    # If we get here, no method succeeded
    warnings.warn("C++ bindings not found in any expected location, falling back to Python implementation")
    return False

# Try to import when module is loaded
_try_import_cpp()

class CrossLinkCpp:
    """Python wrapper for the C++ CrossLink implementation"""
    
    def __init__(self, db_path="crosslink.duckdb", debug=False):
        """Initialize the C++ CrossLink instance
        
        Args:
            db_path: Path to the database file
            debug: Enable debug logging
        
        Raises:
            ImportError: If C++ bindings are not available
        """
        global _cpp_available, _cpp_module
        
        if not _cpp_available:
            if not _try_import_cpp():  # Try one more time
                raise ImportError("CrossLink C++ bindings are not available")
        
        # Ensure PyArrow types are available if using C++ bindings
        if pa is None:
             warnings.warn("PyArrow is not installed. C++ bindings require PyArrow.")
             raise ImportError("PyArrow is required for CrossLink C++ bindings.")
        
        self._cpp_impl = _cpp_module.CrossLinkCpp(db_path, debug)
        self.db_path = db_path
        self.debug = debug
        # Expose the bound StreamWriter and StreamReader classes directly if needed
        # Or rely on the return types of push/pull_stream
        self.StreamWriter = getattr(_cpp_module, "StreamWriter", None)
        self.StreamReader = getattr(_cpp_module, "StreamReader", None)
    
    def push(self, table: 'pa.Table', name: str = "", description: str = "") -> str:
        """Push a PyArrow table to CrossLink
        
        Args:
            table: PyArrow Table
            name: Optional name for the dataset
            description: Optional description
        
        Returns:
            str: Dataset ID
        
        Raises:
            RuntimeError: If push operation fails
        """
        try:
            return self._cpp_impl.push(table, name, description)
        except Exception as e:
            if self.debug:
                print(f"C++ push operation failed: {e}")
            raise RuntimeError(f"Failed to push data: {e}")
    
    def pull(self, identifier: str) -> 'pa.Table':
        """Pull a dataset as a PyArrow table
        
        Args:
            identifier: Dataset ID or name
        
        Returns:
            pyarrow.Table: Arrow table
        
        Raises:
            RuntimeError: If pull operation fails
        """
        try:
            return self._cpp_impl.pull(identifier)
        except Exception as e:
            if self.debug:
                print(f"C++ pull operation failed: {e}")
            raise RuntimeError(f"Failed to pull data for {identifier}: {e}")
    
    def query(self, sql: str) -> 'pa.Table':
        """Execute a SQL query and return results as a PyArrow table
        
        Args:
            sql: SQL query string
        
        Returns:
            pyarrow.Table: Query results
            
        Raises:
            RuntimeError: If query operation fails
        """
        try:
            return self._cpp_impl.query(sql)
        except Exception as e:
            if self.debug:
                print(f"C++ query operation failed: {e}")
            raise RuntimeError(f"Failed to execute query: {e}")
    
    def list_datasets(self) -> list[str]:
        """List available datasets
        
        Returns:
            list: List of dataset IDs
            
        Raises:
            RuntimeError: If list operation fails
        """
        try:
            return self._cpp_impl.list_datasets()
        except Exception as e:
            if self.debug:
                print(f"C++ list_datasets operation failed: {e}")
            raise RuntimeError(f"Failed to list datasets: {e}")
    
    def register_notification(self, callback) -> str:
        """Register a callback for dataset notifications
        
        Args:
            callback: Function to call when a dataset changes
        
        Returns:
            str: Registration ID
            
        Raises:
            RuntimeError: If registration fails
        """
        try:
            return self._cpp_impl.register_notification(callback)
        except Exception as e:
            if self.debug:
                print(f"C++ register_notification operation failed: {e}")
            raise RuntimeError(f"Failed to register notification: {e}")
    
    def unregister_notification(self, registration_id: str):
        """Unregister a notification callback
        
        Args:
            registration_id: Registration ID from register_notification
            
        Raises:
            RuntimeError: If unregistration fails
        """
        try:
            return self._cpp_impl.unregister_notification(registration_id)
        except Exception as e:
            if self.debug:
                print(f"C++ unregister_notification operation failed: {e}")
            raise RuntimeError(f"Failed to unregister notification: {e}")
    
    def cleanup(self):
        """Clean up all resources
        
        Raises:
            RuntimeError: If cleanup fails
        """
        try:
            return self._cpp_impl.cleanup()
        except Exception as e:
            if self.debug:
                print(f"C++ cleanup operation failed: {e}")
            raise RuntimeError(f"Failed to cleanup resources: {e}")
    
    def get_from_shared_memory(self, shared_memory_key, metadata=None):
        """Get data from shared memory
        
        Args:
            shared_memory_key: Shared memory key
            metadata: Optional metadata about the shared memory
            
        Returns:
            pyarrow.Table or None: Retrieved data or None if not found
            
        Raises:
            RuntimeError: If operation fails (but not if key not found)
        """
        try:
            # Handle None or empty key
            if shared_memory_key is None or shared_memory_key == "":
                if self.debug:
                    print("Cannot get from shared memory: key is None or empty")
                return None
                
            return self._cpp_impl.get_from_shared_memory(shared_memory_key, metadata or {})
        except KeyError:
            # Specific exception for key not found
            if self.debug:
                print(f"Shared memory key not found: {shared_memory_key}")
            return None
        except Exception as e:
            if self.debug:
                print(f"C++ get_from_shared_memory operation failed: {e}")
            raise RuntimeError(f"Failed to get data from shared memory: {e}")
    
    def setup_shared_memory(self, table, dataset_id):
        """Set up shared memory for a table
        
        Args:
            table: PyArrow table
            dataset_id: ID to associate with the shared memory
        
        Returns:
            str: Shared memory key
        
        Raises:
            RuntimeError: If setup fails
        """
        try:
            return self._cpp_impl.setup_shared_memory(table, dataset_id)
        except Exception as e:
            if self.debug:
                print(f"C++ setup_shared_memory operation failed: {e}")
            raise RuntimeError(f"Failed to setup shared memory: {e}")
    
    def cleanup_shared_memory(self, shared_memory_key=None):
        """Clean up shared memory regions
        
        Args:
            shared_memory_key: Specific key to clean up, or None for all
            
        Returns:
            int: Number of regions cleaned up
            
        Raises:
            RuntimeError: If cleanup fails
        """
        try:
            return self._cpp_impl.cleanup_shared_memory(shared_memory_key)
        except Exception as e:
            if self.debug:
                print(f"C++ cleanup_shared_memory operation failed: {e}")
            raise RuntimeError(f"Failed to cleanup shared memory: {e}")

    # === New Streaming Methods ===

    def push_stream(self, schema: 'pa.Schema', name: str = "") -> tuple[str, object]:
        """Start pushing a stream of Arrow RecordBatches.

        Args:
            schema: The pyarrow.Schema for the data that will be streamed.
            name: Optional name for the stream. A unique ID will be generated if empty.

        Returns:
            tuple[str, StreamWriter]: A tuple containing the stream ID and the
                                      StreamWriter object to write batches to.

        Raises:
            RuntimeError: If starting the stream fails.
            ImportError: If C++ bindings or required StreamWriter class not found.
        """
        if not self._cpp_impl or not self.StreamWriter:
             raise ImportError("Streaming requires available C++ bindings and StreamWriter class.")
        try:
            # The C++ binding returns std::pair<string, shared_ptr<StreamWriter>>
            # pybind11 converts this to a Python tuple(str, StreamWriter_bound_object)
            stream_id, writer_obj = self._cpp_impl.push_stream(schema, name)
            return stream_id, writer_obj
        except Exception as e:
            if self.debug:
                print(f"C++ push_stream operation failed: {e}")
            raise RuntimeError(f"Failed to start stream: {e}")

    def pull_stream(self, stream_id: str) -> object:
        """Connect to an existing stream to pull Arrow RecordBatches.

        Args:
            stream_id: The ID of the stream to connect to.

        Returns:
            StreamReader: A StreamReader object to read batches from.
                          The reader is iterable (for batch in reader: ...).

        Raises:
            RuntimeError: If connecting to the stream fails (e.g., not found).
            ImportError: If C++ bindings or required StreamReader class not found.
        """
        if not self._cpp_impl or not self.StreamReader:
             raise ImportError("Streaming requires available C++ bindings and StreamReader class.")
        try:
             # The C++ binding returns shared_ptr<StreamReader>
             # pybind11 converts this to a Python StreamReader_bound_object
            reader_obj = self._cpp_impl.pull_stream(stream_id)
            return reader_obj
        except Exception as e:
            if self.debug:
                print(f"C++ pull_stream operation failed: {e}")
            # Distinguish between 'not found' and other errors if C++ provides specific exceptions
            # For now, raise a general error.
            raise RuntimeError(f"Failed to connect to stream '{stream_id}': {e}")

# Expose the check function at the module level
def is_cpp_available():
    """Check if the C++ bindings for CrossLink are loaded and available"""
    global _cpp_available
    return _cpp_available 