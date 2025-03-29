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
        
        # Look in multiple possible locations
        possible_paths = [
            current_dir / "crosslink_cpp.so",  # Linux/macOS
            current_dir / "crosslink_cpp.pyd",  # Windows
            project_dir / "cpp" / "build" / "crosslink_cpp.so",
            project_dir / "cpp" / "build" / "crosslink_cpp.pyd",
            project_dir / "cpp" / "crosslink_cpp.so",
            project_dir / "cpp" / "crosslink_cpp.pyd"
        ]
        
        for path in possible_paths:
            if path.exists():
                # Load using importlib
                spec = importlib.util.spec_from_file_location("crosslink_cpp", path)
                cpp_mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(cpp_mod)
                
                _cpp_module = cpp_mod
                _cpp_available = True
                return True
    except Exception as e:
        warnings.warn(f"Warning: Failed to load C++ module: {e}")
    
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
        
        self._cpp_impl = _cpp_module.CrossLinkCpp(db_path, debug)
        self.db_path = db_path
        self.debug = debug
    
    def push(self, table, name="", description=""):
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
    
    def pull(self, identifier):
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
    
    def query(self, sql):
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
    
    def list_datasets(self):
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
    
    def register_notification(self, callback):
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
    
    def unregister_notification(self, registration_id):
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
            table: PyArrow Table
            dataset_id: Dataset ID
            
        Returns:
            dict or None: Shared memory information or None if failed
            
        Raises:
            RuntimeError: If setup fails
        """
        try:
            return self._cpp_impl.setup_shared_memory(table, dataset_id)
        except Exception as e:
            if self.debug:
                print(f"C++ setup_shared_memory operation failed: {e}")
            raise RuntimeError(f"Failed to set up shared memory: {e}")
    
    def cleanup_shared_memory(self, shared_memory_key=None):
        """Clean up shared memory
        
        Args:
            shared_memory_key: Optional specific key to clean up, or None for all
            
        Returns:
            int: Number of regions cleaned up
            
        Raises:
            RuntimeError: If cleanup fails
        """
        try:
            if shared_memory_key is not None:
                return self._cpp_impl.cleanup_shared_memory(shared_memory_key)
            else:
                return self._cpp_impl.cleanup_all_shared_memory()
        except Exception as e:
            if self.debug:
                print(f"C++ cleanup_shared_memory operation failed: {e}")
            raise RuntimeError(f"Failed to clean up shared memory: {e}")

# Define module variables for easier importing
is_cpp_available = _cpp_available 