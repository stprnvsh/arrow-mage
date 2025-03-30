"""
Core module for CrossLink with main class definition
"""
import os
import warnings
import time
import uuid
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple

# Try to import C++ bindings
try:
    # Only import names actually defined at module level in cpp_wrapper
    from ..shared_memory.cpp_wrapper import CrossLinkCpp, is_cpp_available
    _cpp_available = is_cpp_available()
    # Define StreamWriter/Reader as None initially, they might be set later if needed
    StreamWriter = None 
    StreamReader = None
except ImportError:
    _cpp_available = False
    CrossLinkCpp = None
    StreamWriter = None
    StreamReader = None

# Import pyarrow for type hints
try:
    import pyarrow as pa
except ImportError:
    pa = None

class CrossLink:
    _instances = {}  # Class variable to store instances for reuse
    
    @classmethod
    def get_instance(cls, db_path="crosslink.duckdb"):
        """Get or create a CrossLink instance for the given database path
        
        This enables connection pooling to avoid repeated initialization costs.
        """
        # Normalize path for consistent lookup
        normalized_path = os.path.abspath(db_path)
        
        # Return existing instance if available
        if normalized_path in cls._instances:
            return cls._instances[normalized_path]
        
        # Create new instance
        instance = cls(db_path)
        cls._instances[normalized_path] = instance
        return instance
    
    def __init__(self, db_path="crosslink.duckdb"):
        """
        Initialize CrossLink with a database path.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self.conn = None
        self.arrow_available = False
        self._arrow_initialized = False
        self._metadata_cache = {}
        self._arrow_files = set()  # Track created arrow files for cleanup
        self._debug = False  # Debug flag for verbose logging
        self._shared_memory_regions = []  # Track shared memory regions for cleanup
        
        # Try to use C++ implementation if available
        self._cpp_instance: Optional[CrossLinkCpp] = None
        if _cpp_available and CrossLinkCpp:
            try:
                # Create instance of CrossLinkCpp (which uses _cpp_module internally)
                self._cpp_instance = CrossLinkCpp(db_path, self._debug)
                self.arrow_available = True
                self._log("Using C++ implementation for better cross-language performance")
            except Exception as e:
                # Print the detailed error during CrossLinkCpp instantiation
                warnings.warn(f"ERROR during CrossLinkCpp instantiation: {e}", category=RuntimeWarning)
                import traceback
                traceback.print_exc() # Print full traceback
                warnings.warn(f"Failed to initialize C++ implementation, falling back to Python.")
                self._cpp_instance = None
        
        # If C++ implementation failed or isn't available, use pure Python
        if self._cpp_instance is None:
            # Initialize connection with robust error handling
            self._init_connection()
            
            # Initialize metadata manager right away (needed for core functionality)
            if self.conn is not None:
                from ..metadata.metadata import CrossLinkMetadataManager
                self.metadata_manager = CrossLinkMetadataManager(self.conn)
            else:
                # Create a dummy metadata manager if connection failed
                warnings.warn("Database connection failed, using limited functionality")
                self.metadata_manager = None
            
    def _log(self, message, level="info"):
        """Log a message if debug mode is enabled"""
        if not self._debug and level == "info":
            return
            
        if level == "warning":
            warnings.warn(message)
        elif level == "error":
            print(f"ERROR: {message}")
        else:
            print(f"INFO: {message}")
            
    def set_debug(self, debug=True):
        """Enable or disable debug mode"""
        self._debug = debug
        
        # Update C++ instance if available
        if self._cpp_instance is not None:
            # Recreate C++ instance with new debug setting
            self._cpp_instance = CrossLinkCpp(self.db_path, self._debug)
            
        return self
    
    def _init_connection(self):
        """Initialize the database connection with robust error handling"""
        try:
            # Lazy import to improve startup time
            import duckdb
            
            # Create parent directory if it doesn't exist
            db_dir = os.path.dirname(self.db_path)
            if db_dir and not os.path.exists(db_dir):
                os.makedirs(db_dir, exist_ok=True)
                
            # Connect to database
            self.conn = duckdb.connect(self.db_path)
            
            # Set some essential pragmas immediately
            self._set_basic_pragmas()
        except Exception as e:
            warnings.warn(f"Failed to initialize database connection: {e}")
            self.conn = None
    
    def _set_basic_pragmas(self):
        """Set essential pragmas for better performance and compatibility"""
        if self.conn is None:
            return
            
        try:
            # Enable object cache for better performance with repeated queries
            self.conn.execute("PRAGMA enable_object_cache")
        except Exception:
            pass
    
    def _ensure_arrow_extension(self):
        """Lazily load the Arrow extension when needed"""
        # If using C++ implementation, it always supports Arrow
        if self._cpp_instance is not None:
            return True
            
        if self._arrow_initialized:
            return self.arrow_available
            
        self._arrow_initialized = True
        
        # Try to import PyArrow
        try:
            # Lazy import to improve startup time
            import pyarrow as pa
            
            # Install and load Arrow extension in DuckDB
            try:
                # First check if Arrow is already loaded
                self.conn.execute("SELECT arrow_version()")
                self.arrow_available = True
            except Exception:
                try:
                    # Try to install and load
                    self.conn.execute("INSTALL arrow")
                    self.conn.execute("LOAD arrow")
                    self.arrow_available = True
                except Exception as e:
                    warnings.warn(f"Arrow extension installation or loading failed: {e}")
                    self.arrow_available = False
        except Exception as e:
            warnings.warn(f"PyArrow import failed: {e}")
            warnings.warn("Some Arrow functionality may not be available")
            self.arrow_available = False
            
        return self.arrow_available
    
    def _configure_duckdb(self):
        """Configure DuckDB for optimal performance"""
        # Skip if using C++ implementation
        if self._cpp_instance is not None:
            return
            
        # Skip if already configured or connection failed
        if self.conn is None or (hasattr(self, '_duckdb_configured') and self._duckdb_configured):
            return
            
        try:
            # Set memory limit to a specific amount instead of percentage
            try:
                # Import psutil lazily
                import psutil
                total_memory = psutil.virtual_memory().total
                memory_limit_mb = int(total_memory * 0.8 / (1024 * 1024))  # 80% of total memory in MB
                self.conn.execute(f"PRAGMA memory_limit='{memory_limit_mb}MB'")
            except Exception:
                # Default memory limit if psutil fails
                self.conn.execute("PRAGMA memory_limit='4GB'")
            
            # Set threads to a reasonable number
            try:
                import multiprocessing
                num_cores = max(1, multiprocessing.cpu_count() - 1)
                self.conn.execute(f"PRAGMA threads={num_cores}")
            except Exception:
                # Default to 4 threads if detection fails
                self.conn.execute("PRAGMA threads=4")
            
            # Try various optimizations with fallbacks
            self._try_execute("PRAGMA force_parallelism")
            self._try_execute("PRAGMA preserve_insertion_order=false")  # Allows reordering for memory efficiency
            self._try_execute("PRAGMA temp_directory=':memory:'")  # Use memory for temp files when possible
            self._try_execute("PRAGMA checkpoint_threshold='4GB'")  # Less frequent checkpoints
            self._try_execute("PRAGMA cache_size=2048")  # 2GB cache
            
            # Mark as configured
            self._duckdb_configured = True
        except Exception as e:
            warnings.warn(f"Failed to configure DuckDB performance settings: {e}")
    
    def _try_execute(self, sql):
        """Try to execute SQL with fallback if it fails"""
        if self.conn is None:
            return False
            
        try:
            self.conn.execute(sql)
            return True
        except Exception:
            return False
            
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        
    def close(self):
        """Close the CrossLink instance and clean up resources."""
        # Clean up C++ instance if available
        if self._cpp_instance is not None:
            try:
                self._cpp_instance.cleanup()
                self._cpp_instance = None
            except Exception as e:
                warnings.warn(f"Failed to clean up C++ resources: {e}")
                
        # Clean up Python resources
        if self.conn is not None:
            try:
                # Close connection
                self.conn.close()
                self.conn = None
            except Exception as e:
                warnings.warn(f"Failed to close database connection: {e}")
                
        # Clean up shared memory regions
        for shm in self._shared_memory_regions:
            try:
                shm.close()
                shm.unlink()
            except Exception:
                pass
                
        self._shared_memory_regions = []
        
        # Remove from instances cache
        normalized_path = os.path.abspath(self.db_path)
        if normalized_path in self._instances:
            del self._instances[normalized_path] 

    # === Public API Methods ===

    def push(self, table: 'pa.Table', name: str = "", description: str = "") -> str:
        """Push a PyArrow table for batch sharing.

        Args:
            table: pyarrow.Table object.
            name: Optional name for the dataset.
            description: Optional description for the dataset.

        Returns:
            Unique ID assigned to the dataset.
        """
        if not self._ensure_arrow_extension():
            raise RuntimeError("Arrow extension is not available or failed to load.")

        if self._cpp_instance:
            try:
                return self._cpp_instance.push(table, name, description)
            except Exception as e:
                self._log(f"C++ push failed: {e}", level="error")
                raise # Re-raise the exception caught in the wrapper for clarity
        else:
            # Fallback to pure Python (if implemented, currently not)
            raise NotImplementedError("Pure Python push is not implemented. C++ bindings required.")


    def pull(self, identifier: str) -> 'pa.Table':
        """Pull a shared dataset as a PyArrow table.

        Args:
            identifier: ID or name of the dataset to pull.

        Returns:
            pyarrow.Table object.
        """
        if not self._ensure_arrow_extension():
            raise RuntimeError("Arrow extension is not available or failed to load.")

        if self._cpp_instance:
            try:
                return self._cpp_instance.pull(identifier)
            except Exception as e:
                self._log(f"C++ pull failed for {identifier}: {e}", level="error")
                raise # Re-raise
        else:
             raise NotImplementedError("Pure Python pull is not implemented. C++ bindings required.")


    def query(self, sql: str) -> 'pa.Table':
        """Execute a SQL query over shared datasets.

        Args:
            sql: SQL query string.

        Returns:
            pyarrow.Table containing the query results.
        """
        if not self._ensure_arrow_extension():
            # Query might work without Arrow depending on C++ or Python implementation
            pass

        if self._cpp_instance:
            try:
                return self._cpp_instance.query(sql)
            except Exception as e:
                self._log(f"C++ query failed: {e}", level="error")
                raise # Re-raise
        else:
            # Pure Python fallback using DuckDB connection
            if self.conn is None:
                 raise ConnectionError("Database connection is not available.")
            try:
                 # Assuming query results should be Arrow table
                 # Need to ensure Arrow extension is loaded for Python fallback too
                 if not self._ensure_arrow_extension():
                     raise RuntimeError("Arrow extension required for Python query fallback.")
                 return self.conn.execute(sql).arrow()
            except Exception as e:
                 raise RuntimeError(f"Failed to execute query in Python: {e}")

    def list_datasets(self) -> list[str]:
        """List available shared datasets."""
        if self._cpp_instance:
            try:
                return self._cpp_instance.list_datasets()
            except Exception as e:
                 self._log(f"C++ list_datasets failed: {e}", level="error")
                 raise # Re-raise
        else:
            # Python fallback
            if self.metadata_manager:
                 return self.metadata_manager.list_datasets()
            else:
                 warnings.warn("Metadata manager not available, cannot list datasets.")
                 return []

    # === New Streaming Methods ===

    def push_stream(self, schema: 'pa.Schema', name: str = "") -> Tuple[str, 'StreamWriter']:
        """Start pushing a stream of Arrow RecordBatches.

        Requires the C++ bindings.

        Args:
            schema: The pyarrow.Schema for the data that will be streamed.
            name: Optional name for the stream. A unique ID will be generated if empty.

        Returns:
            tuple[str, StreamWriter]: A tuple containing the stream ID and the
                                      StreamWriter object (obtained from C++ bindings).

        Raises:
            RuntimeError: If starting the stream fails.
            ImportError: If C++ bindings are not available.
            NotImplementedError: If C++ bindings are not available.
        """
        if not self._cpp_instance:
             raise NotImplementedError("Streaming functionality requires the C++ bindings.")
        if not self._ensure_arrow_extension(): # Arrow is fundamental for streaming
            raise RuntimeError("Arrow extension is not available or failed to load.")

        try:
            # Delegate directly to the C++ instance method via the wrapper
            return self._cpp_instance.push_stream(schema, name)
        except Exception as e:
            self._log(f"C++ push_stream failed: {e}", level="error")
            raise # Re-raise

    def pull_stream(self, stream_id: str) -> 'StreamReader':
        """Connect to an existing stream to pull Arrow RecordBatches.

        Requires the C++ bindings. The returned reader is iterable.

        Args:
            stream_id: The ID of the stream to connect to.

        Returns:
            StreamReader: A StreamReader object (obtained from C++ bindings)
                          to read batches from.

        Raises:
            RuntimeError: If connecting to the stream fails (e.g., not found).
            ImportError: If C++ bindings are not available.
            NotImplementedError: If C++ bindings are not available.
        """
        if not self._cpp_instance:
             raise NotImplementedError("Streaming functionality requires the C++ bindings.")
        if not self._ensure_arrow_extension():
            raise RuntimeError("Arrow extension is not available or failed to load.")

        try:
            # Delegate directly to the C++ instance method via the wrapper
            return self._cpp_instance.pull_stream(stream_id)
        except Exception as e:
            self._log(f"C++ pull_stream failed for {stream_id}: {e}", level="error")
            raise # Re-raise


    # ... (existing context manager __enter__, __exit__, close methods) ...
    def __enter__(self):
        # ... (code as previously shown) ...
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        # ... (code as previously shown) ...
        pass

    def close(self):
        """Close the CrossLink instance and clean up resources."""
        # Clean up C++ instance if available
        if self._cpp_instance is not None:
            try:
                # Rely on the cleanup method exposed in the wrapper
                self._cpp_instance.cleanup()
                # Remove instance from class dict to allow re-creation if needed
                normalized_path = os.path.abspath(self.db_path)
                if normalized_path in CrossLink._instances:
                    del CrossLink._instances[normalized_path]
                self._cpp_instance = None # Mark as cleaned up
            except Exception as e:
                warnings.warn(f"Failed to clean up C++ resources: {e}")

        # ... (rest of existing close method for Python resources) ...
        if self.conn is not None:
            try:
                # Close connection
                self.conn.close()
                self.conn = None
            except Exception as e:
                warnings.warn(f"Failed to close database connection: {e}")

        # Remove instance from class dict if Python-only instance
        if self._cpp_instance is None: # Ensure we remove if it wasn't a cpp instance
             normalized_path = os.path.abspath(self.db_path)
             if normalized_path in CrossLink._instances:
                 del CrossLink._instances[normalized_path]

        # Clean up shared memory regions
        for shm in self._shared_memory_regions:
            try:
                shm.close()
                shm.unlink()
            except Exception:
                pass
                
        self._shared_memory_regions = []
        
        # Remove from instances cache
        normalized_path = os.path.abspath(self.db_path)
        if normalized_path in self._instances:
            del self._instances[normalized_path] 