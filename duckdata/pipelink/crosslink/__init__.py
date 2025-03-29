"""
CrossLink - Cross-language data sharing with zero-copy optimization

CrossLink enables easy data sharing between Python, R, Julia, and C++
with metadata-based zero-copy optimizations across languages.
"""

import os
import importlib.util
import warnings
import sys

# Cache for loaded implementations
_IMPL_CACHE = {}

def _lazy_import(module_name, package=None):
    """Lazily import a module only when needed for better startup performance"""
    if module_name in _IMPL_CACHE:
        return _IMPL_CACHE[module_name]
    
    try:
        module = importlib.import_module(module_name, package)
        _IMPL_CACHE[module_name] = module
        return module
    except ImportError as e:
        warnings.warn(f"Failed to import {module_name}: {e}")
        return None

def get_implementation(language=None):
    """Get the appropriate CrossLink implementation for the specified language
    
    Args:
        language: Language to get implementation for (python, r, julia, cpp).
                 If None, auto-detect based on current environment.
                 
    Returns:
        CrossLink implementation module
    """
    # Auto-detect language if not specified
    if language is None:
        # Check if we're in a Python environment
        language = "python"
        
        # Check if we're in R, Julia, or C++
        for env_var in ["R_HOME", "JULIA_HOME", "CPLUSPLUS"]:
            if os.environ.get(env_var):
                language = env_var.split("_")[0].lower()
                break
    
    # For non-Python languages, check if the runtime is actually available
    if language != "python":
        if language == "r" and not _check_r_available():
            warnings.warn("R environment detected but R is not available or rpy2 is not installed. Falling back to Python.")
            language = "python"
        elif language == "julia" and not _check_julia_available():
            warnings.warn("Julia environment detected but Julia is not available or PyJulia is not installed. Falling back to Python.")
            language = "python"
        elif language == "cpp" and not _check_cpp_available():
            warnings.warn("C++ environment detected but required libraries are not available. Falling back to Python.")
            language = "python"
    
    # Return the appropriate implementation
    if language == "python":
        return _lazy_import("duckdata.pipelink.crosslink.python.crosslink")
    elif language == "r":
        return _lazy_import("duckdata.pipelink.crosslink.r.crosslink")
    elif language == "julia":
        return _lazy_import("duckdata.pipelink.crosslink.julia.crosslink")
    elif language == "cpp":
        return _lazy_import("duckdata.pipelink.crosslink.cpp.crosslink")
    else:
        raise ValueError(f"Unsupported language: {language}")

def _check_r_available():
    """Check if R is available for use"""
    try:
        import rpy2  # noqa
        return True
    except ImportError:
        return False

def _check_julia_available():
    """Check if Julia is available for use"""
    try:
        import julia  # noqa
        return True
    except ImportError:
        return False

def _check_cpp_available():
    """Check if C++ libraries are available for use"""
    try:
        import ctypes  # noqa
        return True
    except ImportError:
        return False

def get_instance(*args, **kwargs):
    """Get a CrossLink instance for the current language
    
    Args:
        *args: Positional arguments to pass to the CrossLink constructor
        **kwargs: Keyword arguments to pass to the CrossLink constructor
        
    Returns:
        CrossLink instance
        
    Raises:
        ImportError: If no CrossLink implementation is available
    """
    try:
        impl = get_implementation()
        if impl and hasattr(impl, "CrossLink"):
            return impl.CrossLink.get_instance(*args, **kwargs)
        elif impl and hasattr(impl, "get_instance"):
            return impl.get_instance(*args, **kwargs)
        elif impl and hasattr(impl, "CrossLinkManager"):
            # Handle Julia-style constructor
            return impl.CrossLinkManager(*args, **kwargs)
        elif impl and hasattr(impl, "crosslink_connect"):
            # Handle R-style constructor
            return impl.crosslink_connect(*args, **kwargs)
        else:
            raise ImportError("No compatible CrossLink implementation available")
    except Exception as e:
        # Provide more helpful error message
        sys.stderr.write(f"Error initializing CrossLink: {str(e)}\n")
        sys.stderr.write("This could be due to incompatible versions or missing dependencies.\n")
        sys.stderr.write("Please make sure DuckDB and other required packages are installed correctly.\n")
        raise

# Expose common functionality directly at module level for convenience
try:
    # Create proxy for common methods
    def push(df, *args, **kwargs):
        """Push a DataFrame to CrossLink"""
        return get_instance().push(df, *args, **kwargs)
    
    def pull(identifier, *args, **kwargs):
        """Pull a dataset from CrossLink"""
        return get_instance().pull(identifier, *args, **kwargs)
    
    def query(sql, *args, **kwargs):
        """Execute a SQL query with CrossLink"""
        return get_instance().query(sql, *args, **kwargs)
except Exception:
    # If proxy methods can't be created, just skip them
    pass

# Set version
__version__ = "0.2.0"

# Define what gets imported with 'from pipelink.crosslink import *'
__all__ = ['get_instance', 'push', 'pull', 'query', 'get_implementation']

# Only attempt to import from Python module for compatibility
# Don't fail if it's not available
try:
    from .python.crosslink import CrossLink, CrossLinkMetadataManager
    __all__.extend(['CrossLink', 'CrossLinkMetadataManager'])
except ImportError:
    pass 