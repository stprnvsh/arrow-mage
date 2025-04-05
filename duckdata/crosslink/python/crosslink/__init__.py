"""
CrossLink Python API

CrossLink is a library for efficient cross-language data sharing,
with support for distributed mode using Apache Arrow Flight.
"""

import os
import sys
import warnings
from typing import Dict, Any, Optional, Union, List, Tuple, Callable

# Try to import the C++ extension
try:
    from .crosslink_cpp import (
        CrossLinkCpp,
        CrossLinkConfig,
        OperationMode,
        StreamWriter,
        StreamReader,
        is_cpp_available
    )
    _HAS_CPP = True
except ImportError as e:
    # Set flag and show warning
    _HAS_CPP = False
    warnings.warn(f"Failed to import CrossLink C++ extension: {e}. Using pure Python implementation.")
    
    # Create placeholder classes if C++ is not available
    # These will be overridden by a pure Python implementation if needed
    class CrossLinkCpp:
        def __init__(self, *args, **kwargs):
            raise ImportError("CrossLink C++ extension is not available")
    
    class CrossLinkConfig:
        def __init__(self, *args, **kwargs):
            raise ImportError("CrossLink C++ extension is not available")
    
    class OperationMode:
        LOCAL = 0
        DISTRIBUTED = 1
    
    class StreamWriter:
        def __init__(self, *args, **kwargs):
            raise ImportError("CrossLink C++ extension is not available")
    
    class StreamReader:
        def __init__(self, *args, **kwargs):
            raise ImportError("CrossLink C++ extension is not available")

# Define version
__version__ = "0.1.0"

def get_instance(db_path: str = "crosslink.duckdb", 
                debug: bool = False,
                config: Optional[CrossLinkConfig] = None) -> CrossLinkCpp:
    """
    Get a CrossLink instance with either the C++ backend (preferred) or a pure Python implementation.
    
    Args:
        db_path: Path to the DuckDB database file (default: "crosslink.duckdb")
        debug: Enable debug mode (default: False)
        config: Optional configuration object (takes precedence over db_path and debug)
        
    Returns:
        A CrossLink instance
    """
    if not _HAS_CPP:
        raise ImportError("CrossLink C++ extension is not available")
    
    # If a config is provided, use it
    if config is not None:
        return CrossLinkCpp(config)
    
    # Otherwise use the db_path and debug parameters
    return CrossLinkCpp(db_path, debug)

# Export key classes and functions
__all__ = [
    'get_instance',
    'CrossLinkCpp',
    'CrossLinkConfig',
    'OperationMode',
    'StreamWriter',
    'StreamReader',
] 