"""
    CrossLink - High-Performance Cross-Language Data Sharing

This package provides Python bindings for the CrossLink library, which enables
efficient data sharing between processes written in different programming languages
(C++, Python, R, Julia) with support for distributed operation via Apache Arrow Flight.
"""

# Try to import from our C++ bindings first
try:
    from .crosslink import (
        get_instance, CrossLinkCpp, CrossLinkConfig, OperationMode,
        StreamWriter, StreamReader
    )
    # Mark that we have C++ bindings available
    HAS_CPP_BINDINGS = True
except ImportError:
    # If C++ bindings are not available, we'll use the pure Python implementation
    HAS_CPP_BINDINGS = False

# Import from pure Python implementation
from .core.core import CrossLink
from .data_operations.data_operations import push, pull, get_table_reference, register_external_table, list_datasets, query
from .arrow_integration.arrow_integration import share_arrow_table, get_arrow_table, create_duckdb_view_from_arrow
from .shared_memory.shared_memory import setup_shared_memory, get_from_shared_memory, cleanup_all_shared_memory
from .utilities.utilities import cleanup_all_arrow_files, is_file_safe_to_delete
from .metadata.metadata import CrossLinkMetadataManager

# Define version
__version__ = "0.1.0"

__all__ = [
    # Core classes
    'CrossLink', 
    
    # Flight-related classes and functions (if available)
    'get_instance', 'CrossLinkCpp', 'CrossLinkConfig', 'OperationMode',
    'StreamWriter', 'StreamReader',
    
    # Data operations
    'push', 'pull', 'get_table_reference', 'register_external_table', 
    'list_datasets', 'query',
    
    # Arrow integration
    'share_arrow_table', 'get_arrow_table', 'create_duckdb_view_from_arrow',
    
    # Shared memory
    'setup_shared_memory', 'get_from_shared_memory',
    
    # Utilities
    'cleanup_all_arrow_files', 'cleanup_all_shared_memory', 'is_file_safe_to_delete',
    
    # Metadata
    'CrossLinkMetadataManager'
] 