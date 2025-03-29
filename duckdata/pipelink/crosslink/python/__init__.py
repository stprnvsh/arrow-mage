"""
CrossLink: Simple cross-language data sharing library with zero-copy optimization
"""

from .core.core import CrossLink
from .data_operations.data_operations import push, pull, get_table_reference, register_external_table, list_datasets, query
from .arrow_integration.arrow_integration import share_arrow_table, get_arrow_table, create_duckdb_view_from_arrow
from .shared_memory.shared_memory import setup_shared_memory, get_from_shared_memory, cleanup_all_shared_memory
from .utilities.utilities import cleanup_all_arrow_files, is_file_safe_to_delete
from .metadata.metadata import CrossLinkMetadataManager

__all__ = [
    'CrossLink', 
    'push', 'pull', 'get_table_reference', 'register_external_table', 'list_datasets', 'query',
    'share_arrow_table', 'get_arrow_table', 'create_duckdb_view_from_arrow',
    'setup_shared_memory', 'get_from_shared_memory',
    'cleanup_all_arrow_files', 'cleanup_all_shared_memory', 'is_file_safe_to_delete',
    'CrossLinkMetadataManager'
] 