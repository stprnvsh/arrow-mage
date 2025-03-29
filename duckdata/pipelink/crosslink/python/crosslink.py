"""
CrossLink: Simple cross-language data sharing with true zero-copy optimization

This compatibility layer maintains backward compatibility with code that imports directly
from crosslink.py rather than using the new modular structure.
"""

# Import from modular structure
from .core.core import CrossLink
from .data_operations.data_operations import (
    push, pull, get_table_reference, register_external_table, list_datasets, query
)
from .arrow_integration.arrow_integration import (
    share_arrow_table, get_arrow_table, create_duckdb_view_from_arrow
)
from .shared_memory.shared_memory import (
    setup_shared_memory, get_from_shared_memory, cleanup_all_shared_memory
)
from .utilities.utilities import (
    cleanup_all_arrow_files, is_file_safe_to_delete
)
from .metadata.metadata import CrossLinkMetadataManager

# For backward compatibility - expose the important utility functions
_check_shared_memory_available = None
_shared_memory_available = None
_import_pyarrow = None
_import_psutil = None
_mmapped_tables = {}
_shared_memory_regions = {}

# Import the utility functions from the appropriate modules
try:
    from .shared_memory.shared_memory import _check_shared_memory_available, _shared_memory_available
except ImportError:
    pass

try:
    from .arrow_integration.arrow_integration import _import_pyarrow, _mmapped_tables
except ImportError:
    pass

try:
    from .utilities.utilities import _import_psutil
except ImportError:
    pass

# Add monkeypatching for backward compatibility
# Use the CrossLink instance methods for push, pull, etc.
def _patch_crosslink_instance_methods():
    CrossLink.push = push
    CrossLink.pull = pull
    CrossLink.get_table_reference = get_table_reference
    CrossLink.register_external_table = register_external_table
    CrossLink.share_arrow_table = share_arrow_table
    CrossLink.get_arrow_table = get_arrow_table
    CrossLink.create_duckdb_view_from_arrow = create_duckdb_view_from_arrow
    CrossLink.cleanup_all_arrow_files = staticmethod(cleanup_all_arrow_files)
    CrossLink.cleanup_all_shared_memory = staticmethod(cleanup_all_shared_memory)
    CrossLink._is_file_safe_to_delete = staticmethod(is_file_safe_to_delete)

# Apply the patches
_patch_crosslink_instance_methods()

# Clean up the namespace to avoid exposing internal helpers
del _patch_crosslink_instance_methods