"""
PipeDuck: Enhanced Cross-Language Pipeline Orchestration

This package provides tools for building and executing data processing pipelines
across multiple languages (Python, R, and Julia) with advanced features
such as checkpointing, monitoring, and efficient cross-language data sharing.
"""

# Import the main PipeDuck class and utilities
try:
    from .pipeduck import PipeDuck, run_pipeline
except ImportError:
    from pipeduck import PipeDuck, run_pipeline

# Set version
__version__ = '0.1.0'

# Define public interface
__all__ = [
    'PipeDuck',
    'run_pipeline',
]

# Try to import DuckConnect
try:
    # Try direct import first
    from duck_connect import DuckConnect
    __all__.append('DuckConnect')
except ImportError:
    try:
        # Try relative import
        from .duck_connect.core.facade import DuckConnect
        __all__.append('DuckConnect')
    except ImportError:
        # If not available, don't add to __all__
        pass

# Try to import Python-specific components
try:
    from .python.duck_connector import DuckConnector
    from .python.duck_context import DuckContext
    __all__.extend(['DuckConnector', 'DuckContext'])
except ImportError:
    # If not available, don't add to __all__
    pass

# Use a function to defer imports until they're actually needed
def _import_when_needed():
    # Import these modules only when someone actually tries to use them
    from .python.duck_context import DuckContext
    
    # Add to the module's global namespace
    globals()['DuckContext'] = DuckContext

# Trigger the imports when this module is imported
_import_when_needed() 