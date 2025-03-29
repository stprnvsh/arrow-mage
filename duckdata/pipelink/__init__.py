"""
PipeLink: Cross-Language Pipeline Orchestration

This package enables seamless data pipeline construction across Python, R, and Julia
with zero-copy optimization for efficient data sharing between languages.
"""

import os
import sys
import importlib.util

# Set version
__version__ = "1.0.0"

# Import core functionality
try:
    from .python.pipelink import run_pipeline
    from .python.node_utils import NodeContext, get_optimized_data_access
    from .python.data_node_context import DataNodeContext
    from .crosslink.python.crosslink import CrossLink
except ImportError as e:
    # Provide informative error for missing dependencies
    print(f"Error importing PipeLink core modules: {e}")
    print("Make sure all dependencies are installed.")

# Setup daemon pool for language daemons
from .python.language_daemon import LanguageDaemonPool

# Create global daemon pool with reasonable defaults
_daemon_pool = None

def get_daemon_pool(min_daemons=1, max_daemons=4):
    """
    Get or create the global daemon pool
    
    Args:
        min_daemons: Minimum number of daemons per language
        max_daemons: Maximum number of daemons per language
        
    Returns:
        LanguageDaemonPool instance
    """
    global _daemon_pool
    if _daemon_pool is None:
        _daemon_pool = LanguageDaemonPool(min_daemons=min_daemons, max_daemons=max_daemons)
    return _daemon_pool

# Try to import optional Dask integration
try:
    from .python.dask_context import DaskNodeContext
    HAS_DASK = True
except ImportError:
    HAS_DASK = False

def run_optimized_pipeline(pipeline_file, only_nodes=None, start_from=None, 
                         verbose=False, max_concurrency=None, use_daemon=True):
    """
    Run a pipeline with optimized concurrency and zero-copy optimization
    
    Args:
        pipeline_file: Path to pipeline YAML file
        only_nodes: Only execute these nodes (and their dependencies)
        start_from: Start execution from this node
        verbose: Enable verbose output
        max_concurrency: Maximum number of nodes to run concurrently (defaults to CPU count)
        use_daemon: Use daemon processes for language execution
    """
    # Set environment variable for zero-copy optimization
    os.environ['PIPELINK_ENABLE_ZERO_COPY'] = '1'
    
    # Ensure daemon pool is initialized if using daemons
    if use_daemon:
        get_daemon_pool()
    
    # Import and run the enhanced pipeline
    from .python.pipelink import run_pipeline
    return run_pipeline(
        pipeline_file=pipeline_file,
        only_nodes=only_nodes,
        start_from=start_from,
        verbose=verbose,
        use_daemon=use_daemon,
        max_concurrency=max_concurrency
    )

# Define what's exported when doing 'from pipelink import *'
__all__ = [
    'run_pipeline', 
    'run_optimized_pipeline',
    'NodeContext', 
    'DataNodeContext', 
    'CrossLink',
    'get_optimized_data_access',
    'get_daemon_pool',
    '__version__'
]

if HAS_DASK:
    __all__.append('DaskNodeContext') 