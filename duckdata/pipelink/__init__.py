"""
PipeLink: Cross-Language Pipeline Orchestration

PipeLink enables building and executing data processing pipelines 
across Python, R, and Julia with seamless data sharing between steps.
"""

# Set version
__version__ = "0.2.0"

# Define what gets imported with 'from pipelink import *'
__all__ = ['run_pipeline', 'NodeContext']

# Use a function to defer imports until they're actually needed
def _import_when_needed():
    # Import these modules only when someone actually tries to use them
    from pipelink.python.pipelink import run_pipeline
    from pipelink.python.node_utils import NodeContext
    
    # Add to the module's global namespace
    globals()['run_pipeline'] = run_pipeline
    globals()['NodeContext'] = NodeContext

# Trigger the imports when this module is imported
_import_when_needed() 