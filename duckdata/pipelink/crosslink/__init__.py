"""
CrossLink: Simple cross-language data sharing

CrossLink enables easy data sharing between Python, R, Julia, and C++
"""

# Set version
__version__ = "0.1.0"

# Define what gets imported with 'from pipelink.crosslink import *'
__all__ = ['CrossLink']

# Use a function to defer imports until they're actually needed
def _import_when_needed():
    # Import these modules only when someone actually tries to use them
    from pipelink.crosslink.python.crosslink import CrossLink
    
    # Add to the module's global namespace
    globals()['CrossLink'] = CrossLink

# Trigger the imports when this module is imported
_import_when_needed() 