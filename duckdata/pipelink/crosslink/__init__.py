"""
CrossLink: Simple cross-language data sharing with zero-copy optimization

CrossLink enables easy data sharing between Python, R, Julia, and C++
with metadata-based zero-copy optimizations across languages.
"""

# Set version
__version__ = "0.2.0"

# Define what gets imported with 'from pipelink.crosslink import *'
__all__ = ['CrossLink', 'CrossLinkMetadataManager']

# Use a function to defer imports until they're actually needed
def _import_when_needed():
    # Import these modules only when someone actually tries to use them
    from pipelink.crosslink.python.crosslink import CrossLink, CrossLinkMetadataManager
    
    # Add to the module's global namespace
    globals()['CrossLink'] = CrossLink
    globals()['CrossLinkMetadataManager'] = CrossLinkMetadataManager

# Trigger the imports when this module is imported
_import_when_needed() 