"""
Arrow Cache MCP - Data Science Toolkit for AI Agents
"""

__version__ = "0.1.1"

# Import core components
from src.arrow_cache_mcp.core import (
    get_arrow_cache, 
    clear_cache_files, 
    close_cache,
    remove_dataset,
    get_datasets_list,
    get_memory_usage,
    remove_and_update
)

# Import data loading functions
from src.arrow_cache_mcp.loaders import (
    load_dataset_from_path,
    load_dataset_from_upload,
    load_dataset_from_url,
    guess_format_from_path,
    SUPPORTED_FORMATS
)

# Import visualization components
from src.arrow_cache_mcp.visualization import (
    create_plot,
    render_dataset_card,
    get_size_display
)

# Import AI interaction functions
from src.arrow_cache_mcp.ai import (
    get_claude_api_key,
    extract_and_run_queries,
    ask_claude,
    display_conversation_history,
    add_clear_history_button
)

# Import utilities
from src.arrow_cache_mcp.utils import (
    clean_dataset_name,
    extract_table_references
)

# Make all these available when importing arrow_cache_mcp
__all__ = [
    # Core
    "get_arrow_cache", "clear_cache_files", "close_cache",
    "remove_dataset", "get_datasets_list", "get_memory_usage",
    "remove_and_update",
    
    # Data loading
    "load_dataset_from_path", "load_dataset_from_upload", 
    "load_dataset_from_url", "guess_format_from_path",
    "SUPPORTED_FORMATS",
    
    # Visualization
    "create_plot", "render_dataset_card", "get_size_display",
    
    # AI
    "get_claude_api_key", "extract_and_run_queries", "ask_claude",
    "display_conversation_history", "add_clear_history_button",
    
    # Utilities
    "clean_dataset_name", "extract_table_references"
]