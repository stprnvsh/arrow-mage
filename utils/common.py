import streamlit as st
import os
import sys
import math
import logging
import psutil
import datetime
import time

# Configure logging
logger = logging.getLogger(__name__)

# Ensure arrow-cache-mcp is in path
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.abspath(os.path.join(script_dir, '..'))
arrow_mcp_path = os.path.join(workspace_root, 'arrow-cache-mcp')
arrow_mcp_src_path = os.path.join(arrow_mcp_path, 'src')
if arrow_mcp_src_path not in sys.path:
    sys.path.insert(0, arrow_mcp_src_path)
    logger.info(f"Added {arrow_mcp_src_path} to sys.path")


try:
    # Core cache functionality
    from arrow_cache_mcp.core import (
        get_arrow_cache, 
        clear_cache_files, 
        close_cache,
        remove_dataset,
        get_datasets_list,
        get_memory_usage
    )
    
    # AI interaction functions
    from arrow_cache_mcp.ai import (
        get_ai_config,
        get_ai_provider,
        ask_ai,
        display_conversation_history,
        get_supported_providers
    )
    
    # Utilities
    from arrow_cache_mcp.utils import (
        clean_dataset_name
    )
    
except ImportError as e:
    st.error(f"Failed to import from arrow-cache-mcp: {e}")
    logger.error(f"Import error: {e}")

# Get or create the cache (singleton pattern)
@st.cache_resource
def get_cache():
    SPILL_DIRECTORY = ".arrow_cache_spill_streamlit"
    return get_arrow_cache({
        "spill_directory": SPILL_DIRECTORY,
        "memory_limit": 20 * 1024 * 1024 * 1024  # 20 GB
    })

import re
import ast
from typing import Dict, Any, List, Optional, Tuple

def extract_udf_name(code: str) -> str:
    """Extract the UDF function name from code"""
    # Look for function definition
    match = re.search(r'def\s+([a-zA-Z0-9_]+)\s*\(', code)
    if match:
        return match.group(1)
    return "unnamed_udf"

def extract_udf_docs(code: str) -> str:
    """Extract docstring from UDF code"""
    # Parse the code
    try:
        tree = ast.parse(code)
        
        # Look for function definitions
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef):
                # Check if it has a docstring
                if ast.get_docstring(node):
                    return ast.get_docstring(node)
    except SyntaxError:
        # If code can't be parsed, try regex
        pass
    
    # Fallback to regex if AST parsing fails
    match = re.search(r'def\s+[a-zA-Z0-9_]+\s*\([^)]*\):\s*[\'"]([^\'"]*)[\'"]', code, re.DOTALL)
    if match:
        return match.group(1).strip()
    
    return "No description available"

def extract_udf_params(code):
    """Extract parameter information from UDF code."""
    params = {}
    param_pattern = r'def\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\((.*?)\):'
    param_match = re.search(param_pattern, code, re.DOTALL)
    
    if param_match:
        param_str = param_match.group(1).strip()
        if param_str and param_str != "":
            param_parts = []
            current_part = ""
            brace_level = 0
            
            for char in param_str:
                if char == ',' and brace_level == 0:
                    param_parts.append(current_part.strip())
                    current_part = ""
                else:
                    current_part += char
                    if char in '[{(':
                        brace_level += 1
                    elif char in ']})':
                        brace_level = max(0, brace_level - 1)
            
            if current_part:
                param_parts.append(current_part.strip())
            
            for part in param_parts:
                # Skip self parameter
                if part.strip() == 'self':
                    continue
                    
                # Parameter with default value
                if '=' in part:
                    name, default = part.split('=', 1)
                    name = name.strip()
                    
                    # Check for type hints
                    if ':' in name:
                        name, type_hint = name.split(':', 1)
                        name = name.strip()
                        type_hint = type_hint.strip()
                    else:
                        type_hint = "any"
                    
                    # Try to evaluate the default value
                    try:
                        default_val = eval(default.strip())
                        param_type = type(default_val).__name__
                    except:
                        default_val = default.strip()
                        param_type = "str"
                    
                    params[name] = {
                        "type": param_type,
                        "type_hint": type_hint,
                        "default": default_val
                    }
                else:
                    # Parameter without default value
                    name = part.strip()
                    
                    # Check for type hints
                    if ':' in name:
                        name, type_hint = name.split(':', 1)
                        name = name.strip()
                        type_hint = type_hint.strip()
                    else:
                        type_hint = "any"
                        
                    params[name] = {
                        "type": "any",
                        "type_hint": type_hint,
                        "required": True
                    }
    
    return params 
# Improved size display function
def get_size_display(bytes_value):
    """
    Convert a size in bytes to a human-readable string with appropriate units.
    
    Args:
        bytes_value: Size in bytes
        
    Returns:
        String like "12 KB" or "3.45 MB"
    """
    # Safety check for None or negative values
    if bytes_value is None or bytes_value < 0:
        return "0 B"
    
    # Force to float for consistent division
    bytes_value = float(bytes_value)
    
    # Minimum display size should be 0.01 of whatever unit we're using
    if bytes_value < 1:
        return "1 B"
    
    # Define unit prefixes 
    units = ['B', 'KB', 'MB', 'GB', 'TB', 'PB']
    
    # Calculate the appropriate unit index
    unit_idx = min(int(math.log(bytes_value, 1024)), len(units) - 1) if bytes_value > 0 else 0
    
    # Get the value in the chosen units (bytes, KB, MB, etc)
    value = bytes_value / (1024 ** unit_idx)
    
    # Format with 2 decimal places if MB or larger, otherwise round to integer
    if unit_idx >= 2:  # MB or larger
        return f"{value:.2f} {units[unit_idx]}"
    else:
        return f"{int(value)} {units[unit_idx]}"

# Memory usage display
def render_memory_usage():
    """Render memory usage information in Streamlit sidebar"""
    process = psutil.Process(os.getpid())
    memory_info = process.memory_info()
    
    # Convert to MB
    memory_usage = memory_info.rss / 1024 / 1024  # MB
    
    # Display memory usage
    st.metric(
        label="Memory Usage", 
        value=f"{memory_usage:.1f} MB",
        delta=None
    )
    
    # Get system memory info
    system_memory = psutil.virtual_memory()
    system_memory_used_percent = system_memory.percent
    
    # Create a progress bar for system memory
    st.progress(system_memory_used_percent / 100)
    st.caption(
        f"System Memory: {system_memory_used_percent}% used "
        f"({(system_memory.used / 1024 / 1024 / 1024):.1f} GB of "
        f"{(system_memory.total / 1024 / 1024 / 1024):.1f} GB)"
    )

# Custom renderer for dataset cards
def render_dataset_card(dataset):
    """
    Render a dataset card with its info and action buttons.
    
    Args:
        dataset: Dataset information dictionary
    """
    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Get size information
            size_bytes = dataset.get('size_bytes', 0)
            size_display = get_size_display(size_bytes)
            
            # Format row count with commas
            row_count = dataset.get('row_count', 'Unknown')
            if isinstance(row_count, int) or (isinstance(row_count, str) and row_count.isdigit()):
                row_count = f"{int(row_count):,}"
            
            # Get column information
            if 'column_count' in dataset:
                column_count = dataset.get('column_count', 0)
            # Check in the metadata field 
            elif 'metadata' in dataset and isinstance(dataset['metadata'], dict) and 'column_count' in dataset['metadata']:
                column_count = dataset['metadata'].get('column_count', 0)
            # Use length of columns array if available
            elif 'columns' in dataset:
                column_count = len(dataset.get('columns', []))
            # If present in metadata.columns
            elif 'metadata' in dataset and isinstance(dataset['metadata'], dict) and 'columns' in dataset['metadata']:
                column_count = len(dataset['metadata'].get('columns', []))
            else:
                column_count = 0
            
            # Format metadata for display
            metadata_html = ""
            
            # Add format info if available
            if 'format' in dataset:
                metadata_html += f"<strong>Format:</strong> {dataset['format']} | "
                
            # Add source if available (truncated for display)
            if 'source' in dataset:
                source = dataset['source']
                if len(source) > 30:
                    source = source[:27] + "..."
                metadata_html += f"<strong>Source:</strong> {source} | "
                
            # Add created time if available
            if 'created_at' in dataset:
                from datetime import datetime
                created_time = datetime.fromtimestamp(dataset['created_at']).strftime("%Y-%m-%d %H:%M")
                metadata_html += f"<strong>Created:</strong> {created_time}"
            
            # Generate tooltip with column names and types
            columns_tooltip = ""
            # First check columns in the dataset directly
            if 'columns' in dataset and dataset['columns']:
                columns_list = dataset['columns']
            # Then check in metadata
            elif 'metadata' in dataset and isinstance(dataset['metadata'], dict) and 'columns' in dataset['metadata']:
                columns_list = dataset['metadata']['columns']
            else:
                columns_list = []
                
            # If we have columns and type information
            if columns_list:
                # Use column types from metadata if available
                column_types = {}
                if 'metadata' in dataset and isinstance(dataset['metadata'], dict) and 'column_types' in dataset['metadata']:
                    column_types = dataset['metadata']['column_types']
                elif 'metadata' in dataset and isinstance(dataset['metadata'], dict) and 'dtypes' in dataset['metadata']:
                    column_types = dataset['metadata']['dtypes']
                    
                # Create list of columns with types for tooltip
                col_type_pairs = []
                for col in columns_list[:20]:  # Limit to 20 columns for tooltip
                    dtype = column_types.get(col, '') if column_types else ''
                    col_type_pairs.append(f"{col} ({dtype})")
                
                if len(columns_list) > 20:
                    col_type_pairs.append("... and more")
                    
                columns_tooltip = "<br>".join(col_type_pairs)
            
            st.markdown(f"""
            <div class="dataset-card" title="{columns_tooltip}">
                <h4>{dataset['name']}</h4>
                <p>
                    <strong>Rows:</strong> {row_count} | 
                    <strong>Columns:</strong> {column_count} | 
                    <strong>Size:</strong> {size_display}
                </p>
                <p style="font-size: 0.9em; opacity: 0.8;">
                    {metadata_html}
                </p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.button("View", key=f"view_{dataset['name']}", 
                      on_click=lambda: st.session_state.update({'selected_dataset': dataset['name']}))
            st.button("Remove", key=f"remove_{dataset['name']}", 
                      on_click=lambda: remove_and_update(dataset['name']))

# Remove dataset and update UI
def remove_and_update(dataset_name):
    """Remove a dataset and update the UI."""
    success, message = remove_dataset(dataset_name)
    if success:
        st.success(message)
        if 'selected_dataset' in st.session_state and st.session_state.selected_dataset == dataset_name:
            st.session_state.pop('selected_dataset')
    else:
        st.error(message)
    st.rerun()

# Display AI conversation history
def display_ai_conversation_history():
    """Display the conversation history in a chat-like interface"""
    if "conversation_history" not in st.session_state or not st.session_state.conversation_history:
        st.info("No conversation history yet. Ask a question to start.")
        return
    
    for entry in st.session_state.conversation_history:
        if entry["role"] == "user":
            st.markdown(f"""
            <div class="user-message">
                <strong>You:</strong><br/>
                {entry["content"]}
            </div>
            """, unsafe_allow_html=True)
        elif entry["role"] == "assistant":
            # For AI responses, we need to handle the markdown content safely
            # First create the message container with minimal HTML
            st.markdown(f"""<div class="claude-message"><strong>AI:</strong></div>""", 
                       unsafe_allow_html=True)
            
            # Then use a container to avoid the white background issue
            with st.container():
                # Handle each part of the response separately
                content = entry["content"]
                
                # Split content at code blocks to handle them separately
                parts = []
                last_end = 0
                
                # Find code blocks
                code_pattern = r"\n```(?:sql)?\n(.*?)\n```\n"
                for match in re.finditer(code_pattern, content, re.DOTALL):
                    # Add text before the code block
                    if match.start() > last_end:
                        parts.append(("text", content[last_end:match.start()]))
                        
                    # Add the code block
                    parts.append(("code", match.group(1)))
                    last_end = match.end()
                
                # Add any remaining text
                if last_end < len(content):
                    parts.append(("text", content[last_end:]))
                
                # If we found parts, render them
                if parts:
                    for part_type, part_content in parts:
                        if part_type == "code":
                            st.code(part_content, language="sql")
                        else:
                            st.markdown(part_content)
                else:
                    # No code blocks found, render everything as markdown
                    st.markdown(content)
        else:  # system messages
            st.error(entry["content"])

# Add a clear history button to the UI
def add_clear_history_button():
    """Add a button to clear the conversation history"""
    if "conversation_history" in st.session_state and st.session_state.conversation_history:
        if st.button("Clear Conversation History", key="clear_history_btn"):
            st.session_state.conversation_history = []
            st.session_state.last_ai_result = None
            st.rerun()

# Check if we need to import the re module
import re 

def format_timestamp(timestamp):
    """Format a timestamp for display."""
    if timestamp is None:
        return "N/A"
        
    if isinstance(timestamp, (int, float)):
        dt = datetime.datetime.fromtimestamp(timestamp)
    elif isinstance(timestamp, datetime.datetime):
        dt = timestamp
    else:
        try:
            dt = datetime.datetime.fromisoformat(timestamp)
        except:
            return timestamp
    
    # Format the datetime
    return dt.strftime("%Y-%m-%d %H:%M:%S") 