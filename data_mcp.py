import streamlit as st
import re
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import time
import os
import sys
import json
import anthropic
from textwrap import dedent
import urllib.request
import tempfile
import glob
from urllib.parse import urlparse
import matplotlib.pyplot as plt
import io
import base64
import logging
import duckdb
import math

# --- Configure logging ---
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Must be the first Streamlit command!
st.set_page_config(layout="wide", page_title="Data Sandbox", page_icon="📊")

# --- Ensure arrow-cache-mcp is in the path ---
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.abspath(os.path.join(script_dir, '..'))
arrow_mcp_path = os.path.join(workspace_root, 'arrow-cache-mcp')
if arrow_mcp_path not in sys.path:
    sys.path.insert(0, arrow_mcp_path)
    logger.info(f"Added {arrow_mcp_path} to sys.path")

# --- Import from arrow-cache-mcp package ---
try:
    # Core cache functionality
    from arrow_cache_mcp.core import (
        get_arrow_cache, 
        clear_cache_files, 
        close_cache,
        remove_dataset,
        get_datasets_list,
        get_memory_usage,
        import_data_directly
    )
    
    # Data loading functions
    from arrow_cache_mcp.loaders import (
        load_dataset_from_path,
        load_dataset_from_upload,
        load_dataset_from_url,
        guess_format_from_path,
        SUPPORTED_FORMATS
    )
    
    # Visualization components
    from arrow_cache_mcp.visualization import (
        create_plot,
        render_dataset_card,
        get_size_display
    )
    
    # AI interaction functions
    from arrow_cache_mcp.ai import (
        get_ai_config,
        get_claude_api_key,  # For backward compatibility
        get_ai_provider,
        ask_ai,  # New main function
        ask_claude,  # For backward compatibility
        extract_and_run_queries,
        display_conversation_history,
        get_supported_providers
    )
    
    # Utilities
    from arrow_cache_mcp.utils import (
        clean_dataset_name
    )
    
except ImportError as e:
    st.error(f"Failed to import from arrow-cache-mcp: {e}")
    st.stop()


# Override size display function for better handling of small values
def get_size_display_improved(bytes_value):
    """
    Convert a size in bytes to a human-readable string with appropriate units.
    Handles very small values better than the original function.
    
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
# --- Add custom CSS for the interface ---
st.markdown("""
<style>
.user-message {
    background-color:rgb(2, 65, 94);
    border-radius: 10px;
    padding: 10px;
    margin-bottom: 10px;
}
.claude-message {
    background-color:rgb(65, 75, 97);
    border-radius: 10px;
    padding: 10px;
    margin-bottom: 10px;
}
.query-info {
    background-color:rgb(53, 43, 43);
    border-left: 3px solid #2e6fdb;
    padding: 10px;
    margin: 10px 0;
    font-family: monospace;
}
.dataset-card {
    background-color:rgb(68, 57, 57);
    border-radius: 5px;
    padding: 15px;
    margin-bottom: 10px;
    border-left: 4px solid #2e6fdb;
}
.memory-gauge {
    margin: 10px 0;
}
.memory-gauge .used {
    background-color: #2e6fdb;
    height: 10px;
    border-radius: 5px;
}
.memory-gauge .container {
    background-color:rgb(11, 8, 8);
    height: 10px;
    border-radius: 5px;
    margin-bottom: 5px;
}
/* Fix for the white background in markdown content */
.claude-message .markdown-text-container {
    background-color: transparent !important;
}
.claude-message pre {
    background-color:rgb(32, 5, 5) !important;
    border-radius: 5px;
}
/* Remove default white background from Streamlit markdown */
.stMarkdown {
    background-color: transparent !important;
}
</style>
""", unsafe_allow_html=True)

# --- Initialize the Arrow Cache ---
# Spill directory for cache
SPILL_DIRECTORY = ".arrow_cache_spill_streamlit"

# Use the get_arrow_cache function from the package
cache = get_arrow_cache({
    "spill_directory": SPILL_DIRECTORY,
    "memory_limit": 20 * 1024 * 1024 * 1024  # 20 GB
})

# --- Helper function to remove dataset and update UI ---
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

# --- Helper function to display the Claude conversation history ---
def display_streamlit_conversation_history():
    """Display the conversation history in a chat-like interface"""
    if "conversation_history" not in st.session_state or not st.session_state.conversation_history:
        st.info("No conversation history yet. Ask Claude a question to start.")
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
            # For Claude's responses, we need to handle the markdown content safely
            # First create the message container with minimal HTML
            st.markdown(f"""<div class="claude-message"><strong>Claude:</strong></div>""", 
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

# --- Add a clear history button to the UI ---
def add_clear_history_button():
    """Add a button to clear the conversation history"""
    if "conversation_history" in st.session_state and st.session_state.conversation_history:
        if st.button("Clear Conversation History", key="clear_history_btn"):
            st.session_state.conversation_history = []
            st.session_state.last_claude_result = None
            st.rerun()

# --- Custom renderer for dataset cards ---
def render_streamlit_dataset_card(dataset):
    """Render a dataset card with its info and action buttons."""
    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Get size information directly from dataset_info
            size_bytes = dataset.get('size_bytes', 0)
            
            # Convert to appropriate unit for display using our improved helper function
            size_display = get_size_display_improved(size_bytes)
            
            # Format row count with commas for readability
            row_count = dataset.get('row_count', 'Unknown')
            if isinstance(row_count, int) or (isinstance(row_count, str) and row_count.isdigit()):
                row_count = f"{int(row_count):,}"
            
            # Get column information - first check if column_count exists in metadata directly
            if 'column_count' in dataset:
                column_count = dataset.get('column_count', 0)
            # Then check in the metadata field 
            elif 'metadata' in dataset and isinstance(dataset['metadata'], dict) and 'column_count' in dataset['metadata']:
                column_count = dataset['metadata'].get('column_count', 0)
            # Finally use length of columns array if available
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

# --- UI: Main Interface ---
st.title("📊 Data Sandbox")
st.markdown("A versatile data analysis sandbox powered by ArrowCache and Claude")

# --- UI: Sidebar ---
with st.sidebar:
    st.header("Memory Usage")
    memory_info = get_memory_usage()
    
    # Memory gauge
    st.markdown(f"""
    <div class="memory-gauge">
        <h4>Cache Memory</h4>
        <div class="container">
            <div class="used" style="width: {min(100, memory_info['cache_utilization_percent'])}%;"></div>
        </div>
        <p>{memory_info['cache_size_mb']:.1f} MB of {memory_info['memory_limit_mb']:.1f} MB 
           ({memory_info['cache_utilization_percent']:.1f}%)</p>
    </div>
    
    <div class="memory-gauge" style="margin-top: 15px;">
        <h4>Process Memory</h4>
        <div class="container">
            <div class="used" style="width: {min(100, (memory_info['process_rss_mb']/memory_info['memory_limit_mb'])*100)}%;"></div>
        </div>
        <p>{memory_info['process_rss_mb']:.1f} MB total ({memory_info['app_overhead_mb']:.1f} MB overhead)</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.caption(f"Number of datasets: {memory_info['entry_count']}")
    
    # Sidebar actions
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Refresh Status"):
            st.rerun()
    with col2:
        if st.button("Force Cleanup", help="Clear cache files and restart"):
            try:
                import gc
                import time
                
                # Close and clear the cache
                with st.status("Closing cache..."):
                    close_cache()
                
                # Run garbage collection to release memory
                with st.status("Releasing memory..."):
                    for _ in range(3):
                        gc.collect()
                        time.sleep(0.5)
                
                # Clear files
                with st.status("Cleaning up files..."):
                    clear_cache_files()
                
                with st.status("Reinitializing cache..."):
                    # Clear the cached resource to force reinitializing the cache
                    st.cache_resource.clear()
                    
                st.success("Cache cleaned up successfully! Reloading...")
                time.sleep(1)  # Brief pause for UI feedback
                
                # Rerun the app
                st.rerun()
            except Exception as e:
                st.error(f"Error during cleanup: {e}")
                import traceback
                st.code(traceback.format_exc())

    # Claude API key input
    st.header("AI Model Configuration")
    
    # Get current configuration
    ai_config = get_ai_config()
    
    # Provider selection
    available_providers = get_supported_providers()
    provider_options = list(available_providers.keys())
    
    selected_provider = st.selectbox(
        "AI Provider:", 
        provider_options,
        index=provider_options.index(ai_config["provider"]) if ai_config["provider"] in provider_options else 0,
        help="Select the AI provider to use for natural language queries"
    )
    
    # Update session state with selected provider
    if "ai_provider" not in st.session_state or st.session_state.ai_provider != selected_provider:
        st.session_state.ai_provider = selected_provider
    
    # Model selection based on provider
    if selected_provider in available_providers:
        model_options = available_providers[selected_provider]
        
        # Get current model or default
        current_model = ai_config["model"]
        if not current_model or current_model not in model_options:
            if selected_provider == "anthropic":
                current_model = "claude-3-haiku-20240307"
            elif selected_provider == "openai":
                current_model = "gpt-3.5-turbo"
            else:
                current_model = model_options[0] if model_options else ""
                
        model_index = model_options.index(current_model) if current_model in model_options else 0
        
        selected_model = st.selectbox(
            "Model:", 
            model_options,
            index=model_index,
            help=f"Select which {selected_provider} model to use"
        )
        
        # Update session state
        st.session_state[f"{selected_provider}_model"] = selected_model
    
    # API key input for selected provider
    api_key = ai_config["api_key"]
    
    # Display API key info or input
    if not api_key:
        api_key = st.text_input(f"Enter your {selected_provider.capitalize()} API Key:", 
                             type="password", 
                             help=f"Get an API key from {selected_provider.capitalize()}",
                             key=f"sidebar_{selected_provider}_api_key")
        if api_key:
            st.session_state[f"{selected_provider}_api_key"] = api_key
            st.success("API key saved for this session!")
        else:
            st.warning(f"Please enter an {selected_provider.capitalize()} API key.")
    else:
        st.success(f"{selected_provider.capitalize()} API key loaded from {ai_config['source']}")
        
        # Option to change API key
        if st.button("Change API Key"):
            # Clear the stored API key
            if f"{selected_provider}_api_key" in st.session_state:
                del st.session_state[f"{selected_provider}_api_key"]
            st.rerun()

# --- Main Content Tabs ---
tab1, tab2, tab3, tab4 = st.tabs(["Datasets", "SQL Query", "Ask AI", "Visualize"])

# --- Tab 1: Datasets Management ---
with tab1:
    st.header("Manage Datasets")
    
    # Dataset import section
    with st.expander("Import Dataset", expanded=True):
        try:
            import_tab1, import_tab2, import_tab3, import_tab4 = st.tabs(["Upload File", "From URL", "From Fused UDF", "From PostgreSQL"])
            
            # Upload file tab
            with import_tab1:
                uploaded_file = st.file_uploader("Choose a file", 
                                               type=[ext[1:] for format_info in SUPPORTED_FORMATS.values() 
                                                     for ext in format_info['extensions']])
                col1, col2 = st.columns(2)
                with col1:
                    upload_name = st.text_input("Dataset Name (optional):", 
                                              help="Leave blank to use the filename")
                with col2:
                    upload_format = st.selectbox("Format (optional):", 
                                               ["Auto-detect"] + list(SUPPORTED_FORMATS.keys()),
                                               help="Leave as Auto-detect to guess from file extension")
                
                # Additional options based on format
                st.markdown("##### Advanced Options")
                if upload_format == "csv" or (upload_format == "Auto-detect" and uploaded_file and 
                                             uploaded_file.name.lower().endswith('.csv')):
                    delimiter = st.text_input("Delimiter:", value=",")
                    header = st.checkbox("File has header", value=True)
                    advanced_args = {"delimiter": delimiter}
                    if not header:
                        advanced_args["header"] = None
                else:
                    advanced_args = {}
                
                if st.button("Upload Dataset") and uploaded_file:
                    format_to_use = None if upload_format == "Auto-detect" else upload_format
                    success, result = load_dataset_from_upload(uploaded_file, upload_name, format_to_use, **advanced_args)
                    if success:
                        st.success(f"Dataset loaded successfully as '{result['name']}'")
                        st.json(result)
                        # Force a refresh of the app
                        st.rerun()
                    else:
                        st.error(result)
            
            # URL tab
            with import_tab2:
                url = st.text_input("Dataset URL:", 
                                   placeholder="https://example.com/dataset.csv")
                col1, col2 = st.columns(2)
                with col1:
                    url_name = st.text_input("Dataset Name (optional):", key="url_name", 
                                           help="Leave blank to use the filename from URL")
                with col2:
                    url_format = st.selectbox("Format (optional):", 
                                            ["Auto-detect"] + list(SUPPORTED_FORMATS.keys()),
                                            key="url_format",
                                            help="Leave as Auto-detect to guess from URL extension")
                
                # Advanced options for URL import
                st.markdown("##### Advanced Options")
                if url_format == "csv" or (url_format == "Auto-detect" and url and url.lower().endswith('.csv')):
                    delimiter = st.text_input("Delimiter:", value=",", key="url_delimiter")
                    header = st.checkbox("File has header", value=True, key="url_header")
                    url_advanced_args = {"delimiter": delimiter}
                    if not header:
                        url_advanced_args["header"] = None
                else:
                    url_advanced_args = {}
                
                if st.button("Import from URL") and url:
                    format_to_use = None if url_format == "Auto-detect" else url_format
                    with st.spinner("Downloading and importing dataset..."):
                        success, result = load_dataset_from_url(url, url_name, format_to_use, **url_advanced_args)
                        if success:
                            st.success(f"Dataset loaded successfully as '{result['name']}'")
                            st.json(result)
                            # Force a refresh of the app
                            st.rerun()
                        else:
                            st.error(result)
            
            # Fused UDF tab
            with import_tab3:
                fused_url = st.text_input("Fused UDF Endpoint URL:", 
                                          placeholder="https://www.fused.io/server/v1/...",
                                          key="fused_url")
                fused_name = st.text_input("Dataset Name (optional):", key="fused_name", 
                                           help="Leave blank to suggest a name based on the UDF")
                
                # Add format selection for Fused UDFs
                fused_format = st.selectbox("Expected Format:", 
                                            list(SUPPORTED_FORMATS.keys()),
                                            key="fused_format",
                                            index=list(SUPPORTED_FORMATS.keys()).index('parquet'), # Default to Parquet
                                            help="Select the expected data format returned by the UDF (e.g., parquet, csv)")

                st.caption("Example (DEM Raster to Vector): `https://www.fused.io/server/v1/realtime-shared/1e35c9b9cadf900265443073b0bd99072f859b8beddb72a45e701fb5bcde807d/run/file?min_elevation=500&dtype_out_vector=parquet`")

                if st.button("Import from Fused UDF") and fused_url:
                    with st.spinner("Executing Fused UDF and importing data..."):
                        # Pass the explicitly selected format
                        success, result = load_dataset_from_url(fused_url, fused_name, format=fused_format) 
                        if success:
                            st.success(f"Dataset loaded successfully from Fused UDF as '{result['name']}'")
                            st.json(result)
                            st.rerun() # Refresh the app state
                        else:
                            st.error(f"Failed to load from Fused UDF: {result}")
            
            # PostgreSQL tab
            with import_tab4:
                st.subheader("Connect to PostgreSQL")
                
                # Debug option for advanced users
                show_debug = st.checkbox("Enable PostgreSQL Debug Mode", key="pg_debug")
                if show_debug:
                    st.warning("Debug mode enabled - this will show detailed database information")
                    
                    debug_tab1, debug_tab2 = st.tabs(["Connection Tester", "Table Structure"])
                    
                    with debug_tab1:
                        st.subheader("PostgreSQL Connection Tester")
                        pg_debug_host = st.text_input("Host:", value="localhost", key="pg_debug_host")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            pg_debug_port = st.text_input("Port:", value="5432", key="pg_debug_port")
                        with col2:
                            pg_debug_dbname = st.text_input("Database:", key="pg_debug_dbname")
                        
                        col1, col2 = st.columns(2)
                        with col1:
                            pg_debug_user = st.text_input("Username:", key="pg_debug_user")
                        with col2:
                            pg_debug_password = st.text_input("Password:", type="password", key="pg_debug_password")
                        
                        if st.button("Test Connection and List Tables", key="pg_debug_test"):
                            if not pg_debug_host or not pg_debug_dbname or not pg_debug_user:
                                st.error("Host, database name, and username are required.")
                            else:
                                try:
                                    # Create connection string
                                    debug_conn_string = f"postgresql://{pg_debug_user}:{pg_debug_password}@{pg_debug_host}:{pg_debug_port}/{pg_debug_dbname}"
                                    
                                    # Create a temp DuckDB connection
                                    temp_conn = duckdb.connect(":memory:")
                                    
                                    with st.status("Testing connection...") as status:
                                        # Install postgres extension
                                        try:
                                            temp_conn.execute("INSTALL postgres")
                                        except:
                                            # Extension might already be installed
                                            pass
                                        
                                        temp_conn.execute("LOAD postgres")
                                        status.update(label="Extension loaded, connecting...")
                                        
                                        # Try to connect and get version info
                                        try:
                                            temp_conn.execute(f"ATTACH '{debug_conn_string}' AS debug_pg (TYPE postgres)")
                                            version = temp_conn.execute("SELECT version() FROM debug_pg.pg_catalog.pg_settings WHERE name = 'server_version'").fetchone()
                                            status.update(label=f"Connected to PostgreSQL {version[0] if version else 'unknown version'}")
                                            
                                            # List all schemas
                                            schemas = temp_conn.execute("""
                                                SELECT schema_name 
                                                FROM debug_pg.information_schema.schemata 
                                                ORDER BY schema_name
                                            """).fetchall()
                                            
                                            status.update(label="Fetching schema information...")
                                            
                                            # Create a table to display the schema and table counts
                                            schema_data = []
                                            
                                            for schema_row in schemas:
                                                schema_name = schema_row[0]
                                                
                                                # Count tables in this schema
                                                table_count = temp_conn.execute(f"""
                                                    SELECT COUNT(*) 
                                                    FROM debug_pg.information_schema.tables 
                                                    WHERE table_schema = '{schema_name}' 
                                                    AND table_type = 'BASE TABLE'
                                                """).fetchone()[0]
                                                
                                                # Only add schema with tables or if it's a non-system schema
                                                if table_count > 0 or schema_name not in ('pg_catalog', 'information_schema'):
                                                    schema_data.append({
                                                        "Schema": schema_name,
                                                        "Tables": table_count
                                                    })
                                            
                                            status.update(label="Fetching detailed table information...")
                                            
                                            # Show schema summary
                                            st.success(f"Connected successfully to {pg_debug_dbname} at {pg_debug_host}")
                                            st.dataframe(pd.DataFrame(schema_data))
                                            
                                            # Get all tables details
                                            all_tables = temp_conn.execute("""
                                                SELECT 
                                                    t.table_schema, 
                                                    t.table_name,
                                                    (SELECT COUNT(*) FROM debug_pg.information_schema.columns c 
                                                     WHERE c.table_schema = t.table_schema AND c.table_name = t.table_name) as column_count
                                                FROM 
                                                    debug_pg.information_schema.tables t
                                                WHERE 
                                                    t.table_type = 'BASE TABLE'
                                                ORDER BY 
                                                    t.table_schema, t.table_name
                                            """).fetchall()
                                            
                                            # Display tables in a nice format
                                            table_data = []
                                            for schema, table, col_count in all_tables:
                                                table_data.append({
                                                    "Schema": schema,
                                                    "Table": table,
                                                    "Columns": col_count
                                                })
                                            
                                            if table_data:
                                                st.subheader(f"All Tables ({len(table_data)})")
                                                st.dataframe(pd.DataFrame(table_data))
                                            else:
                                                st.info("No tables found in the database.")
                                            
                                            status.update(label="Done!", state="complete")
                                            
                                        except Exception as e:
                                            status.update(label=f"Connection error: {str(e)}", state="error")
                                            st.error(f"Failed to connect: {str(e)}")
                                        
                                        finally:
                                            # Close connection
                                            try:
                                                temp_conn.execute("DETACH debug_pg")
                                            except:
                                                pass
                                            temp_conn.close()
                                            
                                except Exception as e:
                                    st.error(f"Error: {str(e)}")
                    
                    with debug_tab2:
                        st.subheader("Examine Table Structure")
                        
                        # Fields to enter schema and table name
                        col1, col2 = st.columns(2)
                        with col1:
                            debug_schema = st.text_input("Schema name:", key="debug_schema", placeholder="public")
                        with col2:
                            debug_table = st.text_input("Table name:", key="debug_table")
                        
                        # Connection details (could reuse from previous tab)
                        st.info("Uses the same connection details as the Connection Tester tab")
                        
                        if st.button("Examine Table Structure", key="debug_examine") and debug_table:
                            # Use the same connection details as the tester
                            pg_debug_host = st.session_state.get("pg_debug_host", "localhost") 
                            pg_debug_port = st.session_state.get("pg_debug_port", "5432")
                            pg_debug_dbname = st.session_state.get("pg_debug_dbname", "")
                            pg_debug_user = st.session_state.get("pg_debug_user", "")
                            pg_debug_password = st.session_state.get("pg_debug_password", "")
                            
                            if not pg_debug_host or not pg_debug_dbname or not pg_debug_user:
                                st.error("Host, database name, and username are required in the Connection Tester tab.")
                            elif not debug_table:
                                st.error("Table name is required.")
                            else:
                                try:
                                    # Create connection string
                                    debug_conn_string = f"postgresql://{pg_debug_user}:{pg_debug_password}@{pg_debug_host}:{pg_debug_port}/{pg_debug_dbname}"
                                    
                                    # Create a temp DuckDB connection
                                    temp_conn = duckdb.connect(":memory:")
                                    
                                    # Install and load postgres extension
                                    try:
                                        temp_conn.execute("INSTALL postgres")
                                    except:
                                        pass
                                    
                                    temp_conn.execute("LOAD postgres")
                                    temp_conn.execute(f"ATTACH '{debug_conn_string}' AS debug_pg (TYPE postgres)")
                                    
                                    # Build the schema condition
                                    schema_condition = f"AND c.table_schema = '{debug_schema}'" if debug_schema else ""
                                    
                                    # Get column information
                                    columns_query = f"""
                                    SELECT 
                                        c.column_name, 
                                        c.data_type,
                                        c.is_nullable,
                                        c.column_default
                                    FROM 
                                        debug_pg.information_schema.columns c
                                    WHERE 
                                        c.table_name = '{debug_table}'
                                        {schema_condition}
                                    ORDER BY 
                                        c.ordinal_position
                                    """
                                    
                                    columns = temp_conn.execute(columns_query).fetchall()
                                    
                                    if columns:
                                        # Show column details
                                        column_data = []
                                        for col_name, data_type, nullable, default in columns:
                                            column_data.append({
                                                "Column": col_name,
                                                "Type": data_type,
                                                "Nullable": nullable,
                                                "Default": default or ""
                                            })
                                        
                                        # Show schema.table name
                                        schema_name = debug_schema or "public"
                                        st.success(f"Found table: {schema_name}.{debug_table}")
                                        
                                        # Show column details
                                        st.dataframe(pd.DataFrame(column_data))
                                        
                                        # Try to count rows
                                        try:
                                            count_query = f"""
                                            SELECT COUNT(*) FROM debug_pg.{schema_name}.{debug_table}
                                            """
                                            row_count = temp_conn.execute(count_query).fetchone()[0]
                                            st.info(f"Table contains approximately {row_count:,} rows")
                                        except Exception as e:
                                            st.warning(f"Could not count rows: {str(e)}")
                                            
                                        # Build a proper postgres_scan example
                                        st.subheader("Table Import Sample Code")
                                        code = f"""
# Using DuckDB postgres extension directly
con = duckdb.connect(':memory:')
con.execute('INSTALL postgres; LOAD postgres;')

# Two ways to reference schema-qualified tables:
# Method 1: Using separate table_name and schema params
result1 = con.execute(f"SELECT * FROM postgres_scan('{debug_conn_string}', '{debug_table}', '{schema_name}')").arrow()

# Method 2: Using schema-qualified name as a single parameter
result2 = con.execute(f"SELECT * FROM postgres_scan('{debug_conn_string}', '{schema_name}.{debug_table}')").arrow()
                                        """
                                        st.code(code, language="python")
                                        
                                    else:
                                        st.error(f"Table '{debug_table}' not found in schema '{debug_schema or 'public'}'")
                                    
                                    # Clean up
                                    temp_conn.execute("DETACH debug_pg")
                                    temp_conn.close()
                                    
                                except Exception as e:
                                    st.error(f"Error examining table structure: {str(e)}")
                
                # Connection details
                pg_host = st.text_input("Host:", value="localhost", key="pg_host")
                col1, col2 = st.columns(2)
                with col1:
                    pg_port = st.text_input("Port:", value="5432", key="pg_port")
                with col2:
                    pg_dbname = st.text_input("Database:", key="pg_dbname")
                
                col1, col2 = st.columns(2)
                with col1:
                    pg_user = st.text_input("Username:", key="pg_user")
                with col2:
                    pg_password = st.text_input("Password:", type="password", key="pg_password")
                
                # Dataset options
                pg_dataset_prefix = st.text_input("Dataset Name Prefix:", key="pg_dataset_prefix", 
                                               help="For single tables: complete name. For all tables: prefix_tablename will be used.")
                
                # Source selection: table, query, or all tables
                pg_source_type = st.radio("Data Source:", ["Single Table", "SQL Query", "All Tables"], horizontal=True, key="pg_source_type")
                
                if pg_source_type == "Single Table":
                    col1, col2 = st.columns(2)
                    with col1:
                        pg_schema = st.text_input("Schema:", value="public", key="pg_schema")
                    with col2:
                        pg_table = st.text_input("Table Name:", key="pg_table")
                elif pg_source_type == "SQL Query":
                    pg_query = st.text_area("SQL Query:", height=100, key="pg_query", 
                                          placeholder="SELECT * FROM users LIMIT 1000")
                else:  # All Tables
                    pg_schema = st.text_input("Schema (leave empty for all schemas):", value="public", key="pg_all_schema")
                    st.info("This will import all tables from the selected schema as separate datasets.")
                
                # Show advanced options
                show_advanced = st.checkbox("Show Advanced Options", key="show_advanced_pg")
                if show_advanced:
                    limit_rows = st.number_input("Limit Rows Per Table (0 = no limit):", value=0, min_value=0, step=1000, key="limit_rows")
                    skip_empty = st.checkbox("Skip Empty Tables", value=True, key="skip_empty")
                    include_system = st.checkbox("Include System Tables", value=False, key="include_system")
                else:
                    # Set default values if advanced options aren't shown
                    if "limit_rows" not in st.session_state:
                        st.session_state.limit_rows = 0
                    if "skip_empty" not in st.session_state:
                        st.session_state.skip_empty = True
                    if "include_system" not in st.session_state:
                        st.session_state.include_system = False
                
                # Import button
                if st.button("Import from PostgreSQL"):
                    if not pg_host or not pg_dbname or not pg_user:
                        st.error("Host, database name, and username are required.")
                    elif pg_source_type == "Single Table" and not pg_table:
                        st.error("Table name is required.")
                    elif pg_source_type == "SQL Query" and not pg_query:
                        st.error("SQL query is required.")
                    elif not pg_dataset_prefix:
                        st.error("Dataset name prefix is required.")
                    else:
                        with st.spinner("Connecting to PostgreSQL and importing data..."):
                            try:
                                # Prepare connection options
                                connection_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}"
                                
                                # Add SSL parameters to connection string for Azure PostgreSQL
                                if "azure.com" in pg_host:
                                    connection_string += "?sslmode=require"
                                
                                # Call the import_data_directly function
                                if pg_source_type == "Single Table":
                                    success, result = import_data_directly(
                                        key=pg_dataset_prefix,
                                        source=connection_string,
                                        source_type="postgres",
                                        table_name=pg_table,
                                        schema=pg_schema
                                    )
                                    
                                    if success:
                                        st.success(f"Successfully imported PostgreSQL table as '{pg_dataset_prefix}'")
                                        st.json(result)
                                        st.rerun()
                                    else:
                                        st.error(f"Failed to import from PostgreSQL: {result}")
                                
                                elif pg_source_type == "SQL Query":
                                    success, result = import_data_directly(
                                        key=pg_dataset_prefix,
                                        source=connection_string,
                                        source_type="postgres",
                                        query=pg_query
                                    )
                                    
                                    if success:
                                        st.success(f"Successfully imported PostgreSQL query result as '{pg_dataset_prefix}'")
                                        st.json(result)
                                        st.rerun()
                                    else:
                                        st.error(f"Failed to import from PostgreSQL: {result}")
                                
                                else:  # All Tables
                                    # First get list of tables using a DuckDB query
                                    import duckdb
                                    
                                    # Create a progress bar
                                    progress_placeholder = st.empty()
                                    progress_bar = progress_placeholder.progress(0)
                                    info_placeholder = st.empty()
                                    
                                    # Create a temporary connection to get the list of tables
                                    temp_conn = duckdb.connect(":memory:")
                                    
                                    # Install the postgres extension if needed
                                    try:
                                        temp_conn.execute("INSTALL postgres")
                                        logger.info("PostgreSQL extension installed in DuckDB")
                                    except Exception as e:
                                        # Extension might already be installed, so we can ignore this error
                                        logger.info(f"Note when installing PostgreSQL extension: {e}")
                                    
                                    temp_conn.execute("LOAD postgres")
                                    logger.info("PostgreSQL extension loaded in DuckDB")
                                    
                                    # Attach the PostgreSQL database
                                    try:
                                        # Add SSL parameters to connection string for Azure PostgreSQL
                                        if "azure.com" in connection_string:
                                            if "?" not in connection_string:
                                                connection_string += "?sslmode=require"
                                            elif "sslmode=" not in connection_string:
                                                connection_string += "&sslmode=require"
                                        
                                        # Redact password for logging
                                        safe_connection = connection_string
                                        if "@" in connection_string:
                                            user_part = connection_string.split("@")[0]
                                            host_part = connection_string.split("@")[1]
                                            if ":" in user_part:
                                                user = user_part.split("//")[1].split(":")[0]
                                                safe_connection = f"postgresql://{user}:[REDACTED]@{host_part}"
                                        
                                        logger.info(f"Connecting to PostgreSQL database: {safe_connection}")
                                        temp_conn.execute(f"ATTACH '{connection_string}' AS pg (TYPE postgres)")
                                        logger.info("Successfully attached PostgreSQL database")
                                        
                                        # Test the connection with a simple query
                                        version_result = temp_conn.execute("SELECT version() FROM pg.pg_catalog.pg_settings WHERE name = 'server_version'").fetchone()
                                        if version_result:
                                            logger.info(f"Connected to PostgreSQL server version: {version_result[0]}")
                                    except Exception as e:
                                        error_msg = str(e)
                                        logger.error(f"PostgreSQL connection error: {error_msg}")
                                        
                                        if "password authentication failed" in error_msg:
                                            st.error("Authentication failed: The username or password is incorrect.")
                                        elif "no pg_hba.conf entry" in error_msg:
                                            st.error("Access denied: Your IP address is not allowed to connect to this PostgreSQL server. Please check firewall rules.")
                                        elif "connection timed out" in error_msg:
                                            st.error("Connection timeout: Could not reach the PostgreSQL server. Please check the host and port.")
                                        else:
                                            st.error(f"PostgreSQL connection error: {error_msg}")
                                        # Skip the rest of the process
                                        progress_placeholder.empty()
                                        st.stop()
                                    
                                    # Query to get all tables
                                    schema_filter = f"AND table_schema = '{pg_schema}'" if pg_schema else ""
                                    # Get the include_system value safely
                                    include_system_tables = st.session_state.get("include_system", False)
                                    system_filter = "AND table_schema NOT IN ('pg_catalog', 'information_schema')" if not include_system_tables else ""
                                    
                                    tables_query = f"""
                                    SELECT table_schema, table_name 
                                    FROM pg.information_schema.tables 
                                    WHERE table_type = 'BASE TABLE' 
                                    {schema_filter}
                                    {system_filter}
                                    ORDER BY table_schema, table_name
                                    """
                                    
                                    logger.info(f"Tables query: {tables_query}")
                                    try:
                                        tables = temp_conn.execute(tables_query).fetchall()
                                        logger.info(f"Found {len(tables)} tables")
                                        # Log table details
                                        for schema, table in tables:
                                            logger.info(f"Found table: {schema}.{table}")
                                    except Exception as e:
                                        logger.error(f"Error fetching tables: {e}")
                                        st.error(f"Error fetching tables: {e}")
                                        tables = []
                                    
                                    temp_conn.close()
                                    
                                    if not tables:
                                        st.error("No tables found in the database with the specified criteria.")
                                    else:
                                        # Import each table
                                        successful_imports = []
                                        failed_imports = []
                                        
                                        for i, (schema, table) in enumerate(tables):
                                            # Update progress
                                            progress = (i / len(tables))
                                            progress_bar.progress(progress)
                                            info_placeholder.info(f"Importing table {i+1}/{len(tables)}: {schema}.{table}")
                                            logger.info(f"Attempting to import {schema}.{table}")
                                            
                                            try:
                                                # Create a dataset name with prefix
                                                dataset_name = f"{pg_dataset_prefix}_{schema}_{table}"
                                                dataset_name = clean_dataset_name(dataset_name)
                                                logger.info(f"Using dataset name: {dataset_name}")
                                                
                                               
                                                # Import the whole table
                                                logger.info(f"Calling import_data_directly for {schema}.{table}")
                                                success, result = import_data_directly(
                                                    key=dataset_name,
                                                    source=connection_string,
                                                    source_type="postgres",
                                                    table_name=table,
                                                    schema=schema
                                                )
                                                
                                                if success:
                                                    logger.info(f"Successfully imported {schema}.{table}: {result.get('row_count', 0)} rows")
                                                    # Check if the table is empty and we should skip it
                                                    if st.session_state.skip_empty and result.get('row_count', 0) == 0:
                                                        # Remove the empty table
                                                        remove_dataset(dataset_name)
                                                        logger.info(f"Skipped empty table: {schema}.{table}")
                                                    else:
                                                        successful_imports.append({
                                                            "schema": schema,
                                                            "table": table,
                                                            "dataset": dataset_name,
                                                            "rows": result.get('row_count', 0)
                                                        })
                                                else:
                                                    logger.error(f"Failed to import {schema}.{table}: {result}")
                                                    failed_imports.append({
                                                        "schema": schema,
                                                        "table": table,
                                                        "error": str(result)
                                                    })
                                            except Exception as e:
                                                logger.error(f"Exception importing {schema}.{table}: {e}")
                                                failed_imports.append({
                                                    "schema": schema,
                                                    "table": table,
                                                    "error": str(e)
                                                })
                                        
                                        # Complete the progress bar
                                        progress_bar.progress(1.0)
                                        progress_placeholder.empty()
                                        
                                        # Show summary
                                        st.success(f"Import complete. Successfully imported {len(successful_imports)} tables.")
                                        
                                        if successful_imports:
                                            with st.expander("Successfully Imported Tables", expanded=True):
                                                for imp in successful_imports:
                                                    st.markdown(f"✅ **{imp['schema']}.{imp['table']}** → {imp['dataset']} ({imp['rows']} rows)")
                                        
                                        if failed_imports:
                                            # Use a container instead of an expander to avoid nesting violation
                                            st.subheader(f"Failed Imports ({len(failed_imports)})")
                                            failed_container = st.container()
                                            with failed_container:
                                                for imp in failed_imports:
                                                    st.markdown(f"❌ **{imp['schema']}.{imp['table']}**: {imp['error']}")
                                        
                                        # Refresh the UI to show the imported tables
                                        if successful_imports:
                                            st.rerun()
                                
                            except Exception as e:
                                st.error(f"Error connecting to PostgreSQL: {str(e)}")
                                import traceback
                                st.code(traceback.format_exc())
            
        except Exception as e:
            st.error(f"Error creating import tabs: {e}")
            st.write("Please try refreshing the page.")
    
    # List of loaded datasets
    st.header("Loaded Datasets")
    datasets = get_datasets_list()
    
    if not datasets:
        st.info("No datasets loaded. Import a dataset using the options above.")
    else:
        # Display datasets as cards
        for dataset in datasets:
            render_streamlit_dataset_card(dataset)
    
    # Selected dataset details
    if 'selected_dataset' in st.session_state and st.session_state.selected_dataset:
        selected_name = st.session_state.selected_dataset
        
        if not cache.contains(selected_name):
            st.warning(f"Dataset '{selected_name}' no longer exists.")
            st.session_state.pop('selected_dataset')
        else:
            st.header(f"Dataset: {selected_name}")
            
            # Get dataset sample
            sample = cache.get(selected_name, limit=10)
            
            # Get dataset details
            selected_info = next((d for d in datasets if d['name'] == selected_name), {})
            
            # Display dataset details
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Rows", selected_info.get('row_count', 'Unknown'))
            with col2:
                st.metric("Columns", selected_info.get('column_count', len(selected_info.get('columns', []))))
            
            st.subheader("Preview")
            st.dataframe(sample, use_container_width=True)
            
            # Column details
            if 'columns' in selected_info:
                st.subheader("Columns")
                for col in sample.column_names:
                    dtype = str(sample[col].type)
                    # Fetch sample values correctly
                    try:
                        # Handle potential complexities in accessing values (e.g., nested types)
                        sample_vals_list = sample[col].slice(0, 3).to_pylist()
                        sample_values = ", ".join([str(v) for v in sample_vals_list])
                    except Exception as e:
                        logger.error(f"Error getting sample values for column {col}: {e}")
                        sample_values = "[Error fetching]"

                    st.markdown(f"**{col}** ({dtype}) - Sample: {sample_values}...")

# --- Tab 2: SQL Query ---
with tab2:
    st.header("SQL Query")
    
    # Dataset reference section
    if datasets:
        st.subheader("Available Tables")
        table_refs = []
        for ds in datasets:
            table_refs.append(f"_cache_{ds['name']}")
        
        st.code("\n".join(table_refs))
        
        # SQL query editor
        st.subheader("Query Editor")
        default_query = f"""SELECT * 
FROM _cache_{datasets[0]['name']}
LIMIT 10;"""
        
        query = st.text_area("Enter your DuckDB SQL query:", value=default_query, height=150)

        if st.button("Run Query", key="run_query_btn"):
            if query:
                with st.spinner("Executing query..."):
                    try:
                        start_query_time = time.time()
                        result_df = cache.query(query)
                        query_time = time.time() - start_query_time

                        st.success(f"Query executed successfully in {query_time:.3f}s.")
                        
                        # Create a container for the result display
                        result_container = st.container()
                        with result_container:
                            st.subheader("Query Results")
                            st.dataframe(result_df, use_container_width=True)
                            st.caption(f"Result shape: {result_df.shape}")

                    except Exception as e:
                        st.error(f"Query failed: {type(e).__name__}: {e}")
            else:
                st.warning("Please enter a SQL query.")
    else:
        st.info("No datasets loaded. Please load at least one dataset to run SQL queries.")

# --- Tab 3: Claude-powered Natural Language Querying ---
with tab3:
    st.header("Ask AI about your Data")
    
    if not datasets:
        st.info("No datasets loaded. Please load at least one dataset before asking questions.")
    else:
        # Two columns - one for input, one for history
        input_col, history_col = st.columns([3, 5])
        
        with input_col:
            # Get current AI configuration
            ai_config = get_ai_config()
            selected_provider = st.session_state.get("ai_provider", ai_config["provider"])
            selected_model = st.session_state.get(f"{selected_provider}_model", ai_config["model"])
            
            # Show current configuration
            st.info(f"Using {selected_provider.capitalize()} ({selected_model or 'default model'})")
            
            # Check if API key is available
            api_key = ai_config["api_key"]
            if not api_key:
                api_key = st.text_input(f"Enter your {selected_provider.capitalize()} API Key:", 
                                      type="password", 
                                      help=f"Get an API key from {selected_provider.capitalize()}",
                                      key="ai_tab_api_key")
                if not api_key:
                    st.warning(f"Please enter an {selected_provider.capitalize()} API key.")
                    
            # Example questions based on loaded datasets
            st.subheader("Example Questions")
            example_questions = [
                f"How many rows are in the dataset '{datasets[0]['name']}'?",
                f"What columns are available in '{datasets[0]['name']}'?",
                f"Show me the first 5 rows of '{datasets[0]['name']}' with all columns",
            ]
            
            # Add more dataset-specific examples if we have NYC taxi data
            if any('yellow_tripdata' in ds.get('name', '') for ds in datasets):
                example_questions.extend([
                    "What is the average fare amount for trips?",
                    "Which day of the week had the most trips?",
                    "What's the average trip distance by payment type?"
                ])
            
            # Display example questions in a more compact way
            for i, q in enumerate(example_questions):
                if st.button(q, key=f"example_{i}"):
                    st.session_state.claude_question = q
                    st.rerun()
                    
            # Question input
            if "claude_question" not in st.session_state:
                st.session_state.claude_question = ""
                
            claude_question = st.text_area("Ask a question about your datasets:", 
                                        value=st.session_state.claude_question,
                                        height=100,
                                        help="Ask in plain English, AI will generate SQL queries.")
            
            if st.button("Ask", key="ask_ai_btn", disabled=not api_key):
                if claude_question:
                    # Initialize conversation history if it doesn't exist
                    if "conversation_history" not in st.session_state:
                        st.session_state.conversation_history = []
                        
                    with st.spinner(f"{selected_provider.capitalize()} is analyzing the data..."):
                        # Use the provider-agnostic ask_ai function
                        response = ask_ai(
                            question=claude_question, 
                            api_key=api_key,
                            provider=selected_provider,
                            model=selected_model,
                            conversation_history=st.session_state.conversation_history
                        )
                        
                        # Save the question in session state and clear for next question
                        st.session_state.claude_question = ""
                        st.rerun()
                else:
                    st.warning("Please enter a question to ask.")
            
            # Add clear history button
            add_clear_history_button()
            
            # If there's a result DataFrame, show it under the input section
            if hasattr(st.session_state, 'last_claude_result') and st.session_state.last_claude_result is not None:
                st.subheader("Latest Query Results")
                st.dataframe(st.session_state.last_claude_result, use_container_width=True)
        
        # Show conversation history in the second column
        with history_col:
            display_streamlit_conversation_history()

# --- Tab 4: Visualization ---
with tab4:
    st.header("Data Visualization")
    
    if not datasets:
        st.info("No datasets loaded. Please load at least one dataset to create visualizations.")
    else:
        # Dataset selection
        viz_dataset = st.selectbox("Select Dataset:", [ds['name'] for ds in datasets], key="viz_dataset")
        
        # Get dataset info for selected dataset
        selected_ds = next((ds for ds in datasets if ds['name'] == viz_dataset), None)
        
        if selected_ds:
            # Get sample to extract columns
            try:
                sample = cache.get(viz_dataset, limit=5)
                columns = list(sample.columns)
                
                # Plot configuration
                st.subheader("Plot Configuration")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    x_col = st.selectbox("X-axis:", columns, key="x_col")
                with col2:
                    y_options = ["None"] + columns
                    y_col = st.selectbox("Y-axis:", y_options, key="y_col")
                    y_col = None if y_col == "None" else y_col
                with col3:
                    plot_type = st.selectbox("Plot Type:", 
                                          ["line", "bar", "scatter", "hist", "box", "pie"], 
                                          key="plot_type")
                
                # Advanced options
                st.markdown("#### Advanced Options")
                col1, col2 = st.columns(2)
                with col1:
                    title = st.text_input("Plot Title:", key="plot_title")
                    width = st.number_input("Width:", min_value=4, max_value=20, value=10, key="plot_width")
                with col2:
                    height = st.number_input("Height:", min_value=3, max_value=15, value=6, key="plot_height")
                    dpi = st.number_input("DPI:", min_value=72, max_value=300, value=100, key="plot_dpi")
                
                # Create plot button
                if st.button("Generate Plot", key="gen_plot_btn"):
                    with st.spinner("Creating plot..."):
                        # Prepare plot arguments
                        plot_args = {
                            "figsize": (width, height),
                            "dpi": dpi
                        }
                        if title:
                            plot_args["title"] = title
                        
                        # Create the plot
                        plot_img, message = create_plot(viz_dataset, x_col, y_col, plot_type, **plot_args)
                        
                        if plot_img:
                            st.image(f"data:image/png;base64,{plot_img}", use_column_width=True)
                        else:
                            st.error(message)
            except Exception as e:
                st.error(f"Error accessing dataset: {e}")

# --- Cleanup ---
# Import atexit for cleanup
import atexit
atexit.register(lambda: close_cache() if cache else None)
