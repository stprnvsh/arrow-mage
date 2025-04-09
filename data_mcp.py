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

# Must be the first Streamlit command!
st.set_page_config(layout="wide", page_title="Data Sandbox", page_icon="📊")

# Add custom CSS for the interface
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

# --- Add arrow_cache to path ---
# This assumes the script is run from the root of the arrow-mage workspace
# Adjust if necessary based on your execution context
script_dir = os.path.dirname(os.path.abspath(__file__))
# Heuristic to find the workspace root (might need adjustment)
workspace_root = os.path.abspath(os.path.join(script_dir, '..')) 
arrow_cache_path = os.path.join(workspace_root, 'arrow_cache')
if arrow_cache_path not in sys.path:
    sys.path.insert(0, arrow_cache_path)
    print(f"Added {arrow_cache_path} to sys.path")

try:
    from arrow_cache import ArrowCache, ArrowCacheConfig
except ImportError as e:
    st.error(f"Failed to import ArrowCache. Make sure it's installed or the path is correct: {e}")
    st.stop()

# --- Configuration ---
# Create the spill directory path before the config
SPILL_DIRECTORY = ".arrow_cache_spill_streamlit"
CACHE_CONFIG = ArrowCacheConfig(
    memory_limit=1 * 1024 * 1024 * 1024, # 1 GB limit (adjust as needed)
    spill_to_disk=True,                  # Allow spilling if memory is exceeded
    spill_directory=SPILL_DIRECTORY,
    # Let's keep partitioning simple for this demo
    auto_partition=True,
)

# Supported file formats
SUPPORTED_FORMATS = {
    'csv': {'read_func': pd.read_csv, 'extensions': ['.csv']},
    'parquet': {'read_func': pd.read_parquet, 'extensions': ['.parquet', '.pq']},
    'arrow': {'read_func': pd.read_feather, 'extensions': ['.arrow', '.feather']},
    'feather': {'read_func': pd.read_feather, 'extensions': ['.feather']},
    'json': {'read_func': lambda path, **kwargs: pd.read_json(path, orient="records", **kwargs), 
             'extensions': ['.json']},
    'excel': {'read_func': pd.read_excel, 'extensions': ['.xlsx', '.xls']},
}

# Try to import optional geospatial dependencies
try:
    import geopandas as gpd
    SUPPORTED_FORMATS['geojson'] = {'read_func': gpd.read_file, 'extensions': ['.geojson']}
    SUPPORTED_FORMATS['geoparquet'] = {'read_func': gpd.read_parquet, 'extensions': ['.geoparquet']}
    HAS_GEOPANDAS = True
except ImportError:
    HAS_GEOPANDAS = False
    st.sidebar.warning("GeoPandas not installed. Geospatial formats won't be available.")

# --- Anthropic (Claude) API Configuration ---
def get_claude_api_key():
    """Get API key from environment variable or Streamlit secrets, with fallback to session state."""
    # Try to get from env var first
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    
    # If not in env var, try to get from Streamlit secrets
    if not api_key:
        try:
            if hasattr(st, "secrets") and "anthropic" in st.secrets:
                api_key = st.secrets["anthropic"]["api_key"]
        except Exception:
            # Handle missing secrets gracefully
            pass
    
    # If not found in env or secrets, check if we have it in session state
    if not api_key and "claude_api_key" in st.session_state:
        api_key = st.session_state.claude_api_key
        
    return api_key

# --- Cache Initialization ---
# Store cache instance in session state to persist across reruns
@st.cache_resource
def get_arrow_cache():
    print("Initializing ArrowCache...")
    # Ensure spill directory exists
    os.makedirs(SPILL_DIRECTORY, exist_ok=True)
    
    # Create the config using the pattern from advanced_features.py
    config = ArrowCacheConfig(
        memory_limit=10 * 1024 * 1024 * 1024,  # 10 GB limit
        spill_to_disk=True,                   # Allow spilling if memory is exceeded
        spill_directory=SPILL_DIRECTORY,
        auto_partition=True,                  # Enable auto-partitioning for large datasets
        compression_type="lz4",               # Use LZ4 compression
        enable_compression=True,              # Enable compression
        dictionary_encoding=True,             # Use dictionary encoding for strings
        cache_query_plans=True                # Cache query plans for better performance
    )
    
    # Initialize the cache with the config
    cache = ArrowCache(config=config)
    print(f"ArrowCache Initialized. Using spill dir: {SPILL_DIRECTORY}")
    
    # Register a cleanup handler to close the cache when the app exits
    import atexit
    atexit.register(lambda: cache.close() if cache else None)
    
    return cache

cache = get_arrow_cache()

# --- Dataset Management Functions ---
def guess_format_from_path(file_path):
    """Guess the format of a file based on its extension."""
    # Extract just the filename from path or URL
    if '://' in file_path:  # It's a URL
        parsed_url = urlparse(file_path)
        file_path = os.path.basename(parsed_url.path)
    
    ext = os.path.splitext(file_path.lower())[1]
    if not ext:
        return None
        
    for format_name, format_info in SUPPORTED_FORMATS.items():
        if ext in format_info['extensions']:
            return format_name
            
    # If we get here, the extension wasn't recognized
    return None

def load_dataset_from_path(path, dataset_name=None, format=None, **kwargs):
    """Load a dataset from a local path or URL into the cache."""
    if not dataset_name:
        # Extract name from path (remove extension)
        dataset_name = os.path.basename(path).split('.')[0]
        # Clean up name (remove special chars, etc.)
        dataset_name = re.sub(r'[^\w]', '_', dataset_name)
    
    # Check if dataset with this name already exists
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    # Determine format if not provided
    if not format:
        format = guess_format_from_path(path)
        if not format:
            return False, f"Could not determine format from file extension. Please specify it explicitly."
    
    if format not in SUPPORTED_FORMATS:
        return False, f"Unsupported format: {format}. Supported formats: {list(SUPPORTED_FORMATS.keys())}"
    
    try:
        # Get the read function for this format
        read_func = SUPPORTED_FORMATS[format]['read_func']
        
        # Load the dataset
        start_time = time.time()
        df = read_func(path, **kwargs)
        load_time = time.time() - start_time
        
        # Get dataframe memory usage (approximate)
        df_memory = df.memory_usage(deep=True).sum()
        
        # Prepare metadata
        metadata = {
            'source': path,
            'format': format,
            'loaded_at': time.time(),
            'load_time_seconds': load_time,
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'memory_bytes': int(df_memory),
            'dtypes': {str(col): str(dtype) for col, dtype in df.dtypes.items()}
        }
        
        # Add geospatial metadata if applicable
        if hasattr(df, 'crs') and df.crs is not None:
            metadata['is_geospatial'] = True
            metadata['crs'] = str(df.crs)
            if hasattr(df, 'geometry'):
                metadata['geometry_column'] = 'geometry'
        
        # Store in cache
        start_cache_time = time.time()
        cache.put(dataset_name, df, metadata=metadata)
        cache_time = time.time() - start_cache_time
        
        # Update metadata with timing info
        metadata['cache_time_seconds'] = cache_time
        metadata['total_time_seconds'] = load_time + cache_time
        
        # Add dataset name to metadata so it can be accessed in success messages
        metadata['name'] = dataset_name
        
        return True, metadata
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return False, f"Error loading dataset: {str(e)}"

def load_dataset_from_upload(uploaded_file, dataset_name=None, format=None, **kwargs):
    """Load a dataset from a Streamlit uploaded file into the cache."""
    if not dataset_name:
        dataset_name = uploaded_file.name.split('.')[0]
        dataset_name = re.sub(r'[^\w]', '_', dataset_name)
    
    # Check if dataset already exists
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    # Save uploaded file to a temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=f"_{uploaded_file.name}") as tmp_file:
        tmp_file.write(uploaded_file.getvalue())
        temp_path = tmp_file.name
    
    try:
        # Now load from the temporary file
        success, result = load_dataset_from_path(temp_path, dataset_name, format, **kwargs)
        # Clean up temp file
        os.unlink(temp_path)
        return success, result
    except Exception as e:
        # Clean up temp file even on error
        try:
            os.unlink(temp_path)
        except:
            pass
        return False, f"Error loading uploaded file: {str(e)}"

def load_dataset_from_url(url, dataset_name=None, format=None, **kwargs):
    """Load a dataset from a URL into the cache."""
    if not dataset_name:
        # Extract name from URL
        path = urlparse(url).path
        dataset_name = os.path.basename(path).split('.')[0]
        dataset_name = re.sub(r'[^\w]', '_', dataset_name)
    
    # Check if dataset already exists
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    try:
        # Download to a temporary file
        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            st.text(f"Downloading from {url}...")
            urllib.request.urlretrieve(url, tmp_file.name)
            temp_path = tmp_file.name
        
        # Now load from the temporary file
        success, result = load_dataset_from_path(temp_path, dataset_name, format, **kwargs)
        # Clean up temp file
        os.unlink(temp_path)
        
        # Ensure dataset name is in the result
        if success and isinstance(result, dict) and 'name' not in result:
            result['name'] = dataset_name
            
        return success, result
    except Exception as e:
        # Clean up temp file even on error
        try:
            os.unlink(temp_path)
        except:
            pass
        return False, f"Error downloading or loading from URL: {str(e)}"

def remove_dataset(dataset_name):
    """Remove a dataset from the cache."""
    if not cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' not found."
    
    try:
        cache.remove(dataset_name)
        return True, f"Dataset '{dataset_name}' removed successfully."
    except Exception as e:
        return False, f"Error removing dataset: {str(e)}"

def get_datasets_list():
    """Get a list of all datasets in the cache with their metadata."""
    try:
        # Get cache status for basic info
        status = cache.status()
        datasets = []
        
        # Debug output
        print(f"Cache status: {status}")
        
        # If no entries, check using cache.get_keys() directly
        if 'entries' not in status or not status.get('entries'):
            print("No entries in status, checking keys directly...")
            keys = cache.get_keys()
            print(f"Direct keys from cache: {keys}")
            
            if not keys:
                print("No keys found in cache")
                return []
                
            # Build dataset info from keys
            for key in keys:
                print(f"Processing dataset from key: {key}")
                
                # Create basic dataset info
                dataset_info = {
                    'name': key,
                    'size_bytes': 0,  # We don't have this info from keys alone
                    'size_mb': 0,
                }
                
                # Try to get additional metadata if available
                try:
                    # Get metadata if available
                    metadata = cache.get_metadata(key)
                    if metadata:
                        print(f"Found metadata for {key}: {metadata}")
                        # Add the metadata directly to dataset_info
                        if 'metadata' in metadata and isinstance(metadata['metadata'], dict):
                            dataset_info.update(metadata['metadata'])
                        # Also store the full metadata object
                        dataset_info['metadata'] = metadata
                    
                    # Get a sample to extract schema info
                    sample = cache.get(key, limit=5)
                    if sample is not None and len(sample) > 0:
                        if 'row_count' not in dataset_info:
                            dataset_info['row_count'] = len(sample) if len(sample) < 5 else "5+ rows"
                        if 'columns' not in dataset_info:
                            dataset_info['columns'] = list(sample.columns)
                        if 'column_count' not in dataset_info:
                            dataset_info['column_count'] = len(sample.columns)
                        
                        # If dtypes not in metadata, add them from sample
                        if 'dtypes' not in dataset_info:
                            dataset_info['dtypes'] = {str(col): str(sample[col].dtype) for col in sample.columns}
                        
                        # Try to get total row count using SQL
                        try:
                            count_df = cache.query(f"SELECT COUNT(*) as row_count FROM _cache_{key}")
                            dataset_info['row_count'] = int(count_df['row_count'].iloc[0])
                        except Exception as sql_e:
                            print(f"Error getting row count for {key}: {sql_e}")
                except Exception as e:
                    print(f"Error getting details for dataset {key}: {e}")
                
                datasets.append(dataset_info)
            
            return datasets
            
        # Process entries from status
        for entry in status.get('entries', []):
            if not entry or 'name' not in entry:
                print(f"Skipping invalid entry: {entry}")
                continue
                
            name = entry.get('name')
            if not name:
                print("Skipping entry with empty name")
                continue
                
            print(f"Processing dataset: {name}")
            
            # Create basic dataset info
            dataset_info = {
                'name': name,
                'size_bytes': entry.get('size_bytes', 0),
                'size_mb': entry.get('size_bytes', 0) / (1024*1024),
                'created_at': entry.get('created_at'),
                'last_accessed': entry.get('last_accessed_at'),
            }
            
            # Try to get additional metadata if available
            try:
                # Get metadata if available
                metadata = cache.get_metadata(name)
                if metadata:
                    print(f"Found metadata for {name}: {metadata}")
                    # Add the metadata directly to dataset_info
                    if 'metadata' in metadata and isinstance(metadata['metadata'], dict):
                        dataset_info.update(metadata['metadata'])
                    # Also store the full metadata object
                    dataset_info['metadata'] = metadata
                
                # Get a sample to extract schema info
                sample = cache.get(name, limit=5)
                if sample is not None and len(sample) > 0:
                    if 'row_count' not in dataset_info:
                        dataset_info['row_count'] = len(sample) if len(sample) < 5 else "5+ rows"
                    if 'columns' not in dataset_info:
                        dataset_info['columns'] = list(sample.columns)
                    if 'column_count' not in dataset_info:
                        dataset_info['column_count'] = len(sample.columns)
                    
                    # If dtypes not in metadata, add them from sample
                    if 'dtypes' not in dataset_info:
                        dataset_info['dtypes'] = {str(col): str(sample[col].dtype) for col in sample.columns}
                    
                    # Try to get total row count using SQL
                    try:
                        count_df = cache.query(f"SELECT COUNT(*) as row_count FROM _cache_{name}")
                        dataset_info['row_count'] = int(count_df['row_count'].iloc[0])
                    except Exception as sql_e:
                        print(f"Error getting row count for {name}: {sql_e}")
            except Exception as e:
                print(f"Error getting details for dataset {name}: {e}")
            
            datasets.append(dataset_info)
        
        print(f"Found {len(datasets)} datasets")
        return datasets
    except Exception as e:
        print(f"Error getting datasets list: {e}")
        import traceback
        traceback.print_exc()
        return []

def get_memory_usage():
    """Get current memory usage statistics."""
    status = cache.status()
    
    # Get memory limit directly from the status if available, otherwise use our configured value
    memory_limit = status.get('max_size_bytes', 10 * 1024 * 1024 * 1024)  # Default to 10GB
    
    # Calculate memory usage by summing the memory usage of all datasets
    # This is more reliable than current_size_bytes which often stays at 0
    current_size = 0
    datasets = get_datasets_list()
    
    for dataset in datasets:
        # Get memory usage from dataset metadata if available
        if 'memory_bytes' in dataset:
            current_size += dataset['memory_bytes']
        # Otherwise try to get it from size_bytes in the dataset info
        elif 'size_bytes' in dataset and dataset['size_bytes'] > 0:
            current_size += dataset['size_bytes']
    
    # If we still have no memory usage, try to get it from the memory info in status
    if current_size == 0 and 'memory' in status and 'system_memory_used' in status.get('memory', {}):
        # Use system memory as a fallback
        baseline = 200 * 1024 * 1024  # Estimate ~200MB baseline Streamlit usage
        system_used = status['memory']['system_memory_used']
        if system_used > baseline:
            current_size = system_used - baseline  # Approximate memory used by our app
    
    # If all else fails, use current_size_bytes which might be 0
    if current_size == 0:
        current_size = status.get('current_size_bytes', 0)
    
    # Debug output
    print(f"Calculated memory usage: {current_size / (1024*1024):.2f} MB")
    print(f"Raw status: {status}")
    
    memory_usage = {
        'current_size_bytes': current_size,
        'current_size_mb': current_size / (1024*1024),
        'memory_limit_bytes': memory_limit,
        'memory_limit_mb': memory_limit / (1024*1024),
        'utilization_percent': (current_size / memory_limit) * 100 if memory_limit > 0 else 0,
        'entry_count': status.get('entry_count', 0) or len(datasets)  # Use dataset count as fallback
    }
    
    return memory_usage

def create_plot(dataset_name, x, y=None, kind='line', **kwargs):
    """Create a plot from a dataset and return as base64 image."""
    if not cache.contains(dataset_name):
        return None, f"Dataset '{dataset_name}' not found."
    
    try:
        # Get data (limit rows for plotting)
        df = cache.get(dataset_name, limit=10000)
        
        # Check columns exist
        if x not in df.columns:
            return None, f"Column '{x}' not found in dataset '{dataset_name}'."
        
        if y and isinstance(y, str) and y not in df.columns:
            return None, f"Column '{y}' not found in dataset '{dataset_name}'."
        
        if y and isinstance(y, list):
            for col in y:
                if col not in df.columns:
                    return None, f"Column '{col}' not found in dataset '{dataset_name}'."
        
        # Extract dpi parameter for savefig (not for plot function)
        dpi = kwargs.pop('dpi', 100) if 'dpi' in kwargs else 100
        
        # Create plot
        plt.figure(figsize=kwargs.get('figsize', (10, 6)))
        
        plot_func = getattr(df.plot, kind)
        ax = plot_func(x=x, y=y, **kwargs)
        fig = ax.get_figure()
        
        # Set title if provided
        if 'title' in kwargs:
            plt.title(kwargs['title'])
        
        # Save to buffer as PNG with dpi parameter here
        buf = io.BytesIO()
        fig.savefig(buf, format='png', dpi=dpi)
        buf.seek(0)
        
        # Convert to base64
        plot_base64 = base64.b64encode(buf.read()).decode('utf-8')
        
        # Clean up
        plt.close(fig)
        
        return plot_base64, "Plot created successfully."
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        return None, f"Error creating plot: {str(e)}"

# --- Query Functions ---
def extract_and_run_queries(claude_response):
    """Extract SQL queries from Claude's response, execute them, and add the results"""
    parts = []
    current_pos = 0
    executed_queries = []  # Track executed queries
    results_data = []  # Store query results for potential followup
    
    # Look for query blocks
    while True:
        query_start = claude_response.find("<query>", current_pos)
        if query_start == -1:
            # Add the remaining text
            if current_pos < len(claude_response):
                parts.append(claude_response[current_pos:])
            break
            
        # Add text before the query
        if query_start > current_pos:
            parts.append(claude_response[current_pos:query_start])
            
        query_end = claude_response.find("</query>", query_start)
        if query_end == -1:
            # Malformed response, just add everything
            parts.append(claude_response[current_pos:])
            break
            
        # Extract the query
        query = claude_response[query_start + 8:query_end].strip()
        executed_queries.append(query)  # Add to executed queries list
        
        # Execute the query
        try:
            start_time = time.time()
            result_df = cache.query(query)
            query_time = time.time() - start_time
            
            # Store the results for potential followup
            results_data.append({"query": query, "result_df": result_df, "query_time": query_time})
            
            # Format in a more streamlit-friendly way without HTML directly
            parts.append("\n\n**Query:**\n```sql\n")
            parts.append(query)
            parts.append("\n```\n\n")
            parts.append(f"*Query executed in {query_time:.3f}s*\n\n")
            parts.append("**Results:**\n\n")
            
            # Convert DataFrame to markdown table for better rendering
            # For smaller results, convert to markdown
            if len(result_df) < 10 and len(result_df.columns) < 8:
                parts.append(result_df.to_markdown(index=False))
            else:
                # For larger results, provide a message that the results will be shown below
                parts.append(f"*Showing {len(result_df)} rows with {len(result_df.columns)} columns. Full results displayed below.*\n\n")
                
                # Store the result DataFrame in session state to display separately
                st.session_state.last_claude_result = result_df
                
        except Exception as e:
            # Format error without HTML
            parts.append("\n\n**Query:**\n```sql\n")
            parts.append(query)
            parts.append("\n```\n\n**Error:** ")
            parts.append(str(e))
            parts.append("\n\n")
            
        current_pos = query_end + 8  # Move past </query>
    
    return "".join(parts), executed_queries, results_data

def ask_claude(question, api_key):
    """Ask Claude a question about the datasets and track the conversation"""
    if not api_key:
        return "Error: Anthropic API key not found. Please set the ANTHROPIC_API_KEY environment variable or configure it in Streamlit secrets."
    
    # Initialize conversation history if not exists
    if "conversation_history" not in st.session_state:
        st.session_state.conversation_history = []
    
    # Add the user question to the history
    st.session_state.conversation_history.append({
        "role": "user",
        "content": question,
        "timestamp": time.time()
    })
    
    # Get datasets info for context with enhanced metadata
    datasets = get_datasets_list()
    datasets_info = ""
    
    for ds in datasets:
        # Basic dataset info
        datasets_info += f"\n- Dataset '{ds['name']}': {ds.get('row_count', 'Unknown')} rows, {len(ds.get('columns', []))} columns"
        
        # Add format information if available
        if 'format' in ds:
            datasets_info += f"\n  Format: {ds['format']}"
            
        # Add source information if available
        if 'source' in ds:
            datasets_info += f"\n  Source: {ds['source']}"
        
        # Add column details with data types
        if 'columns' in ds and 'dtypes' in ds:
            datasets_info += "\n  Columns with data types:"
            for col in ds['columns']:
                dtype = ds['dtypes'].get(col, 'unknown')
                datasets_info += f"\n    - {col}: {dtype}"
        elif 'columns' in ds:
            datasets_info += f"\n  Columns: {', '.join(ds['columns'])}"
            
        # Add additional stats if available
        if 'memory_bytes' in ds:
            datasets_info += f"\n  Memory usage: {ds['memory_bytes'] / (1024*1024):.2f} MB"
            
        # Add any other metadata that would be useful for Claude
        if 'metadata' in ds:
            for key, value in ds['metadata'].items():
                if key not in ['columns', 'dtypes', 'format', 'source', 'row_count'] and not key.startswith('_'):
                    datasets_info += f"\n  {key}: {value}"
    
    client = anthropic.Anthropic(api_key=api_key)
    
    # Include conversation history context for the system prompt
    conversation_context = ""
    if len(st.session_state.conversation_history) > 1:
        # Include last few exchanges for context if this isn't the first message
        last_exchanges = st.session_state.conversation_history[-3:]  # Last 3 exchanges
        conversation_context = "Recent conversation history:\n"
        for entry in last_exchanges:
            if entry["role"] == "user":
                conversation_context += f"User: {entry['content']}\n"
            else:
                # Just include the text part without the query results for context
                content = entry["content"]
                if "query_executed" in entry:
                    # If there was a query, add a note about it
                    conversation_context += f"Claude: [Answered and ran query: {entry['query_executed']}]\n"
                else:
                    conversation_context += f"Claude: [Provided answer without query]\n"
    
    # Build the context and prompt
    system_prompt = dedent(f"""
    You are a data analyst with access to a data sandbox containing multiple datasets.
    The following datasets are currently available in the cache:
    {datasets_info}
    
    When querying a dataset, use the following FROM clause syntax: FROM _cache_<dataset_name>
    
    {conversation_context}
    
    Your task is to help the user analyze these datasets by:
    1. Whenever possible, use the dataset metadata I've provided to answer simple questions about column names, data types, or dataset properties without running a query
    2. For more complex analysis, generate appropriate SQL queries 
    3. Explain what the query does
    4. Generate only queries at this stage - do not try to predict what the results will be
    5. Include the query within <query> tags
    
    For example:
    User: What columns are in the dataset 'customers'?
    You: The dataset 'customers' contains the following columns:
    - customer_id (int64)
    - name (string)
    - email (string)
    - signup_date (date)
    
    User: How many rows are in the dataset 'customers'?
    You: The customers dataset contains 1,500 rows.
    
    User: What's the average order value by customer segment?
    You: To find the average order value by customer segment, I'll run a query:
    
    <query>
    SELECT segment, AVG(order_value) as avg_order_value
    FROM _cache_customers
    GROUP BY segment
    ORDER BY avg_order_value DESC;
    </query>
    
    At this stage, I'll just provide the query to calculate the results.
    """)
    
    try:
        # STEP 1: Call the Claude API to get query
        message = client.messages.create(
            model="claude-3-opus-20240229",
            max_tokens=1000,
            temperature=0,
            system=system_prompt,
            messages=[
                {"role": "user", "content": question}
            ]
        )
        
        initial_response = message.content[0].text
        
        # Extract and execute any SQL queries in the response
        response_with_results, executed_queries, results_data = extract_and_run_queries(initial_response)
        
        # STEP 2: If queries were executed, ask Claude to interpret the results
        if results_data:
            # Prepare result information for Claude
            query_result_info = ""
            for idx, data in enumerate(results_data):
                query = data["query"]
                result_df = data["result_df"]
                
                # Convert DataFrame to string representation for Claude
                if len(result_df) > 10:
                    # If large result, provide summary of first 10 rows
                    result_str = f"First 10 rows out of {len(result_df)} total rows:\n{result_df.head(10).to_string()}"
                else:
                    # If small result, provide all rows
                    result_str = result_df.to_string()
                
                query_result_info += f"\nQuery {idx+1}:\n{query}\n\nResults:\n{result_str}\n"
            
            # Create a follow-up prompt for Claude to interpret results
            followup_system_prompt = dedent(f"""
            You are a data analyst with access to a data sandbox containing multiple datasets.
            
            I ran the query you provided and now need you to interpret the results. 
            Please provide a clear, accurate interpretation based ONLY on the actual data from the query results.
            Do not try to predict or make up values that aren't in the results.
            
            Be precise about the actual values in the results. For numbers, use the exact values shown.
            For timestamps and dates, use the exact format shown in the results.
            
            The query and its results are provided below:
            {query_result_info}
            """)
            
            # Get Claude's interpretation of the results
            followup_message = client.messages.create(
                model="claude-3-opus-20240229",
                max_tokens=1000,
                temperature=0,
                system=followup_system_prompt,
                messages=[
                    {"role": "user", "content": "Please interpret these query results accurately."}
                ]
            )
            
            interpretation_response = followup_message.content[0].text
            
            # Combine the original query with the interpretation
            final_response = response_with_results + "\n\n**Interpretation:**\n" + interpretation_response
        else:
            # If no queries were executed, just use the original response
            final_response = response_with_results
        
        # Add Claude's response to the history with query info
        history_entry = {
            "role": "assistant",
            "content": final_response,
            "timestamp": time.time(),
        }
        
        # Add query information if queries were executed
        if executed_queries:
            history_entry["query_executed"] = executed_queries[0] if executed_queries else None
            history_entry["query_count"] = len(executed_queries)
        
        st.session_state.conversation_history.append(history_entry)
        
        return final_response
    except Exception as e:
        error_message = f"Error connecting to Claude API: {str(e)}"
        # Add error to conversation history
        st.session_state.conversation_history.append({
            "role": "system",
            "content": error_message,
            "timestamp": time.time()
        })
        return error_message

# --- Add function to display conversation history ---
def display_conversation_history():
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

# --- Render dataset cards ---
def render_dataset_card(dataset):
    """Render a dataset card with its info and action buttons."""
    with st.container():
        col1, col2 = st.columns([3, 1])
        
        with col1:
            # Get memory size with proper precedence
            memory_size = 0
            if 'memory_bytes' in dataset:
                memory_size = dataset['memory_bytes'] / (1024*1024)
            elif 'size_bytes' in dataset and dataset['size_bytes'] > 0:
                memory_size = dataset['size_bytes'] / (1024*1024)
                
            # Use proper formatting to avoid 0.00 MB for small datasets
            if memory_size < 0.01 and memory_size > 0:
                size_display = f"{memory_size*1024:.2f} KB"
            else:
                size_display = f"{memory_size:.2f} MB"
                
            st.markdown(f"""
            <div class="dataset-card">
                <h4>{dataset['name']}</h4>
                <p>
                    <strong>Rows:</strong> {dataset.get('row_count', 'Unknown')} | 
                    <strong>Columns:</strong> {dataset.get('column_count', len(dataset.get('columns', [])))} | 
                    <strong>Size:</strong> {size_display}
                </p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.button("View", key=f"view_{dataset['name']}", 
                      on_click=lambda: st.session_state.update({'selected_dataset': dataset['name']}))
            st.button("Remove", key=f"remove_{dataset['name']}", 
                      on_click=lambda: remove_and_update(dataset['name']))

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

# --- Streamlit App UI ---
st.title("📊 Data Sandbox")
st.markdown("A versatile data analysis sandbox powered by ArrowCache and Claude")

# Sidebar for memory info and actions
with st.sidebar:
    st.header("Memory Usage")
    memory_info = get_memory_usage()
    
    # Memory gauge
    st.markdown(f"""
    <div class="memory-gauge">
        <div class="container">
            <div class="used" style="width: {min(100, memory_info['utilization_percent'])}%;"></div>
        </div>
        <p>{memory_info['current_size_mb']:.1f} MB of {memory_info['memory_limit_mb']:.1f} MB 
           ({memory_info['utilization_percent']:.1f}%)</p>
    </div>
    """, unsafe_allow_html=True)
    
    st.caption(f"Number of datasets: {memory_info['entry_count']}")
    
    if st.button("Refresh Status"):
        st.rerun()
    
    # Claude API key input
    st.header("Claude API")
    api_key = get_claude_api_key()
    if not api_key:
        api_key = st.text_input("Enter your Anthropic API Key:", type="password", 
                             help="Get an API key from https://console.anthropic.com/",
                             key="sidebar_api_key")
        if api_key:
            st.session_state.claude_api_key = api_key
            st.success("API key saved for this session!")
        else:
            st.warning("Please enter an Anthropic API key to use Claude.")

# Main content area with tabs
tab1, tab2, tab3, tab4 = st.tabs(["Datasets", "SQL Query", "Ask Claude", "Visualize"])

# Tab 1: Dataset Management
with tab1:
    st.header("Manage Datasets")
    
    # Dataset import section
    with st.expander("Import Dataset", expanded=True):
        try:
            import_tab1, import_tab2, import_tab3 = st.tabs(["Upload File", "From URL", "Sample Data"])
            
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
                
                # Additional options based on format - use columns instead of expander
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
                
                # Advanced options for URL import - use columns instead of expander
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
            
            # Sample data tab
            with import_tab3:
                sample_options = {
                    "NYC Yellow Taxi (Jan 2023)": {"url": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet", "format": "parquet"},
                    "Titanic Passengers": {"url": "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv", "format": "csv"},
                    "Iris Flower Dataset": {"url": "https://raw.githubusercontent.com/mwaskom/seaborn-data/master/iris.csv", "format": "csv"},
                    "US Housing Data": {"url": "https://raw.githubusercontent.com/datasciencedojo/datasets/master/housing.csv", "format": "csv"}
                }
                
                sample_dataset = st.selectbox("Choose a sample dataset:", list(sample_options.keys()))
                sample_name = st.text_input("Dataset Name (optional):", key="sample_name")
                
                if st.button("Load Sample Dataset"):
                    dataset_info = sample_options[sample_dataset]
                    url = dataset_info["url"]
                    format = dataset_info["format"]
                    name = sample_name if sample_name else sample_dataset.lower().replace(" ", "_")
                    
                    with st.spinner(f"Loading sample dataset: {sample_dataset}"):
                        # Clear any existing dataset with this name
                        if cache.contains(name):
                            success, message = remove_dataset(name)
                            if not success:
                                st.error(message)
                                
                        # Load the dataset
                        success, result = load_dataset_from_url(url, name, format)
                        
                        if success:
                            # Add a short delay to ensure the cache is updated
                            time.sleep(0.5)
                            
                            # Double-check that the dataset was loaded
                            if cache.contains(name):
                                st.success(f"Sample dataset loaded successfully as '{name}'")
                                # Force a refresh of the app to ensure datasets are recognized
                                st.rerun()
                            else:
                                st.error(f"Dataset was loaded but not found in cache. Please try again.")
                        else:
                            st.error(result)
        except Exception as e:
            st.error(f"Error creating tabs: {e}")
            st.write("Please try refreshing the page.")
    
    # List of loaded datasets
    st.header("Loaded Datasets")
    datasets = get_datasets_list()
    
    if not datasets:
        st.info("No datasets loaded. Import a dataset using the options above.")
    else:
        # Display datasets as cards
        for dataset in datasets:
            render_dataset_card(dataset)
    
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
                for col in sample.columns:
                    dtype = str(sample[col].dtype)
                    sample_values = ", ".join([str(v) for v in sample[col].head(3).values])
                    st.markdown(f"**{col}** ({dtype}) - Sample: {sample_values}...")

# Tab 2: SQL Query
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

# Tab 3: Claude-powered Natural Language Querying
with tab3:
    st.header("Ask Claude about your Data")
    
    if not datasets:
        st.info("No datasets loaded. Please load at least one dataset before asking Claude questions.")
    else:
        # Two columns - one for input, one for history
        input_col, history_col = st.columns([3, 5])
        
        with input_col:
            # Get API key (and allow manual entry if not found)
            api_key = get_claude_api_key()
            if not api_key:
                api_key = st.text_input("Enter your Anthropic API Key:", type="password", 
                                    help="Get an API key from https://console.anthropic.com/",
                                    key="claude_tab_api_key")
                if not api_key:
                    st.warning("Please enter an Anthropic API key to use Claude.")
                    
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
                                        help="Ask in plain English, Claude will generate SQL queries.")
            
            if st.button("Ask Claude", key="ask_claude_btn", disabled=not api_key):
                if claude_question:
                    with st.spinner("Claude is analyzing the data..."):
                        response = ask_claude(claude_question, api_key)
                        # We don't need to display the response here since it will be shown in the history
                        
                        # Save the question in session state and clear for next question
                        st.session_state.claude_question = ""
                        st.rerun()
                else:
                    st.warning("Please enter a question for Claude.")
            
            # Add clear history button
            add_clear_history_button()
            
            # If there's a result DataFrame, show it under the input section
            if hasattr(st.session_state, 'last_claude_result') and st.session_state.last_claude_result is not None:
                st.subheader("Latest Query Results")
                st.dataframe(st.session_state.last_claude_result, use_container_width=True)
        
        # Show conversation history in the second column
        with history_col:
            display_conversation_history()

# Tab 4: Visualization
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
                
                # Advanced options using st.markdown + columns instead of nested expander
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
atexit.register(lambda: cache.close() if cache else None) 