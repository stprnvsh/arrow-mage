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

# --- Initialize Logger ---
logger = logging.getLogger(__name__)

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
    memory_limit=20 * 1024 * 1024 * 1024,  # 20 GB limit
    spill_to_disk=True,                   # Allow spilling if memory is exceeded
    spill_directory=SPILL_DIRECTORY,
    auto_partition=True,                  # Enable auto-partitioning for large datasets
    compression_type="lz4",               # Use LZ4 compression
    enable_compression=True,              # Enable compression
    dictionary_encoding=True,             # Use dictionary encoding for strings
    cache_query_plans=True,
    thread_count=10,
    background_threads=5,
    partition_size_rows=10000000,         # 10M rows per partition
    partition_size_bytes=1 * 1024 * 1024 * 1024,  # 1GB per partition
    streaming_chunk_size=500000,          # Larger streaming chunks
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
    
    # Import and run the cache cleanup utility
    try:
        from utils.cache_cleanup import clear_arrow_cache
        clear_arrow_cache(spill_dir=SPILL_DIRECTORY)
        print("Cleaned up any leftover cache files from previous runs")
    except ImportError:
        print("Cache cleanup utility not found, skipping initial cleanup")
    
    # Ensure spill directory exists
    os.makedirs(SPILL_DIRECTORY, exist_ok=True)
    
    # Create the config using the pattern from advanced_features.py
    config = ArrowCacheConfig(
        memory_limit=20 * 1024 * 1024 * 1024,  # 20 GB limit
        spill_to_disk=True,                   # Allow spilling if memory is exceeded
        spill_directory=SPILL_DIRECTORY,
        auto_partition=True,                  # Enable auto-partitioning for large datasets
        compression_type="lz4",               # Use LZ4 compression
        enable_compression=True,              # Enable compression
        dictionary_encoding=True,             # Use dictionary encoding for strings
        cache_query_plans=True,
        thread_count=10,
        background_threads=5,
        partition_size_rows=10000000,         # 10M rows per partition
        partition_size_bytes=1 * 1024 * 1024 * 1024,  # 1GB per partition
        streaming_chunk_size=500000,          # Larger streaming chunks
    )
    
    # Function to completely clear cache files
    def clear_cache_files():
        """Clear all cache files including spill directory and any persisted files"""
        import shutil
        import os
        
        # Clear spill directory
        if os.path.exists(SPILL_DIRECTORY):
            try:
                shutil.rmtree(SPILL_DIRECTORY)
                print(f"Removed spill directory: {SPILL_DIRECTORY}")
            except Exception as e:
                print(f"Error clearing spill directory: {e}")
        
        # Create empty spill directory
        os.makedirs(SPILL_DIRECTORY, exist_ok=True)
        
        # Clear any DuckDB-related files that might be persisting
        duckdb_files = [f for f in os.listdir('.') if f.endswith('.duckdb') or f.endswith('.duckdb.wal')]
        for file in duckdb_files:
            try:
                os.remove(file)
                print(f"Removed DuckDB file: {file}")
            except Exception as e:
                print(f"Error removing DuckDB file {file}: {e}")
        
        print("Cache files cleared")
    
    # Clear cache files before initializing
    clear_cache_files()
    
    # Initialize the cache with the config
    cache = ArrowCache(config=config)
    print(f"ArrowCache Initialized. Using spill dir: {SPILL_DIRECTORY}")
    
    # Register both normal close and forced cleanup handlers
    import atexit
    
    def close_cache():
        """Close the cache and ensure all resources are released"""
        if cache:
            print("Closing ArrowCache and cleaning up resources...")
            try:
                # First try to get keys
                try:
                    keys = cache.get_keys()
                except Exception as e:
                    print(f"Error getting cache keys: {e}")
                    keys = []
                
                # Manually unregister and remove all tables first
                for key in keys:
                    try:
                        # Try to remove the dataset
                        cache.remove(key)
                    except Exception as e:
                        print(f"Error removing dataset {key}: {e}")
                        # Fallback: try to unregister the table directly
                        try:
                            cache.metadata_store.unregister_table(key)
                        except Exception as inner_e:
                            print(f"Error unregistering table {key}: {inner_e}")
                
                # Reset DuckDB connection if the metadata store exists
                try:
                    if hasattr(cache, 'metadata_store') and hasattr(cache.metadata_store, 'con'):
                        # Close and reopen the connection to reset its state
                        original_path = cache.metadata_store.db_path
                        cache.metadata_store.con.close()
                        import duckdb
                        cache.metadata_store.con = duckdb.connect(original_path)
                except Exception as e:
                    print(f"Error resetting DuckDB connection: {e}")
                
                # Force cache to clear all entries
                try:
                    cache.clear()
                except Exception as e:
                    print(f"Error clearing cache: {e}")
                
                # Close the cache
                try:
                    cache.close()
                except Exception as e:
                    print(f"Error closing cache: {e}")
                
                # Clear any remaining files
                try:
                    clear_cache_files()
                except Exception as e:
                    print(f"Error clearing cache files: {e}")
                
                # Run Python garbage collection to release memory
                import gc
                for _ in range(3):  # Multiple collection passes can help
                    gc.collect()
                
                print("ArrowCache successfully closed and cleaned up")
            except Exception as e:
                print(f"Error during cache cleanup: {e}")
                import traceback
                traceback.print_exc()
    
    # Register the enhanced close function
    atexit.register(close_cache)
    
    return cache

cache = get_arrow_cache()

# Add this near the top of the file, after imports but before cache initialization
def patch_partitioned_table():
    """
    Add compatibility patch for PartitionedTable to work with the eviction policy.
    
    The eviction policy expects a size_bytes attribute, but PartitionedTable uses total_size_bytes.
    This patch adds a size_bytes property to make it compatible.
    """
    try:
        from arrow_cache.partitioning import PartitionedTable
        
        # Add a size_bytes property if it doesn't already exist
        if not hasattr(PartitionedTable, 'size_bytes'):
            # Using the descriptor protocol to create a property
            PartitionedTable.size_bytes = property(
                lambda self: self.total_size_bytes,
                lambda self, value: setattr(self, 'total_size_bytes', value)
            )
            print("Applied PartitionedTable compatibility patch")
    except Exception as e:
        print(f"Failed to apply PartitionedTable patch: {e}")

# Add this line after the import but before cache initialization
patch_partitioned_table()

# Add this function after get_arrow_cache but before other functions
def enhance_memory_management():
    """
    Enhance memory management for ArrowCache by adding custom spill handlers
    and fixing any issues with partitioned tables.
    """
    
    def custom_spill_handler(needed_bytes):
        """Custom spill handler that properly handles partitioned tables"""
        print(f"Custom spill handler called, trying to free {needed_bytes/1024/1024:.2f} MB")
        
        # Get all partitioned tables
        freed_bytes = 0
        if hasattr(cache, 'partitioned_tables'):
            # Create a list and sort by last accessed time (oldest first)
            candidates = sorted(
                [(key, table) for key, table in cache.partitioned_tables.items() 
                 if hasattr(table, 'spill_partitions')],
                key=lambda x: cache.metadata_store.get_entry_metadata(x[0]).get("last_accessed_at", 0) 
                              if cache.metadata_store.get_entry_metadata(x[0]) else 0
            )
            
            spill_dir = cache.config["spill_directory"]
            
            # Spill partitions from each table until we've freed enough
            for key, table in candidates:
                if freed_bytes >= needed_bytes:
                    break
                    
                # Use the table's spill_partitions method
                try:
                    freed = table.spill_partitions(spill_dir, needed_bytes - freed_bytes)
                    print(f"Freed {freed/1024/1024:.2f} MB from {key}")
                    freed_bytes += freed
                except Exception as e:
                    print(f"Error spilling partitions from {key}: {e}")
                    # Try the slow but safer approach - unload individual partitions
                    try:
                        # Try to unload partitions manually
                        if hasattr(table, 'partitions'):
                            for part_id, partition in table.partitions.items():
                                if partition.is_loaded and not partition.is_pinned:
                                    estimated_size = getattr(partition, 'size_bytes', 0)
                                    partition.free()  # Release memory
                                    freed_bytes += estimated_size
                                    print(f"Manually freed partition {part_id} ({estimated_size/1024/1024:.2f} MB)")
                                    if freed_bytes >= needed_bytes:
                                        break
                    except Exception as inner_e:
                        print(f"Error in manual partition unloading: {inner_e}")
        
        # If we still need more space, let cache handle it
        print(f"Custom spill handler freed {freed_bytes/1024/1024:.2f} MB")
        return freed_bytes
    
    # Register our custom spill handler with the memory manager
    if hasattr(cache, 'memory_manager') and hasattr(cache.memory_manager, 'spill_callback'):
        # Store the original spill callback as a fallback
        original_spill_callback = cache.memory_manager.spill_callback
        
        # Create a wrapper function to use both our handler and the original
        def combined_spill_handler(needed_bytes):
            # First try our custom handler
            freed_bytes = custom_spill_handler(needed_bytes)
            
            # If we need more space, call the original handler
            if freed_bytes < needed_bytes and original_spill_callback:
                try:
                    more_freed = original_spill_callback(needed_bytes - freed_bytes)
                    freed_bytes += more_freed
                except Exception as e:
                    print(f"Error in original spill callback: {e}")
            
            return freed_bytes
        
        # Replace the original spill callback with our combined one
        cache.memory_manager.spill_callback = combined_spill_handler
        print("Enhanced memory management installed")

# Call the function after patching PartitionedTable
enhance_memory_management()

# --- Add helper function for cleaning dataset names ---
def _clean_dataset_name(name: str) -> str:
    """Clean a dataset name to be a valid identifier."""
    # Replace parentheses and common separators with underscores
    name = re.sub(r'[\s().,-]+', '_', name) # Corrected regex
    # Remove any characters that are not alphanumeric or underscore
    name = re.sub(r'[^\w_]+', '', name) # Corrected regex
    # Replace multiple consecutive underscores with a single underscore
    name = re.sub(r'_+', '_', name) # Corrected regex
    # Remove leading/trailing underscores
    name = name.strip('_')
    return name

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
        base_name = os.path.basename(path).split('.')[0]
        # Clean up name using the helper function
        dataset_name = _clean_dataset_name(base_name)
    
    # Check if dataset with this name already exists
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    # Determine format if not provided
    if not format:
        # Try using arrow_cache.converters for format detection
        try:
            from arrow_cache.converters import guess_format
            format = guess_format(path)
            print(f"Format detection from converters: {format}")
        except (ImportError, Exception) as e:
            print(f"Error using converter's format detection: {e}")
            # Fall back to our own detection
            format = guess_format_from_path(path)
        
        if not format:
            return False, f"Could not determine format from file extension. Please specify it explicitly."
    
    if format not in SUPPORTED_FORMATS:
        return False, f"Unsupported format: {format}. Supported formats: {list(SUPPORTED_FORMATS.keys())}"
    
    try:
        # Special case for yellow taxi dataset which has partitioning issues
        if "yellow_tripdata" in path and format == "parquet":
            print(f"Detected NYC yellow taxi dataset. Using Arrow-native direct loading approach.")
            
            # Custom parameters for reading the taxi dataset to prevent over-partitioning
            config_dict = cache.config.to_dict()
            config_dict.update({
                'partition_size_rows': 5000000,  # Force much larger rows per partition 
                'partition_size_bytes': 500 * 1024 * 1024,  # Force larger partition size (500MB)
                'dictionary_encoding': False,  # Completely disable dictionary encoding
                'enable_compression': False,  # Disable compression for initial load
            })
            custom_config = ArrowCacheConfig(**config_dict)
            
            # Use Arrow-native loading with memory mapping for performance
            import pyarrow.parquet as pq
            start_time = time.time()
            
            # Use memory mapping and direct table read without any transformations
            table = pq.read_table(
                path, 
                use_threads=True,
                memory_map=True,
                use_pandas_metadata=False
            )
            load_time = time.time() - start_time
            
            # Use ArrowCache's built-in size estimation instead of custom calculation
            from arrow_cache.converters import estimate_size_bytes
            size_bytes = estimate_size_bytes(table)
            
            # Prepare metadata
            metadata = {
                'source': path,
                'format': format,
                'loaded_at': time.time(),
                'load_time_seconds': load_time,
                'row_count': table.num_rows,
                'column_count': len(table.column_names),
                'columns': list(table.column_names),
                'dtypes': {col: str(table.schema.field(col).type) for col in table.column_names},
                'custom_loading': 'Used fully native Arrow loading without dictionary encoding or compression',
                'raw_data': True,  # Flag that this is raw data with no transformations
                'memory_bytes': size_bytes,
                'size_bytes': size_bytes
            }
            
            # Store with custom config
            start_cache_time = time.time()
            # Force replacement of config temporarily
            original_config = cache.config
            cache.config = custom_config
            
            try:
                # Use built-in partitioning functionality from cache.py and partitioning.py
                from arrow_cache.partitioning import partition_table
                
                # Let Arrow Cache handle partitioning decisions based on config
                if table.num_rows > custom_config["partition_size_rows"]:
                    print(f"Table size ({table.num_rows} rows) exceeds partition size threshold, using partitioning")
                    cache.put(dataset_name, table, metadata=metadata, auto_partition=True)
                else:
                    # For smaller tables, use normal put without partitioning
                    cache.put(dataset_name, table, metadata=metadata, auto_partition=False)
            finally:
                # Restore original config
                cache.config = original_config
                
            cache_time = time.time() - start_cache_time
            
            # Update metadata with timing info
            metadata['cache_time_seconds'] = cache_time
            metadata['total_time_seconds'] = load_time + cache_time
            metadata['name'] = dataset_name
            
            return True, metadata
            
        # Regular case for other files
        start_time = time.time()
        
        # Use arrow_cache.converters for direct Arrow conversion when possible
        try:
            from arrow_cache.converters import to_arrow_table, estimate_size_bytes
            
            # Check if this format is directly supported by converters
            if format in ['parquet', 'arrow', 'feather', 'csv', 'json']:
                print(f"Using arrow_cache.converters.to_arrow_table for {format}")
                # Pass through kwargs for CSV options etc.
                table = to_arrow_table(path, format=format, preserve_index=True, **kwargs)
                load_time = time.time() - start_time
                
                # Prepare metadata using Arrow info
                size_bytes = estimate_size_bytes(table)
                
                metadata = {
                    'source': path,
                    'format': format,
                    'loaded_at': time.time(),
                    'load_time_seconds': load_time,
                    'row_count': table.num_rows,
                    'column_count': len(table.column_names),
                    'columns': list(table.column_names),
                    'memory_bytes': size_bytes,
                    'size_bytes': size_bytes,
                    'dtypes': {col: str(table.schema.field(col).type) for col in table.column_names},
                    'converted_by': 'arrow_cache.converters.to_arrow_table'
                }
                
                # Store the Arrow table directly
                start_cache_time = time.time()
                cache.put(dataset_name, table, metadata=metadata, 
                         auto_partition=cache.config["auto_partition"])
                cache_time = time.time() - start_cache_time
                
                # Update metadata with timing info
                metadata['cache_time_seconds'] = cache_time
                metadata['total_time_seconds'] = load_time + cache_time
                metadata['name'] = dataset_name
                
                return True, metadata
        except (ImportError, Exception) as e:
            print(f"Direct conversion failed, falling back to pandas: {e}")
        
        # Fall back to pandas-based loading for other formats
        # Get the read function for this format
        read_func = SUPPORTED_FORMATS[format]['read_func']
        
        # Load the dataset
        df = read_func(path, **kwargs)
        load_time = time.time() - start_time
        
        # Get dataframe memory usage
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
            'size_bytes': int(df_memory),
            'dtypes': {str(col): str(dtype) for col, dtype in df.dtypes.items()},
            'converted_by': 'pandas'
        }
        
        # Add geospatial metadata if applicable
        if hasattr(df, 'crs') and df.crs is not None:
            metadata['is_geospatial'] = True
            metadata['crs'] = str(df.crs)
            if hasattr(df, 'geometry'):
                metadata['geometry_column'] = 'geometry'
        
        # Let ArrowCache optimize the conversion and storage
        try:
            # Use ArrowCache's preferred storage method
            # The cache's put method will handle the conversion and optimization
            start_cache_time = time.time()
            cache.put(dataset_name, df, metadata=metadata, 
                     auto_partition=cache.config["auto_partition"])
            cache_time = time.time() - start_cache_time
        except Exception as cache_error:
            # If ArrowCache's put method fails, try the direct approach
            print(f"Error using ArrowCache's put method: {cache_error}. Trying direct approach.")
            # Convert to Arrow table directly 
            import pyarrow as pa
            try:
                arrow_table = pa.Table.from_pandas(df)
                start_cache_time = time.time()
                cache.put(dataset_name, arrow_table, metadata=metadata, 
                         auto_partition=cache.config["auto_partition"])
                cache_time = time.time() - start_cache_time
            except Exception as arrow_error:
                return False, f"Error storing dataset: {arrow_error}"
        
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
        base_name = uploaded_file.name.split('.')[0]
        # Clean up name using the helper function
        dataset_name = _clean_dataset_name(base_name)
    
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
        base_name = os.path.basename(path).split('.')[0]
        # Clean up name using the helper function
        dataset_name = _clean_dataset_name(base_name)
    
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
        # Get datasets directly from metadata store - this is the most reliable source
        metadata_df = cache.metadata_store.get_all_entries()
        
        if metadata_df.empty:
            print("No entries found in metadata store")
            return []
        
        datasets = []
        for _, row in metadata_df.iterrows():
            # Extract key fields from metadata DataFrame
            key = row['key']
            size_bytes = row['size_bytes']
            num_rows = row['num_rows']
            created_at = row['created_at']
            last_accessed_at = row['last_accessed_at']
            
            # Parse schema from schema_json - FIXED IMPLEMENTATION
            try:
                schema_json = row['schema_json']
                
                # Parse schema directly from metadata_store instead of trying to reconstruct it
                # This avoids using pa.Schema.from_string which doesn't exist
                import pyarrow as pa
                
                # Try to extract column names directly from the schema_json string
                import re
                column_matches = re.findall(r'field_name: (.*?)[,\)]', schema_json)
                if not column_matches:
                    # Alternative pattern
                    column_matches = re.findall(r'Field\((.*?):', schema_json)
                
                column_names = [name.strip().strip("'\"") for name in column_matches]
                column_count = len(column_names)
                
                # If we couldn't extract column names, try accessing metadata_json for column info
                if not column_names and row['metadata_json']:
                    metadata = json.loads(row['metadata_json'])
                    if 'columns' in metadata:
                        column_names = metadata['columns']
                        column_count = len(column_names)
                
            except Exception as e:
                print(f"Error parsing schema for {key}: {e}")
                column_names = []
                column_count = 0
            
            # Parse metadata from metadata_json
            try:
                if row['metadata_json']:
                    metadata = json.loads(row['metadata_json'])
                else:
                    metadata = {}
            except Exception as e:
                print(f"Error parsing metadata for {key}: {e}")
                metadata = {}
            
            # Create dataset info dict with all important information
            dataset_info = {
                'name': key,
                'size_bytes': size_bytes,
                'size_mb': size_bytes / (1024*1024),
                'row_count': num_rows,
                'columns': column_names,
                'column_count': column_count,
                'created_at': created_at,
                'last_accessed': last_accessed_at,
            }
            
            # Add metadata fields directly to dataset_info for easy access
            if metadata:
                dataset_info.update(metadata)
            
            # Store the full metadata separately
            dataset_info['metadata'] = metadata
            
            datasets.append(dataset_info)
        
        print(f"Found {len(datasets)} datasets from metadata store")
        return datasets
    except Exception as e:
        print(f"Error getting datasets list: {e}")
        import traceback
        traceback.print_exc()
        
        # Fallback if metadata store fails: try direct cache status
        try:
            print("Attempting fallback using cache.status()")
            status = cache.status()
            if 'entries' not in status:
                print("No entries in cache status")
                return []
                
            # Get keys
            keys = cache.get_keys()
            print(f"Found {len(keys)} keys")
            
            datasets = []
            for key in keys:
                try:
                    # Get sample to extract basic info
                    sample = cache.get(key, limit=5)
                    
                    # Try to get metadata
                    metadata = cache.get_metadata(key) or {}
                    
                    # Create dataset info with basic properties
                    dataset_info = {
                        'name': key,
                        'size_bytes': status.get('current_size_bytes', 0) // len(keys),  # Estimate
                        'size_mb': status.get('current_size_bytes', 0) / (1024*1024) / len(keys),
                        'row_count': 0,  # Will try to get accurate count below
                        'column_count': len(sample.column_names) if sample else 0,
                        'columns': list(sample.column_names) if sample else [],
                    }
                    
                    # Try to get accurate row count
                    try:
                        count_df = cache.query(f"SELECT COUNT(*) as row_count FROM _cache_{key}")
                        dataset_info['row_count'] = int(count_df['row_count'][0].as_py())
                    except Exception as count_e:
                        print(f"Error getting row count for {key}: {count_e}")
                    
                    # Add metadata
                    if metadata:
                        # Add metadata to dataset_info
                        if 'metadata' in metadata and isinstance(metadata['metadata'], dict):
                            dataset_info.update(metadata['metadata'])
                        
                        # Add size info from metadata if available
                        if 'size_bytes' in metadata:
                            dataset_info['size_bytes'] = metadata['size_bytes']
                            dataset_info['size_mb'] = metadata['size_bytes'] / (1024*1024)
                    
                    datasets.append(dataset_info)
                except Exception as e:
                    print(f"Error getting info for key {key}: {e}")
            
            return datasets
        except Exception as fallback_e:
            print(f"Fallback also failed: {fallback_e}")
            return []

def get_memory_usage():
    """Get current memory usage statistics directly from cache and memory manager."""
    # Get complete cache status which contains all the information we need
    status = cache.status()
    
    # The memory_manager in ArrowCache has detailed memory tracking
    memory_info = status.get('memory', {})
    
    # Get memory limit from config or status
    memory_limit = status.get('max_size_bytes', cache.config["memory_limit"])
    if memory_limit is None:
        # If still None, use system memory as reference
        import psutil
        memory_limit = int(psutil.virtual_memory().total * 0.8)  # 80% of system memory
    
    # Get current cache size 
    cache_size_bytes = status.get('current_size_bytes', 0)
    
    # Get process memory metrics
    process_rss = memory_info.get('process_rss', 0)
    
    # Estimate of application overhead (Streamlit, Python interpreter, etc.)
    app_overhead = min(process_rss, 200 * 1024 * 1024)  # Cap at process_rss or 200MB
    
    # Debug output
    print(f"Cache memory: {cache_size_bytes / (1024*1024):.2f} MB")
    print(f"Process memory: {process_rss / (1024*1024):.2f} MB")
    print(f"Estimated overhead: {app_overhead / (1024*1024):.2f} MB")
    
    # Return comprehensive memory stats
    memory_usage = {
        # Cache-specific metrics
        'cache_size_bytes': cache_size_bytes,
        'cache_size_mb': cache_size_bytes / (1024*1024),
        'memory_limit_bytes': memory_limit,
        'memory_limit_mb': memory_limit / (1024*1024),
        'cache_utilization_percent': (cache_size_bytes / memory_limit) * 100 if memory_limit > 0 else 0,
        'entry_count': status.get('entry_count', 0),
        
        # Process metrics
        'process_rss_bytes': process_rss,
        'process_rss_mb': process_rss / (1024*1024),
        'app_overhead_mb': app_overhead / (1024*1024),
        
        # System memory metrics from ArrowCache
        'system_memory_total': memory_info.get('system_memory_total', 0),
        'system_memory_available': memory_info.get('system_memory_available', 0),
        'system_memory_used_percent': memory_info.get('system_memory_percent', 0),
        
        # Arrow memory pool metrics if available
        'pool_bytes_allocated': memory_info.get('pool_bytes_allocated', 0),
        'pool_max_memory': memory_info.get('pool_max_memory', 0),
        
        # For backward compatibility
        'current_size_bytes': cache_size_bytes,
        'current_size_mb': cache_size_bytes / (1024*1024),
        'utilization_percent': (cache_size_bytes / memory_limit) * 100 if memory_limit > 0 else 0,
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
    error_occurred = False  # Track if any errors occurred
    
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
            # Use the query optimizer for better performance
            from arrow_cache.query_optimization import explain_query
            
            # --- Explicitly register tables before explaining ---
            table_refs = []
            try:
                # Extract table references using the optimizer's method
                table_refs = cache.query_optimizer._extract_table_references(query)
                if table_refs:
                    # Ensure these tables are registered before explaining
                    cache._ensure_tables_registered(table_refs)
                    logger.info(f"Ensured tables {table_refs} are registered before explaining.")
            except Exception as reg_err:
                logger.warning(f"Error ensuring tables registered before explain: {reg_err}")
                # Continue anyway, explain might still work or fail gracefully
            # --------------------------------------------------
            
            # Get query plan explanation for insights
            try:
                explanation = explain_query(cache.metadata_store.con, query)
                query_plan_info = explanation
            except Exception as explain_e:
                logger.warning(f"Error getting query plan explanation: {explain_e}") # Changed from print to logger.warning
                query_plan_info = f"Query plan explanation failed: {explain_e}" # Include error in info
            
            start_time = time.time()
            
            # Execute the query using cache's internal optimization
            # The optimizer will handle registration again, but it should be idempotent
            result_df = cache.query(query, optimize=True)
            query_time = time.time() - start_time
            
            # Convert Arrow table to pandas DataFrame for easier display
            # (PyArrow Tables don't have to_markdown method)
            if isinstance(result_df, pa.Table):
                pandas_df = result_df.to_pandas()
            else:
                pandas_df = result_df  # In case it's already a DataFrame
            
            # Store the results and query plan for potential followup
            results_data.append({
                "query": query, 
                "result_df": pandas_df,  # Store pandas DataFrame 
                "query_time": query_time,
                "query_plan": query_plan_info
            })
            
            # Format in a more streamlit-friendly way without HTML directly
            parts.append("\n\n**Query:**\n```sql\n")
            parts.append(query)
            parts.append("\n```\n\n")
            parts.append(f"*Query executed in {query_time:.3f}s*\n\n")
            parts.append("**Results:**\n\n")
            
            # Convert DataFrame to markdown table for better rendering
            # For smaller results, convert to markdown
            if len(pandas_df) < 10 and len(pandas_df.columns) < 8:
                parts.append(pandas_df.to_markdown(index=False))
            else:
                # For larger results, provide a message that the results will be shown below
                parts.append(f"*Showing {len(pandas_df)} rows with {len(pandas_df.columns)} columns. Full results displayed below.*\n\n")
                
                # Store the result DataFrame in session state to display separately
                st.session_state.last_claude_result = pandas_df
                
        except Exception as e:
            error_occurred = True
            error_message = str(e)
            
            # Provide more helpful error messages for common issues
            if "Table with name _cache_" in str(e) and "does not exist" in str(e):
                # Extract table name from error message
                import re
                table_name_match = re.search(r"_cache_(\w+)", str(e))
                if table_name_match:
                    table_name = table_name_match.group(1)
                    available_tables = cache.get_keys() 
                    error_message = f"Table '{table_name}' not found. Available tables: {', '.join(available_tables)}"
            
            # Format error without HTML
            parts.append("\n\n**Query:**\n```sql\n")
            parts.append(query)
            parts.append("\n```\n\n**Error:** ")
            parts.append(error_message)
            parts.append("\n\n")
            
            # Store error info for potential followup
            results_data.append({"query": query, "error": error_message, "query_time": 0})
            
        current_pos = query_end + 8  # Move past </query>
    
    return "".join(parts), executed_queries, results_data, error_occurred

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
    
    IMPORTANT QUERY INSTRUCTIONS:
    - When querying a dataset, ALWAYS use the FROM clause syntax: FROM _cache_<dataset_name>
    - ALWAYS verify the dataset name exists in the list above before using it in a query
    - The dataset name in _cache_<dataset_name> must EXACTLY match one of the dataset names listed above
    - If you're unsure if a dataset exists, ONLY use datasets explicitly listed above
    - Double-check column names before using them in queries
    
    {conversation_context}
    
    Your task is to help the user analyze these datasets by:
    1. Whenever possible, use the dataset metadata I've provided to answer simple questions without running a query
    2. For more complex analysis, generate appropriate SQL queries 
    3. Generate ONLY DuckDB-compatible SQL queries
    4. Include the query within <query> tags
    
    Important SQL syntax notes for DuckDB:
    - Tables are accessed with _cache_ prefix: FROM _cache_<dataset_name>
    - Case sensitivity: Table and column names are case-sensitive
    - WITH clauses must be at the beginning of the query
    - Each CTE must have a name followed by AS and then a subquery in parentheses
    - Multiple CTEs must be separated by commas
    - DuckDB supports standard SQL functions like SUM, AVG, COUNT, etc.
    - For dates, use functions like DATE_TRUNC, EXTRACT, etc.
    - Use single quotes for string literals: WHERE column = 'value'
    
    Example correct query:
    <query>
    SELECT 
      column1, 
      SUM(column2) AS total,
      AVG(column3) AS average
    FROM _cache_dataset_name
    WHERE column4 = 'value'
    GROUP BY column1
    ORDER BY total DESC
    LIMIT 10;
    </query>
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
        response_with_results, executed_queries, results_data, error_occurred = extract_and_run_queries(initial_response)
        
        # STEP 2: If queries were executed, ask Claude to interpret the results
        if results_data:
            # Prepare result information for Claude
            query_result_info = ""
            for idx, data in enumerate(results_data):
                query = data["query"]
                
                if "error" in data:
                    # Handle error case
                    error_message = data["error"]
                    query_result_info += f"\nQuery {idx+1}:\n{query}\n\nError: {error_message}\n"
                    
                else:
                    # Handle successful query
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
            
            I ran the SQL query you provided, and I need you to analyze the results.
            
            Rules for your response:
            1. Provide a clear, accurate interpretation based ONLY on the actual data in the query results
            2. Use precise values from the results - exact numbers, formats, and data types
            3. Highlight key patterns, trends, or notable outliers in the data
            4. Never make up values or predict data that isn't in the results
            5. Keep your analysis focused on what the data actually shows
            
            {f'''
            ERROR CORRECTION INSTRUCTIONS:
            The query had errors. Please:
            1. Identify the specific error in the original query
            2. Provide a corrected query that should work
            3. Explain the correction you made
            
            Common errors to check for:
            - Incorrect table name (check if the dataset exists exactly as referenced)
            - Column name errors (check spelling and case)
            - SQL syntax errors (check keywords, operators, quotes, etc.)
            - Type mismatches (e.g., comparing string to number)
            ''' if error_occurred else ''}
            
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
                    {"role": "user", "content": "Please interpret these query results accurately." + (" If there were SQL errors, please provide a corrected query." if error_occurred else "")}
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
            if error_occurred:
                history_entry["error_occurred"] = True
        
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
            # Get size information directly from dataset_info
            size_bytes = dataset.get('size_bytes', 0)
            
            # Convert to appropriate unit for display using our helper function
            size_display = get_size_display(size_bytes)
            
            # Format row count with commas for readability
            row_count = dataset.get('row_count', 'Unknown')
            if isinstance(row_count, int) or (isinstance(row_count, str) and row_count.isdigit()):
                row_count = f"{int(row_count):,}"
            
            # Get column information
            column_count = dataset.get('column_count', len(dataset.get('columns', [])))
            
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
            if 'columns' in dataset and dataset['columns']:
                # First try to get column types from metadata if available
                if 'dtypes' in dataset and isinstance(dataset['dtypes'], dict):
                    # Display columns with their types
                    col_type_pairs = []
                    for col in dataset['columns'][:20]:  # Limit to 20 columns for tooltip
                        dtype = dataset['dtypes'].get(col, '').replace('DataType', '')
                        col_type_pairs.append(f"{col} ({dtype})")
                    
                    if len(dataset['columns']) > 20:
                        col_type_pairs.append("... and more")
                        
                    columns_tooltip = "<br>".join(col_type_pairs)
                else:
                    # Just list column names
                    display_columns = dataset['columns'][:20]
                    if len(dataset['columns']) > 20:
                        display_columns.append("... and more")
                    columns_tooltip = "<br>".join(display_columns)
            
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

def get_size_display(size_bytes):
    """Convert size in bytes to human-readable format"""
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.2f} MB"
    else:
        return f"{size_bytes/(1024*1024*1024):.2f} GB"

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
    
    # Add CSS for better gauge display
    st.markdown("""
    <style>
    .memory-gauge h4 {
        margin-bottom: 5px;
        font-size: 0.9em;
        font-weight: bold;
    }
    .memory-gauge .container {
        width: 100%;
        background-color: #f0f0f0;
        border-radius: 4px;
        height: 10px;
        margin-bottom: 5px;
    }
    .memory-gauge .used {
        background-color: #4285F4;
        height: 100%;
        border-radius: 4px;
    }
    .memory-gauge p {
        font-size: 0.8em;
        margin: 0;
    }
    </style>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns(2)
    with col1:
        if st.button("Refresh Status"):
            st.rerun()
    with col2:
        if st.button("Force Cleanup", help="Clear cache files and restart"):
            try:
                import gc
                import time
                
                # Close the current cache first if it exists
                if 'cache' in locals() or 'cache' in globals():
                    with st.status("Closing cache..."):
                        # Get all keys
                        keys = cache.get_keys()
                        for key in keys:
                            try:
                                # Unregister from DuckDB
                                cache.metadata_store.unregister_table(key)
                                # Remove from cache
                                cache.remove(key)
                            except Exception as e:
                                print(f"Error removing dataset {key}: {e}")
                        
                        # Clear everything
                        cache.clear()
                        # Close the cache to release resources
                        cache.close()
                
                # Run garbage collection to release memory
                with st.status("Releasing memory..."):
                    # Run multiple garbage collection passes
                    for _ in range(3):
                        gc.collect()
                        time.sleep(0.5)
                        
                    # Import and call cleanup function with verbose output
                    from utils.cache_cleanup import clear_arrow_cache
                    clear_arrow_cache(spill_dir=SPILL_DIRECTORY, verbose=True)
                
                with st.status("Reinitializing cache..."):
                    # Clear the cached resource to force reinitializing the cache
                    st.cache_resource.clear()
                    
                st.success("Cache cleaned up successfully! Reloading...")
                time.sleep(1)  # Brief pause for UI feedback
                
                # Try to forcibly release more memory before reloading
                import sys
                try:
                    if hasattr(sys, "set_asyncgen_hooks"):
                        # Python 3.6+ - finalize async generators
                        # This helps release resources that might be held by async code
                        sys.set_asyncgen_hooks(firstiter=None, finalizer=None)
                except Exception:
                    pass
                
                # Rerun the app
                st.rerun()
            except Exception as e:
                st.error(f"Error during cleanup: {e}")
                import traceback
                st.code(traceback.format_exc())

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
                for col in sample.column_names: # Use .column_names here
                    dtype = str(sample[col].type) # Use .type instead of .dtype
                    # Fetch sample values correctly
                    try:
                        # Handle potential complexities in accessing values (e.g., nested types)
                        sample_vals_list = sample[col].slice(0, 3).to_pylist()
                        sample_values = ", ".join([str(v) for v in sample_vals_list])
                    except Exception as e:
                        print(f"Error getting sample values for column {col}: {e}")
                        sample_values = "[Error fetching]"

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