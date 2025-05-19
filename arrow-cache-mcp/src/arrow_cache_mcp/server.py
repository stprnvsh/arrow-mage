import asyncio
import json
import os
import re
import traceback
import pandas as pd
import numpy as np
import io
import tempfile
import time
import psutil
import glob
import logging
from typing import Dict, List, Optional, Union, Tuple, Any
from pathlib import Path
from urllib.parse import urlparse
import base64
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

# Configure logging
logger = logging.getLogger(__name__)

# Import our core modules
from arrow_cache_mcp.core import (
    get_arrow_cache, 
    clear_cache_files, 
    close_cache,
    remove_dataset,
    get_datasets_list,
    get_memory_usage
)

# Import data loading functions
from arrow_cache_mcp.loaders import (
    load_dataset_from_path,
    load_dataset_from_upload,
    load_dataset_from_url,
    guess_format_from_path,
    SUPPORTED_FORMATS
)

# Import visualization components
from arrow_cache_mcp.visualization import (
    create_plot,
    render_dataset_card,
    get_size_display
)

# Import AI interaction functions
from arrow_cache_mcp.ai import (
    get_ai_config,
    get_claude_api_key,
    get_ai_provider,
    ask_ai,
    ask_claude,
    extract_and_run_queries,
    display_conversation_history,
    _format_datasets_info,
    HAVE_ANTHROPIC
)

# Import utilities
from arrow_cache_mcp.utils import (
    clean_dataset_name
)

# ArrowCache for memory management
from arrow_cache import ArrowCache, ArrowCacheConfig

# MCP imports
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
from pydantic import AnyUrl
import mcp.server.stdio

# Constants
DEFAULT_MEMORY_LIMIT = 4 * 1024 * 1024 * 1024  # 4GB
DEFAULT_SPILL_DIRECTORY = ".arrow_cache_spill_mcp"
MAX_QUERY_RESULT_ROWS = 200
DEFAULT_PLOT_DPI = 100
SUPPORTED_FORMATS = ["csv", "parquet", "arrow", "feather", "json", "excel", "geojson", "geoparquet"]

class DataScienceSandbox:
    """
    A production-grade data science sandbox for MCP agents.
    
    Provides a memory-managed environment for working with multiple datasets 
    (pandas, geopandas, arrow, etc.) with SQL querying capabilities.
    """
    
    def __init__(self, 
                 memory_limit: int = DEFAULT_MEMORY_LIMIT, 
                 spill_directory: str = DEFAULT_SPILL_DIRECTORY,
                 spill_to_disk: bool = True):
        """
        Initialize the data science sandbox.
        
        Args:
            memory_limit: Maximum memory usage in bytes (default: 4GB)
            spill_directory: Directory for spilling data to disk when memory limit is reached
            spill_to_disk: Whether to allow spilling to disk
        """
        # Create spill directory if needed
        if spill_to_disk:
            os.makedirs(spill_directory, exist_ok=True)
            
        # Initialize ArrowCache for memory management
        self.config = ArrowCacheConfig(
            memory_limit=memory_limit,
            spill_to_disk=spill_to_disk,
            spill_directory=spill_directory,
            auto_partition=True  # Enable auto-partitioning for better performance
        )
        
        try:
            self.cache = ArrowCache(config=self.config)
            self.initialized = True
            print(f"DataScienceSandbox initialized with {memory_limit/(1024*1024*1024):.1f}GB memory limit")
            self.format_handlers = self._setup_format_handlers()
            
            # Track dataset metadata
            self.datasets_metadata = {}
            
            # Import optional dependencies
            self._import_optional_dependencies()
            
        except Exception as e:
            print(f"ERROR: Failed to initialize DataScienceSandbox: {e}")
            self.initialized = False
            self.cache = None
            
    def _import_optional_dependencies(self):
        """Import optional dependencies for additional functionality."""
        self.has_geopandas = False
        self.has_plotly = False
        
        try:
            import geopandas
            self.has_geopandas = True
            print("GeoPandas support enabled")
        except ImportError:
            print("GeoPandas not available. Geospatial features will be limited.")
            
        try:
            import plotly.express as px
            import plotly.graph_objects as go
            self.has_plotly = True
            self.px = px
            self.go = go
            print("Plotly support enabled")
        except ImportError:
            print("Plotly not available. Using Matplotlib for visualizations.")
    
    def _setup_format_handlers(self) -> Dict:
        """Set up handlers for different file formats."""
        handlers = {
            "csv": {
                "read": pd.read_csv,
                "write": lambda df, path: df.to_csv(path, index=False),
                "extensions": [".csv"],
            },
            "parquet": {
                "read": pd.read_parquet,
                "write": lambda df, path: df.to_parquet(path, index=False),
                "extensions": [".parquet", ".pq"],
            },
            "arrow": {
                "read": pd.read_feather,
                "write": lambda df, path: df.to_feather(path),
                "extensions": [".arrow", ".feather"],
            },
            "feather": {
                "read": pd.read_feather,
                "write": lambda df, path: df.to_feather(path),
                "extensions": [".feather"],
            },
            "json": {
                "read": lambda path: pd.read_json(path, orient="records"),
                "write": lambda df, path: df.to_json(path, orient="records", indent=2),
                "extensions": [".json"],
            },
            "excel": {
                "read": pd.read_excel,
                "write": lambda df, path: df.to_excel(path, index=False),
                "extensions": [".xlsx", ".xls"],
            },
        }
        
        # Add geopandas handlers if available
        if self.has_geopandas:
            import geopandas as gpd
            handlers.update({
                "geojson": {
                    "read": gpd.read_file,
                    "write": lambda df, path: df.to_file(path, driver="GeoJSON"),
                    "extensions": [".geojson"],
                },
                "geoparquet": {
                    "read": gpd.read_parquet,
                    "write": lambda df, path: df.to_parquet(path),
                    "extensions": [".geoparquet"],
                },
            })
        
        return handlers
    
    def guess_format(self, file_path: str) -> str:
        """Guess the format of a file based on its extension."""
        ext = os.path.splitext(file_path.lower())[1]
        for fmt, handler in self.format_handlers.items():
            if ext in handler["extensions"]:
                return fmt
        raise ValueError(f"Unsupported file extension: {ext}. Supported formats: {SUPPORTED_FORMATS}")
    
    def load_dataset(self, 
                    source: str, 
                    name: Optional[str] = None, 
                    format: Optional[str] = None, 
                    **kwargs) -> Dict:
        """
        Load a dataset into the sandbox.
        
        Args:
            source: Path or URL to the dataset
            name: Name to assign to the dataset (defaults to filename without extension)
            format: Format of the dataset (autodetected from extension if None)
            **kwargs: Additional arguments to pass to the reader function
            
        Returns:
            Dict with metadata about the loaded dataset
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
        
        # Determine dataset name if not provided
        if name is None:
            name = os.path.basename(source).split('.')[0]
            
        # Normalize name (remove spaces, special chars)
        name = re.sub(r'[^\w]', '_', name)
        
        # Check if dataset with this name already exists
        if self.cache.contains(name):
            raise ValueError(f"Dataset '{name}' already exists. Use a different name or remove the existing dataset first.")
        
        # Determine format if not provided
        if format is None:
            format = self.guess_format(source)
            
        if format not in self.format_handlers:
            raise ValueError(f"Unsupported format: {format}. Supported formats: {list(self.format_handlers.keys())}")
        
        # Check available memory before loading
        available_memory = self.get_available_memory()
        print(f"Available memory before loading: {available_memory/(1024*1024):.1f} MB")
        
        try:
            start_time = time.time()
            # Load the dataset using the appropriate handler
            reader_func = self.format_handlers[format]["read"]
            df = reader_func(source, **kwargs)
            load_time = time.time() - start_time
            
            # Get approximate dataframe size in memory
            df_size = df.memory_usage(deep=True).sum()
            
            # Check if we have enough memory
            if df_size > available_memory and not self.config.spill_to_disk:
                raise MemoryError(f"Dataset size ({df_size/(1024*1024):.1f} MB) exceeds available memory ({available_memory/(1024*1024):.1f} MB) and spill_to_disk is disabled")
            
            # Store in ArrowCache
            start_cache_time = time.time()
            
            # Add metadata
            metadata = {
                "source": source,
                "format": format,
                "load_time": load_time,
                "loaded_at": time.time(),
                "original_columns": list(df.columns),
                "row_count": len(df),
                "column_count": len(df.columns),
                "memory_usage_bytes": int(df_size),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()}
            }
            
            # Add geospatial metadata if it's a geodataframe
            if hasattr(df, 'crs') and df.crs is not None:
                metadata["is_geospatial"] = True
                metadata["crs"] = str(df.crs)
                if hasattr(df, 'geometry'):
                    metadata["geometry_column"] = "geometry"
            
            # Store in cache
            self.cache.put(name, df, metadata=metadata)
            cache_time = time.time() - start_cache_time
            
            # Save metadata
            self.datasets_metadata[name] = metadata
            
            # Update metadata with timing information
            metadata["cache_time"] = cache_time
            metadata["total_time"] = load_time + cache_time
            
            return metadata
            
        except Exception as e:
            error_msg = f"Failed to load dataset from {source}: {str(e)}"
            print(error_msg)
            traceback.print_exc()
            raise RuntimeError(error_msg)
    
    def get_dataset(self, name: str, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Retrieve a dataset from the sandbox.
        
        Args:
            name: Name of the dataset
            limit: Maximum number of rows to retrieve (None for all)
            
        Returns:
            DataFrame containing the dataset (or a subset if limit is specified)
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        if not self.cache.contains(name):
            raise ValueError(f"Dataset '{name}' not found. Available datasets: {self.list_datasets()}")
            
        return self.cache.get(name, limit=limit)
    
    def remove_dataset(self, name: str) -> bool:
        """
        Remove a dataset from the sandbox.
        
        Args:
            name: Name of the dataset to remove
            
        Returns:
            True if the dataset was removed, False otherwise
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        if not self.cache.contains(name):
            return False
            
        try:
            self.cache.remove(name)
            if name in self.datasets_metadata:
                del self.datasets_metadata[name]
            return True
        except Exception as e:
            print(f"Error removing dataset '{name}': {e}")
            return False
    
    def list_datasets(self) -> List[Dict]:
        """
        List all datasets in the sandbox with their metadata.
        
        Returns:
            List of dictionaries containing dataset information
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        results = []
        status = self.cache.status()
        
        for entry in status.get('entries', []):
            name = entry.get('name')
            
            # Get basic info from cache status
            dataset_info = {
                "name": name,
                "size_bytes": entry.get('size_bytes', 0),
                "size_mb": entry.get('size_bytes', 0) / (1024 * 1024),
                "created_at": entry.get('created_at'),
                "last_accessed": entry.get('last_accessed_at'),
            }
            
            # Add metadata if available
            if name in self.datasets_metadata:
                dataset_info.update(self.datasets_metadata[name])
                
            # If we don't have column info in metadata, try to get it
            if "original_columns" not in dataset_info:
                try:
                    sample = self.cache.get(name, limit=1)
                    dataset_info["original_columns"] = list(sample.columns)
                    dataset_info["column_count"] = len(sample.columns)
                except Exception:
                    pass
                    
            results.append(dataset_info)
            
        return results
    
    def query(self, sql_query: str) -> pd.DataFrame:
        """
        Execute a SQL query against the cached datasets.
        
        Args:
            sql_query: SQL query to execute (uses DuckDB syntax)
            
        Returns:
            DataFrame with the query results
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        try:
            return self.cache.query(sql_query)
        except Exception as e:
            raise ValueError(f"Query failed: {str(e)}")
    
    def get_dataset_sample(self, name: str, n: int = 5) -> pd.DataFrame:
        """
        Get a sample of rows from a dataset.
        
        Args:
            name: Name of the dataset
            n: Number of rows to retrieve
            
        Returns:
            DataFrame with sample rows
        """
        return self.get_dataset(name, limit=n)
    
    def get_dataset_info(self, name: str) -> Dict:
        """
        Get detailed information about a dataset.
        
        Args:
            name: Name of the dataset
            
        Returns:
            Dictionary with dataset metadata and statistics
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        if not self.cache.contains(name):
            raise ValueError(f"Dataset '{name}' not found")
            
        # Get basic info
        status = self.cache.status()
        entry_info = next((e for e in status.get('entries', []) if e['name'] == name), {})
        
        # Start with any existing metadata
        info = self.datasets_metadata.get(name, {}).copy()
        
        # Add status info
        info.update({
            "name": name,
            "size_bytes": entry_info.get('size_bytes', 0),
            "size_mb": entry_info.get('size_bytes', 0) / (1024 * 1024),
            "created_at": entry_info.get('created_at'),
            "last_accessed": entry_info.get('last_accessed_at'),
        })
        
        # Get statistics if not already present
        if "statistics" not in info:
            try:
                # Get sample for stats calculation
                sample = self.get_dataset(name, limit=1000)
                
                # Calculate statistics for numerical columns
                numeric_cols = sample.select_dtypes(include=[np.number]).columns.tolist()
                if numeric_cols:
                    # Use SQL for better performance with large datasets
                    stats_query = f"""
                    SELECT 
                        {', '.join([f'MIN({col}) as min_{col}, MAX({col}) as max_{col}, AVG({col}) as avg_{col}' 
                                  for col in numeric_cols])}
                    FROM _cache_{name}
                    """
                    stats_df = self.query(stats_query)
                    
                    # Format into dictionary
                    stats = {}
                    for col in numeric_cols:
                        stats[col] = {
                            "min": stats_df[f'min_{col}'].iloc[0],
                            "max": stats_df[f'max_{col}'].iloc[0],
                            "avg": stats_df[f'avg_{col}'].iloc[0],
                        }
                    
                    info["statistics"] = stats
            except Exception as e:
                print(f"Error calculating statistics for '{name}': {e}")
                
        return info
    
    def get_dataset_schema(self, name: str) -> Dict:
        """
        Get the schema of a dataset.
        
        Args:
            name: Name of the dataset
            
        Returns:
            Dictionary mapping column names to data types
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        if not self.cache.contains(name):
            raise ValueError(f"Dataset '{name}' not found")
            
        # Check if we have schema in metadata
        if name in self.datasets_metadata and "dtypes" in self.datasets_metadata[name]:
            return self.datasets_metadata[name]["dtypes"]
            
        # Otherwise, get sample and extract schema
        sample = self.get_dataset(name, limit=1)
        return {col: str(dtype) for col, dtype in sample.dtypes.items()}
    
    def get_memory_usage(self) -> Dict:
        """
        Get current memory usage of the sandbox.
        
        Returns:
            Dictionary with memory usage statistics
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        status = self.cache.status()
        process = psutil.Process(os.getpid())
        
        return {
            "cache_size_bytes": status.get('current_size_bytes', 0),
            "cache_size_mb": status.get('current_size_bytes', 0) / (1024 * 1024),
            "memory_limit_bytes": self.config.memory_limit,
            "memory_limit_mb": self.config.memory_limit / (1024 * 1024),
            "process_memory_usage_bytes": process.memory_info().rss,
            "process_memory_usage_mb": process.memory_info().rss / (1024 * 1024),
            "memory_utilization_percent": (status.get('current_size_bytes', 0) / self.config.memory_limit) * 100,
            "spill_to_disk_enabled": self.config.spill_to_disk,
            "spill_directory": self.config.spill_directory,
            "entry_count": status.get('entry_count', 0)
        }
    
    def get_available_memory(self) -> int:
        """
        Get available memory in the sandbox before hitting the limit.
        
        Returns:
            Available memory in bytes
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        status = self.cache.status()
        used_memory = status.get('current_size_bytes', 0)
        available = self.config.memory_limit - used_memory
        return max(0, available)
    
    def create_plot(self, 
                   dataset_name: str, 
                   x: str, 
                   y: Optional[str] = None, 
                   kind: str = 'line', 
                   **kwargs) -> Tuple[str, Dict]:
        """
        Create a plot from a dataset.
        
        Args:
            dataset_name: Name of the dataset
            x: Column name for x-axis
            y: Column name(s) for y-axis (can be a list for multiple lines)
            kind: Type of plot ('line', 'bar', 'scatter', 'hist', etc.)
            **kwargs: Additional arguments to pass to plotting function
            
        Returns:
            Tuple of (base64-encoded plot image, plot metadata)
        """
        if not self.initialized:
            raise RuntimeError("DataScienceSandbox not properly initialized")
            
        if not self.cache.contains(dataset_name):
            raise ValueError(f"Dataset '{dataset_name}' not found")
            
        # Limit data for plotting to prevent memory issues
        df = self.get_dataset(dataset_name, limit=10000)
        
        # Check columns exist
        if x not in df.columns:
            raise ValueError(f"Column '{x}' not found in dataset '{dataset_name}'")
            
        if y is not None and isinstance(y, str) and y not in df.columns:
            raise ValueError(f"Column '{y}' not found in dataset '{dataset_name}'")
            
        if y is not None and isinstance(y, list):
            for col in y:
                if col not in df.columns:
                    raise ValueError(f"Column '{col}' not found in dataset '{dataset_name}'")
        
        # Create plot
        plt.figure(figsize=kwargs.get('figsize', (10, 6)))
        
        if self.has_plotly and kwargs.get('use_plotly', False):
            # Use plotly for interactive plots if available
            try:
                if kind == 'line':
                    fig = self.px.line(df, x=x, y=y, **kwargs)
                elif kind == 'bar':
                    fig = self.px.bar(df, x=x, y=y, **kwargs)
                elif kind == 'scatter':
                    fig = self.px.scatter(df, x=x, y=y, **kwargs)
                elif kind == 'hist':
                    fig = self.px.histogram(df, x=x, **kwargs)
                else:
                    raise ValueError(f"Unsupported plot kind for Plotly: {kind}")
                    
                # Convert to image
                img_bytes = fig.to_image(format="png", scale=2)
                base64_img = base64.b64encode(img_bytes).decode('utf-8')
                
            except Exception as e:
                print(f"Plotly error: {e}. Falling back to Matplotlib.")
                # Fall back to matplotlib
                plot_func = getattr(df.plot, kind)
                ax = plot_func(x=x, y=y, **kwargs)
                fig = ax.get_figure()
                
                # Save to buffer
                buf = io.BytesIO()
                fig.savefig(buf, format='png', dpi=kwargs.get('dpi', DEFAULT_PLOT_DPI))
                buf.seek(0)
                base64_img = base64.b64encode(buf.read()).decode('utf-8')
                plt.close(fig)
        else:
            # Use matplotlib
            plot_func = getattr(df.plot, kind)
            ax = plot_func(x=x, y=y, **kwargs)
            fig = ax.get_figure()
            
            # Save to buffer
            buf = io.BytesIO()
            fig.savefig(buf, format='png', dpi=kwargs.get('dpi', DEFAULT_PLOT_DPI))
            buf.seek(0)
            base64_img = base64.b64encode(buf.read()).decode('utf-8')
            plt.close(fig)
        
        # Return base64 image and metadata
        metadata = {
            "dataset": dataset_name,
            "x": x,
            "y": y,
            "kind": kind,
            "rows_used": len(df),
            "created_at": time.time()
        }
        
        return base64_img, metadata
    
    def close(self):
        """Close the sandbox and free resources."""
        if self.initialized and self.cache:
            try:
                self.cache.close()
                print("DataScienceSandbox closed.")
            except Exception as e:
                print(f"Error closing DataScienceSandbox: {e}")

# --- Initialize the MCP server ---
server = Server("arrow-cache-mcp")

# Create a singleton instance of DataScienceSandbox
# Get configuration from environment variables
def load_config_from_env():
    memory_limit = int(os.environ.get("ARROW_CACHE_MEMORY_LIMIT", DEFAULT_MEMORY_LIMIT))
    spill_directory = os.environ.get("ARROW_CACHE_SPILL_DIRECTORY", DEFAULT_SPILL_DIRECTORY)
    spill_to_disk = os.environ.get("ARROW_CACHE_SPILL_TO_DISK", "true").lower() == "true"
    
    return {
        "memory_limit": memory_limit,
        "spill_directory": spill_directory,
        "spill_to_disk": spill_to_disk
    }

# Create sandbox instance
sandbox_config = load_config_from_env()
sandbox = DataScienceSandbox(**sandbox_config)

# --- MCP server implementation ---
@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    """
    List available datasets stored in the sandbox.
    Each dataset is exposed as a resource with an arrowcache:/// URI scheme.
    """
    if not sandbox.initialized:
        print("Warning: list_resources called but sandbox is not initialized.")
        return []
    
    try:
        datasets = sandbox.list_datasets()
        
        resources = []
        for dataset in datasets:
            name = dataset["name"]
            size_mb = dataset.get("size_mb", 0)
            row_count = dataset.get("row_count", "N/A")
            
            description = f"Dataset: {name} ({row_count} rows, {size_mb:.2f} MB)"
            if dataset.get("is_geospatial"):
                description += " [Geospatial]"
                
            resources.append(
                types.Resource(
                    uri=AnyUrl(f"arrowcache:///{name}"),
                    name=name,
                    description=description,
                    mimeType="application/json",
                )
            )
        
        # Add a special resource for memory usage info
        memory_usage = sandbox.get_memory_usage()
        memory_desc = f"Memory usage: {memory_usage['memory_utilization_percent']:.1f}% of {memory_usage['memory_limit_mb']:.0f} MB"
        
        resources.append(
            types.Resource(
                uri=AnyUrl("arrowcache:///system/memory"),
                name="Memory Status",
                description=memory_desc,
                mimeType="application/json",
            )
        )
        
        return resources
    except Exception as e:
        print(f"Error listing resources: {e}")
        traceback.print_exc()
        return []

@server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    """
    Read detailed metadata (schema, row count, etc.) for a specific dataset or system resource.
    Returns metadata as a JSON string.
    """
    if not sandbox.initialized:
        raise ValueError("Sandbox not initialized")
    
    if uri.scheme != "arrowcache":
        raise ValueError(f"Unsupported URI scheme: {uri.scheme}")
    
    path = uri.path.lstrip("/")
    
    # Special system resources
    if path.startswith("system/"):
        if path == "system/memory":
            memory_info = sandbox.get_memory_usage()
            return json.dumps(memory_info, indent=2)
        else:
            raise ValueError(f"Unknown system resource: {path}")
    
    # Regular dataset resources
    try:
        dataset_info = sandbox.get_dataset_info(path)
        return json.dumps(dataset_info, indent=2)
    except Exception as e:
        print(f"Error reading resource metadata for '{path}': {e}")
        traceback.print_exc()
        raise ValueError(f"Could not retrieve metadata for dataset '{path}': {str(e)}")

@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """
    List available tools in the sandbox.
    """
    tools = [
        types.Tool(
            name="run_sql_query",
            description="Execute a DuckDB SQL query against cached datasets. Use _cache_<dataset_name> syntax in FROM clause. always run INSTALL spatial; LOAD spatial; before your main query if you are using spatial functions. Always read the metadata for the dataset to understand columns and storage format.",
            inputSchema={
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "The DuckDB SQL query to execute."},
                },
                "required": ["query"],
            },
        ),
        types.Tool(
            name="load_dataset",
            description="Load a dataset from a file or URL into the sandbox.",
            inputSchema={
                "type": "object",
                "properties": {
                    "source": {"type": "string", "description": "Path or URL to the dataset file"},
                    "name": {"type": "string", "description": "Name to assign to the dataset (defaults to filename)"},
                    "format": {"type": "string", "description": f"Format of the dataset (one of: {', '.join(SUPPORTED_FORMATS)})"},
                },
                "required": ["source"],
            },
        ),
        types.Tool(
            name="get_dataset_sample",
            description="Get a sample of rows from a dataset.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Name of the dataset"},
                    "n": {"type": "integer", "description": "Number of rows to sample (default: 5)"},
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="get_dataset_info",
            description="Get detailed information about a dataset, including schema and statistics.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Name of the dataset"},
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="remove_dataset",
            description="Remove a dataset from the sandbox.",
            inputSchema={
                "type": "object",
                "properties": {
                    "name": {"type": "string", "description": "Name of the dataset to remove"},
                },
                "required": ["name"],
            },
        ),
        types.Tool(
            name="create_plot",
            description="Create a plot from a dataset and return a base64-encoded image.",
            inputSchema={
                "type": "object",
                "properties": {
                    "dataset_name": {"type": "string", "description": "Name of the dataset"},
                    "x": {"type": "string", "description": "Column name for x-axis"},
                    "y": {"type": ["string", "array"], "description": "Column name(s) for y-axis"},
                    "kind": {"type": "string", "description": "Type of plot (line, bar, scatter, hist, etc.)"},
                    "title": {"type": "string", "description": "Title for the plot"},
                },
                "required": ["dataset_name", "x", "kind"],
            },
        ),
        types.Tool(
            name="get_memory_usage",
            description="Get current memory usage statistics for the sandbox.",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
    ]

    # Add the new Ask Data Analyst tool
    tools.append(
        types.Tool(
            name="ask_data_analyst",
            description=(
                "Asks an AI data analyst a question about the loaded datasets. "
                "The analyst can run SQL queries (using DuckDB syntax and referencing tables as _cache_datasetname) "
                "and potentially generate visualizations based on the question and data. "
                "Supports multiple AI providers including Anthropic's Claude and OpenAI models."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The question to ask the data analyst."
                    },
                },
                "required": ["question"],
            }
        )
    )

    return tools

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Handle tool calls from the client."""
    logger.info(f"Received tool call: {name} with args: {arguments}")
    
    # Ensure cache is initialized (it should be by the time tools are called)
    try:
        get_arrow_cache()
    except Exception as e:
        logger.error(f"Arrow Cache not initialized when handling tool call {name}: {e}")
        return [types.TextContent(text=f"Error: Cache not available - {e}")]

    # Dispatch to the appropriate handler based on tool name
    if name == "run_sql_query":
        return await handle_run_sql_query(arguments)
    elif name == "load_dataset":
        return await handle_load_dataset(arguments)
    elif name == "get_dataset_sample":
        return await handle_get_dataset_sample(arguments)
    elif name == "get_dataset_info":
        return await handle_get_dataset_info(arguments)
    elif name == "remove_dataset":
        return await handle_remove_dataset(arguments)
    elif name == "create_plot":
        return await handle_create_plot(arguments)
    elif name == "get_memory_usage":
        return await handle_get_memory_usage(arguments)
    elif name == "ask_data_analyst":
        # Calling the restored full version
        return await handle_ask_data_analyst(arguments)
    else:
        return [
            types.TextContent(text=f"Error: Unknown tool name '{name}'.")
        ]

# --- Tool Handler Implementations ---

async def handle_run_sql_query(arguments: dict | None) -> list[types.TextContent]:
    """Handles the 'run_sql_query' tool call."""
    if not arguments or 'query' not in arguments:
        return [types.TextContent(text="Error: 'query' argument is required.")]

    query = arguments['query']
    logger.info(f"Executing SQL query: {query}")
    cache = get_arrow_cache()

    try:
        start_time = time.time()
        result_df = cache.query(query, optimize=True)
        query_time = time.time() - start_time

        # Convert to pandas for consistent handling
        if hasattr(result_df, 'to_pandas'):
            pandas_df = result_df.to_pandas()
        else:
            pandas_df = result_df 

        logger.info(f"Query successful. Rows: {len(pandas_df)}, Time: {query_time:.3f}s")

        # Limit rows for display
        if len(pandas_df) > MAX_QUERY_RESULT_ROWS:
            result_text = f"Query returned {len(pandas_df)} rows (displaying first {MAX_QUERY_RESULT_ROWS}):\n\n"
            result_text += pandas_df.head(MAX_QUERY_RESULT_ROWS).to_markdown(index=False)
        else:
            result_text = f"Query returned {len(pandas_df)} rows:\n\n"
            result_text += pandas_df.to_markdown(index=False)
            
        result_text += f"\n\n*Query executed in {query_time:.3f}s*"
        return [types.TextContent(text=result_text)]

    except Exception as e:
        logger.error(f"Error executing query '{query}': {e}")
        return [types.TextContent(text=f"Error executing query: {e}")]

async def handle_load_dataset(arguments: dict | None) -> list[types.TextContent]:
    """Handles the 'load_dataset' tool call."""
    if not arguments:
        return [types.TextContent(text="Error: No arguments provided for load_dataset.")]

    source = arguments.get('source')
    name = arguments.get('name')
    format = arguments.get('format')
    url = arguments.get('url')

    # Determine source type (path, upload, url)
    load_func = None
    source_desc = ""
    if source: # Treat source as path first
        source_desc = f"path '{source}'"
        load_func = load_dataset_from_path
        load_args = {'file_path': source, 'dataset_name': name, 'file_format': format}
    elif url: # Then check URL
        source_desc = f"URL '{url}'"
        load_func = load_dataset_from_url
        load_args = {'url': url, 'dataset_name': name, 'file_format': format}
    # Simplified: Assume upload handling happens elsewhere or is not the primary focus here
    # elif 'upload_token' in arguments? Requires more context on MCP uploads.
    else:
        return [types.TextContent(text="Error: Missing required argument: 'source' (file path) or 'url'.")]

    logger.info(f"Attempting to load dataset '{name or 'auto-named'}' from {source_desc}")

    try:
        # Use the appropriate loading function from loaders.py
        success, message, loaded_name, metadata = await load_func(**load_args) # Use await if load funcs are async
        
        if success:
            logger.info(f"Successfully loaded dataset '{loaded_name}'")
            # Render a card with metadata
            card_html = render_dataset_card(metadata) 
            # Return success message and card
            return [
                types.TextContent(text=message),
                types.EmbeddedResource(mime_type="text/html", data=card_html)
            ]
        else:
            logger.error(f"Failed to load dataset from {source_desc}: {message}")
            return [types.TextContent(text=f"Error loading dataset: {message}")]

    except Exception as e:
        logger.exception(f"Unexpected error loading dataset from {source_desc}: {e}")
        return [types.TextContent(text=f"Unexpected error loading dataset: {e}")]

async def handle_get_dataset_sample(arguments: dict | None) -> list[types.TextContent]:
    """Handles the 'get_dataset_sample' tool call."""
    if not arguments or 'dataset' not in arguments:
         return [types.TextContent(text="Error: 'dataset' argument is required.")]

    dataset_name = arguments['dataset']
    n_rows = arguments.get('n', 5) # Default to 5 rows
    logger.info(f"Getting sample ({n_rows} rows) for dataset: {dataset_name}")
    cache = get_arrow_cache()

    if not cache.contains(dataset_name):
        return [types.TextContent(text=f"Error: Dataset '{dataset_name}' not found.")]

    try:
        # Use cache.get() for efficient sampling
        sample_data = cache.get(dataset_name, limit=n_rows)
        
        # Convert to pandas for display
        if hasattr(sample_data, 'to_pandas'):
            pandas_df = sample_data.to_pandas()
        else:
            pandas_df = sample_data
        
        response_text = f"Sample ({len(pandas_df)} rows) from dataset '{dataset_name}':\n\n"
        response_text += pandas_df.to_markdown(index=False)
        return [types.TextContent(text=response_text)]

    except Exception as e:
        logger.error(f"Error getting sample for '{dataset_name}': {e}")
        return [types.TextContent(text=f"Error getting sample: {e}")]

async def handle_get_dataset_info(arguments: dict | None) -> list[types.TextContent | types.EmbeddedResource]:
    """Handles the 'get_dataset_info' tool call. Returns metadata card."""
    if not arguments or 'dataset' not in arguments:
         return [types.TextContent(text="Error: 'dataset' argument is required.")]

    dataset_name = arguments['dataset']
    logger.info(f"Getting info for dataset: {dataset_name}")
    cache = get_arrow_cache()
    
    # Use get_datasets_list and filter for the specific dataset
    all_datasets = get_datasets_list() 
    dataset_info = next((ds for ds in all_datasets if ds['name'] == dataset_name), None)

    if not dataset_info:
        return [types.TextContent(text=f"Error: Dataset '{dataset_name}' not found.")]

    try:
        # Render the dataset card using the info
        card_html = render_dataset_card(dataset_info)
        # Return the card as embedded HTML
        return [types.EmbeddedResource(mime_type="text/html", data=card_html)]

    except Exception as e:
        logger.error(f"Error generating info card for '{dataset_name}': {e}")
        # Fallback to text description if card fails
        info_text = json.dumps(dataset_info, indent=2, default=str) # Use default=str for non-serializable types
        return [types.TextContent(text=f"Dataset Info for '{dataset_name}':\n\n{info_text}\n\n(Error generating card: {e})")]

async def handle_remove_dataset(arguments: dict | None) -> list[types.TextContent]:
    """Handles the 'remove_dataset' tool call."""
    if not arguments or 'dataset' not in arguments:
        return [types.TextContent(text="Error: 'dataset' argument is required.")]

    dataset_name = arguments['dataset']
    logger.info(f"Attempting to remove dataset: {dataset_name}")
    
    # Use the core remove_dataset function
    success, message = remove_dataset(dataset_name)
    
    if success:
        logger.info(f"Dataset '{dataset_name}' removed successfully.")
    else:
        logger.warning(f"Failed to remove dataset '{dataset_name}': {message}")
        
    # Return the message from the core function
    return [types.TextContent(text=message)]


# --- Internal Plotting Logic ---
async def _create_plot_internal(
    dataset_name: str, 
    geometry_col: Optional[str] = None,
    x: Optional[str] = None, 
    y: Optional[str] = None, 
    kind: str = 'map', # Default to map if geometry is provided
    **kwargs
) -> Tuple[Optional[bytes], Optional[str]]:
    """Internal logic to generate plot bytes and format.
    
    Returns:
        Tuple of (image_bytes, error_message)
    """
    logger.info(f"Initiating internal plot creation for {dataset_name}. Kind: {kind}")
    cache = get_arrow_cache()

    if not cache.contains(dataset_name):
        return None, f"Dataset '{dataset_name}' not found."

    # Check if required columns exist
    try:
        # Determine required columns for the query
        required_cols = []
        if geometry_col: required_cols.append(geometry_col)
        if x: required_cols.append(x)
        if y: required_cols.append(y)

        # Use cache.query for robust schema checking if columns are needed
        available_columns = []
        if required_cols:
            try:
                schema_query = f"SELECT * FROM _cache_{dataset_name} LIMIT 0;"
                schema_df = cache.query(schema_query).to_pandas() # Get schema via empty query
                available_columns = schema_df.columns.tolist()
                logger.info(f"Dataset {dataset_name} columns: {available_columns}")
            except Exception as schema_e:
                logger.warning(f"Could not get exact schema for {dataset_name}: {schema_e}. Plotting may fail if columns are missing.")
                # Attempt to proceed, checks below will handle missing cols error from query
        else: # No specific columns needed (e.g., just mapping geometry)
            logger.info("No specific columns requested beyond geometry, skipping detailed schema check.")

        # Check only if we could get the schema
        if available_columns:
            missing_cols = [col for col in required_cols if col not in available_columns]
            if missing_cols:
                return None, f"Missing required columns in dataset '{dataset_name}': {', '.join(missing_cols)}. Available: {', '.join(available_columns)}"
        
        # Specific check for geometry column existence if provided
        if geometry_col and available_columns and geometry_col not in available_columns:
             return None, f"Geometry column '{geometry_col}' not found in dataset '{dataset_name}'. Available: {', '.join(available_columns)}"

    except Exception as e:
        logger.error(f"Error checking schema/columns for {dataset_name}: {e}")
        return None, f"Error accessing schema for dataset '{dataset_name}': {e}"

    # Determine plot kind if geometry is involved
    if geometry_col and kind not in ['map', 'scatter']: # Allow scatter for points
        kind = 'map' # Force to map if geometry provided
        logger.info(f"Geometry column '{geometry_col}' provided. Setting plot kind to 'map'.")

    try:
        # Import the visualization create_plot function
        from .visualization import create_plot
        from matplotlib.figure import Figure
        
        # Pass the dataset name directly to create_plot, which will handle data loading
        logger.info(f"Calling create_plot with dataset_name: {dataset_name}")
        fig = create_plot(
            data=dataset_name,  # Pass the dataset name as a string
            x=x, 
            y=y, 
            geometry_col=geometry_col, 
            kind=kind, 
            title=f"{kind.capitalize()} plot for {dataset_name}",
            **kwargs
        )

        if fig is None:
             return None, "Plot generation failed (returned None)."

        # Save plot to bytes buffer
        buf = io.BytesIO()
        # Check if it's a matplotlib figure
        if isinstance(fig, Figure):
            logger.info("Saving Matplotlib figure to buffer.")
            fig.savefig(buf, format='png', dpi=DEFAULT_PLOT_DPI, bbox_inches='tight')
            plt.close(fig) # Close the figure to free memory
        else:
             # Assume it's plotly or other object with write_image
             try:
                 logger.info("Saving Plotly figure to buffer.")
                 fig.write_image(buf, format='png')
             except Exception as write_err:
                 logger.error(f"Failed to save non-matplotlib figure: {write_err}")
                 return None, f"Unsupported figure type or error saving: {write_err}"

        buf.seek(0)
        image_bytes = buf.read()
        logger.info(f"Plot generated successfully ({len(image_bytes)} bytes).")
        return image_bytes, None

    except ImportError as ie:
         logger.error(f"Plotting dependencies missing: {ie}")
         return None, f"Plotting library error: {ie}. GeoPandas and Matplotlib might be required."
    except Exception as e:
        logger.exception(f"Error generating plot for {dataset_name}: {e}") # Log full traceback
        return None, f"Error creating plot: {traceback.format_exc()}"

# --- Tool Handlers ---

async def handle_create_plot(arguments: dict | None) -> list[types.ImageContent | types.TextContent]:
    """Handles the 'create_plot' tool call."""
    if not arguments:
        return [types.TextContent(text="Error: No arguments provided for create_plot.")]

    logger.info(f"Handling create_plot tool call with arguments: {arguments}")
    
    dataset_name = arguments.get('dataset')
    if not dataset_name:
        return [types.TextContent(text="Error: 'dataset' argument is required for create_plot.")]

    # Extract arguments, providing defaults
    geometry_col = arguments.get('geometry_col')
    x_col = arguments.get('x')
    y_col = arguments.get('y')
    kind = arguments.get('kind', 'map' if geometry_col else 'line') # Default based on geometry

    # Pass other arguments through
    plot_kwargs = {k: v for k, v in arguments.items() if k not in ['dataset', 'geometry_col', 'x', 'y', 'kind']}

    # Use the internal plotting function
    image_bytes, error_message = await _create_plot_internal(
        dataset_name=dataset_name,
        geometry_col=geometry_col,
        x=x_col,
        y=y_col,
        kind=kind,
        **plot_kwargs
    )

    if error_message:
        logger.error(f"Plot creation failed for tool call: {error_message}")
        return [types.TextContent(text=f"Error creating plot: {error_message}")]
    
    if image_bytes:
        logger.info("Returning plot image content.")
        b64_image = base64.b64encode(image_bytes).decode('utf-8')
        return [types.ImageContent(source=types.ImageContentSource(media_type="image/png", data=b64_image))]
    else:
        # Should not happen if error_message is None, but handle defensively
        logger.warning("Plot creation returned no bytes and no error.")
        return [types.TextContent(text="Plot generation failed for an unknown reason.")]


# --- Restore the full handler for ask_data_analyst ---
async def handle_ask_data_analyst(arguments: dict | None) -> list[types.TextContent | types.ImageContent]:
    """Handles the 'ask_data_analyst' tool call.
    
    Calls an AI provider, executes queries, handles visualization requests.
    """
    if not arguments or 'question' not in arguments:
        return [types.TextContent(text="Error: 'question' argument is required.")]
        
    question = arguments['question']
    logger.info(f"Handling ask_data_analyst tool call. Question: {question[:100]}...")
    
    # Get AI configuration
    ai_config = get_ai_config()
    provider_name = ai_config["provider"]
    api_key = ai_config["api_key"]
    
    if not api_key:
        logger.error(f"API key not found for {provider_name}")
        return [types.TextContent(text=f"Error: API key not configured for {provider_name}.")]
        
    # Get an AI provider instance
    ai_provider = get_ai_provider()
    if not ai_provider:
        logger.error(f"Could not initialize {provider_name} provider")
        return [types.TextContent(text=f"Error: Could not initialize {provider_name} provider.")]

    # Log API provider information for debugging
    logger.info(f"Using AI provider: {provider_name}, model: {ai_provider.model}")
    
    # Test API connectivity
    connection_success, connection_message = ai_provider.test_connection()
    if not connection_success:
        logger.warning(f"API connectivity test failed: {connection_message}")
        logger.info("Will attempt API call anyway, as the test may be unreliable")
    else:
        logger.info(f"API connectivity test successful: {connection_message}")

    conversation_history = []

    try:
        # Call the AI function from ai.py
        response_text = ask_ai(
            question=question,
            api_key=api_key,
            provider=provider_name,
            conversation_history=conversation_history,
            max_retries=1 # Use default retry
        )
        
        logger.info(f"AI response received (length: {len(response_text)}). Checking for visualization tag.")
        
        # --- Check for visualization request tag --- 
        # Regex to find the tag and capture dataset and geometry_col
        viz_regex_patterns = [
            # Pattern with quotes
            r'<visualize\s+dataset=[\'"]([^\'"]+)[\'"]\s+geometry_col=[\'"]([^\'"]+)[\'"]\s*\/?>',
            # Alternative pattern without quotes
            r'<visualize\s+dataset=([^\s>]+)\s+geometry_col=([^\s>]+)\s*\/?>'
        ]
        
        viz_match = None
        for pattern in viz_regex_patterns:
            match = re.search(pattern, response_text, re.IGNORECASE | re.DOTALL)
            if match:
                viz_match = match
                break
        
        final_text_content = response_text
        image_content = None

        if viz_match:
            logger.info("Visualization tag found in response.")
            dataset_to_visualize = viz_match.group(1)
            geometry_col_to_visualize = viz_match.group(2)
            
            if not dataset_to_visualize or not geometry_col_to_visualize:
                logger.warning(f"Visualization tag found but attributes missing/empty: {viz_match.group(0)}")
                # Keep the tag in the text as it's malformed or incomplete
            else:
                logger.info(f"Extracted viz request: dataset='{dataset_to_visualize}', geometry='{geometry_col_to_visualize}'")
                
                # Remove the tag from the text response (handle potential surrounding whitespace)
                pre_tag = response_text[:viz_match.start()].rstrip()
                post_tag = response_text[viz_match.end():].lstrip()
                final_text_content = pre_tag + ("\n\n" if pre_tag and post_tag else "") + post_tag # Add spacing if needed
                
                # Call the internal plotting logic
                logger.info(f"Calling internal plot function for '{dataset_to_visualize}'")
                image_bytes, error_message = await _create_plot_internal(
                    dataset_name=dataset_to_visualize,
                    geometry_col=geometry_col_to_visualize,
                    kind='map' # Assume map for visualize tag
                )
                
                if error_message:
                    logger.error(f"Visualization generation failed: {error_message}")
                    # Append error to the text response
                    final_text_content += f"\n\n**Visualization Error:** Failed to generate map for '{dataset_to_visualize}': {error_message}"
                elif image_bytes:
                    logger.info("Visualization generated successfully.")
                    b64_image = base64.b64encode(image_bytes).decode('utf-8')
                    image_content = types.ImageContent(source=types.ImageContentSource(media_type="image/png", data=b64_image))
                else:
                    logger.warning("Internal plot function returned no image and no error for visualization request.")
                    final_text_content += f"\n\n**Visualization Note:** Could not generate map for '{dataset_to_visualize}'."
        else:
            logger.info("No visualization tag found.")
            
        # Prepare results
        results: list[types.TextContent | types.ImageContent] = []
        # Ensure final_text_content is not empty before adding
        if final_text_content:
            results.append(types.TextContent(text=final_text_content))
        if image_content:
            results.append(image_content)
            
        # Handle case where both text and image might be empty (e.g., error during initial Claude call before tag parsing)
        if not results:
            logger.warning("handle_ask_data_analyst resulted in no content being returned.")
            results.append(types.TextContent(text="(No response generated)"))
            
        return results

    except Exception as e:
        logger.exception(f"Error in handle_ask_data_analyst: {e}")
        return [types.TextContent(text=f"An unexpected error occurred: {e}")]

async def handle_get_memory_usage(arguments: dict | None) -> list[types.TextContent]:
    """Handles the 'get_memory_usage' tool call."""
    try:
        # Use the core get_memory_usage function directly
        memory_info = get_memory_usage() 
        # Format the dictionary as a JSON string for display
        response_text = json.dumps(memory_info, indent=2, default=str) # Add default=str
        return [types.TextContent(text=response_text)]
    except Exception as e:
        logger.exception(f"Error getting memory usage: {e}")
        return [types.TextContent(text=f"Error getting memory usage: {e}")]

async def main():
    # Initialize Arrow Cache on startup
    logger.info("Initializing Arrow Cache for server...")
    try:
        get_arrow_cache() # Call core function to initialize
        logger.info("Arrow Cache initialized successfully.")
    except Exception as e:
        logger.error(f"FATAL: Failed to initialize Arrow Cache: {e}")
        logger.error("Server cannot start without Arrow Cache.")
        return # Exit if cache fails to initialize

    # Run the server using stdin/stdout streams
    logger.info("Starting MCP server...")
    try:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="arrow-cache-mcp",
                    server_version="0.2.0", # Consider updating version
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )
    except Exception as e:
        logger.exception(f"MCP server run failed: {e}")
    finally:
        # Cache cleanup is handled by atexit registered in core.py
        logger.info("MCP server stopped.")

# Add entry point for running the server directly
if __name__ == "__main__":
    # Setup basic logging for server startup
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger.info("Running server main...")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Server interrupted by user.")
    except Exception as e:
        logger.exception("Unhandled exception during server execution.")

# Remove the potentially problematic atexit registration here,
# rely on the one in core.py
# import atexit
# atexit.register(lambda: sandbox.close() if sandbox.initialized else None)