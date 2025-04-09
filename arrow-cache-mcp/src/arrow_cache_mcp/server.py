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
from typing import Dict, List, Optional, Union, Tuple, Any
from pathlib import Path
from urllib.parse import urlparse
import base64
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

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
    return [
        types.Tool(
            name="run_sql_query",
            description="Execute a DuckDB SQL query against cached datasets. Use _cache_<dataset_name> syntax in FROM clause.",
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

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """
    Handle tool execution requests.
    Returns results or errors as structured content.
    """
    if not sandbox.initialized:
        return [types.TextContent(type="text", text=json.dumps({"error": "Sandbox not initialized"}))]
    
    try:
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
        else:
            return [types.TextContent(type="text", text=json.dumps({"error": f"Unknown tool: {name}"}))]
    except Exception as e:
        print(f"Error in handle_call_tool for {name}: {e}")
        traceback.print_exc()
        error_payload = {
            "status": "error",
            "tool": name,
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def handle_run_sql_query(arguments: dict | None) -> list[types.TextContent]:
    """Handle the run_sql_query tool."""
    if not arguments or "query" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing 'query' argument"}))]
    
    sql_query = arguments["query"]
    
    try:
        start_time = time.time()
        result_df = await asyncio.to_thread(sandbox.query, sql_query)
        query_time = time.time() - start_time
        
        num_rows = len(result_df)
        truncated = num_rows > MAX_QUERY_RESULT_ROWS
        
        if truncated:
            result_df = result_df.head(MAX_QUERY_RESULT_ROWS)
        
        # For very wide dataframes, limit columns for display
        if len(result_df.columns) > 20:
            visible_df = result_df.iloc[:, :20].copy()
            visible_df['...'] = '...'
            result_json_str = visible_df.to_json(orient="records", indent=2, date_format='iso')
            truncated_cols = True
        else:
            result_json_str = result_df.to_json(orient="records", indent=2, date_format='iso')
            truncated_cols = False
        
        response_payload = {
            "status": "success",
            "query": sql_query,
            "execution_time_seconds": query_time,
            "rowCount": num_rows,
            "columnCount": len(result_df.columns),
            "truncated_rows": truncated,
            "truncated_columns": truncated_cols,
            "results": json.loads(result_json_str)
        }
        
        if truncated:
            response_payload["message"] = f"Result truncated to the first {MAX_QUERY_RESULT_ROWS} rows."
        
        if truncated_cols:
            response_payload["message"] = response_payload.get("message", "") + f" Showing first 20 of {len(result_df.columns)} columns."
        
        return [types.TextContent(type="text", text=json.dumps(response_payload, indent=2))]
    
    except Exception as e:
        error_payload = {
            "status": "error",
            "query": sql_query,
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def handle_load_dataset(arguments: dict | None) -> list[types.TextContent]:
    """Handle the load_dataset tool."""
    if not arguments or "source" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing 'source' argument"}))]
    
    source = arguments["source"]
    name = arguments.get("name")
    format = arguments.get("format")
    
    # Remove these arguments and pass the rest as kwargs
    kwargs = {k: v for k, v in arguments.items() if k not in ["source", "name", "format"]}
    
    try:
        # Check if we have enough memory before loading
        memory_info = sandbox.get_memory_usage()
        if memory_info["memory_utilization_percent"] > 90 and not sandbox.config.spill_to_disk:
            warning = "Warning: Memory usage is high (>90%) and spill_to_disk is disabled. This operation may fail."
        else:
            warning = None
        
        metadata = await asyncio.to_thread(sandbox.load_dataset, source, name, format, **kwargs)
        
        response_payload = {
            "status": "success",
            "message": f"Dataset '{metadata['name']}' loaded successfully",
            "warning": warning,
            "metadata": metadata
        }
        
        return [types.TextContent(type="text", text=json.dumps(response_payload, indent=2))]
    
    except Exception as e:
        error_payload = {
            "status": "error",
            "source": source,
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def handle_get_dataset_sample(arguments: dict | None) -> list[types.TextContent]:
    """Handle the get_dataset_sample tool."""
    if not arguments or "name" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing 'name' argument"}))]
    
    name = arguments["name"]
    n = arguments.get("n", 5)
    
    try:
        sample_df = await asyncio.to_thread(sandbox.get_dataset_sample, name, n)
        
        response_payload = {
            "status": "success",
            "dataset": name,
            "sample_size": len(sample_df),
            "columns": list(sample_df.columns),
            "data": json.loads(sample_df.to_json(orient="records", indent=2, date_format='iso'))
        }
        
        return [types.TextContent(type="text", text=json.dumps(response_payload, indent=2))]
    
    except Exception as e:
        error_payload = {
            "status": "error",
            "dataset": name,
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def handle_get_dataset_info(arguments: dict | None) -> list[types.TextContent]:
    """Handle the get_dataset_info tool."""
    if not arguments or "name" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing 'name' argument"}))]
    
    name = arguments["name"]
    
    try:
        info = await asyncio.to_thread(sandbox.get_dataset_info, name)
        
        response_payload = {
            "status": "success",
            "dataset": name,
            "info": info
        }
        
        return [types.TextContent(type="text", text=json.dumps(response_payload, indent=2))]
    
    except Exception as e:
        error_payload = {
            "status": "error",
            "dataset": name,
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def handle_remove_dataset(arguments: dict | None) -> list[types.TextContent]:
    """Handle the remove_dataset tool."""
    if not arguments or "name" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing 'name' argument"}))]
    
    name = arguments["name"]
    
    try:
        success = await asyncio.to_thread(sandbox.remove_dataset, name)
        
        if success:
            response_payload = {
                "status": "success",
                "message": f"Dataset '{name}' removed successfully"
            }
        else:
            response_payload = {
                "status": "error",
                "message": f"Dataset '{name}' not found or could not be removed"
            }
        
        return [types.TextContent(type="text", text=json.dumps(response_payload, indent=2))]
    
    except Exception as e:
        error_payload = {
            "status": "error",
            "dataset": name,
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def handle_create_plot(arguments: dict | None) -> list[types.ImageContent | types.TextContent]:
    """Handle the create_plot tool."""
    if not arguments or "dataset_name" not in arguments or "x" not in arguments:
        return [types.TextContent(type="text", text=json.dumps({"error": "Missing required arguments"}))]
    
    dataset_name = arguments["dataset_name"]
    x = arguments["x"]
    y = arguments.get("y")
    kind = arguments.get("kind", "line")
    
    # Extract other arguments to pass to plotting function
    plot_kwargs = {k: v for k, v in arguments.items() 
                  if k not in ["dataset_name", "x", "y", "kind"]}
    
    try:
        base64_img, metadata = await asyncio.to_thread(
            sandbox.create_plot, dataset_name, x, y, kind, **plot_kwargs
        )
        
        # Return as image content
        return [
            types.ImageContent(
                type="image",
                mimeType="image/png",
                data=base64_img
            ),
            types.TextContent(
                type="text", 
                text=json.dumps({
                    "status": "success",
                    "plot_info": metadata
                }, indent=2)
            )
        ]
    
    except Exception as e:
        error_payload = {
            "status": "error",
            "dataset": dataset_name,
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def handle_get_memory_usage(arguments: dict | None) -> list[types.TextContent]:
    """Handle the get_memory_usage tool."""
    try:
        memory_info = await asyncio.to_thread(sandbox.get_memory_usage)
        
        response_payload = {
            "status": "success",
            "memory_info": memory_info
        }
        
        return [types.TextContent(type="text", text=json.dumps(response_payload, indent=2))]
    
    except Exception as e:
        error_payload = {
            "status": "error",
            "error": type(e).__name__,
            "message": str(e),
        }
        return [types.TextContent(type="text", text=json.dumps(error_payload, indent=2))]

async def main():
    if not sandbox.initialized:
        print("Server cannot start: Sandbox initialization failed.")
        return
    
    # Run the server using stdin/stdout streams
    try:
        async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
            await server.run(
                read_stream,
                write_stream,
                InitializationOptions(
                    server_name="arrow-cache-mcp",
                    server_version="0.2.0",
                    capabilities=server.get_capabilities(
                        notification_options=NotificationOptions(),
                        experimental_capabilities={},
                    ),
                ),
            )
    finally:
        # Ensure sandbox is closed properly on server exit
        if sandbox.initialized:
            print("Closing DataScienceSandbox...")
            try:
                # Wrap close in asyncio.to_thread
                await asyncio.to_thread(sandbox.close)
                print("DataScienceSandbox closed.")
            except Exception as e:
                print(f"Error closing DataScienceSandbox: {e}")

# Add entry point for running the server directly
if __name__ == "__main__":
    asyncio.run(main())

# Make sure to properly close the sandbox at exit
import atexit
atexit.register(lambda: sandbox.close() if sandbox.initialized else None)