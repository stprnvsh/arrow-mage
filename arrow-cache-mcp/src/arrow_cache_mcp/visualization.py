"""
Visualization functions for Arrow Cache MCP.

This module provides functions to create visualizations from cached datasets.
"""

import io
import base64
import logging
import json
from typing import Dict, List, Optional, Tuple, Any, Union
import traceback

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.figure import Figure

# Attempt to import geospatial libraries (optional)
try:
    import geopandas as gpd
    from shapely.geometry import shape
    HAVE_GEOPANDAS = True
except ImportError:
    HAVE_GEOPANDAS = False

from .core import get_arrow_cache
from .utils import get_size_display

# Configure logging
logger = logging.getLogger(__name__)

# Default plotting parameters
DEFAULT_PLOT_DPI = 100

def create_plot(data: Union[pd.DataFrame, Any, str], 
               x: Optional[str] = None, 
               y: Optional[str] = None,
               geometry_col: Optional[str] = None,
               kind: str = 'line', 
               **kwargs) -> Union[Figure, Any]:
    """
    Create a plot from data and return a matplotlib Figure.
    
    Args:
        data: DataFrame, Arrow Table, or dataset name string to plot
        x: Column name for x-axis (optional for some plots)
        y: Column name(s) for y-axis (optional for some plots)
        geometry_col: Geometry column name for geospatial plots
        kind: Type of plot ('line', 'bar', 'scatter', 'hist', 'map', etc.)
        **kwargs: Additional arguments to pass to the plotting function
        
    Returns:
        Matplotlib Figure object or other figure type
    """
    logger.info(f"Creating {kind} plot with geometry_col={geometry_col}")
    
    try:
        # If data is a string, assume it's a dataset name and get the data from cache
        if isinstance(data, str):
            logger.info(f"Data is a string, assuming dataset name: {data}")
            from .core import get_arrow_cache
            cache = get_arrow_cache()
            
            if not cache.contains(data):
                raise ValueError(f"Dataset '{data}' not found in cache.")
                
            # Determine what columns we need
            cols = []
            if geometry_col:
                cols.append(geometry_col)
            if x:
                cols.append(x)
            if y and isinstance(y, str):
                cols.append(y)
            elif y and isinstance(y, list):
                cols.extend(y)
                
            # Build a query to get the data
            if cols:
                col_list = ', '.join([f'"{col}"' for col in cols])
                query = f"SELECT {col_list} FROM _cache_{data} LIMIT 10000"
            else:
                query = f"SELECT * FROM _cache_{data} LIMIT 10000"
                
            logger.info(f"Executing query: {query}")
            # Get the data from cache
            data = cache.query(query)
            
        # Handle geospatial data
        if kind == 'map' or geometry_col:
            if not HAVE_GEOPANDAS:
                raise ImportError("GeoPandas is required for geospatial plots but is not installed.")
            
            # Process data into a GeoDataFrame
            if isinstance(data, gpd.GeoDataFrame):
                gdf = data
            else:
                # Convert to pandas DataFrame first
                if not isinstance(data, pd.DataFrame):
                    if hasattr(data, 'to_pandas'):
                        df = data.to_pandas()
                    else:
                        df = pd.DataFrame(data)
                else:
                    df = data
                
                # Check if geometry column exists
                if geometry_col not in df.columns:
                    raise ValueError(f"Geometry column '{geometry_col}' not found in data.")
                
                # Handle different geometry formats
                if df[geometry_col].dtype == 'object':
                    # This could be GeoJSON strings or objects
                    try:
                        # Try to interpret as GeoJSON
                        geometries = []
                        for item in df[geometry_col]:
                            if isinstance(item, str):
                                # Parse the JSON string to get the GeoJSON
                                try:
                                    geo_obj = json.loads(item)
                                    geometries.append(shape(geo_obj))
                                except (json.JSONDecodeError, TypeError):
                                    # If it's not valid JSON, skip it
                                    geometries.append(None)
                            elif isinstance(item, dict):
                                # It's already a dictionary, try to convert directly
                                try:
                                    geometries.append(shape(item))
                                except (TypeError, ValueError):
                                    geometries.append(None)
                            else:
                                # Unknown format, skip it
                                geometries.append(None)
                        
                        # Create a new dataframe with the geometry column
                        df_copy = df.copy()
                        df_copy["geometry"] = geometries
                        gdf = gpd.GeoDataFrame(df_copy, geometry="geometry")
                    except Exception as e:
                        logger.error(f"Error converting GeoJSON to geometry: {e}")
                        raise ValueError(f"Could not convert '{geometry_col}' to geometry: {e}")
                elif hasattr(df[geometry_col].iloc[0], '__geo_interface__'):
                    # Already a geometry object with geo interface
                    gdf = gpd.GeoDataFrame(df, geometry=geometry_col)
                else:
                    # Not a recognized geometry format
                    raise ValueError(f"Column '{geometry_col}' does not contain valid geometry data.")
            
            # Create the map plot
            fig, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 6)))
            
            # Plot the geometries
            gdf.plot(ax=ax, **kwargs)
            
            # Add title if provided
            if 'title' in kwargs:
                plt.title(kwargs['title'])
            
            return fig
        
        # Standard non-geospatial plots
        if not x and not y:
            raise ValueError("Either x or y must be provided for non-geospatial plots.")
        
        # Convert data to pandas DataFrame if needed
        if not isinstance(data, pd.DataFrame):
            if hasattr(data, 'to_pandas'):
                df = data.to_pandas()
            else:
                df = pd.DataFrame(data)
        else:
            df = data
            
        # Check if requested columns exist
        if x and x not in df.columns:
            raise ValueError(f"Column '{x}' not found in data.")
        
        if y and isinstance(y, str) and y not in df.columns:
            raise ValueError(f"Column '{y}' not found in data.")
        
        if y and isinstance(y, list):
            for col in y:
                if col not in df.columns:
                    raise ValueError(f"Column '{col}' not found in data.")
        
        # Create figure
        fig, ax = plt.subplots(figsize=kwargs.get('figsize', (10, 6)))
        
        # Use pandas plotting interface
        plot_func = getattr(df.plot, kind)
        ax = plot_func(x=x, y=y, ax=ax, **kwargs)
        
        # Set title if provided
        if 'title' in kwargs:
            plt.title(kwargs['title'])
        
        return fig
    
    except Exception as e:
        logger.error(f"Error creating plot: {traceback.format_exc()}")
        raise e  # Re-raise to let the caller handle it

def get_size_display(bytes_value: int) -> str:
    """
    Convert a size in bytes to a human-readable string.
    
    Args:
        bytes_value: Size in bytes
        
    Returns:
        String like "12 KB" or "3.45 MB"
    """
    if bytes_value < 1024:
        return f"{bytes_value} B"
    elif bytes_value < 1024 * 1024:
        return f"{bytes_value / 1024:.1f} KB"
    elif bytes_value < 1024 * 1024 * 1024:
        return f"{bytes_value / (1024 * 1024):.1f} MB"
    else:
        return f"{bytes_value / (1024 * 1024 * 1024):.1f} GB"

def render_dataset_card(dataset: Dict[str, Any]) -> Dict[str, Any]:
    """
    Prepare a dataset card with its info for display.
    
    Args:
        dataset: Dataset information dictionary
        
    Returns:
        Dictionary with formatted dataset info
    """
    # Get size information
    size_bytes = dataset.get('size_bytes', 0)
    size_display = get_size_display(size_bytes)
    
    # Format row count with commas for readability
    row_count = dataset.get('row_count', 'Unknown')
    if isinstance(row_count, int) or (isinstance(row_count, str) and row_count.isdigit()):
        row_count = f"{int(row_count):,}"
    
    # Get column information
    column_count = dataset.get('column_count', len(dataset.get('columns', [])))
    
    # Format metadata for display
    metadata = {}
    
    # Add format info if available
    if 'format' in dataset:
        metadata['format'] = dataset['format']
        
    # Add source if available (truncated for display)
    if 'source' in dataset:
        source = dataset['source']
        if len(source) > 50:
            source = source[:47] + "..."
        metadata['source'] = source
        
    # Add created time if available
    if 'created_at' in dataset:
        from datetime import datetime
        created_time = datetime.fromtimestamp(dataset['created_at']).strftime("%Y-%m-%d %H:%M")
        metadata['created_at'] = created_time
    
    # Prepare column preview
    columns_preview = []
    if 'columns' in dataset and dataset['columns']:
        # First try to get column types from metadata if available
        if 'dtypes' in dataset and isinstance(dataset['dtypes'], dict):
            # Display columns with their types
            for col in dataset['columns'][:20]:  # Limit to 20 columns
                dtype = dataset['dtypes'].get(col, '').replace('DataType', '')
                columns_preview.append({"name": col, "type": dtype})
        else:
            # Just list column names
            columns_preview = [{"name": col} for col in dataset['columns'][:20]]
            
        # Add indicator if there are more columns
        if len(dataset['columns']) > 20:
            columns_preview.append({"name": "...", "type": f"(plus {len(dataset['columns']) - 20} more)"})
    
    # Prepare result
    result = {
        "name": dataset['name'],
        "size": size_display,
        "size_bytes": size_bytes,
        "row_count": row_count,
        "column_count": column_count,
        "metadata": metadata,
        "columns": columns_preview,
    }
    
    # Add geospatial info if applicable
    if dataset.get('is_geospatial'):
        result["is_geospatial"] = True
        if "crs" in dataset:
            result["crs"] = dataset["crs"]
        if "geometry_column" in dataset:
            result["geometry_column"] = dataset["geometry_column"]
    
    return result 

def display_conversation_history_dict(history: Dict[str, Any]):
    """Placeholder function to display conversation history."""
    logger.info("display_conversation_history_dict called with: %s", history)
    # In a real implementation, this would format and display the history.
    pass 