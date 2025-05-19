import streamlit as st
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import io
import base64
import logging
import json
from typing import Dict, List, Tuple, Optional, Union, Any

# Configure logging
logger = logging.getLogger(__name__)

def prepare_data_for_visualization(data, limit=None):
    """
    Prepare a dataset for visualization by converting Arrow to pandas,
    handling potential data size issues.
    
    Args:
        data: Arrow table or pandas DataFrame
        limit: Optional row limit
        
    Returns:
        pandas DataFrame ready for visualization
    """
    try:
        # If data is already a pandas DataFrame, just return a copy
        if isinstance(data, pd.DataFrame):
            return data.head(limit) if limit else data.copy()
            
        # Convert Arrow table to pandas
        if hasattr(data, 'to_pandas'):
            df = data.to_pandas() if not limit else data.slice(0, limit).to_pandas()
            return df
            
        # Handle other formats
        logger.warning(f"Unknown data type for visualization: {type(data)}")
        return pd.DataFrame()
    
    except Exception as e:
        logger.error(f"Error preparing data for visualization: {e}")
        return pd.DataFrame()

def infer_column_types(df):
    """
    Infer and classify columns in a DataFrame by their type.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        Dictionary with lists of column names by type
    """
    column_types = {
        'numeric': [],
        'categorical': [],
        'datetime': [],
        'boolean': [],
        'text': [],
        'geometry': []
    }
    
    if df.empty:
        return column_types
        
    for col in df.columns:
        # Check for geometry columns (both GeoJSON and WKT formats)
        if col.lower() in ['geometry', 'geom', 'shape', 'the_geom', 'geojson', 'wkt']:
            column_types['geometry'].append(col)
            continue
            
        # Check data type
        dtype = df[col].dtype
        
        if pd.api.types.is_numeric_dtype(dtype):
            if set(df[col].dropna().unique()) == {0, 1} or df[col].dtype == bool:
                column_types['boolean'].append(col)
            else:
                column_types['numeric'].append(col)
        
        elif pd.api.types.is_datetime64_any_dtype(dtype):
            column_types['datetime'].append(col)
            
        elif pd.api.types.is_categorical_dtype(dtype) or df[col].nunique() < 20:
            # Categorical if explicitly categorical or low cardinality
            column_types['categorical'].append(col)
            
        else:
            # Default to text for other types
            column_types['text'].append(col)
    
    return column_types

def suggest_visualization(df):
    """
    Suggest an appropriate visualization based on the data.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        Tuple of (x_col, y_col, plot_type, additional_params)
    """
    if df.empty:
        return None, None, 'table', {}
        
    column_types = infer_column_types(df)
    
    # Check for geospatial data
    if column_types['geometry']:
        return column_types['geometry'][0], None, 'map', {}
        
    # If we have datetime columns, suggest time series
    if column_types['datetime'] and column_types['numeric']:
        return column_types['datetime'][0], column_types['numeric'][0], 'line', {}
        
    # If we have categorical and numeric, suggest bar chart
    if column_types['categorical'] and column_types['numeric']:
        return column_types['categorical'][0], column_types['numeric'][0], 'bar', {}
        
    # If we have two numeric columns, suggest scatter
    if len(column_types['numeric']) >= 2:
        return column_types['numeric'][0], column_types['numeric'][1], 'scatter', {}
        
    # If we have one numeric column, suggest histogram
    if column_types['numeric']:
        return column_types['numeric'][0], None, 'hist', {}
        
    # Default to table view
    return None, None, 'table', {}

def create_plotly_figure(df, x_col, y_col, plot_type, **kwargs):
    """
    Create a Plotly figure based on data and plot type.
    
    Args:
        df: pandas DataFrame
        x_col: Column for x-axis
        y_col: Column for y-axis (optional for some plot types)
        plot_type: Type of plot to create
        **kwargs: Additional plot options
        
    Returns:
        Plotly figure object
    """
    title = kwargs.get('title', '')
    color_column = kwargs.get('color', None)
    template = kwargs.get('template', 'plotly_white')
    height = kwargs.get('height', 500)
    width = kwargs.get('width', None)
    
    # Handle missing/invalid column selections
    if x_col and x_col not in df.columns:
        logger.warning(f"Column {x_col} not found in dataframe")
        return go.Figure().update_layout(title="Error: Selected column not found")
        
    if y_col and y_col not in df.columns:
        logger.warning(f"Column {y_col} not found in dataframe")
        return go.Figure().update_layout(title="Error: Selected column not found")
    
    # Create the appropriate plot based on type
    try:
        if plot_type == 'line':
            fig = px.line(df, x=x_col, y=y_col, color=color_column, title=title)
            
        elif plot_type == 'bar':
            fig = px.bar(df, x=x_col, y=y_col, color=color_column, title=title)
            
        elif plot_type == 'scatter':
            fig = px.scatter(df, x=x_col, y=y_col, color=color_column, title=title)
            
        elif plot_type == 'hist':
            fig = px.histogram(df, x=x_col, color=color_column, title=title)
            
        elif plot_type == 'box':
            fig = px.box(df, x=x_col, y=y_col, color=color_column, title=title)
            
        elif plot_type == 'pie':
            fig = px.pie(df, names=x_col, values=y_col, title=title)
            
        elif plot_type == 'heatmap':
            # For heatmap, pivot the data if needed
            if 'pivot_columns' in kwargs:
                pivot_columns = kwargs['pivot_columns']
                pivot_df = df.pivot(index=x_col, columns=pivot_columns, values=y_col)
                fig = px.imshow(pivot_df, title=title)
            else:
                fig = px.density_heatmap(df, x=x_col, y=y_col, title=title)
                
        else:
            # Default to a table view
            fig = go.Figure(data=[go.Table(
                header=dict(values=list(df.columns)),
                cells=dict(values=[df[col] for col in df.columns])
            )])
            
        # Apply standard layout settings
        fig.update_layout(
            template=template,
            height=height,
            width=width,
            title=dict(
                text=title,
                font=dict(size=20)
            ),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating plot: {e}")
        return go.Figure().update_layout(title=f"Error creating plot: {str(e)}")

def matplotlib_to_base64(fig, **kwargs):
    """
    Convert a Matplotlib figure to a base64 string.
    
    Args:
        fig: Matplotlib figure
        **kwargs: Additional options for saving the figure
        
    Returns:
        Base64 encoded string of the figure
    """
    buf = io.BytesIO()
    dpi = kwargs.get('dpi', 100)
    
    fig.savefig(buf, format='png', dpi=dpi, bbox_inches='tight')
    buf.seek(0)
    
    img_str = base64.b64encode(buf.read()).decode('utf-8')
    buf.close()
    
    return img_str

def render_plotly_in_streamlit(fig, use_container_width=True):
    """
    Render a Plotly figure in Streamlit.
    
    Args:
        fig: Plotly figure object
        use_container_width: Whether to use the full container width
    """
    st.plotly_chart(fig, use_container_width=use_container_width)

def generate_visualization_code(df, plot_type, x_col, y_col, **kwargs):
    """
    Generate Python code for reproducing the visualization.
    
    Args:
        df: Sample DataFrame (for column names)
        plot_type: Type of plot
        x_col: X-axis column
        y_col: Y-axis column
        **kwargs: Additional plot parameters
        
    Returns:
        String with Python code
    """
    title = kwargs.get('title', 'Plot Title')
    color = kwargs.get('color', None)
    template = kwargs.get('template', 'plotly_white')
    
    # Start with imports
    code = """
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# Assume df is your DataFrame with the data
# df = pd.read_csv('your_data.csv')  # Load your data here

"""
    
    # Add the specific plot code
    if plot_type == 'line':
        code += f"fig = px.line(df, x='{x_col}', y='{y_col}'"
        if color:
            code += f", color='{color}'"
        code += f", title='{title}')\n"
        
    elif plot_type == 'bar':
        code += f"fig = px.bar(df, x='{x_col}', y='{y_col}'"
        if color:
            code += f", color='{color}'"
        code += f", title='{title}')\n"
        
    elif plot_type == 'scatter':
        code += f"fig = px.scatter(df, x='{x_col}', y='{y_col}'"
        if color:
            code += f", color='{color}'"
        code += f", title='{title}')\n"
        
    elif plot_type == 'hist':
        code += f"fig = px.histogram(df, x='{x_col}'"
        if color:
            code += f", color='{color}'"
        code += f", title='{title}')\n"
        
    elif plot_type == 'box':
        code += f"fig = px.box(df, x='{x_col}', y='{y_col}'"
        if color:
            code += f", color='{color}'"
        code += f", title='{title}')\n"
        
    elif plot_type == 'pie':
        code += f"fig = px.pie(df, names='{x_col}', values='{y_col}', title='{title}')\n"
        
    elif plot_type == 'heatmap':
        code += f"fig = px.density_heatmap(df, x='{x_col}', y='{y_col}', title='{title}')\n"
        
    # Add layout customization
    code += f"""
# Customize layout
fig.update_layout(
    template='{template}',
    title=dict(
        text='{title}',
        font=dict(size=20)
    ),
    legend=dict(
        orientation="h",
        yanchor="bottom",
        y=1.02,
        xanchor="right",
        x=1
    )
)

# Show the plot
fig.show()
"""
    
    return code 