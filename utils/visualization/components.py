import streamlit as st
import pandas as pd
import numpy as np
import json
import logging
from typing import Dict, List, Tuple, Optional, Any
import plotly.express as px
import plotly.graph_objects as go
from io import StringIO

from utils.visualization.core import (
    infer_column_types, 
    suggest_visualization, 
    create_plotly_figure, 
    render_plotly_in_streamlit, 
    generate_visualization_code
)
from utils.visualization.geospatial import (
    detect_geometry_column,
    convert_to_geodataframe,
    create_plotly_map,
    create_folium_map,
    render_folium_map,
    generate_map_code,
    MAPBOX_STYLES
)
from utils.visualization.ai import (
    get_ai_visualization_suggestion,
    run_sql_for_visualization,
    execute_data_transformation
)

# Configure logging
logger = logging.getLogger(__name__)

def render_visualization_sidebar(df):
    """
    Render the sidebar controls for data visualization.
    
    Args:
        df: pandas DataFrame with the data
        
    Returns:
        Dictionary with selected visualization options
    """
    column_types = infer_column_types(df)
    
    # Smart column selection for sidebar
    x_col, y_col, default_type, _ = suggest_visualization(df)
    
    # Visualization type selection
    st.sidebar.subheader("Visualization Type")
    viz_type_options = ["table", "line", "bar", "scatter", "hist", "box", "pie", "heatmap"]
    
    # Add map option if we have geometry
    if column_types["geometry"] or detect_geometry_column(df):
        viz_type_options.append("map")
        
    viz_type = st.sidebar.selectbox(
        "Chart Type:", 
        viz_type_options,
        index=viz_type_options.index(default_type) if default_type in viz_type_options else 0
    )
    
    # Column selection based on visualization type
    st.sidebar.subheader("Data Selection")
    
    # Different controls based on chart type
    if viz_type == "map":
        # For maps, handle geometry column or lat/lon pairs
        geometry_col = detect_geometry_column(df)
        if isinstance(geometry_col, tuple):
            # We have lat/lon pair
            lat_col, lon_col = geometry_col
            st.sidebar.info(f"Detected lat/lon columns: {lat_col}, {lon_col}")
            
            # Provide option to override
            use_detected = st.sidebar.checkbox("Use detected lat/lon columns", value=True)
            if not use_detected:
                lat_options = [col for col in df.columns if 'lat' in col.lower()]
                lon_options = [col for col in df.columns if 'lon' in col.lower() or 'lng' in col.lower()]
                
                if lat_options:
                    lat_col = st.sidebar.selectbox("Latitude Column:", df.columns, 
                                                index=df.columns.get_loc(lat_options[0]) if lat_options else 0)
                else:
                    lat_col = st.sidebar.selectbox("Latitude Column:", df.columns)
                    
                if lon_options:
                    lon_col = st.sidebar.selectbox("Longitude Column:", df.columns,
                                                index=df.columns.get_loc(lon_options[0]) if lon_options else 0)
                else:
                    lon_col = st.sidebar.selectbox("Longitude Column:", df.columns)
                    
                geometry_col = (lat_col, lon_col)
        elif geometry_col:
            # We have a geometry column
            st.sidebar.info(f"Detected geometry column: {geometry_col}")
            
            # Provide option to override
            use_detected = st.sidebar.checkbox("Use detected geometry column", value=True)
            if not use_detected:
                geometry_col = st.sidebar.selectbox("Geometry Column:", df.columns)
        else:
            # No geometry detected, let user select
            geometry_options = [col for col in df.columns if col.lower() in ['geometry', 'geom', 'shape', 'the_geom', 'geojson', 'wkt']]
            
            if geometry_options:
                geometry_col = st.sidebar.selectbox("Geometry Column:", df.columns,
                                                  index=df.columns.get_loc(geometry_options[0]))
            else:
                # Fall back to lat/lon selection
                st.sidebar.warning("No geometry column detected. Please select latitude and longitude columns.")
                lat_options = [col for col in df.columns if 'lat' in col.lower()]
                lon_options = [col for col in df.columns if 'lon' in col.lower() or 'lng' in col.lower()]
                
                if lat_options:
                    lat_col = st.sidebar.selectbox("Latitude Column:", df.columns,
                                                index=df.columns.get_loc(lat_options[0]) if lat_options else 0)
                else:
                    lat_col = st.sidebar.selectbox("Latitude Column:", df.columns)
                    
                if lon_options:
                    lon_col = st.sidebar.selectbox("Longitude Column:", df.columns,
                                                index=df.columns.get_loc(lon_options[0]) if lon_options else 0)
                else:
                    lon_col = st.sidebar.selectbox("Longitude Column:", df.columns)
                    
                geometry_col = (lat_col, lon_col)
        
        # Color column for map
        color_options = ["None"] + column_types["numeric"] + column_types["categorical"]
        color_col = st.sidebar.selectbox("Color By:", color_options)
        color_col = None if color_col == "None" else color_col
        
        # Map-specific options
        st.sidebar.subheader("Map Options")
        
        # Map type (Plotly or Folium)
        map_type = st.sidebar.radio("Map Engine:", ["plotly", "folium"])
        
        if map_type == "plotly":
            # Mapbox style for Plotly
            mapbox_style = st.sidebar.selectbox("Map Style:", list(MAPBOX_STYLES.keys()))
            mapbox_style_value = MAPBOX_STYLES[mapbox_style]
            
            # Return map settings
            options = {
                "type": viz_type,
                "map_type": map_type,
                "geometry_col": geometry_col,
                "color_col": color_col,
                "mapbox_style": mapbox_style_value,
                "x_col": None,
                "y_col": None
            }
        else:
            # Folium options
            tiles = st.sidebar.selectbox("Map Style:", 
                                     ["CartoDB positron", "CartoDB dark_matter", "OpenStreetMap", 
                                      "Stamen Terrain", "Stamen Watercolor", "Stamen Toner"])
            
            # Return map settings
            options = {
                "type": viz_type,
                "map_type": map_type,
                "geometry_col": geometry_col,
                "color_col": color_col,
                "tiles": tiles,
                "x_col": None,
                "y_col": None
            }
    else:
        # For standard charts
        if viz_type != "table":
            # For most chart types, we need x-axis selection
            x_options = df.columns.tolist()
            x_col_idx = x_options.index(x_col) if x_col in x_options else 0
            x_col = st.sidebar.selectbox("X-axis:", x_options, index=x_col_idx)
            
            # For many chart types, we need y-axis selection
            if viz_type not in ["hist"]:
                y_options = df.columns.tolist()
                if y_col and y_col in y_options:
                    y_col_idx = y_options.index(y_col)
                else:
                    # Choose a different column than x by default
                    y_col_idx = min(1, len(y_options) - 1) if x_col_idx == 0 else 0
                y_col = st.sidebar.selectbox("Y-axis:", y_options, index=y_col_idx)
            else:
                y_col = None
            
            # Color option for certain chart types
            if viz_type in ["scatter", "line", "bar", "box"]:
                color_options = ["None"] + df.columns.tolist()
                color_col = st.sidebar.selectbox("Color By:", color_options)
                color_col = None if color_col == "None" else color_col
            else:
                color_col = None
                
            # Specific options for each chart type
            st.sidebar.subheader("Chart Options")
            
            if viz_type == "hist":
                bins = st.sidebar.slider("Number of Bins:", min_value=5, max_value=100, value=20)
                options = {
                    "type": viz_type,
                    "x_col": x_col,
                    "y_col": y_col,
                    "color_col": color_col,
                    "bins": bins
                }
            elif viz_type == "heatmap":
                # For heatmap, we might need a pivot
                pivot_options = ["None"] + df.columns.tolist()
                pivot_col = st.sidebar.selectbox("Pivot By (Optional):", pivot_options)
                pivot_col = None if pivot_col == "None" else pivot_col
                
                options = {
                    "type": viz_type,
                    "x_col": x_col,
                    "y_col": y_col,
                    "color_col": color_col,
                    "pivot_col": pivot_col
                }
            else:
                # Standard options for other chart types
                options = {
                    "type": viz_type,
                    "x_col": x_col,
                    "y_col": y_col,
                    "color_col": color_col
                }
        else:
            # Table view doesn't need columns
            options = {
                "type": viz_type,
                "x_col": None,
                "y_col": None,
                "color_col": None
            }
    
    # General visualization options
    st.sidebar.subheader("Appearance")
    title = st.sidebar.text_input("Title:", value=f"{viz_type.capitalize()} Chart")
    height = st.sidebar.slider("Height:", min_value=300, max_value=1000, value=500)
    
    # Add appearance settings to options
    options["title"] = title
    options["height"] = height
    
    return options

def render_visualization(df, options):
    """
    Render a visualization based on the provided options.
    
    Args:
        df: pandas DataFrame with the data
        options: Dictionary with visualization options
    """
    viz_type = options.get("type", "table")
    
    try:
        if viz_type == "table":
            # Simple table view
            st.dataframe(df, use_container_width=True)
            
            # Add download button for the data
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            st.download_button(
                label="Download Data as CSV",
                data=csv_data,
                file_name="visualization_data.csv",
                mime="text/csv"
            )
            
        elif viz_type == "map":
            # Map visualization
            geometry_col = options.get("geometry_col")
            color_col = options.get("color_col")
            map_type = options.get("map_type", "plotly")
            
            if map_type == "plotly":
                # Plotly map
                mapbox_style = options.get("mapbox_style", "carto-positron")
                title = options.get("title", "Map Visualization")
                height = options.get("height", 500)
                
                fig = create_plotly_map(
                    df, 
                    geometry_col=geometry_col, 
                    color_col=color_col,
                    title=title,
                    mapbox_style=mapbox_style,
                    height=height
                )
                
                render_plotly_in_streamlit(fig)
                
                # Generate and show code
                with st.expander("Show Code"):
                    code = generate_map_code(
                        df, 
                        "plotly", 
                        geometry_col, 
                        color_col, 
                        title=title
                    )
                    st.code(code, language="python")
                
            else:
                # Folium map
                tiles = options.get("tiles", "CartoDB positron")
                height = options.get("height", 500)
                
                # Create the map
                map_obj = create_folium_map(
                    df,
                    geometry_col=geometry_col,
                    color_col=color_col,
                    tiles=tiles
                )
                
                # Display the map
                render_folium_map(map_obj, height=height)
                
                # Generate and show code
                with st.expander("Show Code"):
                    code = generate_map_code(
                        df, 
                        "folium", 
                        geometry_col, 
                        color_col
                    )
                    st.code(code, language="python")
        
        else:
            # Standard charts with Plotly
            x_col = options.get("x_col")
            y_col = options.get("y_col")
            color_col = options.get("color_col")
            title = options.get("title", f"{viz_type.capitalize()} Chart")
            height = options.get("height", 500)
            
            # Add chart-specific parameters
            kwargs = {
                "title": title,
                "height": height,
                "color": color_col,
                "template": "plotly_white"
            }
            
            if viz_type == "hist":
                kwargs["bins"] = options.get("bins", 20)
            
            elif viz_type == "heatmap":
                # For heatmap with pivot
                pivot_col = options.get("pivot_col")
                if pivot_col:
                    kwargs["pivot_columns"] = pivot_col
            
            # Create and render the figure
            fig = create_plotly_figure(df, x_col, y_col, viz_type, **kwargs)
            render_plotly_in_streamlit(fig)
            
            # Generate and show code
            with st.expander("Show Code"):
                code = generate_visualization_code(df, viz_type, x_col, y_col, **kwargs)
                st.code(code, language="python")
    
    except Exception as e:
        st.error(f"Error rendering visualization: {str(e)}")
        logger.error(f"Error rendering visualization: {e}")
        # Show a simplified table view as fallback
        st.write("Showing data table as fallback:")
        st.dataframe(df.head(100), use_container_width=True)

def display_ai_visualization_suggestion(dataset_name, api_key, provider="anthropic", model=None, query_context=None):
    """
    Display AI-generated visualization suggestions for a dataset.
    
    Args:
        dataset_name: Name of the dataset
        api_key: API key for the AI provider
        provider: AI provider name
        model: Model to use
        query_context: Optional SQL query or context
    """
    success, suggestion = get_ai_visualization_suggestion(
        dataset_name, 
        api_key, 
        provider=provider, 
        model=model, 
        query_context=query_context
    )
    
    if success and isinstance(suggestion, dict):
        st.markdown("<div class='data-card'>", unsafe_allow_html=True)
        st.subheader("🤖 AI Visualization Suggestion")
        
        # Display the suggestion
        col1, col2 = st.columns([3, 1])
        with col1:
            st.write(f"**Suggestion**: {suggestion.get('description', 'No description provided')}")
            
            # Display chart type and columns
            viz_type = suggestion.get('visualization_type', 'unknown')
            x_col = suggestion.get('x_column')
            y_col = suggestion.get('y_column')
            color_col = suggestion.get('color_column')
            
            info_text = f"**Chart Type**: {viz_type.capitalize()}\n\n"
            if x_col:
                info_text += f"**X-axis**: {x_col}\n\n"
            if y_col:
                info_text += f"**Y-axis**: {y_col}\n\n"
            if color_col:
                info_text += f"**Color By**: {color_col}\n\n"
                
            st.write(info_text)
            
        with col2:
            # Apply button
            if st.button("Apply Suggestion", type="primary"):
                st.session_state.visualization_settings = {
                    "type": viz_type,
                    "x_col": x_col,
                    "y_col": y_col,
                    "color_col": color_col,
                    "title": suggestion.get('title', f"{viz_type.capitalize()} of {dataset_name}")
                }
                
                # Check if there are transformations
                transformations = suggestion.get('transformations', [])
                if transformations and isinstance(transformations, list) and len(transformations) > 0:
                    # Get the first transformation
                    transformation_query = transformations[0]
                    if transformation_query and isinstance(transformation_query, str):
                        st.session_state.transformation_query = transformation_query
                
                st.rerun()
        
        # Transformation suggestions
        transformations = suggestion.get('transformations', [])
        if transformations and isinstance(transformations, list) and len(transformations) > 0:
            st.markdown("##### Suggested Data Transformations")
            for i, transformation in enumerate(transformations):
                if isinstance(transformation, str):
                    with st.expander(f"Transformation {i+1}"):
                        st.code(transformation, language="sql")
                        
                        # Execute transformation button
                        if st.button(f"Run Transformation", key=f"transform_{i}"):
                            st.session_state.transformation_query = transformation
                            st.rerun()
        
        st.markdown("</div>", unsafe_allow_html=True)
    else:
        # Display error or no suggestion
        st.warning(f"Could not generate visualization suggestion: {suggestion}")

def visualize_sql_result(sql_query, limit=None):
    """
    Run a SQL query and visualize the result.
    
    Args:
        sql_query: SQL query to execute
        limit: Optional row limit for visualization
        
    Returns:
        True if visualization was successful, False otherwise
    """
    try:
        success, result = run_sql_for_visualization(sql_query, limit)
        
        if success and isinstance(result, pd.DataFrame) and not result.empty:
            # Store the result in session state for visualization
            st.session_state.visualization_data = result
            
            # Identify the columns and suggest visualization
            x_col, y_col, viz_type, _ = suggest_visualization(result)
            
            # Store suggested visualization settings
            st.session_state.visualization_settings = {
                "type": viz_type,
                "x_col": x_col,
                "y_col": y_col,
                "color_col": None,
                "title": f"Results of SQL Query"
            }
            
            return True
        else:
            # Show error
            st.error(f"Query returned no results or error: {result}")
            return False
            
    except Exception as e:
        st.error(f"Error visualizing SQL result: {str(e)}")
        logger.error(f"Error visualizing SQL result: {e}")
        return False 