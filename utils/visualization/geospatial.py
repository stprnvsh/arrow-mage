import streamlit as st
import pandas as pd
import numpy as np
import geopandas as gpd
import plotly.express as px
import plotly.graph_objects as go
import json
import logging
from typing import Dict, List, Tuple, Optional, Union, Any
import re
import folium
from folium import plugins
from streamlit_folium import folium_static
from branca.colormap import linear, step

# Configure logging
logger = logging.getLogger(__name__)

# Common map styles for Plotly
MAPBOX_STYLES = {
    "Open Street Map": "open-street-map",
    "Carto Positron": "carto-positron",
    "Carto Darkmatter": "carto-darkmatter",
    "Stamen Terrain": "stamen-terrain",
    "Stamen Toner": "stamen-toner",
    "Stamen Watercolor": "stamen-watercolor"
}

def detect_geometry_column(df):
    """
    Detect a geometry column in the DataFrame.
    
    Args:
        df: pandas DataFrame
        
    Returns:
        Name of geometry column or None
    """
    if df.empty:
        return None
    
    # Check for standard geometry column names
    std_geom_cols = ['geometry', 'geom', 'shape', 'the_geom', 'geojson', 'wkt']
    for col in std_geom_cols:
        if col in df.columns:
            return col
            
    # Check for columns that might contain GeoJSON strings
    for col in df.columns:
        # Skip non-string columns
        if not pd.api.types.is_string_dtype(df[col].dtype):
            continue
            
        # Sample a non-null value
        sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
        if not sample:
            continue
            
        # Check if it looks like GeoJSON
        if isinstance(sample, str):
            if sample.strip().startswith('{') and '"type"' in sample and (
                    '"coordinates"' in sample or '"geometry"' in sample):
                return col
    
    # Check for latitude/longitude pairs
    lat_pattern = re.compile(r'lat|latitude', re.IGNORECASE)
    lon_pattern = re.compile(r'lon|lng|long|longitude', re.IGNORECASE)
    
    lat_cols = [col for col in df.columns if lat_pattern.search(str(col))]
    lon_cols = [col for col in df.columns if lon_pattern.search(str(col))]
    
    if lat_cols and lon_cols:
        # Found potential lat/lon columns
        return (lat_cols[0], lon_cols[0])
        
    return None

def convert_to_geodataframe(df, geometry_col=None):
    """
    Convert a pandas DataFrame to a GeoDataFrame.
    
    Args:
        df: pandas DataFrame
        geometry_col: Geometry column name or (lat, lon) tuple
        
    Returns:
        GeoDataFrame or None if conversion fails
    """
    try:
        if geometry_col is None:
            geometry_col = detect_geometry_column(df)
            
        if geometry_col is None:
            logger.warning("No geometry column detected for GeoDataFrame conversion")
            return None
            
        # Handle different geometry formats
        if isinstance(geometry_col, tuple):
            # Lat/lon pair
            lat_col, lon_col = geometry_col
            gdf = gpd.GeoDataFrame(
                df, 
                geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
                crs="EPSG:4326"
            )
            return gdf
            
        elif geometry_col in df.columns:
            col_data = df[geometry_col]
            
            # Check if already a GeoDataFrame
            if isinstance(df, gpd.GeoDataFrame) and df._geometry_column_name == geometry_col:
                return df
                
            # Check if column contains GeoJSON strings
            if pd.api.types.is_string_dtype(col_data.dtype):
                try:
                    # Try parsing as GeoJSON
                    sample = col_data.dropna().iloc[0] if not col_data.dropna().empty else None
                    if sample and isinstance(sample, str):
                        if sample.strip().startswith('{'):
                            # Convert GeoJSON strings to geometry objects
                            from shapely.geometry import shape
                            geometries = col_data.apply(lambda x: shape(json.loads(x)) if isinstance(x, str) else None)
                            gdf = gpd.GeoDataFrame(df.drop(columns=[geometry_col]), geometry=geometries, crs="EPSG:4326")
                            return gdf
                except Exception as e:
                    logger.error(f"Error parsing GeoJSON strings: {e}")
            
            # Try direct conversion (works if column contains geometry objects)
            try:
                gdf = gpd.GeoDataFrame(df, geometry=geometry_col, crs="EPSG:4326")
                return gdf
            except Exception as e:
                logger.error(f"Error converting to GeoDataFrame with column {geometry_col}: {e}")
                
        return None
        
    except Exception as e:
        logger.error(f"Error in convert_to_geodataframe: {e}")
        return None

def create_plotly_map(df, geometry_col=None, color_col=None, **kwargs):
    """
    Create a Plotly map visualization from a DataFrame with geometry.
    
    Args:
        df: pandas DataFrame
        geometry_col: Geometry column name or (lat, lon) tuple
        color_col: Column to use for coloring
        **kwargs: Additional map options
        
    Returns:
        Plotly figure with map
    """
    title = kwargs.get('title', 'Map Visualization')
    mapbox_style = kwargs.get('mapbox_style', 'carto-positron')
    height = kwargs.get('height', 600)
    width = kwargs.get('width', None)
    zoom = kwargs.get('zoom', 9)
    
    try:
        # Detect geometry if not provided
        if geometry_col is None:
            geometry_col = detect_geometry_column(df)
            
        if geometry_col is None:
            logger.warning("No geometry column detected for map visualization")
            return go.Figure().update_layout(title="Error: No geometry data found")
            
        # Create map figure
        if isinstance(geometry_col, tuple):
            # For lat/lon pairs, use px.scatter_mapbox
            lat_col, lon_col = geometry_col
            
            # Check if columns exist
            if lat_col not in df.columns or lon_col not in df.columns:
                return go.Figure().update_layout(title=f"Error: Columns {lat_col}/{lon_col} not found")
                
            fig = px.scatter_mapbox(
                df,
                lat=lat_col,
                lon=lon_col,
                color=color_col,
                hover_data=df.columns[:5],  # Show first 5 columns in hover
                title=title,
                color_continuous_scale="Viridis" if color_col else None,
                zoom=zoom
            )
            
        else:
            # For GeoJSON data, convert to GeoDataFrame and use px.choropleth_mapbox
            gdf = convert_to_geodataframe(df, geometry_col)
            
            if gdf is None:
                return go.Figure().update_layout(title="Error: Could not convert to GeoDataFrame")
                
            # Get the center of the data for the map view
            center_lon, center_lat = gdf.geometry.unary_union.centroid.x, gdf.geometry.unary_union.centroid.y
            
            # Determine if we're dealing with points or polygons
            sample_geom = gdf.geometry.iloc[0] if not gdf.empty else None
            if sample_geom and hasattr(sample_geom, 'geom_type'):
                if sample_geom.geom_type == 'Point':
                    # For points
                    plot_args = {
                        "lat": gdf.geometry.y,
                        "lon": gdf.geometry.x,
                        "hover_data": gdf.columns[:5],
                        "title": title,
                        "zoom": zoom,
                        "mapbox_style": 'carto-positron'
                    }
                    
                    # Add color args if applicable
                    if color_col:
                        plot_args["color"] = color_col
                        plot_args["color_continuous_scale"] = 'Viridis'
                    
                    fig = px.scatter_mapbox(gdf, **plot_args)
                else:
                    # For polygons, lines, etc.
                    # Convert to GeoJSON for Plotly
                    geojson_data = json.loads(gdf.geometry.to_json())
                    
                    # Create a feature ID to link the GeoJSON to the data
                    gdf['id'] = range(len(gdf))
                    
                    plot_args = {
                        "geojson": geojson_data,
                        "locations": 'id',
                        "hover_data": gdf.columns[:5],
                        "title": title,
                        "center": {"lat": center_lat, "lon": center_lon},
                        "zoom": zoom,
                        "mapbox_style": 'carto-positron'
                    }
                    
                    # Add color args if applicable
                    if color_col:
                        plot_args["color"] = color_col
                        plot_args["color_continuous_scale"] = 'Viridis'
                    
                    fig = px.choropleth_mapbox(gdf, **plot_args)
            else:
                return go.Figure().update_layout(title="Error: Invalid geometry data")
        
        # Update layout
        fig.update_layout(
            mapbox_style=mapbox_style,
            autosize=True,
            height=height,
            width=width,
            margin={"r": 0, "t": 40, "l": 0, "b": 0},
            title=dict(
                text=title,
                font=dict(size=20)
            )
        )
        
        return fig
        
    except Exception as e:
        logger.error(f"Error creating Plotly map: {e}")
        return go.Figure().update_layout(title=f"Error creating map: {str(e)}")

def create_folium_map(df, geometry_col=None, color_col=None, **kwargs):
    """
    Create a Folium map visualization from a DataFrame with geometry.
    
    Args:
        df: pandas DataFrame
        geometry_col: Geometry column name or (lat, lon) tuple
        color_col: Column to use for coloring
        **kwargs: Additional map options
        
    Returns:
        Folium map object
    """
    tiles = kwargs.get('tiles', 'CartoDB positron')
    zoom = kwargs.get('zoom', 10)
    height = kwargs.get('height', 400)
    
    try:
        # Detect geometry if not provided
        if geometry_col is None:
            geometry_col = detect_geometry_column(df)
            
        if geometry_col is None:
            logger.warning("No geometry column detected for map visualization")
            m = folium.Map(tiles=tiles, zoom_start=zoom)
            folium.Element("""
                <div style="text-align: center; margin-top: 10px;">
                    <h4>Error: No geometry data found</h4>
                </div>
            """).add_to(m)
            return m
        
        # Convert to GeoDataFrame if needed
        gdf = None
        if isinstance(geometry_col, tuple):
            # For lat/lon pairs
            lat_col, lon_col = geometry_col
            if lat_col in df.columns and lon_col in df.columns:
                gdf = gpd.GeoDataFrame(
                    df, 
                    geometry=gpd.points_from_xy(df[lon_col], df[lat_col]),
                    crs="EPSG:4326"
                )
        else:
            # For GeoJSON or other geometry formats
            gdf = convert_to_geodataframe(df, geometry_col)
            
        if gdf is None or gdf.empty:
            m = folium.Map(tiles=tiles, zoom_start=zoom)
            folium.Element("""
                <div style="text-align: center; margin-top: 10px;">
                    <h4>Error: Could not convert to GeoDataFrame</h4>
                </div>
            """).add_to(m)
            return m
            
        # Get the center of the data for the map view
        center = [gdf.geometry.unary_union.centroid.y, gdf.geometry.unary_union.centroid.x]
        
        # Create the map
        m = folium.Map(location=center, tiles=tiles, zoom_start=zoom)
        
        # Determine if we're dealing with points or polygons
        sample_geom = gdf.geometry.iloc[0] if not gdf.empty else None
        
        if sample_geom and hasattr(sample_geom, 'geom_type'):
            # Set up color scale if color column is provided
            if color_col and color_col in gdf.columns:
                if pd.api.types.is_numeric_dtype(gdf[color_col].dtype):
                    vmin = gdf[color_col].min()
                    vmax = gdf[color_col].max()
                    colormap = linear.viridis.scale(vmin, vmax)
                    colormap.caption = color_col
                    colormap.add_to(m)
                else:
                    # For categorical data
                    categories = gdf[color_col].dropna().unique()
                    colors = px.colors.qualitative.Plotly[:len(categories)]
                    color_dict = dict(zip(categories, colors))
            
            if sample_geom.geom_type == 'Point':
                # For points
                if color_col and color_col in gdf.columns:
                    if pd.api.types.is_numeric_dtype(gdf[color_col].dtype):
                        # Numeric color scale
                        for _, row in gdf.iterrows():
                            if pd.notnull(row[color_col]):
                                color = colormap(row[color_col])
                                popup_content = []
                                for col in gdf.columns[:5]:
                                    if col != 'geometry':
                                        popup_content.append(f"{{col}}: {{row[col]}}")
                                popup_html = "<br>".join(popup_content)
                                folium.CircleMarker(
                                    location=[row.geometry.y, row.geometry.x],
                                    radius=6,
                                    color=color,
                                    fill=True,
                                    fill_color=color,
                                    fill_opacity=0.7,
                                    popup=folium.Popup(popup_html)
                                ).add_to(m)
                    else:
                        # Categorical colors
                        for _, row in gdf.iterrows():
                            if pd.notnull(row[color_col]):
                                color = color_dict.get(row[color_col], 'gray')
                                popup_content = []
                                for col in gdf.columns[:5]:
                                    if col != 'geometry':
                                        popup_content.append(f"{{col}}: {{row[col]}}")
                                popup_html = "<br>".join(popup_content)
                                folium.CircleMarker(
                                    location=[row.geometry.y, row.geometry.x],
                                    radius=6,
                                    color=color,
                                    fill=True,
                                    fill_color=color,
                                    fill_opacity=0.7,
                                    popup=folium.Popup(popup_html)
                                ).add_to(m)
                else:
                    # No color column - use marker cluster
                    marker_cluster = plugins.MarkerCluster().add_to(m)
                    for _, row in gdf.iterrows():
                        popup_content = []
                        for col in gdf.columns[:5]:
                            if col != 'geometry':
                                popup_content.append(f"{{col}}: {{row[col]}}")
                        popup_html = "<br>".join(popup_content)
                        folium.Marker(
                            location=[row.geometry.y, row.geometry.x],
                            popup=folium.Popup(popup_html)
                        ).add_to(marker_cluster)
            else:
                # For polygons, lines, etc.
                if color_col and color_col in gdf.columns:
                    if pd.api.types.is_numeric_dtype(gdf[color_col].dtype):
                        # Numeric color scale
                        style_function = lambda x: {
                            'fillColor': colormap(x['properties'][color_col]),
                            'color': 'black',
                            'weight': 1,
                            'fillOpacity': 0.7
                        }
                    else:
                        # Categorical colors
                        style_function = lambda x: {
                            'fillColor': color_dict.get(x['properties'][color_col], 'gray'),
                            'color': 'black',
                            'weight': 1,
                            'fillOpacity': 0.7
                        }
                else:
                    # No color column - default style
                    style_function = lambda x: {
                        'fillColor': '#3388ff',
                        'color': 'black',
                        'weight': 1,
                        'fillOpacity': 0.7
                    }
                    
                popup_function = lambda x: folium.Popup("<br>".join([f"{{col}}: {{x['properties'][col]}}" for col in gdf.columns[:5] if col != 'geometry']))
                
                # Add GeoJSON to map
                folium.GeoJson(
                    data=gdf.to_json(),
                    style_function=style_function,
                    popup=popup_function
                ).add_to(m)
                
        # Add layer control
        folium.LayerControl().add_to(m)
        
        return m
        
    except Exception as e:
        logger.error(f"Error creating Folium map: {e}")
        m = folium.Map(tiles=tiles, zoom_start=zoom)
        folium.Element(f"""
            <div style="text-align: center; margin-top: 10px;">
                <h4>Error creating map: {str(e)}</h4>
            </div>
        """).add_to(m)
        return m

def render_folium_map(map_obj, height=400):
    """
    Render a Folium map in Streamlit.
    
    Args:
        map_obj: Folium map object
        height: Map height in pixels
    """
    folium_static(map_obj, height=height)

def generate_map_code(df, map_type, geometry_col=None, color_col=None, lat_col=None, lon_col=None, title="Map Visualization", **kwargs):
    """Generate code for creating a map visualization.
    
    This function creates code without using nested f-strings to avoid syntax errors.
    """
    # Use basic string templates instead of nested f-strings
    code = "import pandas as pd\n"
    code += "import folium\n"
    code += "from folium import plugins\n"
    
    if map_type == 'plotly':
        code += "import plotly.express as px\n"
        code += "import json\n"
    
    code += "import geopandas as gpd\n"
    code += "from shapely.geometry import shape\n\n"
    
    # Handle lat/lon points
    if lat_col and lon_col:
        code += "# Convert to GeoDataFrame\n"
        code += "gdf = gpd.GeoDataFrame(\n"
        code += "    df, \n"
        code += f"    geometry=gpd.points_from_xy(df['{lon_col}'], df['{lat_col}']),\n"
        code += "    crs=\"EPSG:4326\"\n"
        code += ")\n\n"
        
        code += "# Get center of data\n"
        
        if map_type == 'plotly':
            code += "center_lon = gdf.geometry.unary_union.centroid.x\n"
            code += "center_lat = gdf.geometry.unary_union.centroid.y\n\n"
            
            # For plotly map (points only)
            code += "# Create point map with Plotly\n"
            code += "fig = px.scatter_mapbox(\n"
            code += "    gdf,\n"
            code += "    lat=gdf.geometry.y,\n"
            code += "    lon=gdf.geometry.x,\n"
            
            if color_col:
                code += f"    color='{color_col}',\n"
                code += "    color_continuous_scale='Viridis',\n"
                
            code += "    hover_data=gdf.columns[:5],\n"
            code += f"    title='{title}',\n"
            code += "    zoom=9,\n"
            code += "    mapbox_style='carto-positron'\n"
            code += ")\n\n"
            
            # Layout
            code += "# Update layout\n"
            code += "fig.update_layout(\n"
            code += "    autosize=True,\n"
            code += "    height=600,\n"
            code += "    margin={\"r\": 0, \"t\": 40, \"l\": 0, \"b\": 0},\n"
            code += "    title=dict(\n"
            code += f"        text='{title}',\n"
            code += "        font=dict(size=20)\n"
            code += "    )\n"
            code += ")\n\n"
            
            code += "# Show the map\n"
            code += "fig.show()\n"
        else:
            # Folium map
            code += "center = [gdf.geometry.unary_union.centroid.y, gdf.geometry.unary_union.centroid.x]\n\n"
            code += "# Create map\n"
            code += "m = folium.Map(location=center, tiles='CartoDB positron', zoom_start=10)\n\n"
            
            if color_col:
                code += "# Add color scale if using a color column\n"
                code += f"if '{color_col}' in gdf.columns:\n"
                code += "    from branca.colormap import linear\n"
                code += f"    vmin = gdf['{color_col}'].min()\n"
                code += f"    vmax = gdf['{color_col}'].max()\n"
                code += "    colormap = linear.viridis.scale(vmin, vmax)\n"
                code += f"    colormap.caption = '{color_col}'\n"
                code += "    colormap.add_to(m)\n\n"
                
                code += "    # Add points with color\n"
                code += "    for _, row in gdf.iterrows():\n"
                code += "        # Create popup content\n"
                code += "        popup_content = []\n"
                code += "        for col in gdf.columns[:5]:\n"
                code += "            if col != 'geometry':\n"
                code += "                popup_content.append(f\"{col}: {row[col]}\")\n"
                code += "        popup_html = \"<br>\".join(popup_content)\n\n"
                
                code += "        folium.CircleMarker(\n"
                code += "            location=[row.geometry.y, row.geometry.x],\n"
                code += "            radius=6,\n"
                code += f"            color=colormap(row['{color_col}']),\n"
                code += "            fill=True,\n"
                code += f"            fill_color=colormap(row['{color_col}']),\n"
                code += "            fill_opacity=0.7,\n"
                code += "            popup=folium.Popup(popup_html)\n"
                code += "        ).add_to(m)\n"
                code += "else:\n"
                code += "    # Use marker cluster\n"
                code += "    marker_cluster = plugins.MarkerCluster().add_to(m)\n"
                code += "    for _, row in gdf.iterrows():\n"
                code += "        # Create popup content\n"
                code += "        popup_content = []\n"
                code += "        for col in gdf.columns[:5]:\n"
                code += "            if col != 'geometry':\n"
                code += "                popup_content.append(f\"{col}: {row[col]}\")\n"
                code += "        popup_html = \"<br>\".join(popup_content)\n\n"
                
                code += "        folium.Marker(\n"
                code += "            location=[row.geometry.y, row.geometry.x],\n"
                code += "            popup=folium.Popup(popup_html)\n"
                code += "        ).add_to(marker_cluster)\n"
            else:
                code += "# Use marker cluster\n"
                code += "marker_cluster = plugins.MarkerCluster().add_to(m)\n"
                code += "for _, row in gdf.iterrows():\n"
                code += "    # Create popup content\n"
                code += "    popup_content = []\n"
                code += "    for col in gdf.columns[:5]:\n"
                code += "        if col != 'geometry':\n"
                code += "            popup_content.append(f\"{col}: {row[col]}\")\n"
                code += "        popup_html = \"<br>\".join(popup_content)\n\n"
                
                code += "        folium.Marker(\n"
                code += "            location=[row.geometry.y, row.geometry.x],\n"
                code += "            popup=folium.Popup(popup_html)\n"
                code += "        ).add_to(marker_cluster)\n"
                
            code += "\n# Add layer control\n"
            code += "folium.LayerControl().add_to(m)\n\n"
            
            code += "# Display the map\n"
            code += "m\n"
    
    # Handle geometry column (GeoJSON data)
    elif geometry_col:
        # Convert to GeoDataFrame code
        code += "# Convert to GeoDataFrame\n"
        code += "try:\n"
        code += f"    # If '{geometry_col}' contains GeoJSON strings\n"
        code += "    import json\n"
        code += "    from shapely.geometry import shape\n"
        code += f"    geometries = df['{geometry_col}'].apply(lambda x: shape(json.loads(x)) if isinstance(x, str) else None)\n"
        code += f"    gdf = gpd.GeoDataFrame(df.drop(columns=['{geometry_col}']), geometry=geometries, crs=\"EPSG:4326\")\n"
        code += "except:\n"
        code += f"    # If '{geometry_col}' is already a geometry column\n"
        code += f"    gdf = gpd.GeoDataFrame(df, geometry='{geometry_col}', crs=\"EPSG:4326\")\n\n"
        
        if map_type == 'plotly':
            code += "# Get the center of the data\n"
            code += "center_lon = gdf.geometry.unary_union.centroid.x\n" 
            code += "center_lat = gdf.geometry.unary_union.centroid.y\n\n"
            
            code += "# Determine if we have points or polygons\n"
            code += "sample_geom = gdf.geometry.iloc[0]\n"
            code += "if sample_geom.geom_type == 'Point':\n"
            code += "    # For points\n"
            code += "    plot_args = {\n"
            code += "        'lat': gdf.geometry.y,\n"
            code += "        'lon': gdf.geometry.x,\n"
            code += "        'hover_data': gdf.columns[:5],\n"
            code += f"        'title': '{title}',\n"
            code += "        'zoom': 9,\n"
            code += "        'mapbox_style': 'carto-positron'\n"
            code += "    }\n\n"
            
            code += "    # Add color args if applicable\n"
            if color_col:
                code += f"    plot_args['color'] = '{color_col}'\n"
                code += "    plot_args['color_continuous_scale'] = 'Viridis'\n\n"
                
            code += "    fig = px.scatter_mapbox(gdf, **plot_args)\n"
            code += "else:\n"
            code += "    # For polygons, lines, etc.\n"
            code += "    geojson_data = json.loads(gdf.geometry.to_json())\n"
            code += "    gdf['id'] = range(len(gdf))\n\n"
            
            code += "    plot_args = {\n"
            code += "        'geojson': geojson_data,\n"
            code += "        'locations': 'id',\n"
            code += "        'hover_data': gdf.columns[:5],\n"
            code += f"        'title': '{title}',\n"
            code += "        'center': {\"lat\": center_lat, \"lon\": center_lon},\n"
            code += "        'zoom': 9,\n"
            code += "        'mapbox_style': 'carto-positron'\n"
            code += "    }\n\n"
            
            code += "    # Add color args if applicable\n"
            if color_col:
                code += f"    plot_args['color'] = '{color_col}'\n"
                code += "    plot_args['color_continuous_scale'] = 'Viridis'\n\n"
                
            code += "    fig = px.choropleth_mapbox(gdf, **plot_args)\n\n"
            
            code += "# Update layout\n"
            code += "fig.update_layout(\n"
            code += "    autosize=True,\n"
            code += "    height=600,\n"
            code += "    margin={\"r\": 0, \"t\": 40, \"l\": 0, \"b\": 0},\n"
            code += "    title=dict(\n"
            code += f"        text='{title}',\n"
            code += "        font=dict(size=20)\n"
            code += "    )\n"
            code += ")\n\n"
            
            code += "# Show the map\n"
            code += "fig.show()\n"
        else:
            # Folium for geometry
            code += "# Get the center of the data\n"
            code += "center = [gdf.geometry.unary_union.centroid.y, gdf.geometry.unary_union.centroid.x]\n\n"
            
            code += "# Create map\n"
            code += "m = folium.Map(location=center, tiles='CartoDB positron', zoom_start=10)\n\n"
            
            code += "# Determine if we have points or polygons\n"
            code += "sample_geom = gdf.geometry.iloc[0]\n"
            code += "if sample_geom.geom_type == 'Point':\n"
            
            if color_col:
                code += "    # Add color scale for points\n"
                code += f"    if '{color_col}' in gdf.columns:\n"
                code += "        from branca.colormap import linear\n"
                code += f"        vmin = gdf['{color_col}'].min()\n"
                code += f"        vmax = gdf['{color_col}'].max()\n"
                code += "        colormap = linear.viridis.scale(vmin, vmax)\n"
                code += f"        colormap.caption = '{color_col}'\n"
                code += "        colormap.add_to(m)\n\n"
                
                code += "        # Add points with color\n"
                code += "        for _, row in gdf.iterrows():\n"
                code += "            # Create popup content\n"
                code += "            popup_content = []\n"
                code += "            for col in gdf.columns[:5]:\n"
                code += "                if col != 'geometry':\n"
                code += "                    popup_content.append(f\"{col}: {row[col]}\")\n"
                code += "            popup_html = \"<br>\".join(popup_content)\n\n"
                
                code += "            folium.CircleMarker(\n"
                code += "                location=[row.geometry.y, row.geometry.x],\n"
                code += "                radius=6,\n"
                code += f"                color=colormap(row['{color_col}']),\n"
                code += "                fill=True,\n"
                code += f"                fill_color=colormap(row['{color_col}']),\n"
                code += "                fill_opacity=0.7,\n"
                code += "                popup=folium.Popup(popup_html)\n"
                code += "            ).add_to(m)\n"
                code += "    else:\n"
                code += "        # Use marker cluster\n"
                code += "        marker_cluster = plugins.MarkerCluster().add_to(m)\n"
                code += "        for _, row in gdf.iterrows():\n"
                code += "            # Create popup content\n"
                code += "            popup_content = []\n"
                code += "            for col in gdf.columns[:5]:\n"
                code += "                if col != 'geometry':\n"
                code += "                    popup_content.append(f\"{col}: {row[col]}\")\n"
                code += "            popup_html = \"<br>\".join(popup_content)\n\n"
                
                code += "            folium.Marker(\n"
                code += "                location=[row.geometry.y, row.geometry.x],\n"
                code += "                popup=folium.Popup(popup_html)\n"
                code += "            ).add_to(marker_cluster)\n"
            else:
                code += "    # Use marker cluster\n"
                code += "    marker_cluster = plugins.MarkerCluster().add_to(m)\n"
                code += "    for _, row in gdf.iterrows():\n"
                code += "        # Create popup content\n"
                code += "        popup_content = []\n"
                code += "        for col in gdf.columns[:5]:\n"
                code += "            if col != 'geometry':\n"
                code += "                popup_content.append(f\"{col}: {row[col]}\")\n"
                code += "        popup_html = \"<br>\".join(popup_content)\n\n"
                
                code += "        folium.Marker(\n"
                code += "            location=[row.geometry.y, row.geometry.x],\n"
                code += "            popup=folium.Popup(popup_html)\n"
                code += "        ).add_to(marker_cluster)\n"
                
            code += "else:\n"
            code += "    # For polygons and other geometries\n"
            code += "    # Popup function\n"
            code += "    popup_function = lambda x: folium.Popup(\"<br>\".join([f\"{col}: {x['properties'][col]}\" for col in gdf.columns[:5] if col != 'geometry']))\n\n"
            
            code += "    # Add GeoJSON to map\n"
            code += "    if hasattr(gdf, '__geo_interface__'):\n"
            code += "        geo_data = gdf.__geo_interface__\n"
            code += "    else:\n"
            code += "        geo_data = json.loads(gdf.to_json())\n\n"
                
            if color_col:
                code += "    # Add color scale\n"
                code += f"    if '{color_col}' in gdf.columns:\n"
                code += "        from branca.colormap import linear\n"
                code += f"        vmin = gdf['{color_col}'].min()\n"
                code += f"        vmax = gdf['{color_col}'].max()\n"
                code += "        colormap = linear.viridis.scale(vmin, vmax)\n"
                code += f"        colormap.caption = '{color_col}'\n"
                code += "        colormap.add_to(m)\n\n"
                
                code += "        # Style function\n"
                code += "        style_function = lambda x: {\n"
                code += "            'fillColor': colormap(x['properties']['" + color_col + "']) if x['properties']['" + color_col + "'] is not None else 'gray',\n"
                code += "            'color': 'black',\n"
                code += "            'weight': 1,\n"
                code += "            'fillOpacity': 0.7\n"
                code += "        }\n"
                
                code += "        # Add choropleth\n"
                code += "        folium.GeoJson(\n"
                code += "            geo_data,\n"
                code += "            style_function=style_function,\n"
                code += "            popup=popup_function\n"
                code += "        ).add_to(m)\n"
                
                code += "        # Add legend\n"
                code += "        colormap.add_to(m)\n"
            else:
                code += "    # Add GeoJSON without styling\n"
                code += "    folium.GeoJson(\n"
                code += "        geo_data,\n"
                code += "        popup=popup_function\n"
                code += "    ).add_to(m)\n"
                
            code += "\n# Add layer control\n"
            code += "folium.LayerControl().add_to(m)\n\n"
            
            code += "# Display map\n"
            code += "m\n"
    else:
        code = "# Error: Missing required geometry information\n"
        code += "# Please provide either:\n"
        code += "# - Latitude and longitude columns, or\n" 
        code += "# - A geometry column containing GeoJSON or shapely geometries\n"
    
    return code 