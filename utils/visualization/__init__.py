"""
Visualization utilities for Data Mage application
"""

# Core visualization utilities
from utils.visualization.core import (
    prepare_data_for_visualization,
    infer_column_types,
    suggest_visualization,
    create_plotly_figure,
    render_plotly_in_streamlit,
    generate_visualization_code,
    matplotlib_to_base64
)

# Geospatial visualization utilities
from utils.visualization.geospatial import (
    detect_geometry_column,
    convert_to_geodataframe,
    create_plotly_map,
    create_folium_map,
    render_folium_map,
    generate_map_code,
    MAPBOX_STYLES
)

# AI-powered visualization utilities
from utils.visualization.ai import (
    get_ai_visualization_suggestion,
    run_sql_for_visualization,
    execute_data_transformation
)

# AI-SQL integration utilities
from utils.visualization.ai_sql import (
    extract_sql_from_ai_response,
    generate_visualization_sql,
    create_visualization_from_natural_language,
    visualize_ai_query_result
)

# Streamlit components
from utils.visualization.components import (
    render_visualization_sidebar,
    render_visualization,
    display_ai_visualization_suggestion,
    visualize_sql_result
)

# CSS styles
from utils.visualization.styles import (
    load_visualization_styles,
    apply_apple_inspired_theme
)

# Define package exports
__all__ = [
    # Core visualization
    'prepare_data_for_visualization',
    'infer_column_types',
    'suggest_visualization',
    'create_plotly_figure',
    'render_plotly_in_streamlit',
    'generate_visualization_code',
    'matplotlib_to_base64',
    
    # Geospatial visualization
    'detect_geometry_column',
    'convert_to_geodataframe',
    'create_plotly_map',
    'create_folium_map',
    'render_folium_map',
    'generate_map_code',
    'MAPBOX_STYLES',
    
    # AI visualization
    'get_ai_visualization_suggestion',
    'run_sql_for_visualization',
    'execute_data_transformation',
    
    # AI-SQL integration
    'extract_sql_from_ai_response',
    'generate_visualization_sql',
    'create_visualization_from_natural_language',
    'visualize_ai_query_result',
    
    # Streamlit components
    'render_visualization_sidebar',
    'render_visualization',
    'display_ai_visualization_suggestion',
    'visualize_sql_result',
    
    # Styles
    'load_visualization_styles',
    'apply_apple_inspired_theme'
] 