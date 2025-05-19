# Data Mage Visualization

The visualization module of Data Mage provides powerful, AI-driven data visualization capabilities. This component allows you to create insightful visualizations from your datasets using both manual configuration and natural language.

## Features

### 🧠 AI-Powered Visualization

- Describe what you want to visualize in natural language
- AI automatically generates SQL queries to prepare your data
- AI suggests appropriate visualization types based on your data
- Supports data transformations and aggregations

### 📊 Manual Visualization Builder

- Full control over visualization settings
- Interactive customization of charts and maps
- Smart column type detection and suggestions
- Export visualization code for external use

### 🗺️ Geospatial Visualization

- Automatic detection of geometry columns
- Support for GeoJSON data
- Interactive maps with Plotly and Folium
- Visualization of points, lines, and polygons

### 🔄 SQL Query Visualization

- Direct SQL querying for visualization
- Natural language to SQL generation
- Integration with the AI assistant

## Using the Visualization Module

### AI-Powered Visualization

1. Select a dataset
2. Describe what you want to visualize in natural language
3. Click "Generate Visualization"
4. Customize the generated visualization if needed

Example natural language queries:
- "Show me the distribution of trip distances"
- "Compare the average fare amount by payment type"
- "Create a map of pickup locations colored by fare amount"

### Manual Visualization

1. Select a dataset and load it
2. Choose columns for x-axis, y-axis, and optional color dimension
3. Select visualization type
4. Customize appearance options
5. Update the visualization

### SQL Query Visualization

1. Enter a SQL query directly or generate one with AI
2. Execute the query
3. Customize the visualization of query results

## Installation

To use all visualization features, install the required dependencies:

```bash
pip install -r requirements-visualization.txt
```

For geospatial features, ensure you have these packages installed:

```bash
pip install geopandas folium streamlit-folium
```

## Integrating with AI

The visualization module integrates with the AI assistant, allowing you to:

1. Generate SQL from natural language
2. Get visualization suggestions
3. Create complex data transformations
4. Analyze data and create appropriate visualizations

To use AI features, make sure you've configured your API key in the application settings. 