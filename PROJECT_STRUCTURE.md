# Project Structure: Data Mage

This document outlines the structure of the Data Mage Streamlit application.

## Root Directory (`/`)

-   `app.py`: Main Streamlit application entry point. Initializes the app, sets up page config, logging, and global components like the Arrow Cache.
-   `PROJECT_STRUCTURE.md`: This file. Tracks the project layout and components.
-   `requirements.txt` (Expected): Lists Python dependencies.
-   `styles/`: Directory for CSS files.
    -   `main.css`: Main stylesheet for the application.
-   `utils/`: Directory for utility functions and modules shared across the application.
-   `arrow-cache-mcp/`: Submodule or directory containing the `arrow-cache-mcp` package for cache management and MCP server functionalities.
    -   `src/arrow_cache_mcp/`: Source code for the `arrow-cache-mcp` library.
-   `.arrow_cache_spill_streamlit/` (Runtime): Directory used by Arrow Cache for spilling data to disk.
-   `claude_ds_studio/` : Directory potentially containing the main application logic or a previous version of it.
    - `claude_data_science_studio.py`: Main application file for "Claude Data Science Studio".
    - `pages/`: Standard Streamlit directory for different app pages.
        - `1_💾_Datasets.py` (Assumed): Page for dataset management (upload, view, etc.).
        - `2_📊_Visualize.py` (Assumed): Page for data visualization.
        - `3_💡_SQL_Workbench.py` (Confirmed from logs): Page for executing SQL queries against loaded datasets.
        - `4_🤖_AI_Assistant.py` (Assumed): Page for interacting with an AI assistant for data analysis.
        - `5_⚙️_MCP_Server_Admin.py` (Assumed): Page for managing the MCP server.
        - `6_🧑‍💻_Developer_Studio.py` (New): Page for developers to write and execute Python scripts with AI support.

## Key Components and Flow

1.  **`app.py`**:
    *   Initializes Streamlit page configuration.
    *   Sets up paths for `arrow-cache-mcp` and `utils`.
    *   Initializes the `ArrowCache` instance.
    *   Loads main CSS.
    *   Displays the main app title and overview.

2.  **Pages (`claude_ds_studio/pages/`)**:
    *   Each `.py` file in this directory corresponds to a navigable page in the Streamlit sidebar.
    *   Pages access the globally initialized `cache` object from `app.py` (or should be updated to do so if they have their own initializations).

3.  **`arrow-cache-mcp`**:
    *   Provides the core caching (`ArrowCache`) and data interaction functionalities.
    *   Likely uses DuckDB for SQL querying on Arrow tables.
    *   Datasets are loaded into the cache and become queryable tables.

4.  **Developer Studio (`6_🧑‍💻_Developer_Studio.py`)**:
    *   Allows direct Python scripting.
    *   Scripts can import and use `streamlit as st`, access the `cache` (e.g., `st.session_state.arrow_cache`), and perform custom data manipulations.
    *   Output from scripts (text, dataframes, plots) will be rendered on the page.

## Notes on Cross-Dataset Querying

-   The `ArrowCache` (via DuckDB) supports SQL queries across any tables (datasets) registered with it.
-   Table names in SQL queries will typically be prefixed (e.g., `_cache_my_dataset_csv`).
-   The SQL Workbench and AI Assistant should be made aware of all available tables to facilitate cross-dataset queries.

## Future Enhancements (Ideas)
- `README.md`: A general readme for the project.
- Comprehensive `requirements.txt` at the root.
- Dockerfile for containerization.
- More detailed documentation for each module/page. 