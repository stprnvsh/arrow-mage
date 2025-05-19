# Project Overview: arrow-mage

This document provides an overview of the `arrow-mage` Streamlit application, its structure, and key components.

## Application Goal

`arrow-mage` is a data analysis platform with an intuitive interface and AI capabilities. It allows users to import, manage, query, and visualize datasets. It also integrates AI assistance for data exploration and exposes UDFs as tools via an MCP server.

## Project Structure

```
arrow-mage/
├── app.py                     # Main Streamlit application entry point (deprecated, see claude_ds_studio/)
├── Dockerfile                 # For containerization
├── README.md                  # Project README
├── requirements.txt           # Python dependencies
├── PROJECT_OVERVIEW.md        # This file
│
├── arrow-cache-mcp/           # Submodule/package for cache and MCP functionalities
│   └── src/
│       └── arrow_cache_mcp/
│           ├── __init__.py
│           ├── core.py        # Core cache logic, initialization, management
│           ├── loaders.py     # Data loading from various sources
│           ├── duckdb_ingest.py # DuckDB specific ingestion logic
│           ├── visualization.py # Plotting and visualization helpers
│           ├── ai.py          # AI interaction logic (e.g., ask_ai)
│           ├── mcp_server.py  # MCP server (if any, or planned)
│           └── utils.py       # Utility functions
│
├── claude_ds_studio/          # Main application GUI and pages
│   ├── claude_data_science_studio.py # Main Streamlit application entry point for the studio
│   └── pages/                   # Streamlit pages
│       ├── 1_🖼️_Data_Explorer.py
│       ├── 2_🔌_Connectors.py
│       ├── 3_💡_SQL_Workbench.py
│       ├── 4_🤖_AI_Analyst.py
│       └── 5_📈_Visualizer.py  (placeholder)
│
├── assets/                    # Static assets like images, logos
│   └── logo.png (example)
│
├── styles/                    # CSS files
│   └── main.css
│
├── tests/                     # Unit and integration tests
│
├── utils/                     # General utility functions (if any, separate from arrow-cache-mcp utils)
│
└── .arrow_cache_spill_studio/ # Default spill directory for the studio's cache (managed by claude_data_science_studio.py)
```

## Key Components and Files

### `claude_ds_studio/claude_data_science_studio.py`
-   **Purpose**: The main entry point for the Claude Data Science Studio Streamlit application.
-   **Functionality**:
    -   Sets overall page configuration, title, icon, layout.
    -   Configures application-wide logging.
    -   Manages `sys.path` for importing `arrow-cache-mcp`.
    -   Initializes and configures the global `ArrowCache` instance via `arrow_cache_mcp.core.get_arrow_cache`.
    -   Loads custom CSS styles.
    -   Configures the sidebar: AI model selection (provider, model, API key), cache status display, and cache management actions (clear cache).
    -   Displays a welcome page with an overview of the studio's features.
    -   Handles application cleanup, ensuring the Arrow Cache is closed gracefully on exit.

### `arrow-cache-mcp/`
This directory contains the core logic for data caching, AI interaction, and MCP functionalities.
-   `core.py`: Initializes and manages the `ArrowCache` instance. Includes functions like `get_arrow_cache`, `close_cache`, `clear_cache_files`, `import_data_directly`, `remove_dataset`, `get_datasets_list`, `get_memory_usage`. Defines `CacheNotInitializedError` and `get_dataset_metadata`.
-   `loaders.py`: Contains functions for loading data from various sources (URLs, local paths, uploads, databases) into the cache. Includes `load_dataset_from_url`, `load_dataset_from_path`, `SAMPLE_DATASETS`.
-   `duckdb_ingest.py`: Handles the specifics of ingesting data using DuckDB.
-   `visualization.py`: Provides functions for creating plots and rendering dataset information (e.g., `create_plot`, `render_dataset_card`).
-   `ai.py`: Contains logic for interacting with AI models, such as `ask_ai`, `get_ai_config`, and `get_supported_providers`. The `ask_ai` function expects `question` (positional), and optional keyword arguments `api_key`, `provider`, `model`, `conversation_history`, `max_retries`.
-   `utils.py`: Utility functions specific to the `arrow-cache-mcp` package.

### `claude_ds_studio/pages/`
Contains the individual Streamlit pages for different functionalities of the studio.
-   `1_🖼️_Data_Explorer.py`: Allows users to import (upload, URL, sample data), view, and manage datasets in the cache. Displays dataset cards with details and provides options to preview or remove datasets. Interacts with `arrow-cache-mcp` for data loading and metadata retrieval. The UI for displaying dataset cards has been refined.
-   `2_🔌_Connectors.py`: Provides UI for connecting to external data sources: PostgreSQL (table or query), Amazon S3 (CSV, Parquet), and a placeholder for Apache Arrow Flight. Uses `import_data_directly` and `load_dataset_from_path` from `arrow-cache-mcp`.
-   `3_💡_SQL_Workbench.py`: Enables users to execute SQL queries (DuckDB syntax) against cached datasets. Features dataset selection (now defaults to first dataset and updates query), a SQL input area, query execution, results display, and query plan view. Improved initial state and user experience.
-   `4_🤖_AI_Analyst.py`: Provides a chat interface for users to interact with an AI model (e.g., Claude) for data analysis. Users can select a dataset, ask questions in natural language, and receive text or SQL query responses. The AI can execute generated SQL queries and display results. Full UI and logic, including chat history and AI interaction, have been restored. Corrected call to `ask_ai` to match its definition and schema string generation. Adjusted response handling as `ask_ai` returns a full string.
-   `5_📈_Visualizer.py`: (Placeholder) Intended for creating various visualizations from data in the cache.

### `styles/main.css`
-   Custom CSS for styling the Streamlit application.

## Recent Changes
- Added placeholder `SAMPLE_DATASETS` to `arrow-cache-mcp/src/arrow_cache_mcp/loaders.py`.
- Added placeholder `display_conversation_history_dict` to `arrow-cache-mcp/src/arrow_cache_mcp/visualization.py` (though AI Analyst now uses `st.chat_message`).
- Added placeholder `get_dataset_metadata` and `CacheNotInitializedError` class to `arrow-cache-mcp/src/arrow_cache_mcp/core.py`.
- Fixed `AttributeError: 'duckdb.duckdb.DuckDBPyConnection' object has no attribute 'is_closed'` in `arrow-cache-mcp/src/arrow_cache_mcp/core.py` by changing to `con.closed`.
- Fixed `AttributeError: 'pyarrow.lib.Table' object has no attribute 'empty'` in `claude_ds_studio/pages/1_🖼️_Data_Explorer.py` by using `table.num_rows > 0` and `to_pandas()` for `st.dataframe`.
- Refined the UI for displaying dataset cards in `1_🖼️_Data_Explorer.py` to correctly show names and details.
- Restored full UI and logic for `3_💡_SQL_Workbench.py` and `4_🤖_AI_Analyst.py`.
- Corrected `TypeError` in `4_🤖_AI_Analyst.py`:
    - Removed invalid `show_metadata` from `schema.to_string()`.
    - Aligned `ask_ai` call parameters with its definition in `arrow-cache-mcp/ai.py`.
    - Adjusted AI response handling in `4_🤖_AI_Analyst.py` as `ask_ai` returns a full string.
- Improved initial state and query update logic in `3_💡_SQL_Workbench.py` to prevent errors with placeholder table names and enhance user experience.

This overview will be updated as the project evolves. 