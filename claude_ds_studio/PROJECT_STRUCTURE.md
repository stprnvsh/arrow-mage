# Claude Data Science Studio - Project Structure

Last Updated: $(date +%Y-%m-%d\ %H:%M:%S)

## Overview

This document outlines the file and directory structure for the Claude Data Science Studio application.
The application is designed as a multi-page Streamlit application, with core functionalities provided by the `arrow-cache-mcp` library.

## Main Application Directory: `claude_ds_studio/`

Located at: `/Users/pranavsateesh/arrow-mage/claude_ds_studio/`

This directory contains the main application script and its associated pages.

### Files at `claude_ds_studio/` root:

-   **`claude_data_science_studio.py`**: 
    -   The main entry point for the Streamlit application.
    -   Handles global configurations (page settings, logging, CSS).
    -   Initializes the Arrow Cache.
    -   Defines the main sidebar structure (AI model config, cache management).
    -   Displays a welcome page.

### Subdirectories in `claude_ds_studio/`:

-   **`pages/`**: 
    -   Contains individual Streamlit pages for different application modules.
    -   Files are named like `1_ICON_Page_Name.py` for ordering and display in the sidebar.
    -   Current Pages:
        -   `1_🖼️_Data_Explorer.py`: For dataset uploading (file, URL, samples), listing, previewing, and management.
        -   `2_🔌_Connectors.py`: UI for connecting to PostgreSQL, S3 (basic), and Arrow Flight (placeholder).
        -   `3_💡_SQL_Workbench.py`: Interface for running SQL queries against cached datasets, with history and CSV download.
        -   `4_🤖_AI_Analyst.py`: Chat interface with configured AI (Claude, OpenAI, etc.) for data analysis, query generation, and insights, using `ask_ai` from `arrow-cache-mcp`.
        -   *(More pages like Connectors, SQL Workbench, AI Analyst, Visualizer to be added here)*

## Workspace Root: `/Users/pranavsateesh/arrow-mage/`

This is the parent directory containing the `claude_ds_studio` application, as well as other related projects or libraries (like `arrow-cache-mcp`).

### Key items at Workspace Root relevant to this app:

-   **`arrow-cache-mcp/`** (Expected):
    -   The directory containing the `arrow-cache-mcp` library, which provides core caching, data loading, AI interaction, and visualization functionalities.
    -   The `claude_data_science_studio.py` adds `arrow-cache-mcp/src` to `sys.path`.
-   **`.arrow_cache_spill_studio/`** (Created by the app):
    -   The spill directory used by Arrow Cache for this specific application if data exceeds memory limits.
-   **`styles/`** (Expected, if custom CSS is used):
    -   Contains `main.css` (or other CSS files) loaded by `claude_data_science_studio.py` for custom styling.
    -   Example: `styles/main.css`
-   **`assets/`** (Optional, for images like logos):
    -   Can contain assets like `logo.png` used in the sidebar.
    -   Example: `assets/logo.png`

## Running the Application

1.  Navigate to the workspace root in your terminal:
    ```bash
    cd /Users/pranavsateesh/arrow-mage
    ```
2.  Run the Streamlit application using the main script inside `claude_ds_studio`:
    ```bash
    streamlit run claude_ds_studio/claude_data_science_studio.py
    ```

## Next Steps / Future Pages Planned

-   **`2_🔌_Connectors.py`**: UI for connecting to S3, PostgreSQL, Arrow Flight. (S3 and Flight need robust backend in mcp)
-   **`3_💡_SQL_Workbench.py`**: Interface for running SQL queries against cached datasets.
-   **`4_🤖_AI_Analyst.py`**: Chat interface with Claude for data analysis, query generation, and insights.
-   **`5_📊_Visualizer.py`**: Tools for creating various plots and charts from the data. (Pending)

---
*This document will be updated as the project evolves.* 