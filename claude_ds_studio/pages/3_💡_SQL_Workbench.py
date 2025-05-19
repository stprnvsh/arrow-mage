import streamlit as st
import os
import sys
import logging
import pandas as pd
import time
import re

# Configure logging for this page
logger = logging.getLogger(__name__)

# --- Add workspace root to sys.path for shared modules ---
script_dir = os.path.dirname(os.path.abspath(__file__))
page_dir = os.path.abspath(script_dir)
app_root_dir = os.path.abspath(os.path.join(page_dir, '..'))
workspace_root = os.path.abspath(os.path.join(app_root_dir, '..'))

if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
    logger.info(f"SQL Workbench: Added workspace root {workspace_root} to sys.path")
if app_root_dir not in sys.path:
    sys.path.insert(0, app_root_dir)
    logger.info(f"SQL Workbench: Added app root {app_root_dir} to sys.path")

# --- Import from arrow-cache-mcp --- 
try:
    from arrow_cache_mcp.core import (
        get_arrow_cache,
        get_datasets_list
    )
    cache = get_arrow_cache()
    logger.info("SQL Workbench: Successfully accessed Arrow Cache.")
except ImportError as e:
    logger.error(f"SQL Workbench: Failed to import from arrow-cache-mcp: {e}", exc_info=True)
    st.error(f"Error loading required modules: {e}. Please ensure 'arrow-cache-mcp' is correctly installed.")
    st.stop()
except Exception as e:
    logger.error(f"SQL Workbench: An unexpected error occurred: {e}", exc_info=True)
    st.error(f"An unexpected error occurred: {e}")
    st.stop()

# --- Page Configuration & Title ---
st.title("💡 SQL Workbench")
st.markdown("Execute SQL queries against your cached datasets. Use standard SQL syntax supported by DuckDB.")

# --- Get Datasets ---
datasets_list = get_datasets_list()
dataset_names = [ds['name'] for ds in datasets_list if ds.get('name')]

# --- Initialize session state for SQL query and selected dataset ---
if not dataset_names:
    # Initialize with placeholder if no datasets are loaded
    if "sql_workbench_query" not in st.session_state:
        st.session_state.sql_workbench_query = "SELECT * FROM <your_dataset_name> LIMIT 100;"
    if "sql_workbench_selected_dataset" not in st.session_state:
        st.session_state.sql_workbench_selected_dataset = None
else:
    # If datasets are available, initialize with the first one
    if "sql_workbench_selected_dataset" not in st.session_state or st.session_state.sql_workbench_selected_dataset not in dataset_names:
        st.session_state.sql_workbench_selected_dataset = dataset_names[0]
    
    default_query = f"SELECT * FROM {st.session_state.sql_workbench_selected_dataset} LIMIT 100;"
    if "sql_workbench_query" not in st.session_state or st.session_state.sql_workbench_query == "SELECT * FROM <your_dataset_name> LIMIT 100;":
        st.session_state.sql_workbench_query = default_query

# --- UI Elements ---
if not dataset_names:
    st.warning("No datasets found in cache. Please load a dataset via the Data Explorer page first.")
    st.stop()

# Determine the index for the selectbox based on current session state
# Add an empty option for user to clear selection or type manually
display_dataset_options = ["-- Select a Dataset --"] + dataset_names
current_selection_in_session = st.session_state.get('sql_workbench_selected_dataset')
selectbox_index = 0 # Default to "-- Select a Dataset --"
if current_selection_in_session and current_selection_in_session in dataset_names:
    try:
        selectbox_index = display_dataset_options.index(current_selection_in_session)
    except ValueError:
        selectbox_index = 0 # Should not happen if checks are correct

# Dataset selection
selected_option_from_ui = st.selectbox(
    "Select a dataset to query (or type table name directly in SQL):",
    options=display_dataset_options,
    index=selectbox_index,
    key="sql_workbench_dataset_selector_ui",
    help="Selecting a dataset here can help auto-fill the table name in your query."
)

# Handle selection change from UI
if selected_option_from_ui != "-- Select a Dataset --":
    if st.session_state.sql_workbench_selected_dataset != selected_option_from_ui:
        st.session_state.sql_workbench_selected_dataset = selected_option_from_ui
        # Update query only if it's still the placeholder or matches the old default for a different table
        # This avoids overwriting a user's custom query just because they re-selected a table
        old_table_name_in_query = re.search(r"FROM\s+([\w_]+)", st.session_state.sql_workbench_query)
        is_placeholder_query = "<your_dataset_name>" in st.session_state.sql_workbench_query or st.session_state.sql_workbench_query.strip() == "SELECT * FROM <your_dataset_name> LIMIT 100;"
        is_default_for_other_table = old_table_name_in_query and old_table_name_in_query.group(1) != selected_option_from_ui

        if is_placeholder_query or is_default_for_other_table:
            st.session_state.sql_workbench_query = f"SELECT * FROM {selected_option_from_ui} LIMIT 100;"
        st.rerun() # Rerun to update the text_area with the new query if changed
elif selected_option_from_ui == "-- Select a Dataset --" and st.session_state.sql_workbench_selected_dataset is not None:
    # User deselected a dataset, revert query to placeholder if it was a default one
    # st.session_state.sql_workbench_selected_dataset = None # Keep last valid selection for now or clear?
    # if st.session_state.sql_workbench_query.startswith(f"SELECT * FROM {st.session_state.sql_workbench_selected_dataset}"):
    #     st.session_state.sql_workbench_query = "SELECT * FROM <your_dataset_name> LIMIT 100;"
    # st.rerun()
    pass # Allow manual typing for now if user deselects

# SQL Query Input
st.subheader("SQL Query")
# Ensure query_input uses the latest from session_state after potential updates
query_input_val = st.session_state.sql_workbench_query
query_input = st.text_area(
    "Enter your SQL query:",
    value=query_input_val,
    height=200,
    key="sql_query_area_workbench",
    help="Cache tables are typically named like `_cache_your_dataset_name` or directly `your_dataset_name` if the cache configuration registers them without prefix."
)
# Update session state if user manually changes the text area
if query_input != query_input_val:
    st.session_state.sql_workbench_query = query_input

# Execute Query Button
execute_button = st.button("🚀 Execute Query", type="primary", key="execute_sql_workbench")

# --- Query Execution and Display ---
if execute_button and query_input and query_input.strip() != "SELECT * FROM <your_dataset_name> LIMIT 100;":
    actual_query = query_input
    # Smartly prefix with _cache_ if not already present and the unprefixed version doesn't exist as a direct view/table
    # but the _cache_ version does.
    # This tries to infer the correct table name if the user types `my_table` instead of `_cache_my_table`
    try:
        potential_table_name_match = re.search(r"FROM\s+([a-zA-Z_][\w]*)", actual_query, re.IGNORECASE)
        if potential_table_name_match:
            table_name_in_query = potential_table_name_match.group(1)
            prefixed_table_name = f"_cache_{table_name_in_query}"
            # Check if the non-prefixed version is NOT a known table/view, but the prefixed one IS
            # This requires a way to check if a table is registered without querying it directly (e.g. cache.is_view() or similar)
            # For now, we assume direct query, DuckDB will error if table not found.
            # A more robust solution might involve checking get_datasets_list()
            is_raw_name_in_cache = any(ds['name'] == table_name_in_query for ds in datasets_list)
            if not is_raw_name_in_cache and any(ds['name'] == prefixed_table_name.replace("_cache_", "") for ds in datasets_list):
                 if not table_name_in_query.startswith("_cache_"):
                    # This is a heuristic. If `my_data` is typed but `_cache_my_data` exists (and `my_data` doesn't as a direct cache key)
                    # we could offer to change it, or just assume it for now.
                    # For simplicity, we'll assume users will learn to use the prefix or the cache is smart.
                    pass 
    except Exception as e:
        logger.warning(f"Regex issue for table name extraction: {e}")

    with st.spinner("Executing query..."):
        try:
            start_time = time.time()
            logger.info(f"Executing SQL Workbench query: {actual_query}")
            results_table = cache.query(actual_query)
            execution_time = time.time() - start_time

            if results_table is not None and results_table.num_rows > 0:
                results_df = results_table.to_pandas()
                st.success(f"Query executed successfully in {execution_time:.2f} seconds. Fetched {len(results_df)} rows.")
                st.dataframe(results_df, use_container_width=True)

                with st.expander("📊 View Query Plan (EXPLAIN)"):
                    try:
                        plan_table = cache.query(f"EXPLAIN {actual_query}")
                        plan_df = plan_table.to_pandas()
                        if not plan_df.empty:
                            plan_str = "\n".join(plan_df.iloc[:, 0].astype(str).tolist())
                            st.text_area("Query Plan:", value=plan_str, height=300, disabled=True)
                        else:
                            st.info("Could not retrieve query plan, or plan was empty.")
                    except Exception as plan_e:
                        logger.error(f"Error getting query plan: {plan_e}", exc_info=True)
                        st.warning(f"Could not retrieve query plan: {plan_e}")
            elif results_table is not None and results_table.num_rows == 0:
                 st.info(f"Query executed successfully in {execution_time:.2f} seconds, but returned no rows.")
            else: # Should ideally not happen if cache.query raises error for bad queries
                st.warning("Query executed, but something went wrong or it returned an unexpected result (e.g., None table).")

        except Exception as e:
            logger.error(f"Error executing SQL query: {actual_query} - {e}", exc_info=True)
            st.error(f"Error executing query: {e}")
            st.markdown("##### Common Issues & Tips:")
            st.markdown("- **Table not found?** Ensure your dataset name is correct. Cached datasets are often prefixed (e.g., `_cache_my_data`). Check Data Explorer for exact names.")
            st.markdown("- **Syntax error?** Double-check your SQL syntax for DuckDB.")

elif execute_button and (not query_input or query_input.strip() == "SELECT * FROM <your_dataset_name> LIMIT 100;"):
    st.warning("Please enter a valid SQL query or select a dataset to populate a default query.")

if not dataset_names: # Redundant check, but good for safety if st.stop() was bypassed
    st.caption("Load datasets in the Data Explorer to use the SQL Workbench.")
else:
    st.markdown("---")
    st.caption("SQL Workbench uses DuckDB. Tables are typically named as they appear in Data Explorer (e.g., `my_dataset` or `_cache_my_dataset`).")

logger.info("SQL Workbench page loaded and UI rendered.") 