import streamlit as st
import os
import sys
import logging
import pandas as pd # For displaying dataframes and advanced options
import time

# Configure logging for this page
logger = logging.getLogger(__name__)

# --- Add workspace root to sys.path to access shared modules like arrow_cache_mcp --- 
# This assumes pages are in a subdirectory of where the main app file is.
script_dir = os.path.dirname(os.path.abspath(__file__))
# pages -> claude_ds_studio -> workspace_root
page_dir = os.path.abspath(script_dir) # .../claude_ds_studio/pages
app_root_dir = os.path.abspath(os.path.join(page_dir, '..')) # .../claude_ds_studio
workspace_root = os.path.abspath(os.path.join(app_root_dir, '..')) # .../

if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
    logger.info(f"Data Explorer: Added workspace root {workspace_root} to sys.path")

if app_root_dir not in sys.path:
    sys.path.insert(0, app_root_dir)
    logger.info(f"Data Explorer: Added app root {app_root_dir} to sys.path")

# --- Import from arrow-cache-mcp --- 
try:
    from arrow_cache_mcp.core import (
        get_arrow_cache,
        get_datasets_list,
        remove_dataset
        # get_dataset_metadata was removed as it's not in arrow_cache_mcp.core
        # CacheNotInitializedError was removed, will rely on general Exception for init issues
    )
    from arrow_cache_mcp.loaders import (
        load_dataset_from_upload,
        load_dataset_from_url,
        SUPPORTED_FORMATS, # Dictionary of supported formats and their details
        SAMPLE_DATASETS # Dictionary of sample datasets for easy loading
    )
    from arrow_cache_mcp.visualization import (
        render_dataset_card # Using the one from the library
    )
    from arrow_cache_mcp.utils import (
        clean_dataset_name,
        get_size_display # For human-readable sizes
    )
    
    cache = get_arrow_cache() 
    logger.info("Data Explorer: Successfully accessed Arrow Cache and imported mcp components.")

except ImportError as e:
    logger.error(f"Data Explorer: Failed to import from arrow-cache-mcp: {e}", exc_info=True)
    st.error(f"Error loading required modules: {e}. Please ensure 'arrow-cache-mcp' is correctly installed.")
    st.stop()
# Removed specific CacheNotInitializedError, general Exception will catch init issues
except Exception as e:
    logger.error(f"Data Explorer: An unexpected error occurred during imports or cache access: {e}", exc_info=True)
    st.error(f"An unexpected error occurred: {e}")
    st.stop()

# --- Page Configuration & Title --- 
st.set_page_config(page_title="Data Explorer - Claude Studio", layout="wide") # Can be set if needed, but main app does it too
st.title("🖼️ Data Explorer")
st.markdown("Manage, upload, and preview your datasets. Datasets loaded here are available across the studio.")

# --- Helper function to remove dataset and update UI ---
def remove_and_update_ui(dataset_name):
    """Remove a dataset and update the UI."""
    success, message = remove_dataset(dataset_name) # From arrow_cache_mcp.core
    if success:
        st.success(message)
        if 'studio_selected_dataset' in st.session_state and st.session_state.studio_selected_dataset == dataset_name:
            st.session_state.pop('studio_selected_dataset')
    else:
        st.error(message)
    st.rerun()

# --- UI: Dataset Import Section --- 
with st.expander("📥 Import New Dataset", expanded=True):
    import_tab1, import_tab2, import_tab3 = st.tabs(["⬆️ Upload File", "🔗 From URL", "🧪 Sample Data"]) #, "☁️ From S3", "🗄️ From PostgreSQL", "✈️ From Arrow Flight"]) Add these later

    # Upload File Tab
    with import_tab1:
        st.subheader("Upload Local File")
        uploaded_file = st.file_uploader(
            "Choose a file", 
            type=[ext.lstrip('.') for fmt_info in SUPPORTED_FORMATS.values() for ext in fmt_info.get('extensions', [])],
            key="explorer_file_uploader"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            upload_dataset_name = st.text_input(
                "Dataset Name (optional):", 
                help="Leave blank to use filename. Will be cleaned.", 
                key="explorer_upload_name"
            )
        with col2:
            upload_format_options = ["Auto-detect"] + list(SUPPORTED_FORMATS.keys())
            upload_file_format = st.selectbox(
                "Format (optional):", 
                upload_format_options,
                key="explorer_upload_format",
                help="'Auto-detect' will guess from file extension."
            )
        
        # Advanced options (e.g., CSV delimiter)
        advanced_args_upload = {}
        if upload_file_format == "csv" or (upload_file_format == "Auto-detect" and uploaded_file and uploaded_file.name.lower().endswith('.csv')):
            st.markdown("###### CSV Options")
            advanced_args_upload["delimiter"] = st.text_input("Delimiter:", value=",", key="upload_csv_delimiter")
            has_header = st.checkbox("File has header row", value=True, key="upload_csv_header")
            if not has_header:
                advanced_args_upload["header"] = None # Pass None to pandas to signify no header
            # infer_schema = st.checkbox("Infer schema (slower for large files)", value=False, key="upload_csv_infer_schema")
            # if infer_schema:
            #     advanced_args_upload["infer_schema"] = True

        if st.button("Load Uploaded Dataset", key="explorer_upload_button", type="primary") and uploaded_file:
            final_upload_name = clean_dataset_name(upload_dataset_name if upload_dataset_name else uploaded_file.name)
            format_to_use = None if upload_file_format == "Auto-detect" else upload_file_format
            
            with st.spinner(f"Loading '{final_upload_name}' from uploaded file..."):
                success, result = load_dataset_from_upload(
                    uploaded_file,
                    dataset_name=final_upload_name,
                    format=format_to_use,
                    **advanced_args_upload
                )
            if success:
                st.success(f"Dataset '{result.get('name', final_upload_name)}' loaded successfully!")
                st.json(result) # Show metadata of loaded dataset
                st.rerun()
            else:
                st.error(f"Failed to load from upload: {result}")

    # From URL Tab
    with import_tab2:
        st.subheader("Import from URL")
        url = st.text_input("Dataset URL:", placeholder="https://example.com/data.csv", key="explorer_url_input")
        
        col1, col2 = st.columns(2)
        with col1:
            url_dataset_name = st.text_input(
                "Dataset Name (optional):", 
                key="explorer_url_name", 
                help="Leave blank to use filename from URL. Will be cleaned."
            )
        with col2:
            url_format_options = ["Auto-detect"] + list(SUPPORTED_FORMATS.keys())
            url_file_format = st.selectbox(
                "Format (optional):", 
                url_format_options,
                key="explorer_url_format",
                help="'Auto-detect' will guess from URL path extension."
            )

        advanced_args_url = {}
        if url_file_format == "csv" or (url_file_format == "Auto-detect" and url and url.lower().endswith('.csv')):
            st.markdown("###### CSV Options")
            advanced_args_url["delimiter"] = st.text_input("Delimiter:", value=",", key="url_csv_delimiter")
            has_header_url = st.checkbox("File has header row", value=True, key="url_csv_header")
            if not has_header_url:
                advanced_args_url["header"] = None

        if st.button("Load from URL", key="explorer_url_button", type="primary") and url:
            final_url_name = clean_dataset_name(url_dataset_name if url_dataset_name else os.path.basename(url))
            format_to_use_url = None if url_file_format == "Auto-detect" else url_file_format

            with st.spinner(f"Loading '{final_url_name}' from URL..."):
                success, result = load_dataset_from_url(
                    url,
                    dataset_name=final_url_name,
                    format=format_to_use_url,
                    **advanced_args_url
                )
            if success:
                st.success(f"Dataset '{result.get('name', final_url_name)}' loaded successfully from URL!")
                st.json(result)
                st.rerun()
            else:
                st.error(f"Failed to load from URL: {result}")

    # Sample Data Tab
    with import_tab3:
        st.subheader("Load Sample Dataset")
        if not SAMPLE_DATASETS:
            st.info("No sample datasets are currently configured in the 'arrow-cache-mcp' library.")
        else:
            sample_options = list(SAMPLE_DATASETS.keys())
            selected_sample_key = st.selectbox("Choose a sample dataset:", sample_options, key="explorer_sample_select")
            
            sample_dataset_name_default = clean_dataset_name(selected_sample_key)
            sample_dataset_name = st.text_input(
                "Dataset Name (as it will appear in cache):", 
                value=sample_dataset_name_default,
                key="explorer_sample_name"
            )

            if st.button("Load Sample Dataset", key="explorer_sample_button", type="primary") and selected_sample_key:
                sample_info = SAMPLE_DATASETS[selected_sample_key]
                final_sample_name = clean_dataset_name(sample_dataset_name if sample_dataset_name else selected_sample_key)

                with st.spinner(f"Loading sample dataset '{final_sample_name}'..."):
                    # load_dataset_from_url can handle this directly if sample_info contains 'url' and 'format'
                    success, result = load_dataset_from_url(
                        sample_info['url'], 
                        dataset_name=final_sample_name, 
                        format=sample_info.get('format') # Format might be optional if guessable
                    )
                if success:
                    st.success(f"Sample dataset '{result.get('name', final_sample_name)}' loaded!")
                    st.json(result)
                    st.rerun()
                else:
                    st.error(f"Failed to load sample dataset: {result}")

# --- UI: List of Loaded Datasets --- 
st.markdown("---")
st.header("🗂️ Cached Datasets")

loaded_datasets = get_datasets_list() # From arrow_cache_mcp.core

if not loaded_datasets:
    st.info("No datasets loaded yet. Use the import options above to add data to the cache.")
else:
    # Search/filter bar for datasets
    search_term = st.text_input("Search datasets by name:", key="explorer_search_datasets").lower()
    
    filtered_datasets = [
        ds for ds in loaded_datasets 
        if search_term in ds.get('name', '').lower() 
           or search_term in ds.get('metadata', {}).get('source', '').lower()
    ]
    
    if not filtered_datasets and search_term:
        st.warning(f"No datasets found matching '{search_term}'.")
    elif not filtered_datasets:
         st.info("No datasets loaded yet.") # Should not happen if loaded_datasets is true

    # Display datasets as cards using the visualization component from mcp
    for dataset_info_raw in filtered_datasets:
        card_data = render_dataset_card(dataset_info_raw) # Get the formatted data

        with st.container():
            st.subheader(card_data.get("name", "Unknown Dataset"))
            
            col1_details, col2_details, col3_details = st.columns(3)
            with col1_details:
                st.metric("Size", card_data.get("size", "N/A"))
            with col2_details:
                st.metric("Rows", card_data.get("row_count", "N/A"))
            with col3_details:
                st.metric("Columns", card_data.get("column_count", "N/A"))

            # Expander for more metadata
            expander_title = "More Details"
            if card_data.get("is_geospatial"):
                expander_title += " (Geospatial)"
            
            with st.expander(expander_title):
                if card_data.get("metadata"):
                    st.markdown(f"**Source:** `{card_data['metadata'].get('source', 'N/A')}`")
                    st.markdown(f"**Format:** {card_data['metadata'].get('format', 'N/A')}")
                    if card_data['metadata'].get('created_at'):
                         st.markdown(f"**Created:** {card_data['metadata']['created_at']}")
                
                if card_data.get("is_geospatial"):
                    st.markdown(f"**CRS:** {card_data.get('crs', 'N/A')}")
                    st.markdown(f"**Geometry Column:** {card_data.get('geometry_column', 'N/A')}")

                if card_data.get("columns"):
                    column_preview_data = []
                    for col_prev in card_data["columns"][:5]: # Show top 5 columns with types
                        column_preview_data.append(f"- **{col_prev.get('name')}**: `{col_prev.get('type', 'unknown')}`")
                    if card_data["columns"]:
                        st.markdown("##### Column Preview (Top 5):")
                        st.markdown("\n".join(column_preview_data))
                    if len(card_data["columns"]) > 5:
                        st.caption(f"...and {len(card_data['columns']) - 5} more columns.")
            
            # Action buttons
            button_cols = st.columns([0.7, 0.15, 0.15]) # Adjusted ratio
            with button_cols[1]:
                if st.button("🔍 View", key=f"view_{dataset_info_raw['name']}_explorer", help="View schema and preview data"):
                    st.session_state.studio_selected_dataset = dataset_info_raw['name']
                    st.rerun() # Rerun to show the selected dataset details section
            with button_cols[2]:
                if st.button("❌ Remove", key=f"remove_{dataset_info_raw['name']}_explorer", help="Remove dataset from cache"):
                    remove_and_update_ui(dataset_info_raw['name']) # This already calls rerun
            st.markdown("<hr style='margin-top: 0.5rem; margin-bottom: 0.5rem;'>", unsafe_allow_html=True)


# --- UI: Selected Dataset Details & Preview --- 
if 'studio_selected_dataset' in st.session_state and st.session_state.studio_selected_dataset:
    selected_name = st.session_state.studio_selected_dataset
    
    if not cache.contains(selected_name):
        st.warning(f"Dataset '{selected_name}' no longer exists in cache. It might have been removed.")
        st.session_state.pop('studio_selected_dataset') # Clear selection
        st.rerun()
    else:
        st.markdown("---")
        st.subheader(f"🔎 Details for: '{selected_name}'")
        
        try:
            # Get dataset sample and full metadata
            data_sample = cache.get(selected_name, limit=20) # Get a sample of rows
            metadata = cache.get_metadata(selected_name) # Use direct cache.get_metadata()
            
            if metadata is None:
                st.error(f"Could not retrieve metadata for dataset '{selected_name}'. It might have been removed or an error occurred.")
                st.stop()
            
            # Display metadata in a structured way
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Rows", f"{metadata.get('row_count', 'N/A'):,}" if isinstance(metadata.get('row_count'), int) else metadata.get('row_count', 'N/A'))
                st.metric("Total Columns", metadata.get('column_count', len(metadata.get('columns',[]))))
            with col2:
                st.metric("In-Memory Size", get_size_display(metadata.get('memory_bytes', 0)))
                if metadata.get('format'):
                    st.markdown(f"**Format:** {metadata.get('format')}")
                if metadata.get('source'):
                     # Truncate long source paths/URLs for display
                    source_display = metadata.get('source')
                    if len(source_display) > 60:
                        source_display = "..." + source_display[-57:]
                    st.markdown(f"**Source:** `{source_display}`")
            
            st.markdown("#### Preview Data")
            if data_sample is not None and data_sample.num_rows > 0:
                st.dataframe(data_sample.to_pandas(), use_container_width=True) # Convert to Pandas for st.dataframe
            else:
                st.info("No data to preview or dataset is empty.")

            # Display column schema
            if metadata.get('columns') and metadata.get('dtypes'):
                st.markdown("#### Column Schema")
                schema_data = []
                for col_name in metadata['columns']:
                    schema_data.append({
                        "Column Name": col_name,
                        "Data Type": metadata['dtypes'].get(col_name, "unknown")
                    })
                st.dataframe(pd.DataFrame(schema_data), use_container_width=True, hide_index=True)
            else:
                st.caption("Detailed column schema not available in metadata.")
            
            with st.expander("View Full Metadata JSON"):
                st.json(metadata)

        except Exception as e:
            logger.error(f"Error displaying details for {selected_name}: {e}", exc_info=True)
            st.error(f"Could not display details for '{selected_name}': {e}")

logger.info("Data Explorer page loaded.") 