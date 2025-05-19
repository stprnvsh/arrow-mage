import streamlit as st
import os
import sys
import logging
import time

# --- Configure page - MUST be the first Streamlit command ---
st.set_page_config(
    page_title="Claude Data Science Studio",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://www.example.com/help', # Replace with actual help link
        'Report a bug': "https://www.example.com/bug", # Replace with actual bug report link
        'About': """
        ## Claude Data Science Studio
        
        Your intelligent partner for data exploration, analysis, and insight generation.
        Powered by Arrow Cache and Claude.
        """
    }
)

# --- Configure logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Add project directories to sys.path ---
# This ensures that modules from arrow-cache-mcp and potentially other local packages can be imported.
try:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # workspace_root is one level up from claude_ds_studio where this file lives
    workspace_root = os.path.abspath(os.path.join(script_dir, '..')) 

    # Add arrow-cache-mcp to sys.path (assuming it's in the workspace_root)
    arrow_mcp_path = os.path.join(workspace_root, 'arrow-cache-mcp')
    arrow_mcp_src_path = os.path.join(arrow_mcp_path, 'src')
    if arrow_mcp_src_path not in sys.path:
        sys.path.insert(0, arrow_mcp_src_path)
        logger.info(f"Added {arrow_mcp_src_path} to sys.path")

except Exception as e:
    logger.error(f"Error adjusting sys.path: {e}", exc_info=True)
    st.error(f"An error occurred during application setup (sys.path): {e}")
    st.stop()

# --- Import from arrow-cache-mcp package ---
try:
    from arrow_cache_mcp.core import (
        get_arrow_cache,
        close_cache,
        get_memory_usage,
        get_datasets_list,
        clear_cache_files # For cleanup
    )
    from arrow_cache_mcp.ai import (
        get_ai_config,
        get_supported_providers
    )
    logger.info("Successfully imported core components from arrow-cache-mcp.")
except ImportError as e:
    logger.error(f"Failed to import from arrow-cache-mcp: {e}", exc_info=True)
    st.error(f"Critical Error: Could not import necessary components from 'arrow-cache-mcp'. Please ensure it's correctly installed and in the PYTHONPATH. Details: {e}")
    st.stop()
except Exception as e:
    logger.error(f"An unexpected error occurred during imports: {e}", exc_info=True)
    st.error(f"An unexpected error occurred during application setup (imports): {e}")
    st.stop()

# --- Initialize the Arrow Cache ---
# Placed here so it's accessible globally and initialized once.
try:
    # SPILL_DIRECTORY will be in the workspace_root
    SPILL_DIRECTORY = os.path.join(workspace_root, ".arrow_cache_spill_studio")
    os.makedirs(SPILL_DIRECTORY, exist_ok=True)
    
    CACHE_CONFIG = {
        "spill_directory": SPILL_DIRECTORY,
        "memory_limit": 20 * 1024 * 1024 * 1024,  # 20 GB
        "auto_partition": True,
        "compression_type": "lz4",
        "enable_compression": True,
        "dictionary_encoding": True,
        "cache_query_plans": True
    }
    cache = get_arrow_cache(CACHE_CONFIG) # get_arrow_cache is from arrow_cache_mcp.core
    logger.info(f"Arrow Cache initialized with spill directory: {SPILL_DIRECTORY}")

    # Store cache in session state to make it accessible across pages
    if 'arrow_cache' not in st.session_state:
        st.session_state.arrow_cache = cache
        logger.info("Arrow Cache (studio) initialized and stored in session state.")
    elif st.session_state.arrow_cache is None: # Handle potential re-runs
        st.session_state.arrow_cache = cache
        logger.info("Arrow Cache (studio) re-initialized and stored in session state.")

except Exception as e:
    logger.error(f"Failed to initialize Arrow Cache: {e}", exc_info=True)
    st.error(f"Critical Error: Could not initialize Arrow Cache. Details: {e}")
    st.stop()

# --- Load Custom CSS ---
def load_css(file_name):
    try:
        # CSS is expected in workspace_root/styles/
        css_path = os.path.join(workspace_root, "styles", file_name)
        if os.path.exists(css_path):
            with open(css_path) as f:
                st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)
            logger.info(f"Loaded CSS from {css_path}")
        else:
            logger.warning(f"CSS file not found at {css_path}. Using default styles. Expected at: {css_path}")
    except Exception as e:
        logger.error(f"Error loading CSS from {file_name}: {e}", exc_info=True)
        st.warning(f"Could not load custom styles: {e}")

load_css("main.css")

# --- Sidebar Configuration ---
with st.sidebar:
    # Consider adding a logo to workspace_root/assets/logo.png
    logo_path = os.path.join(workspace_root, "assets", "logo.png")
    if os.path.exists(logo_path):
        st.image(logo_path, width=100)
    else:
        st.image("https://avatars.githubusercontent.com/u/162559854?s=200&v=4", width=100) # Fallback logo
        logger.warning(f"Logo not found at {logo_path}, using fallback.")

    st.markdown("## Claude Data Science Studio")
    st.markdown("---")

    st.header("AI Model Configuration")
    try:
        # Get base AI config (from .env, arrow_mcp_config.json, etc.)
        base_ai_config = get_ai_config()
        
        available_providers = get_supported_providers()
        provider_options = list(available_providers.keys())

        # Determine selected provider: session state > base_config > default
        default_provider = base_ai_config.get("provider") if base_ai_config.get("provider") in provider_options else provider_options[0]
        selected_provider = st.session_state.get("studio_selected_ai_provider", default_provider)
        
        selected_provider_idx = provider_options.index(selected_provider) if selected_provider in provider_options else 0
        
        new_selected_provider = st.selectbox(
            "AI Provider:",
            provider_options,
            index=selected_provider_idx,
            key="sb_studio_ai_provider",
            help="Select the AI provider for analysis."
        )
        if new_selected_provider != selected_provider:
            st.session_state.studio_selected_ai_provider = new_selected_provider
            # Clear model and API key for the new provider from session if they were provider-specific
            st.session_state.pop(f"studio_selected_ai_model_{selected_provider}", None)
            st.session_state.pop(f"studio_api_key_{selected_provider}", None)
            selected_provider = new_selected_provider # Update for current run before rerun
            st.rerun()
        else:
            selected_provider = new_selected_provider

        # Model selection based on provider
        selected_model = ""
        if selected_provider in available_providers:
            model_options = available_providers[selected_provider]
            if not model_options: # Should not happen if get_supported_providers is robust
                st.error(f"No models listed for provider: {selected_provider}")
            else:
                current_model_key_session = f"studio_selected_ai_model_{selected_provider}"
                
                # Determine default model: session > base_config > provider default > first in list
                default_model_for_provider = base_ai_config.get("model")
                if base_ai_config.get("provider") != selected_provider or default_model_for_provider not in model_options:
                    if selected_provider == "anthropic":
                        default_model_for_provider = "claude-3-haiku-20240307"
                    elif selected_provider == "openai":
                        default_model_for_provider = "gpt-3.5-turbo"
                    else: # Fallback for other providers
                        default_model_for_provider = model_options[0]
                
                # Ensure the determined default is actually valid for the provider
                if default_model_for_provider not in model_options:
                    default_model_for_provider = model_options[0]

                selected_model_val = st.session_state.get(current_model_key_session, default_model_for_provider)
                
                model_idx = model_options.index(selected_model_val) if selected_model_val in model_options else 0

                new_selected_model = st.selectbox(
                    "Model:",
                    model_options,
                    index=model_idx,
                    key=f"sb_studio_ai_model_{selected_provider}",
                    help=f"Select the {selected_provider} model."
                )
                if new_selected_model != selected_model_val:
                    st.session_state[current_model_key_session] = new_selected_model
                    selected_model = new_selected_model
                    st.rerun()
                else:
                    selected_model = new_selected_model

        # API Key Management for selected_provider
        # Priority: Session State > Persisted (env/config) > Input
        session_api_key_name = f"studio_api_key_{selected_provider}"
        persisted_api_key = base_ai_config.get("api_key") if base_ai_config.get("provider") == selected_provider else None
        persisted_source = base_ai_config.get("source") if base_ai_config.get("provider") == selected_provider else None

        current_api_key = st.session_state.get(session_api_key_name, persisted_api_key)

        if current_api_key and current_api_key == persisted_api_key:
            st.success(f"{selected_provider.capitalize()} API key loaded from {persisted_source}.")
            if st.button(f"Clear Session Override for {selected_provider.capitalize()} Key", key=f"clear_session_api_key_btn_studio_{selected_provider}"):
                if session_api_key_name in st.session_state:
                    del st.session_state[session_api_key_name]
                st.info(f"Session override cleared. Using key from {persisted_source} if available, or enter manually.")
                st.rerun()
        elif current_api_key:
             st.success(f"{selected_provider.capitalize()} API key active for this session.")
             if st.button(f"Clear Session Key for {selected_provider.capitalize()}", key=f"clear_entered_api_key_btn_studio_{selected_provider}"):
                if session_api_key_name in st.session_state:
                    del st.session_state[session_api_key_name]
                st.rerun()
        else: # No key in session or persisted for this provider
            entered_api_key = st.text_input(
                f"Enter {selected_provider.capitalize()} API Key:",
                type="password",
                key=f"text_studio_api_key_{selected_provider}",
                help=f"This key will be used for the current session."
            )
            if entered_api_key:
                st.session_state[session_api_key_name] = entered_api_key
                st.success(f"API key for {selected_provider.capitalize()} stored for this session.")
                # Delay rerun to allow success message to show before potential page logic using the key
                time.sleep(0.5) 
                st.rerun() 
            else:
                st.warning(f"No {selected_provider.capitalize()} API key active. AI features may be limited.")
        
        # Make effective AI config available to pages via st.session_state
        # This will be used by a utility function within pages to get the final config.
        st.session_state.studio_effective_ai_config = {
            "provider": selected_provider,
            "model": selected_model,
            "api_key": st.session_state.get(session_api_key_name, persisted_api_key), # Gives current active key
            "source": "session override" if session_api_key_name in st.session_state else persisted_source
        }

    except Exception as e:
        logger.error(f"Error configuring AI settings in sidebar: {e}", exc_info=True)
        st.error(f"Error setting up AI configuration: {e}")

    st.markdown("---")
    st.header("Cache Management")
    try:
        memory_info = get_memory_usage() 
        
        st.markdown(f"""
        **Cache Memory:**
        `{memory_info['cache_size_mb']:.1f} MB / {memory_info['memory_limit_mb']:.1f} MB ({memory_info['cache_utilization_percent']:.1f}%)`
        """)
        st.progress(min(1.0, memory_info['cache_utilization_percent'] / 100.0))
        
        st.markdown(f"""
        **Process Memory:**
        `{memory_info['process_rss_mb']:.1f} MB (App: {memory_info['app_overhead_mb']:.1f} MB)`
        """)
        
        st.caption(f"Datasets in cache: {memory_info['entry_count']}")

        if st.button("Refresh Status", key="refresh_cache_status_studio"):
            st.rerun()

        if st.button("Clear All Cache Files", type="secondary", help="Removes all data from the cache and spill directory. Use with caution!", key="clear_all_cache_studio"):
            with st.spinner("Clearing cache files..."):
                if 'cache' in globals() and cache is not None:
                    close_cache() 
                
                clear_cache_files(spill_directory=SPILL_DIRECTORY)
                st.cache_resource.clear()
            st.success("All cache files cleared. The application will reload and reinitialize the cache.")
            st.rerun()
            
    except Exception as e:
        logger.error(f"Error displaying cache status in sidebar: {e}", exc_info=True)
        st.error(f"Error with cache status: {e}")

    st.markdown("---")
    st.caption(f"Version: 0.1.0 Alpha") 

# --- Main Page Content (Welcome Page) ---
# This content is shown when navigating to the main app URL if no specific page is chosen from 'pages'
# or if this is the first page Streamlit loads.

st.title("🔬 Welcome to Claude Data Science Studio!")

st.markdown("""
Your intelligent partner for end-to-end data workflows. Navigate through the studio using the sidebar to:

- **🖼️ Data Explorer:** Upload, manage, and preview your datasets.
- **🔌 Connectors:** Link to external data sources like databases and cloud storage.
- **💡 SQL Workbench:** Craft and execute SQL queries directly on your cached data.
- **🤖 AI Analyst:** Leverage Claude for natural language querying, automated analysis, and insight generation.
- **📊 Visualizer:** Create compelling charts and graphs to uncover data stories.

Configure your AI model and manage cache settings in the sidebar.
""")

st.info("Select a page from the sidebar to begin your data journey.", icon="👈")

# --- Application Cleanup ---
import atexit

# Ensure cache is closed gracefully. 
# The `cache` object is from `get_arrow_cache` in arrow_cache_mcp.core
# `close_cache` is also from arrow_cache_mcp.core

_cleanup_registered = False

def cleanup_app_studio():
    global _cleanup_registered
    logger.info("Claude Data Science Studio is attempting to shut down. Closing cache...")
    try:
        # Access the global cache instance established earlier
        # No need to call get_arrow_cache() again here
        if 'cache' in globals() and globals()['cache'] is not None:
            close_cache() # This function should handle the actual closing logic
            logger.info("Arrow Cache closed successfully via studio cleanup.")
        else:
            logger.info("Cache was not initialized or already closed prior to studio cleanup.")
    except Exception as e:
        logger.error(f"Error during cache cleanup in studio: {e}", exc_info=True)
    _cleanup_registered = False # Reset for potential re-runs in some dev environments

if not _cleanup_registered:
    atexit.register(cleanup_app_studio)
    _cleanup_registered = True
    logger.info("Registered cleanup_app_studio with atexit.")

logger.info("Claude Data Science Studio main application file (claude_ds_studio/claude_data_science_studio.py) loaded.") 