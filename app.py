import streamlit as st
import os
import sys
import logging

# Configure page - MUST be the first Streamlit command
st.set_page_config(
    page_title="Data Mage",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Ensure arrow-cache-mcp is in the path ---
script_dir = os.path.dirname(os.path.abspath(__file__))
workspace_root = os.path.abspath(os.path.join(script_dir))
arrow_mcp_path = os.path.join(workspace_root, 'arrow-cache-mcp')
arrow_mcp_src_path = os.path.join(arrow_mcp_path, 'src')
if arrow_mcp_src_path not in sys.path:
    sys.path.insert(0, arrow_mcp_src_path)
    logger.info(f"Added {arrow_mcp_src_path} to sys.path")

# Add utils directory to path
utils_path = os.path.join(workspace_root, 'utils')
if utils_path not in sys.path:
    sys.path.insert(0, utils_path)
    logger.info(f"Added {utils_path} to sys.path")

# --- Import from arrow-cache-mcp package ---
try:
    from arrow_cache_mcp.core import (
        get_arrow_cache, 
        close_cache
    )
except ImportError as e:
    st.error(f"Failed to import from arrow-cache-mcp: {e}")
    st.stop()

# --- Initialize the Arrow Cache ---
SPILL_DIRECTORY = ".arrow_cache_spill_streamlit"
cache = get_arrow_cache({
    "spill_directory": SPILL_DIRECTORY,
    "memory_limit": 20 * 1024 * 1024 * 1024  # 20 GB
})

# Store cache in session state to make it accessible across pages
if 'arrow_cache' not in st.session_state:
    st.session_state.arrow_cache = cache
    logger.info("Arrow Cache initialized and stored in session state.")
elif st.session_state.arrow_cache is None: # Handle potential re-runs where it might be None
    st.session_state.arrow_cache = cache
    logger.info("Arrow Cache re-initialized and stored in session state.")

# Load CSS
with open('styles/main.css') as f:
    st.markdown(f'<style>{f.read()}</style>', unsafe_allow_html=True)

# Main app header
st.title("✨ Data Mage")
st.markdown("A powerful data analysis platform with intuitive interface and AI capabilities")

# App overview section
st.markdown("""
## Welcome to Data Mage

Data Mage provides a seamless experience for data exploration, analysis, and visualization. 
With an intuitive interface and powerful AI capabilities, you can:

- **Import & Manage** datasets from various sources
- **Query** your data using SQL or natural language
- **Visualize** insights with beautiful charts and graphs
- **Collaborate** with AI assistants to uncover deeper insights
- **Expose UDFs as Tools** via MCP server for AI agents

Navigate through the pages in the sidebar to get started.
""")

# How to use section
with st.expander("How to use this app"):
    st.markdown("""
    ### Getting Started
    1. Go to the **Datasets** page to import your data
    2. Use the **SQL Query** page to run custom queries
    3. Try the **AI Assistant** page for natural language data exploration
    4. Create beautiful visualizations in the **Visualize** page
    
    Each page contains detailed instructions and examples to help you get started.
    """)

# Cleanup on app exit
import atexit
atexit.register(lambda: close_cache() if 'cache' in globals() else None) 