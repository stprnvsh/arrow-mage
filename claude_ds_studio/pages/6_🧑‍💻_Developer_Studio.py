import streamlit as st
import pandas as pd
import io
import sys
import traceback
import logging
from contextlib import redirect_stdout, redirect_stderr
import os

# Attempt to import get_datasets_list from arrow_cache_mcp.core
try:
    from arrow_cache_mcp.core import get_datasets_list
except ImportError:
    # Fallback if the typical Streamlit page context doesn't have the updated sys.path yet
    # This might happen on the first run or if paths are tricky.
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        workspace_root = os.path.abspath(os.path.join(script_dir, '..', '..')) # Up to workspace root
        arrow_mcp_src_path = os.path.join(workspace_root, 'arrow-cache-mcp', 'src')
        if arrow_mcp_src_path not in sys.path:
            sys.path.insert(0, arrow_mcp_src_path)
        from arrow_cache_mcp.core import get_datasets_list
        logging.info("Developer Studio: Successfully imported get_datasets_list via manual sys.path adjustment.")
    except Exception as e_import:
        logging.error(f"Developer Studio: Failed to import get_datasets_list: {e_import}")
        st.error("Failed to load a core component (get_datasets_list). Table listing might not work.")
        get_datasets_list = None # Ensure it exists but is None

# Attempt to import AI client libraries
try:
    import anthropic
    ANTHROPIC_CLIENT_AVAILABLE = True
except ImportError:
    ANTHROPIC_CLIENT_AVAILABLE = False
    logging.warning("Developer Studio: Anthropic client library not found. AI assistance with Anthropic models will not be available.")

try:
    import openai
    OPENAI_CLIENT_AVAILABLE = True
except ImportError:
    OPENAI_CLIENT_AVAILABLE = False
    logging.warning("Developer Studio: OpenAI client library not found. AI assistance with OpenAI models will not be available.")

# Configure logging
logger = logging.getLogger(__name__)

st.title("🧑‍💻 Developer Studio")
st.markdown("""
Welcome to the Developer Studio! This is a space where you can write and execute Python scripts
to interact with your datasets, perform advanced analyses, and develop custom data processing tasks.

**Key Features:**
- Write and execute Python code directly in the browser.
- Access loaded datasets via the `cache` object (an instance of `ArrowCache`).
- Use familiar libraries like Pandas, NumPy, etc. (ensure they are in your environment).
- View script outputs, errors, and results (e.g., DataFrames, plots).

**Accessing the Arrow Cache:**
The Arrow Cache instance is available as `st.session_state.arrow_cache`.
You can use it to query datasets, e.g., `df = st.session_state.arrow_cache.query("SELECT * FROM _cache_your_dataset_name_csv")`.
""")

# --- Retrieve the Arrow Cache ---
logger.info(f"Developer Studio: Checking st.session_state. Contains arrow_cache key: {'arrow_cache' in st.session_state}")
if 'arrow_cache' in st.session_state:
    logger.info(f"Developer Studio: st.session_state.arrow_cache is None: {st.session_state.arrow_cache is None}")
    if st.session_state.arrow_cache is not None:
        logger.info(f"Developer Studio: Type of st.session_state.arrow_cache: {type(st.session_state.arrow_cache)}")

if 'arrow_cache' not in st.session_state or st.session_state.arrow_cache is None:
    st.error("Arrow Cache is not available. Please ensure it's initialized in the main app.")
    logger.error("Developer Studio: Arrow Cache not found or is None in session_state.")
    # For more detailed debugging, print all session state keys:
    logger.debug(f"Developer Studio: Current st.session_state keys: {list(st.session_state.keys())}")
    st.stop()
cache = st.session_state.arrow_cache
logger.info("Developer Studio: Successfully accessed Arrow Cache.")

st.sidebar.info(f"Arrow Cache instance: {type(cache)}")

# --- Available Datasets ---
if get_datasets_list:
    try:
        datasets_info = get_datasets_list() # Use the imported function
        if datasets_info:
            st.sidebar.subheader("Available Datasets (Tables)")
            available_tables = [info['name'] for info in datasets_info if 'name' in info]
            if available_tables:
                for table_name in available_tables:
                    st.sidebar.markdown(f"- `{table_name}`")
            else:
                st.sidebar.warning("No table names found in dataset info.")
        else:
            st.sidebar.warning("No datasets found in cache.")
    except Exception as e:
        st.sidebar.error(f"Error listing tables: {e}")
        logger.error(f"Developer Studio: Error calling get_datasets_list(): {e}")
else:
    st.sidebar.error("Table listing unavailable (get_datasets_list not loaded).")

# --- Code Editor and Execution ---
st.subheader("Python Code Editor")

default_code = """# Example script:
# Access the cache using st.session_state.arrow_cache

# To list available datasets/tables and their metadata:
# from arrow_cache_mcp.core import get_datasets_list # Ensure this import works in your script's context
# try:
#     all_datasets_info = get_datasets_list()
#     st.write("Available datasets information:", all_datasets_info)
#     if all_datasets_info:
#         available_table_names = [info['name'] for info in all_datasets_info if 'name' in info]
#         st.write("Available table names:", available_table_names)
# except Exception as e:
#     st.error(f"Error getting dataset list: {e}")

# Example: Query a dataset (replace '_cache_your_dataset_name_csv' with an actual table name from the sidebar or above)
# Make sure a dataset is loaded, e.g. _cache_height_weight_genders_csv
TABLE_TO_QUERY = None # Change this to a valid table name

if TABLE_TO_QUERY:
    try:
        st.write(f"Querying table: {TABLE_TO_QUERY}...")
        df = st.session_state.arrow_cache.query(f"SELECT * FROM {TABLE_TO_QUERY} LIMIT 5")
        st.dataframe(df)
    except Exception as e:
        st.error(f"Error querying {TABLE_TO_QUERY}: {e}")
else:
    st.info("Set TABLE_TO_QUERY in the script to an actual table name to run a query.")

# You can also define functions and use other libraries
import pandas as pd
data = {'col1': [1, 2], 'col2': [3, 4]}
df_local = pd.DataFrame(data)
st.write("Local Pandas DataFrame:", df_local)

# For plots, use st.pyplot(), st.altair_chart(), etc.
"""

code = st.text_area("Enter your Python script here:", value=default_code, height=400, key="dev_studio_code_editor")

if st.button("🚀 Execute Script", key="dev_studio_execute_button"):
    if not code.strip():
        st.warning("Code editor is empty. Please enter some Python script.")
    else:
        st.markdown("---")
        st.subheader("Script Output & Results")
        
        # Prepare a dictionary for local variables accessible to exec
        # Include Streamlit (st) and the cache object
        local_vars = {
            "st": st,
            "pd": pd, # Making pandas available by default
            # Add other commonly used libraries if needed: np, plt, etc.
            # "np": np, (ensure numpy is imported if you add it)
            "cache": cache, # Direct access for convenience, though st.session_state.arrow_cache also works
            "st_session_state_arrow_cache": st.session_state.arrow_cache # Explicitly for clarity
        }

        # Capture stdout and stderr
        stdout_capture = io.StringIO()
        stderr_capture = io.StringIO()

        try:
            with redirect_stdout(stdout_capture), redirect_stderr(stderr_capture):
                # Execute the code
                exec(code, local_vars) # Pass local_vars as globals for exec

            stdout_str = stdout_capture.getvalue()
            stderr_str = stderr_capture.getvalue()

            if stdout_str:
                st.text_area("Standard Output (stdout):", value=stdout_str, height=200, disabled=True)
            
            if stderr_str:
                st.error("Standard Error (stderr):")
                st.text_area("", value=stderr_str, height=150, disabled=True)
            
            if not stdout_str and not stderr_str:
                st.success("Script executed successfully with no textual output.")

        except Exception as e:
            st.error(f"An error occurred during script execution:")
            st.code(traceback.format_exc(), language='text')
            logger.error(f"Developer Studio: Error executing script: {traceback.format_exc()}")
        finally:
            stdout_capture.close()
            stderr_capture.close()
        st.markdown("---")

st.markdown("---")
st.subheader("🤖 AI Code Assistance")

if 'studio_effective_ai_config' not in st.session_state or not st.session_state.studio_effective_ai_config.get("api_key"):
    st.warning("AI features are limited. Please configure the AI Provider, Model, and API Key in the sidebar of the main application.")
else:
    st.markdown("Need help writing or debugging your script? Ask the AI!")
    
    ai_question = st.text_area("Your question for the AI Assistant:", height=100, key="dev_studio_ai_question")
    
    # Get current code from the editor (assuming `code` variable holds the editor's content)
    current_script_for_ai = code 

    if st.button("💬 Ask AI", key="dev_studio_ask_ai_button"):
        if not ai_question.strip():
            st.warning("Please enter your question for the AI.")
        elif not ANTHROPIC_CLIENT_AVAILABLE and not OPENAI_CLIENT_AVAILABLE:
             st.error("Neither Anthropic nor OpenAI client libraries are available. Please install them.")
        else:
            with st.spinner("AI Assistant is thinking..."):
                ai_config = st.session_state.get("studio_effective_ai_config", {})
                ai_response = get_ai_assistance(current_script_for_ai, ai_question, ai_config)
                
                # Display AI response
                st.markdown("**AI Assistant's Response:**")
                st.markdown(ai_response) # Markdown will render code blocks if AI uses them

logger.info("Developer Studio page loaded and UI rendered.") 