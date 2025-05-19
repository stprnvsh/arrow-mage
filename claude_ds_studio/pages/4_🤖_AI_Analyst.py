import streamlit as st
import os
import sys
import logging
import time
import re # For parsing Claude's responses
import pandas as pd # For displaying dataframes from SQL queries

# Configure logging for this page
logger = logging.getLogger(__name__)

# --- Add workspace root to sys.path for shared modules ---
script_dir = os.path.dirname(os.path.abspath(__file__))
page_dir = os.path.abspath(script_dir)
app_root_dir = os.path.abspath(os.path.join(page_dir, '..'))
workspace_root = os.path.abspath(os.path.join(app_root_dir, '..'))

if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
    logger.info(f"AI Analyst: Added workspace root {workspace_root} to sys.path")
if app_root_dir not in sys.path:
    sys.path.insert(0, app_root_dir)
    logger.info(f"AI Analyst: Added app root {app_root_dir} to sys.path")

# --- Import from arrow-cache-mcp --- 
try:
    from arrow_cache_mcp.core import (
        get_arrow_cache,
        get_datasets_list
    )
    from arrow_cache_mcp.ai import (
        ask_ai, 
        get_ai_config 
    )
    # Removed display_conversation_history_dict as we'll use st.chat_message
    
    cache = get_arrow_cache()
    logger.info("AI Analyst: Successfully accessed Arrow Cache and imported mcp components.")
except ImportError as e:
    logger.error(f"AI Analyst: Failed to import from arrow-cache-mcp: {e}", exc_info=True)
    st.error(f"Error loading required modules: {e}. Please ensure 'arrow-cache-mcp' is correctly installed.")
    st.stop()
except Exception as e:
    logger.error(f"AI Analyst: An unexpected error occurred: {e}", exc_info=True)
    st.error(f"An unexpected error occurred: {e}")
    st.stop()

# --- Page Configuration & Title ---
st.title("🤖 AI Analyst")
st.markdown("Chat with Claude to analyze your datasets. Ask questions in natural language, request insights, or get help generating SQL queries.")

# --- Initialize session state ---
if "ai_analyst_messages" not in st.session_state:
    st.session_state.ai_analyst_messages = [] # Stores chat messages
if "ai_analyst_selected_dataset" not in st.session_state:
    st.session_state.ai_analyst_selected_dataset = None
if "ai_analyst_query_ran" not in st.session_state:
    st.session_state.ai_analyst_query_ran = False # Flag to track if SQL from AI has run

# --- Helper to get effective AI config from sidebar ---
def get_effective_ai_config_from_session():
    return st.session_state.get("studio_effective_ai_config", get_ai_config()) # Fallback to default from mcp

# --- UI: Dataset Selection ---
st.sidebar.subheader("AI Analyst Settings")
datasets_list = get_datasets_list()
dataset_names = [ds['name'] for ds in datasets_list if ds.get('name')]

if not dataset_names:
    st.warning("No datasets found in cache. AI Analyst needs data to work with. Please load a dataset via the Data Explorer page.")
    st.sidebar.warning("No datasets loaded for AI Analyst.")
    st.stop()

# Use a more specific session state key for the AI analyst's dataset selection
current_selected_dataset_ai = st.session_state.get("ai_analyst_selected_dataset_name", None)

if current_selected_dataset_ai not in dataset_names and dataset_names:
    current_selected_dataset_ai = dataset_names[0] # Default to first if previous selection is invalid
elif not dataset_names: # Should be caught above, but for safety
    current_selected_dataset_ai = None

# Get index for the selectbox
selected_dataset_idx_ai = dataset_names.index(current_selected_dataset_ai) if current_selected_dataset_ai in dataset_names else 0

selected_dataset_name_ai = st.sidebar.selectbox(
    "Select Dataset for Analysis:",
    options=dataset_names,
    index=selected_dataset_idx_ai,
    key="ai_analyst_dataset_selector_sidebar",
    help="The AI will focus its analysis and queries on this dataset."
)

if selected_dataset_name_ai != current_selected_dataset_ai:
    st.session_state.ai_analyst_selected_dataset_name = selected_dataset_name_ai
    st.session_state.ai_analyst_messages = [] # Clear chat on dataset change
    st.session_state.ai_analyst_query_ran = False
    st.rerun()

st.sidebar.info(f"AI Analyst is focused on: **{selected_dataset_name_ai}**")

# --- Display chat messages from history ---
for message in st.session_state.ai_analyst_messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "dataframe" in message and message["dataframe"] is not None:
            st.dataframe(message["dataframe"], use_container_width=True)
        if "error" in message and message["error"]:
            st.error(message["error"])

# --- AI Interaction Logic ---
def process_ai_response(ai_response_text, selected_dataset):
    """Processes AI response, extracts SQL, runs it, and formats for display."""
    extracted_sql = None
    # Regex to find SQL code blocks
    sql_match = re.search(r"```sql\n(.*?)\n```", ai_response_text, re.DOTALL)
    if sql_match:
        extracted_sql = sql_match.group(1).strip()
        # Try to automatically adjust table name if it's a common placeholder
        # This is a basic heuristic
        if selected_dataset and f"FROM {selected_dataset}" not in extracted_sql and "FROM your_table_name" in extracted_sql:
            extracted_sql = extracted_sql.replace("your_table_name", selected_dataset)
        elif selected_dataset and f"FROM _cache_{selected_dataset}" not in extracted_sql and f"FROM {selected_dataset}" in extracted_sql:
             # Check if the user used the direct name and if it should be the cache name
            if not cache.is_registered_view(selected_dataset) and cache.is_registered_view(f"_cache_{selected_dataset}"):
                 extracted_sql = extracted_sql.replace(f"FROM {selected_dataset}", f"FROM _cache_{selected_dataset}")

    # Add AI's textual response to chat
    st.session_state.ai_analyst_messages.append({"role": "assistant", "content": ai_response_text})

    if extracted_sql:
        st.session_state.ai_analyst_messages.append({"role": "assistant", "content": f"I've generated the following SQL query. Let me run it for you on dataset '{selected_dataset}':\n```sql\n{extracted_sql}\n```"})
        try:
            with st.spinner(f"Executing AI-generated SQL on '{selected_dataset}'..."):
                query_result_table = cache.query(extracted_sql) # Assumes cache.query() is robust
                if query_result_table is not None:
                    query_result_df = query_result_table.to_pandas()
                    st.session_state.ai_analyst_messages.append({"role": "assistant", "content": "Query executed successfully:", "dataframe": query_result_df})
                    st.session_state.ai_analyst_query_ran = True
                else:
                    st.session_state.ai_analyst_messages.append({"role": "assistant", "content": "The SQL query ran but returned no results.", "error": "Query returned empty or null."}) 
        except Exception as e:
            logger.error(f"Error executing AI-generated SQL: {extracted_sql} - {e}", exc_info=True)
            st.session_state.ai_analyst_messages.append({"role": "assistant", "content": "I encountered an error trying to run the SQL query.", "error": str(e)})
    st.rerun()

# --- Chat Input ---
prompt = st.chat_input(f"Ask Claude about dataset '{selected_dataset_name_ai}'...")

if prompt:
    st.session_state.ai_analyst_messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    # Get AI config for the call
    ai_config_to_use = get_effective_ai_config_from_session()
    if not ai_config_to_use.get("api_key"):
        st.error(f"API Key for {ai_config_to_use.get('provider', 'the selected provider')} is not set. Please configure it in the sidebar.")
        st.session_state.ai_analyst_messages.append({"role": "assistant", "content": "I can't respond without an API key. Please set it up in the sidebar under AI Model Configuration.", "error": "API Key Missing"})
        st.rerun()
    else:
        # Construct a system prompt to give context to the AI
        # Attempt to get schema for context (simplified)
        dataset_schema_str = "Schema not available."
        if selected_dataset_name_ai and cache.contains(selected_dataset_name_ai):
            try:
                sample_table = cache.get(selected_dataset_name_ai, limit=0) # Get schema without data
                dataset_schema_str = f"Dataset: {selected_dataset_name_ai}\nSchema:\n{sample_table.schema.to_string()}"
            except Exception as schema_e:
                logger.warning(f"Could not get schema for {selected_dataset_name_ai} for AI context: {schema_e}")
        
        system_prompt = f"""
        You are an AI Data Analyst. You are interacting with a user through a Streamlit application.
        The user has selected the dataset named '{selected_dataset_name_ai}' for analysis.
        {dataset_schema_str}
        If you generate a SQL query, enclose it in triple backticks with the language hint sql (e.g., ```sql\nSELECT * FROM ...\n```).
        Ensure your SQL queries are compatible with DuckDB. When referring to the selected dataset in SQL, use its name directly (e.g., `FROM {selected_dataset_name_ai}`). The system will handle prefixing with `_cache_` if necessary.
        Keep your textual explanations concise and directly answer the user's query. If providing a table, the system will display it after your text.
        """

        with st.chat_message("assistant"):
            message_placeholder = st.empty()
            full_response_content = ""
            try:
                with st.spinner("Claude is thinking..."):
                    # Use the ask_ai function from arrow_cache_mcp.ai
                    # It expects the full conversation history for context.
                    conversation_for_ai = [
                        {"role": msg["role"], "content": msg["content"]} 
                        for msg in st.session_state.ai_analyst_messages
                    ]
                    
                    ai_response_generator = ask_ai(
                        prompt,  # This maps to the 'question' parameter
                        conversation_history=conversation_for_ai[:-1],
                        # The following are not direct params of ask_ai based on its definition in ai.py
                        # api_key, provider, model are passed to get_ai_provider within ask_ai
                        # system_prompt is constructed within ask_ai
                        # stream is handled by the provider's get_completion method
                        provider=ai_config_to_use.get('provider'),
                        model=ai_config_to_use.get('model'),
                        api_key=ai_config_to_use.get('api_key')
                        # max_retries can be added if needed, defaults to 1
                    )

                    # Since ask_ai in ai.py does not seem to be a generator for streaming directly,
                    # we assume it returns the full response string.
                    # If ask_ai was intended to stream, its internal logic or the provider's get_completion needs to yield chunks.
                    # For now, treating its return as a single string:
                    full_response_content = ai_response_generator 
                    message_placeholder.markdown(full_response_content)

            except Exception as ai_e:
                logger.error(f"Error calling AI: {ai_e}", exc_info=True)
                full_response_content = f"Sorry, I encountered an error: {ai_e}"
                message_placeholder.error(full_response_content)
        
        # Process the complete AI response once streaming is done
        process_ai_response(full_response_content, selected_dataset_name_ai)

logger.info("AI Analyst page loaded and UI rendered.") 