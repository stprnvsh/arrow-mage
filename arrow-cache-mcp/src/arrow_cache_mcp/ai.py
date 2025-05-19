"""
AI integration for Arrow Cache MCP.

This module provides integration with AI models from various providers like Anthropic's Claude
and OpenAI's GPT models. It allows users to:

1. Ask questions about datasets in the cache
2. Have SQL queries automatically generated and executed
3. Get interpretations of the query results

The module uses a provider-based architecture that can be extended to support additional AI providers.
"""

import os
import re
import time
import logging
import json
import importlib
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Tuple, Any, Union, Type

from .core import get_arrow_cache

# Configure logging
logger = logging.getLogger(__name__)

# --- Constants for backward compatibility ---
ANTHROPIC_DEFAULT_MODEL = "claude-3-haiku-20240307"
OPENAI_DEFAULT_MODEL = "gpt-3.5-turbo"

# --- AI Provider Abstraction ---

class AIProvider(ABC):
    """Base abstract class for AI model providers."""
    
    provider_name: str = "base"
    supported_models: List[str] = []
    
    def __init__(self, api_key: str, model: str = None, **kwargs):
        self.api_key = api_key
        self.model = model or self.get_default_model()
        self.client = None
        self.initialize_client()
        self.additional_config = kwargs
    
    @classmethod
    def get_default_model(cls) -> str:
        """Return the default model for this provider."""
        return cls.supported_models[0] if cls.supported_models else ""
    
    @abstractmethod
    def initialize_client(self) -> None:
        """Initialize the API client."""
        pass
    
    @abstractmethod
    def get_completion(self, 
                       prompt: str, 
                       system_prompt: str = None,
                       conversation_history: List[Dict[str, Any]] = None,
                       **kwargs) -> Tuple[str, Dict[str, Any]]:
        """
        Get a completion from the model.
        
        Args:
            prompt: The prompt to send
            system_prompt: Optional system prompt/instructions
            conversation_history: Optional conversation history
            **kwargs: Additional model-specific parameters
            
        Returns:
            Tuple of (completion_text, response_metadata)
        """
        pass
        
    @abstractmethod
    def format_messages(self, 
                      conversation_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Format the conversation history into the provider-specific format.
        
        Args:
            conversation_history: List of conversation entries with role and content keys
            
        Returns:
            List of formatted message dictionaries ready for the provider's API
        """
        pass
        
    def test_connection(self) -> Tuple[bool, str]:
        """
        Test connection to the provider API.
        
        Returns:
            Tuple of (success, message)
        """
        try:
            import urllib.request
            api_host = self.get_api_host()
            
            # Create request with appropriate headers
            headers = {
                "User-Agent": "arrow-cache-mcp/0.1",
                "Content-Type": "application/json",
                "X-API-Key": "sk-ant-api-connectivity-test" if self.provider_name == "anthropic" else "sk-test",
                "Authorization": f"Bearer {self.api_key}"
            }
            
            request = urllib.request.Request(
                api_host, 
                method="HEAD",
                headers=headers
            )
            
            # Add timeout to prevent hanging
            urllib.request.urlopen(request, timeout=5)
            return True, f"Successfully connected to {api_host}"
        except Exception as e:
            return False, f"Failed to connect to {self.provider_name} API: {str(e)}"
            
    def get_api_host(self) -> str:
        """
        Get the API host for connection testing.
        
        Returns:
            API host URL
        """
        return ""


class AnthropicProvider(AIProvider):
    """Anthropic Claude provider implementation."""
    
    provider_name = "anthropic"
    supported_models = [
        "claude-3-haiku-20240307",
        "claude-3-sonnet-20240229",
        "claude-3-opus-20240229",
        "claude-2.1",
        "claude-2.0",
        "claude-instant-1.2"
    ]
    
    def __init__(self, api_key: str, model: str = None, **kwargs):
        # Check if Anthropic library is installed
        try:
            import anthropic
            self.anthropic = anthropic
            self._has_anthropic = True
        except ImportError:
            self._has_anthropic = False
            logger.warning("Anthropic library not installed. Install with: pip install anthropic")
        
        super().__init__(api_key, model, **kwargs)
    
    def get_default_model(self) -> str:
        """Return the default model for Anthropic."""
        return "claude-3-haiku-20240307"
    
    def initialize_client(self) -> None:
        """Initialize the Anthropic API client."""
        if not self._has_anthropic:
            logger.error("Cannot initialize Anthropic client - library not installed")
            return
            
        try:
            self.client = self.anthropic.Anthropic(api_key=self.api_key)
            logger.debug("Anthropic client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize Anthropic client: {e}")
    
    def get_completion(self, 
                      prompt: str, 
                      system_prompt: str = None,
                      conversation_history: List[Dict[str, Any]] = None,
                      **kwargs) -> Tuple[str, Dict[str, Any]]:
        """Get a completion from Claude."""
        if not self._has_anthropic or not self.client:
            error_msg = "Anthropic library not installed or client not initialized"
            logger.error(error_msg)
            return error_msg, {"error": error_msg}
            
        try:
            if conversation_history is None:
                conversation_history = []
                
            # Add the current prompt to history if not already there
            if not conversation_history or conversation_history[-1]["role"] != "user" or conversation_history[-1]["content"] != prompt:
                conversation_history.append({
                    "role": "user",
                    "content": prompt
                })
                
            # Format messages for Claude API
            api_messages = self.format_messages(conversation_history)
            
            # Set up additional parameters
            params = {
                "model": self.model,
                "max_tokens": kwargs.get("max_tokens", 2048),
                "messages": api_messages,
                "timeout": kwargs.get("timeout", 60)
            }
            
            # Add system prompt if provided
            if system_prompt:
                params["system"] = system_prompt
                
            # Make the API call
            start_time = time.time()
            response = self.client.messages.create(**params)
            elapsed = time.time() - start_time
            
            # Extract the response text
            response_text = response.content[0].text
            
            # Create metadata
            metadata = {
                "model": self.model,
                "response_time": elapsed,
                "usage": {
                    "input_tokens": response.usage.input_tokens,
                    "output_tokens": response.usage.output_tokens
                }
            }
            
            return response_text, metadata
            
        except Exception as e:
            error_msg = f"Error getting completion from Claude: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            return error_msg, {"error": error_msg, "exception_type": type(e).__name__}
    
    def format_messages(self, 
                      conversation_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format messages for Claude API."""
        # Claude accepts messages with role and content
        return [
            {"role": msg["role"], "content": msg["content"]}
            for msg in conversation_history 
            if msg["role"] in ["user", "assistant"]
        ]
        
    def get_api_host(self) -> str:
        """Get the Anthropic API host."""
        return "https://api.anthropic.com/v1/messages"
        
    def validate_api_key(self, api_key: str = None) -> bool:
        """
        Validate Anthropic API key format.
        
        Args:
            api_key: API key to validate, or use the instance key if None
            
        Returns:
            True if the API key format is valid, False otherwise
        """
        key = api_key or self.api_key
        if not key:
            return False
            
        # Basic validation of Anthropic API key format
        if not key.startswith('sk-ant-'):
            return False
            
        # Check length requirements
        if len(key) < 20:
            return False
            
        return True


class OpenAIProvider(AIProvider):
    """OpenAI provider implementation."""
    
    provider_name = "openai"
    supported_models = [
        "gpt-4o", 
        "gpt-4-turbo", 
        "gpt-4",
        "gpt-3.5-turbo"
    ]
    
    def __init__(self, api_key: str, model: str = None, **kwargs):
        # Check if OpenAI library is installed
        try:
            import openai
            self.openai = openai
            self._has_openai = True
        except ImportError:
            self._has_openai = False
            logger.warning("OpenAI library not installed. Install with: pip install openai")
        
        super().__init__(api_key, model, **kwargs)
    
    def get_default_model(self) -> str:
        """Return the default model for OpenAI."""
        return "gpt-3.5-turbo"
    
    def initialize_client(self) -> None:
        """Initialize the OpenAI API client."""
        if not self._has_openai:
            logger.error("Cannot initialize OpenAI client - library not installed")
            return
            
        try:
            self.client = self.openai.OpenAI(api_key=self.api_key)
            logger.debug("OpenAI client initialized")
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
    
    def get_completion(self, 
                      prompt: str, 
                      system_prompt: str = None,
                      conversation_history: List[Dict[str, Any]] = None,
                      **kwargs) -> Tuple[str, Dict[str, Any]]:
        """Get a completion from OpenAI."""
        if not self._has_openai or not self.client:
            error_msg = "OpenAI library not installed or client not initialized"
            logger.error(error_msg)
            return error_msg, {"error": error_msg}
            
        try:
            if conversation_history is None:
                conversation_history = []
                
            # Add the current prompt to history if not already there
            if not conversation_history or conversation_history[-1]["role"] != "user" or conversation_history[-1]["content"] != prompt:
                conversation_history.append({
                    "role": "user",
                    "content": prompt
                })
                
            # Format messages for OpenAI API
            api_messages = self.format_messages(conversation_history)
            
            # Add system message if provided
            if system_prompt:
                api_messages.insert(0, {"role": "system", "content": system_prompt})
                
            # Set up additional parameters
            params = {
                "model": self.model,
                "messages": api_messages,
                "max_tokens": kwargs.get("max_tokens", 2048),
                "temperature": kwargs.get("temperature", 0.7),
                "timeout": kwargs.get("timeout", 60)
            }
                
            # Make the API call
            start_time = time.time()
            response = self.client.chat.completions.create(**params)
            elapsed = time.time() - start_time
            
            # Extract the response text
            response_text = response.choices[0].message.content
            
            # Create metadata
            metadata = {
                "model": self.model,
                "response_time": elapsed,
                "usage": {
                    "prompt_tokens": response.usage.prompt_tokens,
                    "completion_tokens": response.usage.completion_tokens,
                    "total_tokens": response.usage.total_tokens
                },
                "finish_reason": response.choices[0].finish_reason
            }
            
            return response_text, metadata
            
        except Exception as e:
            error_msg = f"Error getting completion from OpenAI: {type(e).__name__}: {str(e)}"
            logger.error(error_msg)
            return error_msg, {"error": error_msg, "exception_type": type(e).__name__}
    
    def format_messages(self, 
                      conversation_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Format messages for OpenAI API."""
        # OpenAI accepts messages with role and content, similar to Anthropic
        return [
            {"role": msg["role"], "content": msg["content"]}
            for msg in conversation_history 
            if msg["role"] in ["user", "assistant", "system"]
        ]
        
    def get_api_host(self) -> str:
        """Get the OpenAI API host."""
        return "https://api.openai.com/v1/chat/completions"
        
    def validate_api_key(self, api_key: str = None) -> bool:
        """
        Validate OpenAI API key format.
        
        Args:
            api_key: API key to validate, or use the instance key if None
            
        Returns:
            True if the API key format is valid, False otherwise
        """
        key = api_key or self.api_key
        if not key:
            return False
            
        # Basic validation of OpenAI API key format
        if not key.startswith('sk-'):
            return False
            
        # Check length requirements
        if len(key) < 20:
            return False
            
        return True


# Try to import anthropic (for backward compatibility)
try:
    import anthropic
    HAVE_ANTHROPIC = True
except ImportError:
    HAVE_ANTHROPIC = False
    logger.warning("Anthropic library not installed. Claude functionality will be limited.")

# Dictionary mapping provider names to their classes
PROVIDER_CLASSES = {
    "anthropic": AnthropicProvider,
    "openai": OpenAIProvider,
}

def get_ai_config() -> Dict[str, Any]:
    """
    Get the AI configuration from environment variables and session state.
    
    Returns:
        Dictionary containing provider, model, and API key information
    """
    config = {
        "provider": os.environ.get("AI_PROVIDER", "anthropic"),
        "model": os.environ.get("AI_MODEL", None),
        "api_key": None,
        "source": None
    }
    
    # Try to get provider-specific API key
    provider = config["provider"].lower()
    
    # Check env vars for this provider
    env_var_name = f"{provider.upper()}_API_KEY"
    config["api_key"] = os.environ.get(env_var_name)
    if config["api_key"]:
        config["source"] = f"{env_var_name} environment variable"
    
    # If no provider-specific key, try generic AI_API_KEY
    if not config["api_key"]:
        config["api_key"] = os.environ.get("AI_API_KEY")
        if config["api_key"]:
            config["source"] = "AI_API_KEY environment variable"
    
    # Try to get from session state (for Streamlit)
    if not config["api_key"]:
        try:
            import streamlit as st
            session_key_name = f"{provider}_api_key"
            
            # Check for provider-specific key in session
            if session_key_name in st.session_state:
                config["api_key"] = st.session_state[session_key_name]
                config["source"] = f"session state ({session_key_name})"
            
            # If no provider-specific key, try generic ai_api_key
            if not config["api_key"] and "ai_api_key" in st.session_state:
                config["api_key"] = st.session_state.ai_api_key
                config["source"] = "session state (ai_api_key)"
            
            # For backward compatibility - check for claude_api_key
            if not config["api_key"] and provider == "anthropic" and "claude_api_key" in st.session_state:
                config["api_key"] = st.session_state.claude_api_key
                config["source"] = "session state (claude_api_key)"
                
            # Check query parameters (for direct sharing via URL)
            if not config["api_key"]:
                try:
                    # Get query parameters using the non-deprecated method
                    query_params = st.query_params
                    
                    # Try provider-specific key in query params
                    query_param_name = f"{provider}_key"
                    if query_param_name in query_params:
                        config["api_key"] = query_params[query_param_name]
                        config["source"] = f"URL query parameter ({query_param_name})"
                        # Store in session state for future use
                        st.session_state[f"{provider}_api_key"] = config["api_key"]
                    
                    # For backward compatibility - check for claude_key
                    elif provider == "anthropic" and "claude_key" in query_params:
                        config["api_key"] = query_params["claude_key"]
                        config["source"] = "URL query parameter (claude_key)"
                        # Store in session state for future use
                        st.session_state.claude_api_key = config["api_key"]
                except Exception as e:
                    logger.warning(f"Error accessing query parameters: {e}")
                
        except (ImportError, RuntimeError):
            # Not running in Streamlit
            pass
    
    # Check if we can get a provider instance for validation
    if config["api_key"] and provider in PROVIDER_CLASSES:
        provider_class = PROVIDER_CLASSES[provider]
        
        # Create a temporary instance for validation
        try:
            provider_instance = provider_class(config["api_key"])
            if not provider_instance.validate_api_key():
                logger.warning(f"API key from {config['source']} has invalid format for {provider}")
                config["api_key"] = None
                config["source"] = None
        except Exception as e:
            logger.warning(f"Error validating API key: {e}")
    
    return config

def get_claude_api_key() -> Optional[str]:
    """
    Get the Claude API key from environment variable or other configuration.
    Validates basic format of the key.
    
    Returns:
        API key or None if not found or invalid
        
    Note:
        This function is kept for backward compatibility.
        New code should use get_ai_config() instead.
    """
    config = get_ai_config()
    if config["provider"] != "anthropic":
        # If using a different provider, try to get a specific Claude key
        anthropic_config = get_ai_config()
        anthropic_config["provider"] = "anthropic"
        return anthropic_config["api_key"]
        
    return config["api_key"]

def get_ai_provider(provider_name: str = None, api_key: str = None, model: str = None) -> Optional[AIProvider]:
    """
    Get an AI provider instance based on configuration.
    
    Args:
        provider_name: Provider name to use, or None to use configured default
        api_key: API key to use, or None to get from configuration
        model: Model to use, or None to use provider's default
        
    Returns:
        AIProvider instance or None if configuration is invalid
    """
    # Get configuration if needed
    config = get_ai_config()
    
    # Use arguments or fall back to config values
    provider = provider_name or config["provider"]
    key = api_key or config["api_key"]
    model_name = model or config["model"]
    
    if not key:
        logger.warning(f"No API key found for provider {provider}")
        return None
        
    # Check if provider class exists
    if provider not in PROVIDER_CLASSES:
        logger.error(f"Unknown AI provider: {provider}")
        return None
        
    # Create provider instance
    try:
        provider_class = PROVIDER_CLASSES[provider]
        instance = provider_class(key, model_name)
        return instance
    except Exception as e:
        logger.error(f"Error creating provider instance: {e}")
        return None

def extract_and_run_queries(ai_response: str) -> Tuple[str, List[str], List[Dict[str, Any]], Optional[Dict[str, str]]]:
    """
    Extract SQL queries from AI model's response, execute them, and add the results.
    Stops execution and returns failure details upon the first error encountered.
    
    Args:
        ai_response: Text response from the AI model
        
    Returns:
        Tuple of (
            processed_response: String containing original text and formatted query results/errors.
            executed_queries: List of queries attempted.
            results_data: List of dictionaries containing results or errors for each attempted query.
            failure_info: Dictionary with 'failed_query' and 'error_message' if an error occurred, else None.
        )
    """
    # Get the cache
    cache = get_arrow_cache()
    
    parts = []
    current_pos = 0
    executed_queries = []  # Track executed queries
    results_data = []  # Store query results for potential followup
    failure_info = None # Track if any errors occurred and store details
    
    # Look for query blocks
    while True:
        query_start = ai_response.find("<query>", current_pos)
        if query_start == -1:
            # Add the remaining text
            if current_pos < len(ai_response):
                parts.append(ai_response[current_pos:])
            break
            
        # Add text before the query
        if query_start > current_pos:
            parts.append(ai_response[current_pos:query_start])
            
        query_end = ai_response.find("</query>", query_start)
        if query_end == -1:
            # Malformed response, just add everything
            parts.append(ai_response[current_pos:])
            break
            
        # Extract the query
        query = ai_response[query_start + 8:query_end].strip()
        executed_queries.append(query)  # Add to executed queries list
        
        # Execute the query
        try:
            start_time = time.time()
            
            # Execute the query using cache's internal optimization
            result_df = cache.query(query, optimize=True)
            query_time = time.time() - start_time
            
            # Convert Arrow table to pandas DataFrame for easier display
            # (PyArrow Tables don't have to_markdown method)
            if hasattr(result_df, 'to_pandas'):
                pandas_df = result_df.to_pandas()
            else:
                pandas_df = result_df  # In case it's already a DataFrame
            
            # Store the results and query plan for potential followup
            results_data.append({
                "query": query, 
                "result_df": pandas_df,
                "query_time": query_time,
            })
            
            # Format results
            parts.append("\n\n**Query:**\n```sql\n")
            parts.append(query)
            parts.append("\n```\n\n")
            parts.append(f"*Query executed in {query_time:.3f}s*\n\n")
            parts.append("**Results:**\n\n")
            
            # Convert DataFrame to markdown table for better rendering
            # For smaller results, convert to markdown
            if len(pandas_df) < 10 and len(pandas_df.columns) < 8:
                try:
                    # Use pandas markdown format if available
                    parts.append(pandas_df.to_markdown(index=False))
                except (AttributeError, ImportError):
                    # Fallback if to_markdown is not available
                    parts.append(str(pandas_df))
            else:
                # For larger results, provide a message about the size
                parts.append(f"*Showing {len(pandas_df)} rows with {len(pandas_df.columns)} columns.*\n\n")
                # Add a sample of the data
                parts.append(pandas_df.head(5).to_string())
                parts.append("\n\n*...more rows...*\n\n")
                
        except Exception as e:
            # Error occurred
            error_message = str(e)
            
            # Provide more helpful error messages for common issues
            if "Table with name _cache_" in str(e) and "does not exist" in str(e):
                # Extract table name from error message
                import re
                table_name_match = re.search(r"_cache_(\w+)", str(e))
                if table_name_match:
                    table_name = table_name_match.group(1)
                    try:
                        available_tables = cache.get_keys() 
                        error_message = f"Table '{table_name}' not found. Available tables: {', '.join(available_tables)}"
                    except Exception as inner_e:
                        logger.error(f"Could not retrieve table keys for error message: {inner_e}")
                        # Keep original error message if getting keys fails
            
            # Format error for the response
            parts.append("\n\n**Query:**\n```sql\n")
            parts.append(query)
            parts.append("\n```\n\n**Error:** ")
            parts.append(error_message)
            parts.append("\n\n")
            
            # Store error info
            results_data.append({"query": query, "error": error_message, "query_time": 0})
            
            # *** Store failure info and return immediately ***
            failure_info = {"failed_query": query, "error_message": error_message}
            
            # Append the rest of the original response after the failed query block
            query_end = ai_response.find("</query>", query_start)
            if query_end != -1:
                current_pos = query_end + 8 # Move past </query>
                # Find the start of the next potential query or end of string
                next_query_start = ai_response.find("<query>", current_pos)
                if next_query_start != -1:
                     parts.append(ai_response[current_pos:next_query_start])
                else:
                    parts.append(ai_response[current_pos:])
            else:
                # Malformed, append remaining original text
                 parts.append(ai_response[current_pos:])

            return "".join(parts), executed_queries, results_data, failure_info
            
        # Successfully executed query, move to the next part of the response
        current_pos = query_end + 8  # Move past </query>
    
    # If loop completes without error
    return "".join(parts), executed_queries, results_data, None

def ask_ai(
    question: str, 
    api_key: str = None,
    provider: str = None,
    model: str = None,
    conversation_history: Optional[List[Dict[str, Any]]] = None,
    max_retries: int = 1  # Allow one retry by default
) -> str:
    """
    Ask an AI model a question about the datasets in the cache, with retry logic for SQL errors.
    
    Args:
        question: Question to ask
        api_key: API key (will use configuration if None)
        provider: Provider name (will use configuration if None)
        model: Model name (will use configuration if None)
        conversation_history: Optional conversation history
        max_retries: Maximum number of times to retry a failed query
        
    Returns:
        AI model's final response, potentially after retries and query execution/interpretation.
    """
    # Get or create provider
    ai_provider = get_ai_provider(provider, api_key, model)
    
    if not ai_provider:
        return f"Error: Could not initialize AI provider. Please check your API key for {provider or get_ai_config()['provider']}."
    
    # Initialize conversation history if not provided
    if conversation_history is None:
        conversation_history = []

    # --- Main interaction loop with retry logic --- 
    current_question = question
    retries_left = max_retries
    final_response_content = ""
    api_connection_retries = 3  # Add specific retries for API connection issues

    # --- First test basic connectivity to the API ---
    connection_success, connection_message = ai_provider.test_connection()
    if not connection_success:
        logger.warning(f"Connectivity test failed: {connection_message}")
        logger.info("Proceeding with API call attempt anyway, as the test may be unreliable")
        
        # Try a simple network test to check general internet connectivity
        try:
            import urllib.request
            internet_test_url = "https://www.google.com"
            urllib.request.urlopen(internet_test_url, timeout=5)
            logger.info("General internet connectivity seems to be working")
        except Exception as e:
            logger.warning(f"General internet connectivity check failed: {e}")
            logger.warning("This may indicate a network issue affecting API calls")

    while True:
        # Add the current question (or retry prompt) to the history
        conversation_history.append({
            "role": "user",
            "content": current_question,
            "timestamp": time.time()
        })
        
        # Get datasets info for context with enhanced metadata
        cache = get_arrow_cache()
        datasets = []
        try:
            from .core import get_datasets_list
            datasets = get_datasets_list()
        except Exception as e:
            logger.error(f"Error getting datasets list: {e}")
        
        # Prepare system prompt with dataset context
        system_prompt = f"""You are an expert data analyst assisting a user with datasets managed by Arrow Cache. 
Your goal is to answer user questions about the data, often by generating and explaining **DuckDB SQL** queries.

Available datasets:
{_format_datasets_info(datasets)}

**Instructions for Generating Queries:**
1.  **Dialect:** ALWAYS generate queries using **DuckDB SQL** syntax.
2.  **Table Prefix:** ALWAYS reference datasets using the `_cache_` prefix (e.g., `FROM _cache_{{dataset_name}}`).
3.  **Check Schema & Types:** BEFORE writing a query, carefully check the dataset's columns and **use the DuckDB data types provided** above (from `column_duckdb_types_json`).
4.  **Function Usage:** Use functions compatible with the column's DuckDB data type. See specific examples below.
5.  **Enclose Queries:** Enclose ALL SQL queries within `<query>...</query>` tags.
6.  **Explain:** After providing a query, briefly explain what it does.

**DuckDB SQL Specifics & Examples:**
*   **Date Truncation:** To get the date part from a TIMESTAMP or DATE column (e.g., `tpep_pickup_datetime`), use `DATE_TRUNC('day', tpep_pickup_datetime)`. 
    *   Example: `SELECT DATE_TRUNC('day', tpep_pickup_datetime) AS pickup_day, COUNT(*) FROM _cache_yellow_tripdata_2023_01 GROUP BY pickup_day;`
*   **Extracting Date Parts:** Use `EXTRACT(unit FROM column_name)` (e.g., `EXTRACT(hour FROM tpep_pickup_datetime)`).
*   **Geospatial (WKB):** If the dataset's 'Geometry Storage' is reported as 'WKB', you MUST use `ST_GeomFromWKB(geometry_column)` to convert the geometry before using other spatial functions (e.g., `ST_X(ST_Centroid(ST_GeomFromWKB(geometry)))`). Check the 'Geometry Storage' field above.
*   **Case Sensitivity:** Table and column names might be case-sensitive depending on creation; refer to the names listed above.
*   **String Literals:** Use single quotes (e.g., `WHERE vendor_id = '1'`).
*   **Avoid Non-DuckDB Functions:** Do NOT use functions like `DATE()` or `GETDATE()` if a DuckDB equivalent like `DATE_TRUNC` or `NOW()` exists.

**Visualization Requests (Optional):**
*   If your analysis involves spatial data (e.g., you query a geometry column), AFTER explaining the query results, you can request a map visualization by adding this tag on its own line: 
    `<visualize dataset="the_dataset_name" geometry_col="the_geometry_column_name" />`
*   Replace `the_dataset_name` and `the_geometry_column_name` with the actual names used.
"""
        
        api_call_successful = False
        api_error = None
        
        # Try multiple times for API connection issues
        for api_attempt in range(api_connection_retries):
            try:
                # Make the AI API call
                logger.info(f"Calling {ai_provider.provider_name} API (attempt {api_attempt+1}/{api_connection_retries})...")
                
                # Get completion from the provider
                initial_response_content, metadata = ai_provider.get_completion(
                    prompt=current_question,
                    system_prompt=system_prompt,
                    conversation_history=conversation_history,
                    max_tokens=2048,
                    timeout=60
                )
                
                # Check for errors in the response
                if "error" in metadata:
                    raise Exception(metadata["error"])
                    
                # Log success
                logger.info(f"{ai_provider.provider_name} API call successful (took {metadata.get('response_time', 0):.2f}s)")
                
                # Add AI's response to history before processing queries
                conversation_history.append({
                    "role": "assistant",
                    "content": initial_response_content,
                    "timestamp": time.time(),
                    "query_executed": None, 
                    "query_count": 0,
                    "error_occurred": False
                })
                
                api_call_successful = True
                break  # Successfully got response, exit retry loop
                
            except Exception as e:
                # Handle general exceptions with backoff
                api_error = f"Error: {str(e)}"
                logger.warning(f"API error (attempt {api_attempt+1}/{api_connection_retries}): {e}")
                time.sleep(2 * (api_attempt + 1))  # Progressive backoff
        
        # If all API call attempts failed
        if not api_call_successful:
            logger.error(f"All API call attempts failed: {api_error}")
            error_message = f"""Error interacting with {ai_provider.provider_name} API after {api_connection_retries} attempts: {api_error}
            
Please check:
1. Your internet connection
2. The validity of your API key
3. {ai_provider.provider_name.capitalize()} service status
            """
            # Add error response to conversation history
            conversation_history.append({
                "role": "assistant",
                "content": error_message,
                "timestamp": time.time(),
                "error_occurred": True
            })
            return error_message

        # STEP 1: Extract and execute queries from the latest response
        response_with_results, executed_queries, results_data, failure_info = extract_and_run_queries(initial_response_content)
        
        # Update the last history entry with execution info
        last_entry = conversation_history[-1]
        last_entry["content"] = response_with_results # Update content with results/errors
        last_entry["query_executed"] = executed_queries[0] if executed_queries else None
        last_entry["query_count"] = len(executed_queries)
        last_entry["error_occurred"] = failure_info is not None

        # --- Retry Logic --- 
        if failure_info and retries_left > 0:
            retries_left -= 1
            logger.info(f"Query failed. Retrying... ({retries_left} retries left)")
            
            # Construct the retry prompt
            current_question = f"""The previous query failed with the following error: 
```
{failure_info['error_message']}
```
The failed query was:
```sql
{failure_info['failed_query']}
```
Please analyze the error and provide a corrected SQL query within `<query>` tags. If you cannot correct it or the error is not SQL-related, explain the issue clearly."""
            
            # Continue the loop to ask AI again with the retry prompt
            continue
        else:
            # No failure, or no retries left - proceed to interpretation or finish
            final_response_content = response_with_results # Store the latest response
            break # Exit the retry loop
    
    # --- End of interaction loop --- 

    # STEP 2: If queries were executed (even if the last attempt failed but we are out of retries), ask AI to interpret the final results/errors
    if executed_queries:
        logger.info(f"Asking {ai_provider.provider_name} to interpret the final query results/errors.")
        
        # Prepare results context for the interpretation prompt
        results_summary = "\n\n--- Query Execution Summary ---"
        for item in results_data:
            results_summary += f"\n\nQuery:\n```sql\n{item['query']}\n```\n"
            if "error" in item:
                results_summary += f"Error: {item['error']}"
            else:
                # Show limited results for interpretation prompt
                df_str = item['result_df'].head(10).to_string()
                results_summary += f"Result (showing up to 10 rows):\n{df_str}"
                if len(item['result_df']) > 10:
                    results_summary += "\n... (more rows)"
            results_summary += f"\n(Execution Time: {item.get('query_time', 0):.3f}s)"
        results_summary += "\n--- End Summary ---"

        # Prepare the interpretation prompt for the user role
        interpretation_prompt = f"""The following SQL {"query was" if len(executed_queries) == 1 else "queries were"} executed based on your previous suggestion. 
{results_summary}

Please interpret these results concisely. {"If an error occurred in the last attempt, analyze the final error message and explain the likely cause or suggest how to proceed." if failure_info else "Focus on explaining what the data shows based on the successful query results."}"""

        # Add user interpretation request to history
        conversation_history.append({
            "role": "user",
            "content": interpretation_prompt,
            "timestamp": time.time()
        })

        # Get fresh system prompt for interpretation
        followup_system_prompt = f"""You are an expert data analyst. You previously provided SQL queries which were executed against datasets managed by Arrow Cache. The results (or errors) of those queries are provided. Your task is to INTERPRET these results for the user. 
Focus on explaining the meaning of the data or the reason for any errors. Do NOT generate new <query> tags in this interpretation step.

**Visualization Request:** If the analysis involved spatial data and a map is appropriate, include the visualization tag on its own line at the end of your interpretation:
`<visualize dataset="the_dataset_name" geometry_col="the_geometry_column_name" />` 
(Remember to use the correct dataset and column names)."""
        
        # Call for interpretation
        api_call_successful = False
        api_error = None
        
        for api_attempt in range(api_connection_retries):
            try:
                # Call AI for interpretation with extended timeout
                logger.info(f"Calling {ai_provider.provider_name} for interpretation (attempt {api_attempt+1}/{api_connection_retries})...")
                
                # Get completion from the provider
                interpretation_text, metadata = ai_provider.get_completion(
                    prompt=interpretation_prompt,
                    system_prompt=followup_system_prompt,
                    conversation_history=conversation_history,
                    max_tokens=1024,
                    timeout=60
                )
                
                # Check for errors in the response
                if "error" in metadata:
                    raise Exception(metadata["error"])
                
                logger.info(f"{ai_provider.provider_name} interpretation call successful (took {metadata.get('response_time', 0):.2f}s)")
                
                # Add interpretation to history
                conversation_history.append({
                    "role": "assistant",
                    "content": interpretation_text,
                    "timestamp": time.time()
                })
                final_response_content = interpretation_text # The interpretation is the final response
                api_call_successful = True
                break  # Successfully got response, exit retry loop
                
            except Exception as e:
                # Handle general exceptions with backoff
                api_error = str(e)
                logger.warning(f"Error during interpretation (attempt {api_attempt+1}/{api_connection_retries}): {e}")
                time.sleep(2 * (api_attempt + 1))  # Progressive backoff
        
        # If all interpretation API call attempts failed
        if not api_call_successful:
            # Append error message to the last response content if interpretation fails
            interpretation_error = f"\n\nError interpreting results after {api_connection_retries} attempts: {api_error}"
            logger.error(f"All interpretation API call attempts failed: {api_error}")
            final_response_content += interpretation_error

    # Return the final response content
    return final_response_content

def ask_claude(
    question: str, 
    api_key: str, 
    conversation_history: Optional[List[Dict[str, Any]]] = None,
    max_retries: int = 1
) -> str:
    """
    Ask Claude a question about the datasets in the cache (legacy function).
    
    This function is kept for backward compatibility.
    New code should use ask_ai() instead.
    
    Args:
        question: Question to ask
        api_key: Claude API key
        conversation_history: Optional conversation history
        max_retries: Maximum number of times to retry a failed query
        
    Returns:
        Claude's final response, potentially after retries and query execution/interpretation.
    """
    return ask_ai(
        question=question,
        api_key=api_key,
        provider="anthropic",
        conversation_history=conversation_history,
        max_retries=max_retries
    )

def _format_datasets_info(datasets: List[Dict[str, Any]]) -> str:
    """Helper to format dataset info for the system prompt."""
    if not datasets:
        return "No datasets currently loaded."
        
    datasets_info = ""
    for ds in datasets:
        # Basic dataset info
        datasets_info += f"\n- Dataset '{ds['name']}': {ds.get('row_count', 'Unknown')} rows, {len(ds.get('columns', []))} columns"
        
        # Add format information if available
        if 'format' in ds:
            datasets_info += f"\n  Format: {ds['format']}"
            
        # Add source information if available
        if 'source' in ds:
            datasets_info += f"\n  Source: {ds['source']}"
        
        # Add geospatial information if available
        if 'geometry_column' in ds:
            datasets_info += f"\n  Geometry Column: {ds['geometry_column']}"
            datasets_info += f"\n  Dominant Geometry Type: {ds.get('geometry_type', 'Unknown')}"
            datasets_info += f"\n  CRS: {ds.get('crs', 'Unknown')}"
            storage_format = ds.get('geometry_storage', 'Unknown')
            datasets_info += f"\n  Geometry Storage: {storage_format}"
            if storage_format == 'WKB':
                datasets_info += " (Hint: Use ST_GeomFromWKB(geometry_column) in queries)"
        
        # Add column details with DuckDB data types (using column_duckdb_types_json)
        column_duckdb_types = ds.get('column_duckdb_types') # From parsed JSON
        if column_duckdb_types and isinstance(column_duckdb_types, dict):
             datasets_info += "\n  Columns (with DuckDB Data Types):"
             for col, dtype in column_duckdb_types.items():
                 datasets_info += f"\n    - {col}: {dtype}"
        elif 'columns' in ds: # Fallback to just names if duckdb types missing
            datasets_info += f"\n  Columns: {', '.join(ds['columns'])}"
            
        # Add size info
        if 'size_mb' in ds:
             datasets_info += f"\n  Size: {ds['size_mb']:.2f} MB"
             
    return datasets_info.strip()

def display_conversation_history(conversation_history: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Format conversation history for display.
    
    Args:
        conversation_history: List of conversation entries
        
    Returns:
        Formatted conversation data
    """
    if not conversation_history:
        return []
    
    formatted_history = []
    for entry in conversation_history:
        formatted_entry = {
            "role": entry["role"],
            "content": entry["content"],
            "timestamp": entry["timestamp"]
        }
        
        # Add query information if available
        if "query_executed" in entry:
            formatted_entry["query_executed"] = entry["query_executed"]
        if "query_count" in entry:
            formatted_entry["query_count"] = entry["query_count"]
        if "error_occurred" in entry:
            formatted_entry["error_occurred"] = entry["error_occurred"]
            
        formatted_history.append(formatted_entry)
        
    return formatted_history

def add_clear_history_button() -> None:
    """
    Add a button to clear the conversation history.
    
    This is a no-op in this context - each MCP client has its own way to handle this.
    """
    # This function exists for compatibility with the original data_mcp.py
    # In the MCP context, clearing history would be handled by the client
    pass 

def get_supported_providers() -> Dict[str, List[str]]:
    """
    Get a dictionary of supported providers and their models.
    
    Returns:
        Dictionary mapping provider names to lists of supported models
    """
    return {
        provider_name: provider_class.supported_models
        for provider_name, provider_class in PROVIDER_CLASSES.items()
    }

def register_provider(provider_class: Type[AIProvider]) -> None:
    """
    Register a new provider class.
    
    Args:
        provider_class: Provider class to register
    """
    if not issubclass(provider_class, AIProvider):
        raise TypeError(f"Provider class must be a subclass of AIProvider")
        
    provider_name = provider_class.provider_name
    PROVIDER_CLASSES[provider_name] = provider_class
    logger.info(f"Registered provider: {provider_name}") 

def ask_ai_for_code(
    question: str,
    current_code: str,
    dataset_name: str = None,
    dataset_info: Dict[str, Any] = None,
    api_key: str = None,
    provider: str = None,
    model: str = None,
    conversation_history: Optional[List[Dict[str, Any]]] = None,
    max_retries: int = 1
) -> str:
    """
    Ask an AI model a question about visualization code, with code generation capabilities.
    
    Args:
        question: Question or request about the code
        current_code: The current code in the editor
        dataset_name: Name of the dataset being visualized
        dataset_info: Optional metadata about the dataset
        api_key: API key (will use configuration if None)
        provider: Provider name (will use configuration if None)
        model: Model name (will use configuration if None)
        conversation_history: Optional conversation history
        max_retries: Maximum number of times to retry a failed call
        
    Returns:
        AI model's response, potentially including generated code
    """
    # Get or create provider
    ai_provider = get_ai_provider(provider, api_key, model)
    
    if not ai_provider:
        return f"Error: Could not initialize AI provider. Please check your API key for {provider or get_ai_config()['provider']}."
    
    # Initialize conversation history if not provided
    if conversation_history is None:
        conversation_history = []
    
    # Get datasets info for reference
    datasets = []
    try:
        from .core import get_datasets_list
        datasets = get_datasets_list()
        if dataset_name and not dataset_info:
            dataset_info = next((ds for ds in datasets if ds['name'] == dataset_name), None)
    except Exception as e:
        logger.error(f"Error getting datasets list: {e}")
    
    # Format dataset info for context
    dataset_context = ""
    if dataset_name:
        dataset_context += f"\nThe user is working with dataset: '{dataset_name}'"
        
        if dataset_info:
            # Add basic dataset properties
            dataset_context += f"\nDataset properties:"
            dataset_context += f"\n- Rows: {dataset_info.get('row_count', 'Unknown')}"
            dataset_context += f"\n- Columns: {len(dataset_info.get('columns', []))}"
            
            # Add column information
            if 'columns' in dataset_info:
                dataset_context += f"\n- Column names: {', '.join(dataset_info['columns'])}"
                
            # Add geometry information if present
            if 'geometry_column' in dataset_info:
                dataset_context += f"\n- Geometry column: {dataset_info['geometry_column']}"
                dataset_context += f"\n- Geometry type: {dataset_info.get('geometry_type', 'Unknown')}"
    
    # Prepare system prompt
    system_prompt = f"""You are an expert Python data visualization assistant, specializing in Plotly, pandas, and geospatial visualization.
Your task is to help the user with visualization code, focusing on creating beautiful, effective, and accurate visualizations.

{dataset_context}

When responding to the user:
1. If you're asked to generate code, always include complete, ready-to-run Python code inside triple backticks with the python tag.
2. When providing code that should replace the user's current code, ask "Would you like me to update your code?" in your response.
3. For syntax or error fixes, clearly explain the issue and provide the corrected code.
4. For visualization improvements, explain your reasoning and provide the enhanced code.
5. For geospatial data, include specialized visualizations like maps using appropriate libraries.

**Current User Code:**
```python
{current_code}
```

**Important Code Patterns to Follow:**
- Get data using `cache.get('{dataset_name}')` or SQL with `cache.query()`
- Convert Arrow tables to pandas with `.to_pandas()`
- For geospatial data, use `geopandas` and appropriate map libraries
- Use Plotly for standard visualizations and Folium/Streamlit-Folium for interactive maps
- End with `st.plotly_chart(fig, use_container_width=True)` for Streamlit rendering

Provide clear, professional guidance focusing on visualization best practices."""
    
    # Add the current question to the history
    conversation_history.append({
        "role": "user",
        "content": question,
        "timestamp": time.time()
    })
    
    # Make the AI API call with retries
    api_call_successful = False
    api_error = None
    api_connection_retries = 3
    
    for api_attempt in range(api_connection_retries):
        try:
            # Call AI for code assistance
            logger.info(f"Calling {ai_provider.provider_name} API for code assistance (attempt {api_attempt+1}/{api_connection_retries})...")
            
            # Get completion from the provider
            response_content, metadata = ai_provider.get_completion(
                prompt=question,
                system_prompt=system_prompt,
                conversation_history=conversation_history,
                max_tokens=2048,
                timeout=60
            )
            
            # Check for errors in the response
            if "error" in metadata:
                raise Exception(metadata["error"])
                
            # Log success
            logger.info(f"{ai_provider.provider_name} API call successful (took {metadata.get('response_time', 0):.2f}s)")
            
            # Add AI's response to history
            conversation_history.append({
                "role": "assistant",
                "content": response_content,
                "timestamp": time.time()
            })
            
            api_call_successful = True
            break  # Successfully got response, exit retry loop
            
        except Exception as e:
            # Handle general exceptions with backoff
            api_error = f"Error: {str(e)}"
            logger.warning(f"API error (attempt {api_attempt+1}/{api_connection_retries}): {e}")
            time.sleep(2 * (api_attempt + 1))  # Progressive backoff
    
    # If all API call attempts failed
    if not api_call_successful:
        logger.error(f"All API call attempts failed: {api_error}")
        error_message = f"""Error interacting with {ai_provider.provider_name} API after {api_connection_retries} attempts: {api_error}
        
Please check:
1. Your internet connection
2. The validity of your API key
3. {ai_provider.provider_name.capitalize()} service status
        """
        # Add error response to conversation history
        conversation_history.append({
            "role": "assistant",
            "content": error_message,
            "timestamp": time.time(),
            "error_occurred": True
        })
        return error_message
    
    return response_content 