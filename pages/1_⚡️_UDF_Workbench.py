import streamlit as st

# Page config - MUST be the first Streamlit command
st.set_page_config(
    page_title="UDF Workbench | Data Mage",
    page_icon="⚡️",
    layout="wide",
    initial_sidebar_state="expanded"
)

import os
import sys
import logging
import pandas as pd
import importlib
import inspect
import tempfile
import re
import textwrap
import concurrent.futures
import functools
import json
import time
from typing import Dict, List, Any, Union, Optional, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.common import (
    get_cache,
    render_memory_usage,
    get_size_display
)

# Import from arrow-cache-mcp
try:
    from arrow_cache_mcp.core import get_datasets_list, import_data_directly
    from arrow_cache_mcp.ai import (
        get_ai_config,
        get_ai_provider,
        ask_ai,
        get_supported_providers,
        ask_ai_for_code
    )
    from arrow_cache_mcp.loaders import load_dataset_from_path
except ImportError as e:
    st.error(f"Failed to import from arrow-cache-mcp: {e}")
    st.stop()

# Initialize the cache
cache = get_cache()

# Try importing visualization libraries 
try:
    import plotly.express as px
    import plotly.graph_objects as go
    PLOTLY_AVAILABLE = True
except ImportError:
    PLOTLY_AVAILABLE = False
    st.warning("Plotly not available. Visualization features will be limited. Install with: pip install plotly")

# Default UDF code example
DEFAULT_UDF_CODE = '''@fused.udf
def udf(story_type: str = "top"):
    """
    Fetches top posts from Hacker News as a dataframe.

    Parameters:
    story_type (str): Type of stories to fetch. Options are:
                      - "top" for top stories
                      - "newest" for latest stories

    Returns:
    pandas.DataFrame: DataFrame containing HN posts with columns:
                     id, title, url, score, by (author), time, descendants (comments)
    """
    import pandas as pd
    import requests
    import time
    from datetime import datetime

    # Validate input
    if story_type not in ["top", "newest"]:
        raise ValueError('Invalid story_type. Must be "top" or "newest"')

    # Map story_type to the appropriate HN API endpoint
    endpoint_map = {"top": "topstories", "newest": "newstories"}

    endpoint = endpoint_map[story_type]

    # Fetch the list of top or newest story IDs
    response = requests.get(f"https://hacker-news.firebaseio.com/v0/{endpoint}.json")
    story_ids = response.json()

    # Only doing 5 stories for now
    story_ids = story_ids[:5]

    # Fetch details for each story ID
    stories = []
    for story_id in story_ids:
        try:
            # Get the story details
            story_response = requests.get(
                f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json"
            )
            story = story_response.json()

            # Skip if not a story or missing key fields
            if not story or story.get("type") != "story" or "title" not in story:
                continue

            # Add to our list
            stories.append(
                {
                    "id": story.get("id"),
                    "title": story.get("title"),
                    "url": story.get("url", ""),
                    "score": story.get("score", 0),
                    "by": story.get("by", ""),
                    "time": datetime.fromtimestamp(story.get("time", 0)),
                    "descendants": story.get("descendants", 0),
                }
            )

            # Brief pause to avoid overloading the API
            time.sleep(0.1)

        except Exception as e:
            print(f"Error fetching story {story_id}: {e}")

    # Convert the list of stories to a DataFrame
    df = pd.DataFrame(stories)

    # Add a timestamp for when the data was fetched
    df["fetched_at"] = datetime.now()

    return df
''' 

def extract_udf_name(code):
    """Extract the UDF function name from the code."""
    # First look for @fused.udf decorated functions
    match = re.search(r'@fused\.udf\s*\n\s*def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', code, re.DOTALL)
    if match:
        return match.group(1)
        
    # If not found, look for any function definition
    match = re.search(r'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\(', code)
    if match:
        return match.group(1)
    
    return "udf"  # Default name if pattern not found

def extract_udf_docs(code):
    """Extract the UDF docstring from the code."""
    match = re.search(r'"""(.*?)"""', code, re.DOTALL)
    if match:
        return match.group(1).strip()
    return ""  # Empty string if no docstring found

def run_udf_code(code, params=None, additional_udfs=None, parallel=False):
    """
    Run the UDF code and return the result.
    
    Args:
        code: The UDF code as a string
        params: Optional parameters to pass to the UDF
        additional_udfs: Dictionary of additional UDF functions to make available
        parallel: Whether to run UDFs in parallel when possible
        
    Returns:
        Tuple of (success, result or error message)
    """
    try:
        # Create a temporary module
        temp_module = tempfile.NamedTemporaryFile(suffix='.py', delete=False)
        module_path = temp_module.name
        
        # Add fused import if it's not in the code
        if "import fused" not in code and "from fused" not in code:
            setup_code = "import fused\n"
        else:
            setup_code = ""
        
        # Add support for parallel execution if requested
        if parallel:
            setup_code += """
# Parallel execution support
import concurrent.futures
import functools

def run_parallel(func_list, *args, **kwargs):
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(func, *args, **kwargs) for func in func_list]
        return [future.result() for future in concurrent.futures.as_completed(futures)]
"""
        
        # Add additional UDFs if provided
        if additional_udfs:
            for udf_name, udf_code in additional_udfs.items():
                setup_code += f"\n# Additional UDF: {udf_name}\n{udf_code}\n"
            
        # Write the code to the temporary file
        with open(module_path, 'w') as f:
            f.write(setup_code + code)
        
        # Import the module
        module_name = os.path.basename(module_path)[:-3]  # Remove .py extension
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Get the UDF function
        udf_name = extract_udf_name(code)
        udf_func = getattr(module, udf_name)
        
        # Monkey patch run function to support additional UDFs
        if hasattr(module, 'fused'):
            original_run = module.fused.run
            
            # Create a patched run function that can access other UDFs
            def patched_run(func, *args, **kwargs):
                # If func is a string, try to look it up in the module
                if isinstance(func, str):
                    if hasattr(module, func):
                        return original_run(getattr(module, func), *args, **kwargs)
                return original_run(func, *args, **kwargs)
            
            # Replace the run function
            module.fused.run = patched_run
            
            # Run the UDF using fused.run
            try:
                # First try to run with the local engine to avoid environment errors
                if params:
                    result = module.fused.run(udf_func, engine='local', **params)
                else:
                    result = module.fused.run(udf_func, engine='local')
            except Exception as e:
                logger.error(f"Error running UDF with local engine: {e}")
                # If local engine fails, try the default engine
                if params:
                    result = module.fused.run(udf_func, **params)
                else:
                    result = module.fused.run(udf_func)
                
            # Clean up the temporary file
            os.unlink(module_path)
            
            return True, result
        else:
            return False, "Error: fused module not imported in the code"
    
    except Exception as e:
        # Make sure to delete the temporary file
        try:
            if os.path.exists(module_path):
                os.unlink(module_path)
        except:
            pass
            
        logger.error(f"Error running UDF: {e}")
        return False, f"Error: {str(e)}"

def register_udf_result(result, dataset_name=None):
    """
    Register the UDF result as a dataset in the arrow cache.
    
    Args:
        result: The result from the UDF (should be a DataFrame)
        dataset_name: Optional name for the dataset
        
    Returns:
        Tuple of (success, dataset info or error message)
    """
    try:
        # Check if the result is a DataFrame-like object
        if not hasattr(result, "columns"):
            return False, "Error: UDF result is not a DataFrame or DataFrame-like object"
            
        # Generate a dataset name if not provided
        if not dataset_name:
            # Get a timestamp for uniqueness
            import time
            timestamp = int(time.time())
            dataset_name = f"udf_result_{timestamp}"
        
        # Convert to pandas if needed
        if hasattr(result, "to_pandas"):
            # It's an Arrow table or similar
            df = result.to_pandas()
        else:
            # Assume it's already a pandas DataFrame
            df = result
        
        # Check if we're dealing with a GeoDataFrame
        is_geo_df = False
        
        # Try to check if it's a GeoDataFrame by type name
        if 'GeoDataFrame' in str(type(df)):
            is_geo_df = True
        # Check for 'geometry' column which is common in GeoDataFrames
        elif 'geometry' in df.columns:
            is_geo_df = True
            
        # First approach: Try using temporary CSV file
        import tempfile
        import os
        
        # Create temporary CSV file
        temp_dir = tempfile.gettempdir()
        temp_csv_path = os.path.join(temp_dir, f"{dataset_name}.csv")
        
        # Save DataFrame to CSV, dropping geometry column if it exists
        # This is a safer approach that avoids geopandas dependency issues
        try:
            # If it's a GeoDataFrame, convert to regular DataFrame dropping geometry
            if is_geo_df:
                # Drop the geometry column for CSV storage
                if 'geometry' in df.columns:
                    df_for_csv = df.drop(columns=['geometry'])
                else:
                    df_for_csv = df
            else:
                df_for_csv = df
                
            # Save to CSV
            df_for_csv.to_csv(temp_csv_path, index=False)
            
            # Import the loader function from arrow-cache-mcp
            from arrow_cache_mcp.loaders import load_dataset_from_path
            
            # Load the CSV into the cache
            success, result_info = load_dataset_from_path(
                temp_csv_path, 
                dataset_name, 
                format="csv"
            )
            
            # Clean up the temporary file
            try:
                os.remove(temp_csv_path)
            except Exception as e:
                logger.warning(f"Failed to clean up temporary CSV file: {e}")
                
            if success:
                # Get dataset info
                datasets = get_datasets_list()
                dataset_info = next((d for d in datasets if d['name'] == dataset_name), {})
                
                if not dataset_info:
                    # Create basic info if needed
                    dataset_info = {
                        'name': dataset_name,
                        'row_count': len(df_for_csv),
                        'column_count': len(df_for_csv.columns),
                        'size_bytes': 0
                    }
                
                return True, {
                    "name": dataset_name,
                    "row_count": len(df_for_csv),
                    "column_count": len(df_for_csv.columns),
                    "size_bytes": dataset_info.get('size_bytes', 0),
                    "columns": list(df_for_csv.columns)
                }
            else:
                return False, f"Error loading dataset from CSV: {result_info}"
            
        except Exception as e:
            logger.error(f"Error in CSV approach: {e}")
            # Continue to next approach - we don't return here
        
        # If we got here, CSV approach failed - try direct import
        try:
            # Get cache and try to use put method directly
            from arrow_cache_mcp.core import import_data_directly
            
            success, result_info = import_data_directly(
                key=dataset_name,
                source=df.to_dict('records'),  # Convert to list of dictionaries
                source_type="dict"
            )
            
            if success:
                # Get dataset info
                datasets = get_datasets_list()
                dataset_info = next((d for d in datasets if d['name'] == dataset_name), {})
                
                if not dataset_info:
                    # Create basic info
                    dataset_info = {
                        'name': dataset_name,
                        'row_count': len(df),
                        'column_count': len(df.columns),
                        'size_bytes': 0
                    }
                
                return True, {
                    "name": dataset_name,
                    "row_count": len(df),
                    "column_count": len(df.columns),
                    "size_bytes": dataset_info.get('size_bytes', 0),
                    "columns": list(df.columns)
                }
            else:
                return False, f"Error with direct import: {result_info}"
        
        except Exception as e:
            logger.error(f"Error in direct import approach: {e}")
            return False, f"Error: {str(e)}"
        
    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        logger.error(f"Error registering UDF result: {e}")
        logger.error(error_details)
        return False, f"Error: {str(e)}" 

def sql_query_datasets(query, cache=None):
    """
    Execute a SQL query against the cached datasets.
    
    Args:
        query: SQL query to execute
        cache: Optional cache instance
        
    Returns:
        Tuple of (success, result or error message)
    """
    if cache is None:
        cache = get_cache()
    
    try:
        # Execute the query
        result = cache.query(query)
        return True, result
    except Exception as e:
        return False, f"Query error: {str(e)}"

def ensure_udf_can_receive_input(code, input_param="input_data"):
    """
    Check if a UDF can accept an input parameter (like input_data).
    If not, modify the code to add the parameter with a default value of None.
    
    Args:
        code: The UDF code as a string
        input_param: The parameter name to check for or add
        
    Returns:
        Modified code (if needed) or original code
    """
    # Check if the parameter already exists
    # First, extract the function signature
    func_pattern = r'def\s+([a-zA-Z_][a-zA-Z0-9_]*)\s*\((.*?)\):'
    match = re.search(func_pattern, code, re.DOTALL)
    
    if not match:
        # Can't find a function, return original code
        return code
    
    func_name = match.group(1)
    params_str = match.group(2).strip()
    
    # Check if the input_param already exists
    param_names = []
    for param in params_str.split(','):
        param = param.strip()
        if not param:
            continue
        # Get the parameter name (before any = or :)
        param_name = param.split('=')[0].split(':')[0].strip()
        param_names.append(param_name)
    
    # If input_param already exists, no change needed
    if input_param in param_names:
        return code
    
    # Otherwise, add the parameter to the function
    # We'll add it at the end with a default value of None
    if params_str:
        # Add to existing parameters
        new_params = f"{params_str}, {input_param}=None"
    else:
        # No existing parameters
        new_params = f"{input_param}=None"
    
    # Replace the function signature
    old_signature = f"def {func_name}({params_str}):"
    new_signature = f"def {func_name}({new_params}):"
    
    return code.replace(old_signature, new_signature)

class UDFAITool:
    """
    A class that provides tools for AI models to interact with UDFs.
    This allows AI agents to run UDFs, inspect their outputs, and chain them together.
    """
    
    def __init__(self, st_session=None):
        """
        Initialize the UDF AI Tool with access to the Streamlit session state.
        
        Args:
            st_session: The Streamlit session state object
        """
        self.session = st_session
        self.execution_history = []
        self.udf_registry = {}
        
        # If we have a session, load UDFs from it
        if self.session:
            self._load_udfs_from_session()
    
    def _load_udfs_from_session(self):
        """Load UDFs from the Streamlit session state."""
        if not self.session:
            return
            
        # Get UDFs from session state
        udf_codes = self.session.get("udf_codes", [])
        udf_tabs = self.session.get("udf_tabs", [])
        
        # Register each UDF by both function name and display name for flexibility
        for i, (code, tab_name) in enumerate(zip(udf_codes, udf_tabs)):
            udf_name = extract_udf_name(code)
            params = self._extract_udf_params(code)
            
            # Store by function name
            self.udf_registry[udf_name] = {
                "code": code,
                "display_name": tab_name,
                "index": i,
                "params": params
            }
            
            # Also store by tab name for easier lookup
            self.udf_registry[tab_name] = {
                "code": code,
                "display_name": tab_name,
                "actual_name": udf_name,
                "index": i,
                "params": params
            }
            
            # Store by index as well to ensure chain steps work
            self.udf_registry[f"index_{i}"] = {
                "code": code, 
                "display_name": tab_name,
                "actual_name": udf_name,
                "index": i,
                "params": params
            }
    
    def _extract_udf_params(self, code: str) -> Dict[str, Dict[str, Any]]:
        """
        Extract parameter information from UDF code.
        
        Args:
            code: The UDF code as a string
            
        Returns:
            Dictionary mapping parameter names to type and default value
        """
        params = {}
        param_pattern = r'def\s+[a-zA-Z_][a-zA-Z0-9_]*\s*\((.*?)\):'
        param_match = re.search(param_pattern, code, re.DOTALL)
        
        if param_match:
            param_str = param_match.group(1).strip()
            if param_str and param_str != "":
                param_parts = []
                current_part = ""
                brace_level = 0
                
                for char in param_str:
                    if char == ',' and brace_level == 0:
                        param_parts.append(current_part.strip())
                        current_part = ""
                    else:
                        current_part += char
                        if char in '[{(':
                            brace_level += 1
                        elif char in ']})':
                            brace_level = max(0, brace_level - 1)
                
                if current_part:
                    param_parts.append(current_part.strip())
                
                for part in param_parts:
                    # Skip self parameter
                    if part.strip() == 'self':
                        continue
                        
                    # Parameter with default value
                    if '=' in part:
                        name, default = part.split('=', 1)
                        name = name.strip()
                        
                        # Check for type hints
                        if ':' in name:
                            name, type_hint = name.split(':', 1)
                            name = name.strip()
                            type_hint = type_hint.strip()
                        else:
                            type_hint = "any"
                        
                        # Try to evaluate the default value
                        try:
                            default_val = eval(default.strip())
                            param_type = type(default_val).__name__
                        except:
                            default_val = default.strip()
                            param_type = "str"
                        
                        params[name] = {
                            "type": param_type,
                            "type_hint": type_hint,
                            "default": default_val
                        }
                    else:
                        # Parameter without default value
                        name = part.strip()
                        
                        # Check for type hints
                        if ':' in name:
                            name, type_hint = name.split(':', 1)
                            name = name.strip()
                            type_hint = type_hint.strip()
                        else:
                            type_hint = "any"
                            
                        params[name] = {
                            "type": "any",
                            "type_hint": type_hint,
                            "required": True
                        }
        
        return params
    
    def list_available_udfs(self) -> List[Dict[str, Any]]:
        """
        List all available UDFs and their parameters.
        
        Returns:
            List of dictionaries containing UDF information
        """
        result = []
        # First collect all unique UDFs (avoid duplicates from display name entries)
        unique_udfs = {}
        
        for udf_name, udf_info in self.udf_registry.items():
            # Skip entries that are just aliases (display name references)
            if "actual_name" in udf_info:
                continue
                
            unique_udfs[udf_name] = udf_info
                
        # Now create the result list
        for udf_name, udf_info in unique_udfs.items():
            result.append({
                "name": udf_name,
                "display_name": udf_info["display_name"],
                "parameters": udf_info["params"],
                "index": udf_info["index"]
            })
            
        # Sort by index to maintain order from editor
        result.sort(key=lambda x: x["index"])
        return result
    
    def get_udf_details(self, udf_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a specific UDF.
        
        Args:
            udf_name: The name of the UDF to get details for
            
        Returns:
            Dictionary containing UDF details or error message
        """
        if udf_name not in self.udf_registry:
            return {"error": f"UDF '{udf_name}' not found"}
            
        udf_info = self.udf_registry[udf_name]
        return {
            "name": udf_name,
            "display_name": udf_info["display_name"],
            "parameters": udf_info["params"],
            "docstring": extract_udf_docs(udf_info["code"])
        }
    
    def execute_udf(self, udf_name: str, parameters: Dict[str, Any] = None) -> Dict[str, Any]:
        """
        Execute a UDF with the given parameters.
        
        Args:
            udf_name: The name of the UDF to execute (function name or display name)
            parameters: Dictionary of parameter values to pass to the UDF
            
        Returns:
            Dictionary containing execution results or error
        """
        # Check if UDF exists
        if udf_name not in self.udf_registry:
            # Try looking for matching display name in case a tab/display name was passed
            display_name_matches = [name for name, info in self.udf_registry.items() 
                                  if info.get("display_name") == udf_name]
            if display_name_matches:
                udf_name = display_name_matches[0]
            else:
                return {"error": f"UDF '{udf_name}' not found"}
            
        udf_info = self.udf_registry[udf_name]
        
        # If this is a display name reference, get the actual UDF name
        if "actual_name" in udf_info:
            actual_udf_name = udf_info["actual_name"]
            udf_info = self.udf_registry[actual_udf_name]
            
        code = udf_info["code"]
        
        # Execute the UDF
        success, result = run_udf_code(code, parameters or {})
        
        # Use display name for recording
        display_name = udf_info.get("display_name", udf_name)
        
        # Record execution in history
        execution_record = {
            "udf": display_name,
            "udf_name": udf_name,
            "parameters": parameters,
            "success": success,
            "timestamp": pd.Timestamp.now().isoformat()
        }
        
        # Add result info to the record
        if success:
            # Store the actual result object for potential chaining
            execution_record["actual_result"] = result
            
            if hasattr(result, 'shape'):
                execution_record["result_shape"] = list(result.shape)
            if hasattr(result, 'columns'):
                execution_record["result_columns"] = list(result.columns)
            if hasattr(result, 'dtypes'):
                execution_record["result_dtypes"] = {
                    str(col): str(dtype) for col, dtype in result.dtypes.items()
                }
        else:
            execution_record["error"] = str(result)
            
        # Save to execution history
        self.execution_history.append(execution_record)
        
        # Return execution info
        response = {
            "execution_id": len(self.execution_history) - 1,
            "success": success
        }
        
        if success:
            # Return dataset info
            if hasattr(result, 'shape'):
                response["shape"] = list(result.shape)
            if hasattr(result, 'columns'):
                response["columns"] = list(result.columns)
            if hasattr(result, 'dtypes'):
                response["dtypes"] = {
                    str(col): str(dtype) for col, dtype in result.dtypes.items()
                }
                
            # Get sample data
            if hasattr(result, 'head'):
                try:
                    sample = result.head(5)
                    if hasattr(sample, 'to_dict'):
                        response["sample_data"] = sample.to_dict('records')
                except Exception as e:
                    response["sample_error"] = str(e)
                    
            # Register the result as a dataset automatically
            if hasattr(result, 'columns'):
                dataset_name = f"udf_{udf_name}_{int(time.time())}"
                success, dataset_info = register_udf_result(result, dataset_name)
                if success:
                    response["dataset"] = dataset_info
                    response["dataset_name"] = dataset_name
        else:
            response["error"] = str(result)
            
        return response
    
    def execute_udf_with_code(self, udf_name: str, parameters: Dict[str, Any] = None, code: str = None) -> Dict[str, Any]:
        """
        Execute a UDF with the given parameters and specific code.
        
        Args:
            udf_name: The name of the UDF to execute
            parameters: Dictionary of parameter values to pass to the UDF
            code: Optional code to use instead of the stored UDF code
            
        Returns:
            Dictionary containing execution results or error
        """
        # If no code provided, get it from the registry
        if code is None:
            if udf_name not in self.udf_registry:
                # Try looking for matching display name
                display_name_matches = [name for name, info in self.udf_registry.items() 
                                      if info.get("display_name") == udf_name]
                if display_name_matches:
                    udf_name = display_name_matches[0]
                else:
                    return {"error": f"UDF '{udf_name}' not found"}
                
            udf_info = self.udf_registry[udf_name]
            
            # If this is a display name reference, get the actual UDF name
            if "actual_name" in udf_info:
                actual_udf_name = udf_info["actual_name"]
                udf_info = self.udf_registry[actual_udf_name]
                
            code = udf_info["code"]
        
        # Execute the UDF with the provided code
        success, result = run_udf_code(code, parameters or {})
        
        # Use the display name and function name for recording
        display_name = udf_name
        if udf_name in self.udf_registry:
            display_name = self.udf_registry[udf_name].get("display_name", udf_name)
        
        # Record execution in history
        execution_record = {
            "udf": display_name,
            "udf_name": udf_name,
            "parameters": parameters,
            "success": success,
            "timestamp": pd.Timestamp.now().isoformat()
        }
        
        # Add result info to the record
        if success:
            # Store the actual result object for potential chaining
            execution_record["actual_result"] = result
            
            if hasattr(result, 'shape'):
                execution_record["result_shape"] = list(result.shape)
            if hasattr(result, 'columns'):
                execution_record["result_columns"] = list(result.columns)
            if hasattr(result, 'dtypes'):
                execution_record["result_dtypes"] = {
                    str(col): str(dtype) for col, dtype in result.dtypes.items()
                }
        else:
            execution_record["error"] = str(result)
            
        # Save to execution history
        self.execution_history.append(execution_record)
        
        # Return execution info
        response = {
            "execution_id": len(self.execution_history) - 1,
            "success": success
        }
        
        if success:
            # Return dataset info
            if hasattr(result, 'shape'):
                response["shape"] = list(result.shape)
            if hasattr(result, 'columns'):
                response["columns"] = list(result.columns)
            if hasattr(result, 'dtypes'):
                response["dtypes"] = {
                    str(col): str(dtype) for col, dtype in result.dtypes.items()
                }
                
            # Get sample data
            if hasattr(result, 'head'):
                try:
                    sample = result.head(5)
                    if hasattr(sample, 'to_dict'):
                        response["sample_data"] = sample.to_dict('records')
                except Exception as e:
                    response["sample_error"] = str(e)
                    
            # Register the result as a dataset automatically
            if hasattr(result, 'columns'):
                dataset_name = f"udf_{udf_name}_{int(time.time())}"
                success, dataset_info = register_udf_result(result, dataset_name)
                if success:
                    response["dataset"] = dataset_info
                    response["dataset_name"] = dataset_name
        else:
            response["error"] = str(result)
            
        return response
    
    def chain_udfs(self, execution_plan: List[Dict[str, Any]], sequential_mode: bool = False) -> Dict[str, Any]:
        """
        Execute a chain of UDFs where outputs from earlier UDFs can be used as inputs to later ones.
        
        Args:
            execution_plan: List of dictionaries specifying UDFs to execute and how to chain them
                Each dict should have:
                - udf_name: Name of the UDF to execute (function name, display name or index)
                - parameters: Dictionary of parameter values
                - input_from: Optional ID of previous execution to use as input
                - input_param: Parameter name to pass the input to
            sequential_mode: If True, execute UDFs sequentially without feeding data between them
        
        Returns:
            Dictionary containing execution results of all UDFs in the chain
        """
        results = []
        intermediate_outputs = {}
        
        # Debug the execution plan
        logger.info(f"Executing UDF chain with plan: {execution_plan}")
        logger.info(f"Available UDFs: {list(self.udf_registry.keys())}")
        logger.info(f"Sequential mode: {sequential_mode}")
        
        for step_idx, step in enumerate(execution_plan):
            udf_name = step.get("udf_name")
            parameters = step.get("parameters", {})
            input_from = step.get("input_from")
            input_param = step.get("input_param")
            
            logger.info(f"Executing step {step_idx}: UDF={udf_name}, params={parameters}")
            
            # Get the UDF code
            udf_info = None
            if udf_name in self.udf_registry:
                udf_info = self.udf_registry[udf_name]
            else:
                # Try looking for matching display name
                for reg_name, reg_info in self.udf_registry.items():
                    if reg_info.get("display_name") == udf_name:
                        udf_info = reg_info
                        break
            
            if not udf_info:
                logger.error(f"UDF {udf_name} not found in registry")
                results.append({
                    "step": step_idx,
                    "udf": udf_name,
                    "result": {"success": False, "error": f"UDF {udf_name} not found"}
                })
                continue
                
            # Get the UDF code
            udf_code = udf_info.get("code", "")
            
            # Check if we need to use output from a previous step - only in non-sequential mode
            if not sequential_mode and input_from is not None and input_from in intermediate_outputs:
                # Get the output from a previous step
                input_data = intermediate_outputs[input_from]
                
                # Ensure the UDF can accept the input parameter
                if input_param and input_param not in parameters:
                    # We're adding it to the parameters, make sure the UDF can accept it
                    udf_code = ensure_udf_can_receive_input(udf_code, input_param)
                    
                    # Add it to the parameters
                    parameters[input_param] = input_data
                    logger.info(f"Using output from step {input_from} as input to parameter {input_param}")
            
            # Execute the UDF - allowing for different ways to identify the UDF
            try:
                # Use the modified code if needed
                execution_result = self.execute_udf_with_code(udf_name, parameters, udf_code)
                logger.info(f"Execution result success: {execution_result.get('success', False)}")
            except Exception as e:
                logger.error(f"Error executing UDF {udf_name}: {e}")
                execution_result = {"success": False, "error": f"Error: {str(e)}"}
            
            # Store the result for potential future use
            if execution_result.get("success", False):
                execution_id = execution_result.get("execution_id")
                if execution_id is not None:
                    # Get the actual output data from the execution history
                    actual_result = self.execution_history[execution_id].get("actual_result")
                    intermediate_outputs[step_idx] = actual_result
                    logger.info(f"Stored result from step {step_idx} for future use")
            
            # Save the result
            results.append({
                "step": step_idx,
                "udf": udf_name,
                "result": execution_result
            })
            
            # Register the final result as a dataset
            if step_idx == len(execution_plan) - 1 and execution_result.get("success", False):
                execution_id = execution_result.get("execution_id")
                if execution_id is not None:
                    actual_result = self.execution_history[execution_id].get("actual_result")
                    if hasattr(actual_result, 'columns'):
                        dataset_name = f"udf_chain_result_{int(time.time())}"
                        success, dataset_info = register_udf_result(actual_result, dataset_name)
                        if success:
                            execution_result["dataset"] = dataset_info
                            execution_result["dataset_name"] = dataset_name
                            logger.info(f"Registered final chain result as dataset: {dataset_name}")
        
        return {
            "chain_results": results,
            "final_result": results[-1] if results else None
        }
    
    def get_execution_history(self) -> List[Dict[str, Any]]:
        """
        Get the execution history of UDFs.
        
        Returns:
            List of dictionaries containing execution history
        """
        # Return execution history without the actual result data (which could be large)
        return [
            {k: v for k, v in record.items() if k != "actual_result"} 
            for record in self.execution_history
        ]
    
    def get_execution_result(self, execution_id: int) -> Dict[str, Any]:
        """
        Get the detailed result of a specific UDF execution.
        
        Args:
            execution_id: The ID of the execution to get results for
            
        Returns:
            Dictionary containing execution details or error
        """
        if execution_id < 0 or execution_id >= len(self.execution_history):
            return {"error": f"Execution ID {execution_id} not found"}
            
        execution = self.execution_history[execution_id]
        
        # Get the actual result
        actual_result = execution.get("actual_result")
        
        response = {
            "execution_id": execution_id,
            "udf": execution.get("udf"),
            "parameters": execution.get("parameters"),
            "success": execution.get("success"),
            "timestamp": execution.get("timestamp")
        }
        
        if execution.get("success", False) and actual_result is not None:
            # Return dataset info
            if hasattr(actual_result, 'shape'):
                response["shape"] = list(actual_result.shape)
            if hasattr(actual_result, 'columns'):
                response["columns"] = list(actual_result.columns)
                
            # Get sample data
            if hasattr(actual_result, 'head'):
                try:
                    sample = actual_result.head(5)
                    if hasattr(sample, 'to_dict'):
                        response["sample_data"] = sample.to_dict('records')
                except Exception as e:
                    response["sample_error"] = str(e)
        else:
            response["error"] = execution.get("error")
            
        return response 

def handle_ai_udf_request(request_data):
    """
    Handle requests from AI models to work with UDFs.
    
    Args:
        request_data: Dictionary containing the request details
        
    Returns:
        Dictionary containing the response
    """
    if not isinstance(request_data, dict):
        return {"error": "Request must be a dictionary"}
    
    # Create UDF AI tool with access to session state
    udf_tool = UDFAITool(st.session_state)
    
    # Get the action to perform
    action = request_data.get("action")
    if not action:
        return {"error": "No action specified in request"}
    
    # Handle different actions
    if action == "list_udfs":
        return {"udfs": udf_tool.list_available_udfs()}
    
    elif action == "get_udf_details":
        udf_name = request_data.get("udf_name")
        if not udf_name:
            return {"error": "No UDF name specified"}
        return udf_tool.get_udf_details(udf_name)
    
    elif action == "execute_udf":
        udf_name = request_data.get("udf_name")
        parameters = request_data.get("parameters", {})
        if not udf_name:
            return {"error": "No UDF name specified"}
        return udf_tool.execute_udf(udf_name, parameters)
    
    elif action == "chain_udfs":
        execution_plan = request_data.get("execution_plan", [])
        if not execution_plan:
            return {"error": "No execution plan specified"}
        return udf_tool.chain_udfs(execution_plan)
    
    elif action == "get_execution_history":
        return {"history": udf_tool.get_execution_history()}
    
    elif action == "get_execution_result":
        execution_id = request_data.get("execution_id")
        if execution_id is None:
            return {"error": "No execution ID specified"}
        try:
            execution_id = int(execution_id)
        except:
            return {"error": "Execution ID must be an integer"}
        return udf_tool.get_execution_result(execution_id)
    
    else:
        return {"error": f"Unknown action: {action}"}

def udf_ai_agent(question, udf_tool=None):
    """
    AI agent that analyzes a user's question and executes UDFs to answer it.
    
    Args:
        question: The user's question as a string
        udf_tool: Optional UDFAITool instance
        
    Returns:
        Dictionary with the analysis results and any executed UDFs
    """
    # Get AI configuration
    ai_config = get_ai_config()
    api_key = ai_config.get("api_key")
    provider = st.session_state.get("ai_provider", "anthropic")
    model = st.session_state.get(f"{provider}_model")
    
    if not api_key:
        return {"error": f"No API key available for {provider}"}
    
    # Create UDF tool if not provided
    if udf_tool is None:
        udf_tool = UDFAITool(st.session_state)
    
    # Get available UDFs
    available_udfs = udf_tool.list_available_udfs()
    if not available_udfs:
        return {"error": "No UDFs available for the agent to work with"}
    
    # Create context for the AI
    udf_context = []
    for udf in available_udfs:
        details = udf_tool.get_udf_details(udf["name"])
        udf_context.append(
            f"UDF: {udf['name']} (Display Name: {udf['display_name']})\n"
            f"Description: {details.get('docstring', 'No description')}\n"
            f"Parameters: {json.dumps(details.get('parameters', {}), indent=2)}"
        )
    
    udf_context_str = "\n\n".join(udf_context)
    
    # Create the prompt for the AI
    prompt = f"""
You are an AI data analyst working with Fused UDFs (User Defined Functions). 
Your job is to understand the user's question, identify which UDFs would help answer it,
and create an execution plan to get the information needed.

AVAILABLE UDFs:
{udf_context_str}

USER QUESTION:
{question}

Your task:
1. Analyze what data the user is asking for
2. Determine which UDFs would provide this data
3. Identify any parameters needed for those UDFs
4. Create an execution plan that chains UDFs together if needed

When referring to UDFs, you can use either their function name or display name.
For example, you can use "udf" (function name) or "HackerNews" (display name).

Respond with a JSON object containing:
- analysis: Your understanding of the user's question
- execution_plan: List of UDFs to execute with their parameters
- chain: Boolean indicating if the UDFs should be chained
- sequential: Boolean indicating if UDFs should run sequentially without sharing data (defaults to false)

Example response format:
```json
{{
  "analysis": "User wants HackerNews stories with more than 50 points",
  "execution_plan": [
    {{
      "udf_name": "udf",
      "parameters": {{ "story_type": "top" }}
    }}
  ],
  "chain": false,
  "sequential": false
}}
```

If UDFs need to be chained where one's output is another's input, indicate which parameter should receive the output.
For example:
```json
{{
  "execution_plan": [
    {{
      "udf_name": "udf_1",
      "parameters": {{ "param1": "value1" }}
    }},
    {{
      "udf_name": "udf_2",
      "parameters": {{ "param2": "value2" }},
      "input_from": 0,
      "input_param": "input_data"
    }}
  ],
  "chain": true,
  "sequential": false
}}
```

If you want to run multiple UDFs sequentially without sharing data between them, set sequential to true.
For example:
```json
{{
  "execution_plan": [
    {{
      "udf_name": "udf_1",
      "parameters": {{ "param1": "value1" }}
    }},
    {{
      "udf_name": "udf_2",
      "parameters": {{ "param2": "value2" }}
    }}
  ],
  "chain": false,
  "sequential": true
}}
```

IMPORTANT: Only include UDFs that actually exist in the available UDFs list.
Only provide the execution plan in the specified JSON format.
"""
    
    # Call the AI to analyze and create an execution plan
    try:
        response = ask_ai(
            question=prompt,
            api_key=api_key,
            provider=provider,
            model=model
        )
        
        # Extract the JSON from the response
        try:
            # Find JSON content between triple backticks
            json_pattern = r"```json\s*([\s\S]*?)\s*```"
            match = re.search(json_pattern, response)
            
            if match:
                json_str = match.group(1)
                execution_plan = json.loads(json_str)
            else:
                # Try parsing the entire response as JSON
                execution_plan = json.loads(response)
        except Exception as e:
            return {
                "error": f"Could not parse AI response as JSON: {str(e)}",
                "ai_response": response
            }
        
        # Execute the plan
        if "execution_plan" in execution_plan and execution_plan["execution_plan"]:
            if execution_plan.get("sequential", False):
                # Execute sequentially without feeding data between UDFs
                chain_results = udf_tool.chain_udfs(execution_plan["execution_plan"], sequential_mode=True)
                execution_plan["results"] = chain_results
            elif execution_plan.get("chain", False):
                # Execute as a chain
                chain_results = udf_tool.chain_udfs(execution_plan["execution_plan"])
                execution_plan["results"] = chain_results
            else:
                # Execute individually
                results = []
                for step in execution_plan["execution_plan"]:
                    result = udf_tool.execute_udf(
                        step["udf_name"], 
                        step.get("parameters", {})
                    )
                    results.append(result)
                execution_plan["results"] = results
        
        return execution_plan
    
    except Exception as e:
        return {"error": f"Error in AI agent: {str(e)}"}

def create_visualization(data, viz_type="auto", x_col=None, y_col=None, title=None):
    """
    Create a visualization from a dataset.
    
    Args:
        data: The dataset to visualize
        viz_type: The type of visualization to create
        x_col: The column to use for the x-axis
        y_col: The column to use for the y-axis
        title: Optional title for the visualization
        
    Returns:
        Plotly figure
    """
    if not PLOTLY_AVAILABLE:
        st.warning("Plotly is not available. Install with: pip install plotly")
        return None
    
    # Convert to pandas if needed
    if hasattr(data, "to_pandas"):
        df = data.to_pandas()
    else:
        df = data
    
    if len(df) == 0:
        st.warning("No data to visualize")
        return go.Figure()
    
    # Auto-determine columns if not specified
    if x_col is None and len(df.columns) > 0:
        x_col = df.columns[0]
    
    if y_col is None and len(df.columns) > 1:
        y_col = df.columns[1]
    
    # Auto-determine visualization type if not specified
    if viz_type == "auto":
        # Simple heuristic based on data types
        if x_col and y_col:
            x_dtype = df[x_col].dtype
            y_dtype = df[y_col].dtype
            
            if pd.api.types.is_numeric_dtype(x_dtype) and pd.api.types.is_numeric_dtype(y_dtype):
                viz_type = "scatter"
            elif pd.api.types.is_categorical_dtype(x_dtype) or x_dtype == object:
                viz_type = "bar"
            else:
                viz_type = "line"
        elif x_col:
            x_dtype = df[x_col].dtype
            if pd.api.types.is_numeric_dtype(x_dtype):
                viz_type = "histogram"
            else:
                viz_type = "bar"
        else:
            viz_type = "table"
    
    # Create the visualization
    if viz_type == "scatter":
        fig = px.scatter(df, x=x_col, y=y_col, title=title)
    elif viz_type == "line":
        fig = px.line(df, x=x_col, y=y_col, title=title)
    elif viz_type == "bar":
        fig = px.bar(df, x=x_col, y=y_col, title=title)
    elif viz_type == "histogram":
        fig = px.histogram(df, x=x_col, title=title)
    elif viz_type == "heatmap":
        if x_col and y_col:
            # Create a pivot table for the heatmap
            pivot_df = df.pivot_table(index=y_col, columns=x_col, aggfunc='size', fill_value=0)
            fig = px.imshow(pivot_df, title=title)
        else:
            fig = px.imshow(df.corr(), title=title or "Correlation Matrix")
    else:
        # Default to a simple table visualization
        fig = go.Figure(data=[go.Table(
            header=dict(values=list(df.columns)),
            cells=dict(values=[df[col] for col in df.columns])
        )])
        if title:
            fig.update_layout(title=title)
    
    # Apply standard formatting
    fig.update_layout(
        template="plotly_white",
        margin=dict(l=10, r=10, t=30, b=10)
    )
    
    return fig 

# Sidebar with memory usage and AI configuration
with st.sidebar:
    st.header("Memory Usage")
    render_memory_usage()
    
    # AI Model Configuration
    st.header("AI Model Configuration")
    
    # Get current configuration
    ai_config = get_ai_config()
    
    # Provider selection
    available_providers = get_supported_providers()
    provider_options = list(available_providers.keys())
    
    selected_provider = st.selectbox(
        "AI Provider:", 
        provider_options,
        index=provider_options.index(ai_config["provider"]) if ai_config["provider"] in provider_options else 0,
        help="Select the AI provider to use for natural language queries"
    )
    
    # Update session state with selected provider
    if "ai_provider" not in st.session_state or st.session_state.ai_provider != selected_provider:
        st.session_state.ai_provider = selected_provider
    
    # Model selection based on provider
    if selected_provider in available_providers:
        model_options = available_providers[selected_provider]
        
        # Get current model or default
        current_model = ai_config["model"]
        if not current_model or current_model not in model_options:
            if selected_provider == "anthropic":
                current_model = "claude-3-haiku-20240307"
            elif selected_provider == "openai":
                current_model = "gpt-3.5-turbo"
            else:
                current_model = model_options[0] if model_options else ""
                
        model_index = model_options.index(current_model) if current_model in model_options else 0
        
        selected_model = st.selectbox(
            "Model:", 
            model_options,
            index=model_index,
            help=f"Select which {selected_provider} model to use"
        )
        
        # Update session state
        st.session_state[f"{selected_provider}_model"] = selected_model
    
    # API key input for selected provider
    api_key = ai_config["api_key"]
    
    # Display API key info or input
    if not api_key:
        api_key = st.text_input(f"Enter your {selected_provider.capitalize()} API Key:", 
                             type="password", 
                             help=f"Get an API key from {selected_provider.capitalize()}",
                             key=f"sidebar_{selected_provider}_api_key")
        if api_key:
            st.session_state[f"{selected_provider}_api_key"] = api_key
            st.success("API key saved for this session!")
        else:
            st.warning(f"Please enter an {selected_provider.capitalize()} API key.")
    else:
        st.success(f"{selected_provider.capitalize()} API key loaded from {ai_config['source']}")
        
        # Option to change API key
        if st.button("Change API Key"):
            # Clear the stored API key
            if f"{selected_provider}_api_key" in st.session_state:
                del st.session_state[f"{selected_provider}_api_key"]
            st.rerun()

# Set up session state for UDFs
if "udf_codes" not in st.session_state:
    st.session_state.udf_codes = [DEFAULT_UDF_CODE]
if "udf_tabs" not in st.session_state:
    st.session_state.udf_tabs = ["HackerNews"]
if "active_tab" not in st.session_state:
    st.session_state.active_tab = 0
if "udf_conversation_history" not in st.session_state:
    st.session_state.udf_conversation_history = []

# Main app title
st.title("⚡️ UDF Workbench")
st.markdown("Create, run, and analyze User-Defined Functions with AI assistance")

# Create three main tabs for the workbench
workbench_tabs = st.tabs(["UDF Editor", "Execution & SQL", "AI Assistant"])

# Tab 1: UDF Editor
with workbench_tabs[0]:
    # Actions for UDF management
    with st.expander("UDF Actions", expanded=True):
        col1, col2, col3 = st.columns([1, 1, 1])
        
        with col1:
            if st.button("Add New UDF", type="primary"):
                st.session_state.udf_codes.append(DEFAULT_UDF_CODE)
                st.session_state.udf_tabs.append(f"UDF {len(st.session_state.udf_tabs) + 1}")
                st.session_state.active_tab = len(st.session_state.udf_tabs) - 1
                st.rerun()
        
        with col2:
            if st.button("Rename Current UDF"):
                # Show a text input for the new name
                new_name = st.text_input("Enter new name:", value=st.session_state.udf_tabs[st.session_state.active_tab])
                if st.button("Save Name"):
                    st.session_state.udf_tabs[st.session_state.active_tab] = new_name
                    st.rerun()
        
        with col3:
            if len(st.session_state.udf_tabs) > 1 and st.button("Delete Current UDF", type="secondary"):
                # Ask for confirmation
                if st.checkbox("Confirm deletion"):
                    del st.session_state.udf_codes[st.session_state.active_tab]
                    del st.session_state.udf_tabs[st.session_state.active_tab]
                    st.session_state.active_tab = 0
                    st.rerun()
    
    # UDF tabs
    udf_tab_titles = st.session_state.udf_tabs
    udf_tabs = st.tabs(udf_tab_titles)
    
    # Display the UDF code editor in each tab
    for i, (tab, code) in enumerate(zip(udf_tabs, st.session_state.udf_codes)):
        with tab:
            # Set active tab when clicked
            if i != st.session_state.active_tab:
                if st.button(f"Make Active", key=f"activate_tab_{i}"):
                    st.session_state.active_tab = i
                    st.rerun()
                    
            # Code editor
            st.markdown(f"### {udf_tab_titles[i]} Code")
            updated_code = st.text_area(
                "UDF Code:", 
                value=code, 
                height=400, 
                key=f"udf_code_{i}"
            )
            
            # Update code in session state when changed
            if updated_code != st.session_state.udf_codes[i]:
                st.session_state.udf_codes[i] = updated_code
            
            # Extract parameters for the UDF
            udf_tool = UDFAITool(st.session_state)
            udf_name = extract_udf_name(updated_code)
            parameter_specs = udf_tool._extract_udf_params(updated_code)
            
            # Run UDF section
            st.markdown("### Run UDF")
            
            # Create input fields for parameters
            param_values = {}
            for param_name, param_info in parameter_specs.items():
                param_type = param_info.get("type", "str")
                default_val = param_info.get("default", "")
                
                if param_type == "bool":
                    param_values[param_name] = st.checkbox(
                        f"{param_name}:", 
                        value=default_val if isinstance(default_val, bool) else False,
                        key=f"param_{i}_{param_name}"
                    )
                elif param_type == "int":
                    param_values[param_name] = st.number_input(
                        f"{param_name}:", 
                        value=default_val if isinstance(default_val, int) else 0,
                        key=f"param_{i}_{param_name}"
                    )
                elif param_type == "float":
                    param_values[param_name] = st.number_input(
                        f"{param_name}:", 
                        value=default_val if isinstance(default_val, float) else 0.0,
                        step=0.1,
                        key=f"param_{i}_{param_name}"
                    )
                else:
                    # Default to string input
                    param_values[param_name] = st.text_input(
                        f"{param_name}:", 
                        value=str(default_val) if default_val else "",
                        key=f"param_{i}_{param_name}"
                    )
            
            # Run button
            col1, col2 = st.columns([1, 4])
            with col1:
                run_button = st.button("Run UDF", key=f"run_udf_{i}", type="primary")
            with col2:
                # Option to register result as dataset
                register_dataset = st.checkbox("Register result as dataset", key=f"register_{i}", value=True)
                dataset_name = st.text_input("Dataset name (optional):", key=f"dataset_name_{i}", 
                                          placeholder="Leave empty for auto-generated name")
            
            # Run the UDF when button is clicked
            if run_button:
                with st.spinner(f"Running {udf_name}..."):
                    # Run the UDF
                    success, result = run_udf_code(updated_code, param_values)
                    
                    if success:
                        st.success(f"UDF {udf_name} executed successfully!")
                        
                        # Display result
                        if hasattr(result, 'shape'):
                            st.write(f"Result shape: {result.shape}")
                            
                        # Show sample data
                        if hasattr(result, 'head'):
                            st.subheader("Sample Data")
                            st.dataframe(result.head(10), use_container_width=True)
                            
                            # Register as dataset if requested
                            if register_dataset:
                                reg_success, reg_result = register_udf_result(result, dataset_name or None)
                                if reg_success:
                                    st.success(f"Result registered as dataset: {reg_result['name']}")
                                    # Save to session state for reference in other tabs
                                    st.session_state.last_dataset = reg_result['name']
                                else:
                                    st.error(f"Failed to register dataset: {reg_result}")
                        else:
                            st.write("Result:", result)
                    else:
                        st.error(f"Error executing UDF: {result}")

# Tab 2: Execution & SQL
with workbench_tabs[1]:
    # Split this tab into SQL Query and Visualization sections
    sql_col, viz_col = st.columns([1, 1])
    
    with sql_col:
        st.subheader("SQL Query")
        
        # List available datasets with details
        datasets = get_datasets_list()
        
        if not datasets:
            st.info("No datasets available. Run a UDF to generate a dataset first.")
        else:
            # Display available tables for querying
            st.markdown("### Available Tables")
            for ds in datasets:
                st.markdown(f"**_cache_{ds['name']}**  ({ds.get('row_count', '?')} rows, {len(ds.get('columns', []))} columns)")
            
            # Query editor
            query = st.text_area(
                "Enter SQL Query:", 
                value=f"SELECT * FROM _cache_{datasets[0]['name']} LIMIT 10" if datasets else "",
                height=150,
                placeholder="SELECT * FROM _cache_dataset_name LIMIT 10"
            )
            
            # Execute button
            query_button = st.button("Execute Query", type="primary", key="execute_sql")
            
            # Execute query
            if query_button and query:
                with st.spinner("Executing query..."):
                    success, result = sql_query_datasets(query)
                    
                    if success:
                        st.success("Query executed successfully!")
                        
                        # Display result
                        st.subheader("Query Result")
                        st.dataframe(result.to_pandas().head(20), use_container_width=True)
                        
                        # Store result for visualization
                        st.session_state.query_result = result
                        
                        # Option to register as dataset
                        if st.checkbox("Register query result as dataset"):
                            dataset_name = st.text_input("Dataset name:", value=f"query_result_{int(time.time())}")
                            if st.button("Register"):
                                reg_success, reg_result = register_udf_result(result.to_pandas(), dataset_name)
                                if reg_success:
                                    st.success(f"Query result registered as dataset: {reg_result['name']}")
                                    st.rerun()
                                else:
                                    st.error(f"Failed to register dataset: {reg_result}")
                    else:
                        st.error(f"Query error: {result}")
    
    with viz_col:
        st.subheader("Visualization")
        
        # Check if we have data to visualize
        if not datasets:
            st.info("No datasets available for visualization.")
        else:
            # Select data source
            viz_source = st.radio("Data Source:", ["Dataset", "Query Result"], horizontal=True)
            
            if viz_source == "Dataset":
                # Select dataset
                dataset_names = [ds['name'] for ds in datasets]
                selected_dataset = st.selectbox("Select Dataset:", dataset_names)
                
                # Load dataset
                data = cache.get(selected_dataset)
                if data is not None:
                    df = data.to_pandas()
                else:
                    st.error(f"Failed to load dataset: {selected_dataset}")
                    df = pd.DataFrame()
            else:
                # Use query result
                if 'query_result' in st.session_state:
                    df = st.session_state.query_result.to_pandas()
                else:
                    st.warning("No query result available. Run a query first.")
                    df = pd.DataFrame()
            
            if not df.empty:
                # Visualization options
                st.markdown("### Visualization Options")
                
                # Select visualization type
                viz_types = ["auto", "scatter", "line", "bar", "histogram", "heatmap", "table"]
                viz_type = st.selectbox("Visualization Type:", viz_types)
                
                # Select columns
                columns = list(df.columns)
                x_col = st.selectbox("X-axis Column:", columns)
                
                # Y-axis column only for certain chart types
                if viz_type in ["scatter", "line", "bar"]:
                    y_col = st.selectbox("Y-axis Column:", columns)
                else:
                    y_col = None
                
                # Title
                title = st.text_input("Chart Title:", value=f"{viz_type.capitalize()} of {x_col}" + (f" vs {y_col}" if y_col else ""))
                
                # Create visualization
                if st.button("Generate Visualization", type="primary"):
                    with st.spinner("Creating visualization..."):
                        fig = create_visualization(df, viz_type, x_col, y_col, title)
                        
                        if fig:
                            st.plotly_chart(fig, use_container_width=True)
                        else:
                            st.error("Failed to create visualization. Please check your data and options.")

# Tab 3: AI Assistant
with workbench_tabs[2]:
    # Split into two columns - chat interface and AI tools
    chat_col, tools_col = st.columns([3, 2])
    
    with chat_col:
        st.subheader("AI Data Assistant")
        
        # Display conversation history
        if st.session_state.udf_conversation_history:
            for entry in st.session_state.udf_conversation_history:
                # User message
                st.markdown(f"<div style='background-color:#f0f2f6; padding:10px; border-radius:5px; margin-bottom:10px;'><strong>You:</strong> {entry['user']}</div>", unsafe_allow_html=True)
                
                # AI response
                st.markdown(f"<div style='background-color:#e0f7fa; padding:10px; border-radius:5px; margin-bottom:20px;'><strong>AI:</strong> {entry['ai']}</div>", unsafe_allow_html=True)
        
        # Input for user question
        user_question = st.text_area("Ask about your UDFs or data:", height=100, 
                                   placeholder="Ask me anything about your UDFs, data analysis, or how to accomplish a task...")
        
        # Select interaction type
        interaction_type = st.radio("Interaction Type:", ["Answer Question", "Generate UDF Code", "Execute UDFs"], horizontal=True)
        
        # Action button
        if st.button("Submit", type="primary"):
            if not user_question:
                st.warning("Please enter a question or request.")
            else:
                with st.spinner(f"AI is processing your request..."):
                    # Get AI configuration
                    ai_config = get_ai_config()
                    api_key = ai_config.get("api_key")
                    provider = st.session_state.get("ai_provider", "anthropic")
                    model = st.session_state.get(f"{provider}_model")
                    
                    if not api_key:
                        st.error(f"No API key available for {provider}. Please add one in the sidebar.")
                    else:
                        if interaction_type == "Answer Question":
                            # Get context about available UDFs and datasets
                            udf_tool = UDFAITool(st.session_state)
                            available_udfs = udf_tool.list_available_udfs()
                            datasets = get_datasets_list()
                            
                            # Create context string
                            context = "Available UDFs:\n"
                            for udf in available_udfs:
                                details = udf_tool.get_udf_details(udf["name"])
                                context += f"- {udf['name']}: {details.get('docstring', 'No description')[:100]}...\n"
                            
                            context += "\nAvailable Datasets:\n"
                            for ds in datasets:
                                context += f"- {ds['name']}: {ds.get('row_count', '?')} rows, {len(ds.get('columns', []))} columns\n"
                                if 'columns' in ds:
                                    context += f"  Columns: {', '.join(ds['columns'][:10])}\n"
                            
                            # Prompt for the AI
                            prompt = f"""
You are an AI assistant for a data analysis workbench that uses User-Defined Functions (UDFs).
Answer the user's question based on the available UDFs and datasets.

{context}

User Question: {user_question}
"""
                            
                            # Get AI response
                            response = ask_ai(prompt, api_key, provider, model)
                            
                            # Store in conversation history
                            st.session_state.udf_conversation_history.append({
                                "user": user_question,
                                "ai": response
                            })
                            
                            # Display response
                            st.markdown(f"<div style='background-color:#e0f7fa; padding:10px; border-radius:5px; margin-top:20px;'><strong>AI:</strong> {response}</div>", unsafe_allow_html=True)
                        
                        elif interaction_type == "Generate UDF Code":
                            # Get the current active UDF and code
                            active_tab = st.session_state.active_tab
                            current_code = st.session_state.udf_codes[active_tab] if active_tab < len(st.session_state.udf_codes) else DEFAULT_UDF_CODE
                            
                            # Generate code using AI
                            response = ask_ai_for_code(
                                question=user_question,
                                current_code=current_code,
                                api_key=api_key,
                                provider=provider,
                                model=model
                            )
                            
                            # Store in conversation history
                            st.session_state.udf_conversation_history.append({
                                "user": user_question,
                                "ai": response
                            })
                            
                            # Extract code blocks from response
                            code_blocks = re.findall(r'```python\n(.*?)```', response, re.DOTALL)
                            
                            # Display response
                            st.markdown(f"<div style='background-color:#e0f7fa; padding:10px; border-radius:5px; margin-top:20px;'><strong>AI:</strong> {response}</div>", unsafe_allow_html=True)
                            
                            # If code blocks found, offer to apply them
                            if code_blocks:
                                st.session_state.generated_code = code_blocks[0]
                                st.subheader("Apply Generated Code")
                                if st.button("Apply to Current UDF"):
                                    st.session_state.udf_codes[active_tab] = code_blocks[0]
                                    st.success("Code applied to the current UDF!")
                                    st.rerun()
                        
                        elif interaction_type == "Execute UDFs":
                            # Run the UDF AI agent
                            udf_tool = UDFAITool(st.session_state)
                            
                            # Check if the user explicitly wants to run a specific UDF by name
                            available_udfs = udf_tool.list_available_udfs()
                            
                            # Create a mapping of possible UDF references (both function names and display names)
                            udf_references = {}
                            for udf in available_udfs:
                                udf_references[udf["name"].lower()] = udf["name"]
                                udf_references[udf["display_name"].lower()] = udf["name"]
                            
                            # Check if the user is explicitly asking to execute a specific UDF
                            question_lower = user_question.lower()
                            direct_execution = False
                            
                            # Patterns that suggest direct UDF execution
                            execution_patterns = [
                                r"execute\s+(?:the\s+)?(?:udf\s+)?([a-zA-Z0-9_\s]+)",
                                r"run\s+(?:the\s+)?(?:udf\s+)?([a-zA-Z0-9_\s]+)",
                                r"use\s+(?:the\s+)?(?:udf\s+)?([a-zA-Z0-9_\s]+)",
                                r"call\s+(?:the\s+)?(?:udf\s+)?([a-zA-Z0-9_\s]+)"
                            ]
                            
                            requested_udf = None
                            execution_params = {}
                            
                            # Check for direct UDF execution requests
                            for pattern in execution_patterns:
                                matches = re.search(pattern, question_lower)
                                if matches:
                                    udf_name_hint = matches.group(1).strip()
                                    
                                    # Find the closest matching UDF
                                    for ref in udf_references:
                                        if ref in udf_name_hint or udf_name_hint in ref:
                                            requested_udf = udf_references[ref]
                                            direct_execution = True
                                            break
                                            
                                    if direct_execution:
                                        # Look for parameter specifications
                                        param_pattern = r"with\s+(?:parameter[s]?\s+)?([a-zA-Z0-9_]+)\s*=\s*[\"\']?([^\"\',]+)[\"\']?"
                                        param_matches = re.findall(param_pattern, user_question)
                                        
                                        if param_matches:
                                            for param_name, param_value in param_matches:
                                                # Try to convert value to appropriate type
                                                try:
                                                    # Try as number
                                                    if param_value.isdigit():
                                                        execution_params[param_name] = int(param_value)
                                                    elif param_value.replace('.', '', 1).isdigit():
                                                        execution_params[param_name] = float(param_value)
                                                    elif param_value.lower() in ['true', 'false']:
                                                        execution_params[param_name] = param_value.lower() == 'true'
                                                    else:
                                                        execution_params[param_name] = param_value
                                                except:
                                                    execution_params[param_name] = param_value
                                        
                                        break
                            
                            # If we found a direct UDF execution request
                            if direct_execution and requested_udf:
                                # Get UDF details for a better response
                                udf_details = udf_tool.get_udf_details(requested_udf)
                                display_name = udf_details.get("display_name", requested_udf)
                                
                                # Execute the UDF directly
                                execution_result = udf_tool.execute_udf(requested_udf, execution_params)
                                
                                if execution_result.get("success", False):
                                    # Format a nice response
                                    response_parts = [
                                        f"I've executed the '{display_name}' UDF for you."
                                    ]
                                    
                                    if execution_params:
                                        response_parts.append(f"Parameters used: {execution_params}")
                                    
                                    if "shape" in execution_result:
                                        response_parts.append(f"Result shape: {execution_result['shape']}")
                                    
                                    if "dataset_name" in execution_result:
                                        response_parts.append(f"Result registered as dataset: {execution_result['dataset_name']}")
                                        
                                    if "sample_data" in execution_result and execution_result["sample_data"]:
                                        sample_count = len(execution_result["sample_data"])
                                        response_parts.append(f"Here's a sample of {sample_count} records from the result:")
                                    
                                    response = "\n\n".join(response_parts)
                                    
                                    # Store in conversation history
                                    st.session_state.udf_conversation_history.append({
                                        "user": user_question,
                                        "ai": response
                                    })
                                    
                                    # Display response
                                    st.markdown(f"<div style='background-color:#e0f7fa; padding:10px; border-radius:5px; margin-top:20px;'><strong>AI:</strong> {response}</div>", unsafe_allow_html=True)
                                    
                                    # Display sample data if available
                                    if "sample_data" in execution_result and execution_result["sample_data"]:
                                        st.dataframe(execution_result["sample_data"])
                                    
                                else:
                                    # Error response
                                    error_msg = execution_result.get("error", "Unknown error")
                                    response = f"I tried to execute the '{display_name}' UDF but encountered an error:\n\n{error_msg}"
                                    
                                    # Store in conversation history
                                    st.session_state.udf_conversation_history.append({
                                        "user": user_question,
                                        "ai": response
                                    })
                                    
                                    # Display response
                                    st.markdown(f"<div style='background-color:#e0f7fa; padding:10px; border-radius:5px; margin-top:20px;'><strong>AI:</strong> {response}</div>", unsafe_allow_html=True)
                            else:
                                # Use the standard AI agent approach
                                agent_result = udf_ai_agent(user_question, udf_tool)
                                
                                if "error" in agent_result:
                                    st.error(f"Agent error: {agent_result['error']}")
                                    if "ai_response" in agent_result:
                                        response = agent_result["ai_response"]
                                    else:
                                        response = f"I encountered an error: {agent_result['error']}"
                                else:
                                    # Format the agent's analysis and execution plan as a response
                                    execution_summary = []
                                    
                                    if "analysis" in agent_result:
                                        execution_summary.append(f"**Analysis:** {agent_result['analysis']}")
                                    
                                    # Show execution mode
                                    if agent_result.get("sequential", False):
                                        execution_mode = "Sequential mode (UDFs executed independently)"
                                    elif agent_result.get("chain", False):
                                        execution_mode = "Chain mode (UDFs chained with output feeding into next UDF)"
                                    else:
                                        execution_mode = "Standard execution"
                                    execution_summary.append(f"**Execution Mode:** {execution_mode}")
                                    
                                    if "execution_plan" in agent_result:
                                        execution_summary.append("**Execution Plan:**")
                                        for step in agent_result['execution_plan']:
                                            execution_summary.append(f"- Run UDF: `{step['udf_name']}` with parameters: {step.get('parameters', {})}")
                                    
                                    if "results" in agent_result:
                                        execution_summary.append("**Results:**")
                                        
                                        results = agent_result["results"]
                                        if isinstance(results, list):
                                            # Multiple individual results
                                            for i, result in enumerate(results):
                                                if result.get("success", False):
                                                    execution_summary.append(f"- Step {i+1}: Successful")
                                                    if "dataset_name" in result:
                                                        execution_summary.append(f"  Registered as dataset: `{result['dataset_name']}`")
                                                else:
                                                    execution_summary.append(f"- Step {i+1}: Failed - {result.get('error', 'Unknown error')}")
                                        elif isinstance(results, dict):
                                            # Chain results
                                            chain_results = results.get("chain_results", [])
                                            for result in chain_results:
                                                step_result = result.get("result", {})
                                                if step_result.get("success", False):
                                                    execution_summary.append(f"- Step {result.get('step', '?')}: Successful")
                                                    if "dataset_name" in step_result:
                                                        execution_summary.append(f"  Registered as dataset: `{step_result['dataset_name']}`")
                                                else:
                                                    execution_summary.append(f"- Step {result.get('step', '?')}: Failed - {step_result.get('error', 'Unknown error')}")
                                    
                                    response = "\n".join(execution_summary)
                                
                                # Store in conversation history
                                st.session_state.udf_conversation_history.append({
                                    "user": user_question,
                                    "ai": response
                                })
                                
                                # Display response
                                st.markdown(f"<div style='background-color:#e0f7fa; padding:10px; border-radius:5px; margin-top:20px;'><strong>AI:</strong> {response}</div>", unsafe_allow_html=True)
        
        # Clear conversation button
        if st.button("Clear Conversation"):
            st.session_state.udf_conversation_history = []
            st.rerun()
    
    with tools_col:
        st.subheader("AI Tools")
        
        # UDF generation templates
        with st.expander("UDF Templates", expanded=True):
            st.markdown("Select a template to generate a new UDF:")
            
            template_options = [
                "Web API Data Fetcher",
                "Data Cleaner & Transformer",
                "Statistical Analysis",
                "Text Analysis",
                "Time Series Analyzer"
            ]
            
            selected_template = st.selectbox("Select template:", template_options)
            
            if st.button("Generate Template UDF"):
                # Get AI configuration
                ai_config = get_ai_config()
                api_key = ai_config.get("api_key")
                provider = st.session_state.get("ai_provider", "anthropic")
                model = st.session_state.get(f"{provider}_model")
                
                if not api_key:
                    st.error(f"No API key available for {provider}.")
                else:
                    with st.spinner("Generating UDF template..."):
                        prompt = f"""
Generate a UDF (User-Defined Function) for {selected_template}. 
The UDF should:
1. Be properly decorated with @fused.udf
2. Have a clear docstring explaining its purpose, parameters, and returns
3. Include necessary imports
4. Return a pandas DataFrame as the result
5. Include error handling

Return only the Python code without any explanation.
"""
                        
                        # Get AI response
                        response = ask_ai(prompt, api_key, provider, model)
                        
                        # Extract code (assume the entire response is code)
                        code = response.strip()
                        
                        # Create a new UDF tab with this template
                        st.session_state.udf_codes.append(code)
                        st.session_state.udf_tabs.append(selected_template)
                        st.session_state.active_tab = len(st.session_state.udf_tabs) - 1
                        st.success(f"Generated {selected_template} template! Switch to the UDF Editor tab to view it.")
        
        # Dataset explorer
        with st.expander("Dataset Explorer", expanded=True):
            st.markdown("Quick view of available datasets:")
            
            datasets = get_datasets_list()
            if not datasets:
                st.info("No datasets available yet.")
            else:
                selected_dataset = st.selectbox("Select dataset to explore:", [ds['name'] for ds in datasets], key="explorer_dataset")
                
                # Fetch and display dataset sample
                data = cache.get(selected_dataset, limit=5)
                if data is not None:
                    st.write("Sample data:")
                    st.dataframe(data.to_pandas(), use_container_width=True)
                    
                    # Quick actions
                    if st.button("Visualize this Dataset"):
                        st.session_state.viz_dataset = selected_dataset
                        # Switch to the Visualization tab
                        st.rerun()
                    
                    if st.button("Generate SQL Query"):
                        # Generate a sample query for this dataset
                        query = f"SELECT * FROM _cache_{selected_dataset} LIMIT 10"
                        st.session_state.sample_query = query
                        st.code(query, language="sql")
                        st.info("Switch to the Execution & SQL tab to run this query.")
                else:
                    st.error(f"Failed to load dataset: {selected_dataset}")
        
        # UDF chaining helper
        with st.expander("UDF Chaining Helper", expanded=True):
            st.markdown("Chain multiple UDFs together:")
            
            # Load all UDFs from tabs, not just active ones
            all_udfs = []
            for i, (tab_name, code) in enumerate(zip(st.session_state.udf_tabs, st.session_state.udf_codes)):
                udf_name = extract_udf_name(code)
                all_udfs.append({
                    "name": udf_name,
                    "display_name": tab_name,
                    "index": i,
                    "code": code
                })
            
            if not all_udfs:
                st.info("No UDFs available for chaining.")
            else:
                # Allow selecting UDFs to chain
                st.write("Select UDFs to chain (in order of execution):")
                
                # Add execution mode selector
                sequential_mode = st.checkbox("Sequential mode (run UDFs independently without feeding data)", 
                                           help="When checked, UDFs will run in sequence but won't automatically pass data between them")
                
                # Init chain if not already in session state
                if "chain_steps" not in st.session_state:
                    # Initialize with the first UDF's display name if available
                    st.session_state.chain_steps = [0]  # Store index instead of name
                else:
                    # Ensure existing values are integers
                    st.session_state.chain_steps = [
                        int(idx) if isinstance(idx, (int, str)) and str(idx).isdigit() else 0 
                        for idx in st.session_state.chain_steps
                    ]
                
                # Display current chain
                for i, step_idx in enumerate(st.session_state.chain_steps):
                    col1, col2 = st.columns([4, 1])
                    with col1:
                        # Show options with tab names
                        options = [f"{udf['display_name']}" for udf in all_udfs]
                        
                        # Ensure step_idx is an integer
                        try:
                            step_idx_int = int(step_idx)
                        except (TypeError, ValueError):
                            step_idx_int = 0
                            
                        selected_display = st.selectbox(
                            f"Step {i+1}:", 
                            options,
                            index=step_idx_int if step_idx_int < len(options) else 0,
                            key=f"chain_step_{i}"
                        )
                        # Store the index of the selected UDF
                        st.session_state.chain_steps[i] = options.index(selected_display)
                    with col2:
                        if i > 0 and st.button("Remove", key=f"remove_step_{i}"):
                            st.session_state.chain_steps.pop(i)
                            st.rerun()
                
                # Add step button
                if st.button("Add Step"):
                    # Add first UDF by default, ensuring it's an integer
                    st.session_state.chain_steps.append(0)
                    st.rerun()
                
                # Execute chain button
                if st.button("Execute UDF Chain", type="primary"):
                    with st.spinner("Executing UDF chain..."):
                        # Initialize UDF tool with all UDFs
                        udf_tool = UDFAITool(st.session_state)
                        
                        # Create execution plan using tab indices
                        execution_plan = []
                        for i, step_idx in enumerate(st.session_state.chain_steps):
                            # Convert step_idx to integer
                            try:
                                step_idx_int = int(step_idx)
                            except (ValueError, TypeError):
                                step_idx_int = 0
                                
                            # Get UDF info from the all_udfs list
                            if step_idx_int < len(all_udfs):
                                udf_info = all_udfs[step_idx_int]
                                display_name = udf_info["display_name"]
                                udf_name = udf_info["name"]
                                
                                # Create the execution step
                                plan_step = {
                                    "udf_name": display_name,  # Use display name for better compatibility
                                    "parameters": {}
                                }
                                
                                # If not the first step and not in sequential mode, use output from previous step
                                if i > 0 and not sequential_mode:
                                    # Get parameters for this UDF to select an appropriate one
                                    udf_params = udf_tool._extract_udf_params(udf_info["code"])
                                    
                                    # If the UDF has no parameters, we can't chain
                                    if not udf_params:
                                        st.warning(f"UDF '{display_name}' has no parameters for chaining input.")
                                    else:
                                        # Default to first parameter if there's no input_data param
                                        first_param = next(iter(udf_params))
                                        input_param = "input_data" if "input_data" in udf_params else first_param
                                        
                                        plan_step["input_from"] = i - 1
                                        plan_step["input_param"] = input_param
                                
                                execution_plan.append(plan_step)
                        
                        # Execute the chain
                        chain_results = udf_tool.chain_udfs(execution_plan, sequential_mode=sequential_mode)
                        
                        # Display results
                        execution_mode = "sequentially" if sequential_mode else "as a chain"
                        st.success(f"UDF chain executed {execution_mode} successfully!")
                        st.write("Results:")
                        
                        # Show each step's result without using nested expanders
                        chain_results_list = chain_results.get("chain_results", [])
                        for result in chain_results_list:
                            step_num = result.get('step', '?')
                            udf_name = result.get('udf', 'Unknown')
                            
                            # Use a container with a border instead of an expander
                            st.markdown(f"### Step {step_num}: {udf_name}")
                            with st.container():
                                st.markdown("---")
                                step_result = result.get("result", {})
                                if step_result.get("success", False):
                                    st.success("Success")
                                    if "sample_data" in step_result:
                                        st.dataframe(step_result["sample_data"])
                                    if "dataset_name" in step_result:
                                        st.write(f"Result registered as dataset: {step_result['dataset_name']}")
                                else:
                                    st.error(f"Failed: {step_result.get('error', 'Unknown error')}")
                                st.markdown("---")