import streamlit as st
import pandas as pd
import numpy as np
import json
import logging
import re
from typing import Dict, List, Tuple, Optional, Any

# Import from parent module
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import get_cache
from utils.visualization.core import infer_column_types, suggest_visualization
from arrow_cache_mcp.ai import ask_ai

# Configure logging
logger = logging.getLogger(__name__)

def generate_visualization_prompt(dataset_name, query_context=None):
    """
    Generate a prompt for the AI to analyze a dataset and suggest visualizations.
    
    Args:
        dataset_name: Name of the dataset to analyze
        query_context: Optional SQL query or context about the data
        
    Returns:
        Prompt string for the AI
    """
    prompt = f"""
You are a data visualization expert. I need you to analyze a dataset called '{dataset_name}' and suggest the best visualizations.

Please analyze the data and recommend:
1. The most appropriate visualization type based on the data
2. Which columns should be used for the x-axis, y-axis, and optionally color
3. Any transformations or aggregations that would make the visualization more insightful

Return your response in a structured JSON format like this:
```json
{{
  "visualization_type": "scatter", 
  "x_column": "column_name",
  "y_column": "column_name",
  "color_column": "column_name", 
  "title": "Suggested title",
  "description": "Brief explanation of why this visualization works well",
  "transformations": ["optional data transformations or SQL to prepare the data"]
}}
```

Only valid visualization types are: line, bar, scatter, hist, box, pie, heatmap, map.
"""

    # Add dataset-specific context
    cache = get_cache()
    try:
        # Get sample data
        sample = cache.get(dataset_name, limit=5)
        df = sample.to_pandas()
        
        # Add column information to prompt
        columns_info = []
        for col in df.columns:
            # Get basic type information
            dtype = str(df[col].dtype)
            try:
                # Add sample value
                sample_val = str(df[col].iloc[0]) if not df[col].isna().all() else "None"
                if len(sample_val) > 100:
                    sample_val = sample_val[:100] + "..."
            except:
                sample_val = "Error getting sample"
                
            columns_info.append(f"- {col}: {dtype}, Example: {sample_val}")
            
        prompt += f"""
Dataset Information:
Dataset Name: {dataset_name}
Number of Columns: {len(df.columns)}

Column details:
{chr(10).join(columns_info)}
"""
        
        # Add query context if provided
        if query_context:
            prompt += f"""
Additional Context:
SQL Query or Analysis Context: {query_context}
"""
    except Exception as e:
        logger.error(f"Error generating visualization prompt: {e}")
        prompt += f"\nNote: Error getting sample data from the dataset: {str(e)}"
    
    return prompt

def parse_visualization_suggestion(ai_response):
    """
    Parse the AI response to extract visualization suggestions.
    
    Args:
        ai_response: Response from the AI
        
    Returns:
        Dictionary with visualization parameters or None if parsing fails
    """
    try:
        # Extract JSON from response (might be wrapped in markdown code blocks)
        json_match = re.search(r"```(?:json)?\s*({.*?})\s*```", ai_response, re.DOTALL)
        
        if json_match:
            json_str = json_match.group(1)
            suggestion = json.loads(json_str)
            
            # Validate required fields
            required_fields = ['visualization_type', 'title', 'description']
            for field in required_fields:
                if field not in suggestion:
                    logger.warning(f"Missing required field in visualization suggestion: {field}")
                    suggestion[field] = None
                    
            return suggestion
        else:
            # Try direct JSON parsing if no code blocks found
            try:
                # Find anything that looks like JSON
                json_match = re.search(r"{.*}", ai_response.replace("\n", " "), re.DOTALL)
                if json_match:
                    json_str = json_match.group(0)
                    suggestion = json.loads(json_str)
                    return suggestion
            except:
                pass
                
            logger.warning("Could not extract JSON from AI response")
            return None
            
    except Exception as e:
        logger.error(f"Error parsing visualization suggestion: {e}")
        return None

def get_ai_visualization_suggestion(dataset_name, api_key, provider="anthropic", model=None, query_context=None):
    """
    Get AI-generated visualization suggestions for a dataset.
    
    Args:
        dataset_name: Name of the dataset
        api_key: API key for the AI provider
        provider: AI provider name
        model: Model to use
        query_context: Optional SQL query or context
        
    Returns:
        Tuple of (success, suggestion dict or error message)
    """
    try:
        # Generate prompt
        prompt = generate_visualization_prompt(dataset_name, query_context)
        
        # Call AI
        response = ask_ai(
            question=prompt,
            api_key=api_key,
            provider=provider,
            model=model,
            conversation_history=[]  # New conversation each time
        )
        
        # Parse response
        suggestion = parse_visualization_suggestion(response)
        
        if suggestion:
            return True, suggestion
        else:
            # Fallback to rule-based suggestion
            cache = get_cache()
            df = cache.get(dataset_name).to_pandas()
            
            x_col, y_col, plot_type, params = suggest_visualization(df)
            
            fallback = {
                "visualization_type": plot_type,
                "x_column": x_col,
                "y_column": y_col,
                "color_column": None,
                "title": f"{plot_type.capitalize()} of {dataset_name}",
                "description": "Fallback visualization suggestion based on data types",
                "transformations": []
            }
            
            return True, fallback
    
    except Exception as e:
        logger.error(f"Error getting AI visualization suggestion: {e}")
        return False, f"Error: {str(e)}"

def execute_data_transformation(dataset_name, transformation_query):
    """
    Execute a data transformation (SQL query) on a dataset for visualization.
    
    Args:
        dataset_name: Name of the dataset
        transformation_query: SQL query to transform the data
        
    Returns:
        Tuple of (success, DataFrame or error message)
    """
    try:
        # Get cache
        cache = get_cache()
        
        # Check if the transformation is a SQL query
        if transformation_query.strip().lower().startswith(('select', 'with')):
            # Execute SQL query
            result_df = cache.query(transformation_query)
            return True, result_df
        else:
            return False, "Transformation must be a SQL query"
    
    except Exception as e:
        logger.error(f"Error executing data transformation: {e}")
        return False, f"Error: {str(e)}"

def run_sql_for_visualization(sql_query, limit=None):
    """
    Run a SQL query and prepare the result for visualization.
    
    Args:
        sql_query: SQL query to execute
        limit: Optional row limit for visualization
        
    Returns:
        Tuple of (success, DataFrame or error message)
    """
    try:
        # Validate SQL query is not empty
        if not sql_query or not sql_query.strip():
            return False, "SQL query is empty"
            
        # Get cache
        cache = get_cache()
        
        # If no limit specified, remove any LIMIT clause in the query and run without limit
        if limit is None:
            # Check for existing LIMIT clause
            has_limit = re.search(r'\bLIMIT\s+\d+\b', sql_query, re.IGNORECASE)
            
            if has_limit:
                # Remove the LIMIT clause
                sql_query = re.sub(r'\bLIMIT\s+\d+\b', '', sql_query, flags=re.IGNORECASE)
        
        # Execute SQL query
        result_df = cache.query(sql_query)
        
        # Apply limit if specified
        if limit is not None and not result_df.empty:
            result_df = result_df.head(limit)
        
        return True, result_df
    
    except Exception as e:
        logger.error(f"Error running SQL for visualization: {e}")
        return False, f"Error: {str(e)}" 