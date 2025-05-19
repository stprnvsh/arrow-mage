import streamlit as st
import pandas as pd
import re
import logging
from typing import Dict, List, Tuple, Optional, Any, Union

# Import necessary modules
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))
from utils.common import get_cache
from arrow_cache_mcp.ai import ask_ai, extract_and_run_queries

# Configure logging
logger = logging.getLogger(__name__)

def extract_sql_from_ai_response(ai_response):
    """
    Extract SQL queries from AI response text.
    
    Args:
        ai_response: Text response from AI
        
    Returns:
        List of extracted SQL queries
    """
    queries = []
    
    # Look for SQL blocks in markdown format
    sql_blocks = re.findall(r"```(?:sql)?\s*(.*?)```", ai_response, re.DOTALL)
    for block in sql_blocks:
        # Clean up the block
        block = block.strip()
        if block.lower().startswith(('select', 'with')):
            queries.append(block)
    
    # If no SQL blocks found in markdown, try to directly extract SQL queries
    if not queries:
        # Look for SELECT or WITH statements
        potential_queries = re.findall(r"((?:select|with)\s+.*?;)", ai_response, re.DOTALL | re.IGNORECASE)
        for query in potential_queries:
            # Clean up the query
            query = query.strip()
            if query.lower().startswith(('select', 'with')):
                queries.append(query)
    
    return queries

def generate_visualization_sql(dataset_name, visualization_goal, api_key, provider="anthropic", model=None):
    """
    Generate SQL for a visualization based on natural language description.
    
    Args:
        dataset_name: Name of the dataset
        visualization_goal: Natural language description of the visualization goal
        api_key: API key for the AI provider
        provider: AI provider name
        model: Model to use
        
    Returns:
        Tuple of (success, result_data or error message)
    """
    try:
        # Get dataset information
        cache = get_cache()
        if not cache.contains(dataset_name):
            return False, f"Dataset '{dataset_name}' not found"
            
        # Get sample data
        sample = cache.get(dataset_name, limit=5)
        columns = list(sample.column_names)
        
        # Generate AI prompt
        prompt = f"""
I need SQL to create a visualization for the dataset '{dataset_name}'. 

Visualization goal: {visualization_goal}

The dataset has the following columns:
{', '.join(columns)}

Here's a sample of the data:
{sample.to_pandas().to_string(index=False)}

Please write a SQL query that will prepare the data for this visualization. The query should:
1. Select the relevant columns
2. Apply any necessary aggregations, groupings, or transformations
3. Order the data appropriately for visualization
4. Be optimized and efficient

Do not include any LIMIT clause as I'll handle that separately.
Your response should ONLY contain the SQL query I can directly execute against this dataset.
"""

        # Call AI
        response = ask_ai(
            question=prompt,
            api_key=api_key,
            provider=provider,
            model=model,
            conversation_history=[]  # New conversation each time
        )
        
        # Extract SQL from response
        queries = extract_sql_from_ai_response(response)
        
        if not queries:
            return False, "Could not extract SQL query from AI response"
            
        # Use the first query
        sql_query = queries[0]
        
        # Return the query
        return True, sql_query
        
    except Exception as e:
        logger.error(f"Error generating visualization SQL: {e}")
        return False, f"Error: {str(e)}"

def create_visualization_from_natural_language(dataset_name, visualization_goal, api_key, provider="anthropic", model=None):
    """
    Create a visualization from a natural language description.
    
    Args:
        dataset_name: Name of the dataset
        visualization_goal: Natural language description of the visualization
        api_key: API key for the AI provider
        provider: AI provider name
        model: Model to use
        
    Returns:
        Tuple of (success, result DataFrame or query or error message)
    """
    try:
        # Generate SQL
        success, sql_or_error = generate_visualization_sql(
            dataset_name, 
            visualization_goal, 
            api_key, 
            provider, 
            model
        )
        
        if not success:
            return False, sql_or_error
            
        # We have a SQL query, now execute it
        cache = get_cache()
        try:
            result_df = cache.query(sql_or_error)
            # Return both the DataFrame and the query for reference
            return True, {"data": result_df, "sql": sql_or_error}
        except Exception as e:
            logger.error(f"Error executing generated SQL: {e}")
            return False, f"Error executing SQL: {str(e)}\n\nGenerated SQL: {sql_or_error}"
            
    except Exception as e:
        logger.error(f"Error creating visualization from natural language: {e}")
        return False, f"Error: {str(e)}"

def visualize_ai_query_result(question, api_key, provider="anthropic", model=None):
    """
    Ask a natural language question, run SQL, and prepare data for visualization.
    
    Args:
        question: Natural language question about the data
        api_key: API key for the AI provider
        provider: AI provider name
        model: Model to use
        
    Returns:
        Tuple of (success, dict with data, SQL, and question or error message)
    """
    try:
        # First get a response from the AI
        response = ask_ai(
            question=question,
            api_key=api_key,
            provider=provider,
            model=model,
            conversation_history=[]  # New conversation each time
        )
        
        if not response:
            return False, "No response received from AI"
        
        # First try to extract SQL using our custom function
        queries = extract_sql_from_ai_response(response)
        
        if queries:
            # We have SQL queries extracted directly
            sql_query = queries[0]  # Use the first query
            
            # Execute the query
            cache = get_cache()
            try:
                result_df = cache.query(sql_query)
                return True, {
                    "data": result_df,
                    "sql": sql_query,
                    "question": question
                }
            except Exception as e:
                logger.error(f"Error executing extracted SQL: {e}")
                return False, f"Error executing SQL: {str(e)}\n\nGenerated SQL: {sql_query}"
        else:
            # No SQL queries found with direct extraction, try using extract_and_run_queries
            try:
                # extract_and_run_queries returns (processed_response, executed_queries, results_data, failure_info)
                processed_response, executed_queries, results_data, failure_info = extract_and_run_queries(response)
                
                if not executed_queries or not results_data:
                    return False, "Could not extract SQL query from AI response"
                    
                # Check for failures
                if failure_info:
                    return False, f"Query execution failed: {failure_info['error_message']}"
                
                # Get the last result (most relevant)
                last_result = results_data[-1]
                
                # Get DataFrame and SQL from the result
                df = last_result.get("result_df")
                sql = last_result.get("query")
                
                if df is None or df.empty:
                    return False, "Query returned no results"
                    
                # Return the result
                return True, {
                    "data": df,
                    "sql": sql,
                    "question": question
                }
            except Exception as e:
                logger.error(f"Error using extract_and_run_queries: {e}")
                return False, f"Could not extract SQL query from AI response: {str(e)}"
            
    except Exception as e:
        logger.error(f"Error in visualize_ai_query_result: {e}")
        return False, f"Error: {str(e)}" 