#!/usr/bin/env python

"""
Test script for Claude API connection in Streamlit environment.
This script tests Claude API with settings similar to the Streamlit app.
"""

import os
import sys
import time
import logging
import httpx
import socket

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s [%(levelname)s] %(message)s',
                   handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

def test_basic_connectivity():
    """Test basic connectivity to Anthropic API endpoints."""
    logger.info("Testing basic connectivity to Anthropic API...")
    
    # Test with simple HTTPS request
    try:
        conn = httpx.get("https://api.anthropic.com/v1/ping", 
                         timeout=10, 
                         headers={"Accept": "application/json"})
        logger.info(f"Basic HTTPS connectivity test: {conn.status_code} {conn.reason_phrase}")
        return True
    except Exception as e:
        logger.error(f"Basic connectivity failed: {type(e).__name__}: {e}")
        
        # Check DNS resolution
        try:
            ip = socket.gethostbyname('api.anthropic.com')
            logger.info(f"DNS lookup for api.anthropic.com: {ip}")
        except socket.gaierror as dns_err:
            logger.error(f"DNS lookup failed: {dns_err}")
            
        return False

def test_streamlit_claude_connection():
    """Test Claude API connection with settings similar to Streamlit app."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        api_key = input("Enter your Claude API key: ")
    
    if not api_key:
        logger.error("No API key provided")
        return False
    
    # First test basic connectivity
    if not test_basic_connectivity():
        logger.error("Basic connectivity test failed. Check your network connection.")
        return False
    
    try:
        import anthropic
        
        # Log library version
        logger.info(f"Anthropic library version: {anthropic.__version__}")
        
        # Create identical system prompt from the data_mcp.py application
        system_prompt = """You are an expert data analyst assisting a user with datasets managed by Arrow Cache. 
Your goal is to answer user questions about the data, often by generating and explaining **DuckDB SQL** queries.

Available datasets:
- Dataset 'Arbon_2': 4427 rows, 7 columns
  Format: geoparquet
  Columns: building_id, height, Footprint_area, volume, geometry, Roof_area, Facade_area

**Instructions for Generating Queries:**
1.  **Dialect:** ALWAYS generate queries using **DuckDB SQL** syntax.
2.  **Table Prefix:** ALWAYS reference datasets using the `_cache_` prefix (e.g., `FROM _cache_{{dataset_name}}`).
3.  **Check Schema & Types:** BEFORE writing a query, carefully check the dataset's columns.
4.  **Function Usage:** Use functions compatible with the column's DuckDB data type.
5.  **Enclose Queries:** Enclose ALL SQL queries within `<query>...</query>` tags.
6.  **Explain:** After providing a query, briefly explain what it does."""
        
        # Create API client - no custom options
        client = anthropic.Anthropic(api_key=api_key)
        
        # Create messages array like in the application
        messages = [{"role": "user", "content": "How many rows are in the Arbon_2 dataset?"}]
        
        # Make the API call with detailed logging
        logger.info("Sending request to Claude API...")
        start_time = time.time()
        
        try:
            response = client.messages.create(
                model="claude-3-haiku-20240307",  # Same as in app
                max_tokens=2048,
                system=system_prompt,
                messages=messages,
                timeout=60  # Longer timeout
            )
            
            elapsed = time.time() - start_time
            logger.info(f"Request successful (took {elapsed:.2f}s)")
            logger.info(f"Response: {response.content[0].text[:100]}...")  # Show first 100 chars
            return True
            
        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(f"Request failed after {elapsed:.2f}s: {type(e).__name__}: {e}")
            
            # Try to get more specific error information
            if isinstance(e, anthropic.APIConnectionError):
                logger.error("This is a connection error. The API could not be reached.")
                logger.error("Check your firewall, proxy settings, or network connection.")
                # Check proxy settings
                logger.info(f"Current proxy settings - HTTP_PROXY: {os.environ.get('HTTP_PROXY')}")
                logger.info(f"Current proxy settings - HTTPS_PROXY: {os.environ.get('HTTPS_PROXY')}")
            elif isinstance(e, anthropic.AuthenticationError):
                logger.error("This is an authentication error. Your API key may be invalid.")
            elif isinstance(e, anthropic.RateLimitError):
                logger.error("This is a rate limit error. You may have exceeded your quota.")
            
            return False
    
    except ImportError as e:
        logger.error(f"Import error: {e}")
        return False

if __name__ == "__main__":
    test_streamlit_claude_connection() 