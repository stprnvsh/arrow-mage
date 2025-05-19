#!/usr/bin/env python

"""
Test script for Claude API connection.
This script tests the connection to Claude API and helps diagnose issues.
"""

import os
import sys
import time
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s [%(levelname)s] %(message)s',
                   handlers=[logging.StreamHandler()])
logger = logging.getLogger(__name__)

def test_imports():
    """Test that required libraries are available."""
    logger.info("Testing imports...")
    try:
        import anthropic
        logger.info(f"✅ Anthropic library is installed (version: {anthropic.__version__})")
        return True
    except ImportError as e:
        logger.error(f"❌ Failed to import Anthropic library: {e}")
        logger.error("Please install it with: pip install anthropic")
        return False

def test_api_key(api_key=None):
    """Test that an API key is available and validates format."""
    logger.info("Testing API key...")
    
    # Try to get from argument
    if not api_key:
        # Try to get from environment
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        
    if not api_key:
        logger.error("❌ No API key provided and ANTHROPIC_API_KEY environment variable not set")
        return False
    
    # Basic format validation
    if not api_key.startswith('sk-ant-'):
        logger.error("❌ API key format is invalid - should start with 'sk-ant-'")
        return False
    
    if len(api_key) < 20:
        logger.error("❌ API key seems too short to be valid")
        return False
        
    logger.info("✅ API key format is valid")
    return True

def test_api_connection(api_key):
    """Test the connection to the Claude API."""
    logger.info("Testing API connection...")
    
    try:
        import anthropic
        client = anthropic.Anthropic(api_key=api_key)
        
        # Create a simple test message
        start_time = time.time()
        logger.info("Sending test message to Claude API...")
        
        try:
            response = client.messages.create(
                model="claude-3-haiku-20240307",
                max_tokens=10,
                system="You are a helpful assistant.",
                messages=[{"role": "user", "content": "Say hello"}],
                timeout=30  # 30 second timeout
            )
            
            elapsed = time.time() - start_time
            logger.info(f"✅ Successfully connected to Claude API in {elapsed:.2f} seconds")
            logger.info(f"Response: {response.content[0].text}")
            return True
            
        except anthropic.RateLimitError as e:
            logger.error(f"❌ Rate limit error: {e}")
            logger.error("This suggests your API key is valid but you've exceeded your rate limits")
            return False
            
        except anthropic.AuthenticationError as e:
            logger.error(f"❌ Authentication error: {e}")
            logger.error("This suggests your API key is invalid or has been revoked")
            return False
            
        except anthropic.APIConnectionError as e:
            logger.error(f"❌ Connection error: {e}")
            logger.error("""
Possible causes:
1. Network connectivity issues
2. Firewall or proxy blocking the connection
3. DNS resolution problems
4. Anthropic API service is down
            """)
            return False
            
        except Exception as e:
            logger.error(f"❌ Unexpected error: {type(e).__name__}: {e}")
            return False
            
    except ImportError as e:
        logger.error(f"❌ Failed to import required libraries: {e}")
        return False

def main():
    """Main function to run all tests."""
    parser = argparse.ArgumentParser(description='Test Claude API connection')
    parser.add_argument('--api-key', '-k', help='Anthropic API key')
    parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose logging')
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    logger.info("Starting Claude API connection tests")
    logger.info("-" * 50)
    
    # Run tests
    imports_ok = test_imports()
    if not imports_ok:
        logger.error("Import test failed, cannot continue")
        return 1
    
    api_key_ok = test_api_key(args.api_key)
    if not api_key_ok:
        logger.error("API key test failed, cannot continue")
        return 1
    
    connection_ok = test_api_connection(args.api_key or os.environ.get("ANTHROPIC_API_KEY"))
    if not connection_ok:
        logger.error("API connection test failed")
        return 1
    
    logger.info("-" * 50)
    logger.info("✅ All tests passed! Claude API is working properly.")
    return 0

if __name__ == "__main__":
    sys.exit(main()) 