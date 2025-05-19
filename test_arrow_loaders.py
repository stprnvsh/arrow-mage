#!/usr/bin/env python
"""
Test script to compare arrow_cache.threading.safe_to_arrow_table vs arrow_cache_mcp.loaders.load_dataset_from_url
"""

import time
import logging
from typing import Optional, Dict, Any, Union, Tuple

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("arrow-cache-test")

# Test URLs
TEST_URL = 'https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet'
FORMAT = 'parquet'

def test_direct_arrow_cache():
    """Test the direct arrow_cache.threading.safe_to_arrow_table method"""
    logger.info("Testing direct arrow_cache.threading.safe_to_arrow_table...")
    
    try:
        start_time = time.time()
        from arrow_cache.threading import safe_to_arrow_table
        
        logger.info(f"Loading data from URL: {TEST_URL}")
        table = safe_to_arrow_table(TEST_URL, format=FORMAT)
        
        end_time = time.time()
        logger.info(f"Successfully loaded table with {table.num_rows} rows and {len(table.column_names)} columns")
        logger.info(f"Time taken: {end_time - start_time:.2f} seconds")
        
        # Show some sample data
        logger.info(f"Column names: {table.column_names}")
        
        return True, table
    except Exception as e:
        logger.error(f"Error testing direct arrow_cache: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False, str(e)

def test_loaders_method():
    """Test the arrow_cache_mcp.loaders.load_dataset_from_url method"""
    logger.info("Testing arrow_cache_mcp.loaders.load_dataset_from_url...")
    
    try:
        start_time = time.time()
        from arrow_cache_mcp.loaders import load_dataset_from_url
        
        logger.info(f"Loading data from URL: {TEST_URL}")
        success, result = load_dataset_from_url(
            url=TEST_URL,
            dataset_name="nyc_taxi_test",
            format=FORMAT
        )
        
        end_time = time.time()
        
        if success:
            logger.info(f"Successfully loaded dataset with metadata: {result}")
            logger.info(f"Row count: {result.get('row_count')}")
            logger.info(f"Column count: {result.get('column_count')}")
            logger.info(f"Time taken: {end_time - start_time:.2f} seconds")
            logger.info(f"Column names: {result.get('columns')}")
            return True, result
        else:
            logger.error(f"Failed to load dataset: {result}")
            return False, result
            
    except Exception as e:
        logger.error(f"Error testing loaders method: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False, str(e)

def compare_results(direct_result, loaders_result):
    """Compare the results from both methods"""
    if not isinstance(direct_result, tuple) or not isinstance(loaders_result, tuple):
        logger.error("Invalid results format for comparison")
        return
    
    direct_success, direct_data = direct_result
    loaders_success, loaders_data = loaders_result
    
    if not direct_success or not loaders_success:
        logger.error("One or both tests failed, can't compare results")
        return
    
    # For direct method, we have the table
    # For loaders method, we have metadata about the cached table
    
    # Compare row counts
    direct_rows = direct_data.num_rows if hasattr(direct_data, 'num_rows') else None
    loaders_rows = loaders_data.get('row_count') if isinstance(loaders_data, dict) else None
    
    if direct_rows is not None and loaders_rows is not None:
        logger.info(f"Row count comparison: Direct={direct_rows}, Loaders={loaders_rows}")
        if direct_rows == loaders_rows:
            logger.info("✅ Row counts match!")
        else:
            logger.warning("❌ Row counts don't match!")
    
    # Compare column counts
    direct_cols = len(direct_data.column_names) if hasattr(direct_data, 'column_names') else None
    loaders_cols = loaders_data.get('column_count') if isinstance(loaders_data, dict) else None
    
    if direct_cols is not None and loaders_cols is not None:
        logger.info(f"Column count comparison: Direct={direct_cols}, Loaders={loaders_cols}")
        if direct_cols == loaders_cols:
            logger.info("✅ Column counts match!")
        else:
            logger.warning("❌ Column counts don't match!")
    
    # Compare column names if possible
    if hasattr(direct_data, 'column_names') and isinstance(loaders_data, dict) and 'columns' in loaders_data:
        direct_names = set(direct_data.column_names)
        loaders_names = set(loaders_data['columns'])
        
        if direct_names == loaders_names:
            logger.info("✅ Column names match!")
        else:
            logger.warning("❌ Column names don't match!")
            logger.info(f"Names only in direct: {direct_names - loaders_names}")
            logger.info(f"Names only in loaders: {loaders_names - direct_names}")

def main():
    """Main test function"""
    logger.info("Starting Arrow Cache loader comparison tests")
    
    # Test the direct method
    direct_result = test_direct_arrow_cache()
    
    # Test the loaders method
    loaders_result = test_loaders_method()
    
    # Compare results
    logger.info("Comparing results from both methods")
    compare_results(direct_result, loaders_result)
    
    logger.info("Test completed")
    
if __name__ == "__main__":
    main() 