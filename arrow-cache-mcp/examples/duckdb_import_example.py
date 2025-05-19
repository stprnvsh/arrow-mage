#!/usr/bin/env python
"""
Example script demonstrating DuckDB import functionality in Arrow Cache MCP.

This script shows how to use DuckDB's native connectors to import data from various sources.
"""

import os
import time
import logging
from arrow_cache_mcp.core import get_arrow_cache
from arrow_cache_mcp.loaders import (
    load_dataset_from_url,
    load_dataset_from_path,
    load_database_table,
    load_database_query
)

# Configure logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    # Initialize ArrowCache
    cache = get_arrow_cache()
    logger.info("Arrow Cache initialized")
    
    # Example 1: Import CSV file from URL
    logger.info("Example 1: Importing CSV from URL")
    csv_url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"
    success, result = load_dataset_from_url(
        url=csv_url,
        dataset_name="titanic",
        format="csv"
    )
    if success:
        logger.info(f"Successfully loaded titanic dataset: {result['row_count']} rows")
    else:
        logger.error(f"Failed to load titanic dataset: {result}")
    
    # Example 2: Import Parquet file (if available locally)
    parquet_path = "example.parquet"
    if os.path.exists(parquet_path):
        logger.info("Example 2: Importing local Parquet file")
        success, result = load_dataset_from_path(
            path=parquet_path,
            dataset_name="example_parquet",
            format="parquet"
        )
        if success:
            logger.info(f"Successfully loaded parquet dataset: {result['row_count']} rows")
        else:
            logger.error(f"Failed to load parquet dataset: {result}")
    
    # Example 3: Run a direct SQL query on the imported data
    logger.info("Example 3: Running SQL query on imported data")
    # Use the cache's query method to run SQL
    try:
        result = cache.query("""
            SELECT 
                Survived, 
                AVG(Age) as avg_age,
                COUNT(*) as count
            FROM _cache_titanic
            GROUP BY Survived
            ORDER BY Survived
        """)
        
        logger.info("Query results:")
        # Convert to pandas for display
        pandas_df = result.to_pandas() if hasattr(result, 'to_pandas') else result
        logger.info(f"\n{pandas_df}")
    except Exception as e:
        logger.error(f"Query failed: {str(e)}")
    
    # Example 4: Import from PostgreSQL (with connection info)
    # Uncomment and configure if you have PostgreSQL available
    """
    logger.info("Example 4: Importing from PostgreSQL")
    success, result = load_database_table(
        connection_string="postgresql://username:password@localhost:5432/mydb",
        table_name="customers",
        dataset_name="pg_customers",
        database_type="postgres",
        schema="public"
    )
    if success:
        logger.info(f"Successfully loaded postgres dataset: {result['row_count']} rows")
    else:
        logger.error(f"Failed to load postgres dataset: {result}")
    """
    
    # Example 5: Display cache status
    logger.info("Example 5: Cache status")
    status = cache.status()
    logger.info(f"Cache entry count: {status.get('entry_count', 0)}")
    logger.info(f"Cache size: {status.get('current_size_bytes', 0) / (1024*1024):.2f} MB")
    
    # Example 6: List all datasets
    logger.info("Example 6: All datasets in cache")
    for key in cache.get_keys():
        metadata = cache.get_metadata(key) or {}
        logger.info(f"Dataset: {key}")
        logger.info(f"  Source: {metadata.get('source', 'Unknown')}")
        logger.info(f"  Format: {metadata.get('source_type', 'Unknown')}")
        logger.info(f"  Rows: {metadata.get('num_rows', 'Unknown')}")
    
    # Clean up
    logger.info("Cleaning up...")
    cache.clear()
    logger.info("Done")

if __name__ == "__main__":
    main() 