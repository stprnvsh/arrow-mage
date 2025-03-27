#!/usr/bin/env python3
"""
Data Node Runner: Execute data connector nodes for PipeLink pipelines

This script is executed by the PipeLink orchestrator to run data connector nodes.
It reads the node configuration from a YAML file, sets up the data connector,
executes queries or loads data, and saves the results.
"""

import os
import sys
import logging
from typing import Dict, Any, Optional, Union

import pandas as pd
import pyarrow as pa

# Use absolute import path so this works when run as a subprocess
try:
    from pipelink.python.data_node_context import DataNodeContext
except ImportError:
    # Fall back to relative import for development mode
    try:
        # Add the parent directory to the path if being run directly
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from pipelink.python.data_node_context import DataNodeContext
    except ImportError:
        raise ImportError("Cannot import DataNodeContext. Make sure the pipelink package is installed.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipelink.data_node_runner')

def run_data_node(meta_path: str) -> bool:
    """
    Run a data connector node
    
    Args:
        meta_path: Path to metadata file
        
    Returns:
        True if successful, False if failed
    """
    try:
        logger.info(f"Running data node with metadata: {meta_path}")
        
        # Create data node context
        with DataNodeContext(meta_path) as ctx:
            # Get connection parameters
            connection = ctx.get_connection_params()
            
            # Get node ID for logging
            node_id = ctx.get_node_id()
            conn_type = connection.get('type')
            logger.info(f"Data node {node_id} using connection type: {conn_type}")
            
            # Process based on connection type
            if conn_type == 'duckdb':
                result = _process_duckdb_node(ctx)
            elif conn_type == 'postgres':
                result = _process_postgres_node(ctx)
            elif conn_type == 'sqlite':
                result = _process_sqlite_node(ctx)
            elif conn_type == 's3':
                result = _process_s3_node(ctx)
            elif conn_type == 'flight':
                result = _process_flight_node(ctx)
            elif conn_type == 'file':
                result = _process_file_node(ctx)
            else:
                logger.error(f"Unsupported connection type: {conn_type}")
                return False
                
            if not result:
                logger.error(f"Data node {node_id} failed to process data")
                return False
                
            logger.info(f"Data node {node_id} completed successfully")
            return True
    except Exception as e:
        logger.error(f"Error running data node: {e}", exc_info=True)
        return False

def _process_duckdb_node(ctx: DataNodeContext) -> bool:
    """Process a DuckDB node"""
    try:
        # Check if we need to run a query
        query = ctx.get_param('query')
        if query:
            logger.info(f"Executing DuckDB query: {query}")
            result = ctx.execute_sql(query)
            if result is None:
                logger.error("Query returned no results")
                return False
                
            # Save output
            output_name = ctx.get_param('output_name')
            if not output_name:
                # If no output name specified, use first output
                outputs = ctx.meta.get('outputs', [])
                if not outputs:
                    logger.error("No outputs defined for node")
                    return False
                output_name = outputs[0]
                
            # Save output
            description = ctx.get_param('description', f"Result of DuckDB query: {query}")
            ctx.save_output(output_name, result, description)
            logger.info(f"Saved query result to output: {output_name}")
            return True
        else:
            logger.error("No query specified for DuckDB node")
            return False
    except Exception as e:
        logger.error(f"Error processing DuckDB node: {e}")
        return False

def _process_postgres_node(ctx: DataNodeContext) -> bool:
    """Process a PostgreSQL node"""
    try:
        # Check if we need to run a query
        query = ctx.get_param('query')
        if query:
            logger.info(f"Executing PostgreSQL query: {query}")
            result = ctx.execute_sql(query)
            if result is None:
                logger.error("Query returned no results")
                return False
                
            # Save output
            output_name = ctx.get_param('output_name')
            if not output_name:
                # If no output name specified, use first output
                outputs = ctx.meta.get('outputs', [])
                if not outputs:
                    logger.error("No outputs defined for node")
                    return False
                output_name = outputs[0]
                
            # Save output
            description = ctx.get_param('description', f"Result of PostgreSQL query: {query}")
            ctx.save_output(output_name, result, description)
            logger.info(f"Saved query result to output: {output_name}")
            return True
        else:
            logger.error("No query specified for PostgreSQL node")
            return False
    except Exception as e:
        logger.error(f"Error processing PostgreSQL node: {e}")
        return False

def _process_sqlite_node(ctx: DataNodeContext) -> bool:
    """Process a SQLite node"""
    try:
        # Check if we need to run a query
        query = ctx.get_param('query')
        if query:
            logger.info(f"Executing SQLite query: {query}")
            result = ctx.execute_sql(query)
            if result is None:
                logger.error("Query returned no results")
                return False
                
            # Save output
            output_name = ctx.get_param('output_name')
            if not output_name:
                # If no output name specified, use first output
                outputs = ctx.meta.get('outputs', [])
                if not outputs:
                    logger.error("No outputs defined for node")
                    return False
                output_name = outputs[0]
                
            # Save output
            description = ctx.get_param('description', f"Result of SQLite query: {query}")
            ctx.save_output(output_name, result, description)
            logger.info(f"Saved query result to output: {output_name}")
            return True
        else:
            logger.error("No query specified for SQLite node")
            return False
    except Exception as e:
        logger.error(f"Error processing SQLite node: {e}")
        return False

def _process_flight_node(ctx: DataNodeContext) -> bool:
    """Process an Apache Flight node"""
    try:
        # Check if we need to run a query
        query = ctx.get_param('query')
        if query:
            logger.info(f"Executing Flight query: {query}")
            result = ctx.query_flight(query)
            if result is None:
                logger.error("Query returned no results")
                return False
                
            # Save output
            output_name = ctx.get_param('output_name')
            if not output_name:
                # If no output name specified, use first output
                outputs = ctx.meta.get('outputs', [])
                if not outputs:
                    logger.error("No outputs defined for node")
                    return False
                output_name = outputs[0]
                
            # Save output
            description = ctx.get_param('description', f"Result of Flight query: {query}")
            ctx.save_output(output_name, result, description)
            logger.info(f"Saved query result to output: {output_name}")
            return True
        else:
            logger.error("No query specified for Flight node")
            return False
    except Exception as e:
        logger.error(f"Error processing Flight node: {e}")
        return False

def _process_s3_node(ctx: DataNodeContext) -> bool:
    """Process an S3 node"""
    try:
        # Get S3 parameters
        bucket = ctx.get_param('bucket')
        key = ctx.get_param('key')
        region = ctx.get_param('region', 'us-east-1')
        file_format = ctx.get_param('file_format')
        aws_access_key_id = ctx.get_param('aws_access_key_id')
        aws_secret_access_key = ctx.get_param('aws_secret_access_key')
        
        if not bucket or not key:
            logger.error("S3 node requires bucket and key parameters")
            return False
            
        logger.info(f"Loading data from S3: s3://{bucket}/{key}")
        result = ctx.load_s3(
            bucket=bucket,
            key=key,
            region=region,
            file_format=file_format,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
        
        if result is None:
            logger.error("Failed to load data from S3")
            return False
            
        # Save output
        output_name = ctx.get_param('output_name')
        if not output_name:
            # If no output name specified, use first output
            outputs = ctx.meta.get('outputs', [])
            if not outputs:
                logger.error("No outputs defined for node")
                return False
            output_name = outputs[0]
            
        # Save output
        description = ctx.get_param('description', f"Data from S3: s3://{bucket}/{key}")
        ctx.save_output(output_name, result, description)
        logger.info(f"Saved S3 data to output: {output_name}")
        return True
    except Exception as e:
        logger.error(f"Error processing S3 node: {e}")
        return False

def _process_file_node(ctx: DataNodeContext) -> bool:
    """Process a file node"""
    try:
        # Get file parameters
        file_path = ctx.get_param('file_path')
        file_format = ctx.get_param('file_format')
        
        if not file_path:
            logger.error("File node requires file_path parameter")
            return False
            
        logger.info(f"Loading data from file: {file_path}")
        result = ctx.load_file(file_path, file_format)
        
        if result is None:
            logger.error("Failed to load data from file")
            return False
            
        # Save output
        output_name = ctx.get_param('output_name')
        if not output_name:
            # If no output name specified, use first output
            outputs = ctx.meta.get('outputs', [])
            if not outputs:
                logger.error("No outputs defined for node")
                return False
            output_name = outputs[0]
            
        # Save output
        description = ctx.get_param('description', f"Data from file: {file_path}")
        ctx.save_output(output_name, result, description)
        logger.info(f"Saved file data to output: {output_name}")
        return True
    except Exception as e:
        logger.error(f"Error processing file node: {e}")
        return False

def main():
    """Main entry point"""
    if len(sys.argv) < 2 or sys.argv[1] in ['-h', '--help']:
        logger.info("Usage: python -m pipelink.python.data_node_runner <metadata_file>")
        logger.info("  metadata_file: Path to the node metadata YAML file")
        logger.info("")
        logger.info("This script is typically run by the PipeLink orchestrator.")
        sys.exit(0 if sys.argv[1] in ['-h', '--help'] else 1)
        
    meta_path = sys.argv[1]
    success = run_data_node(meta_path)
    
    # Exit with appropriate status code
    sys.exit(0 if success else 1)

if __name__ == '__main__':
    main() 