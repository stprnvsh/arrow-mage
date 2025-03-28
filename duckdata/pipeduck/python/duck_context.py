"""
DuckContext: Context Manager for Pipeline Nodes

This module provides a context manager for pipeline nodes to interact with DuckConnect
in a simplified way. It handles loading metadata, accessing input and output datasets,
and managing the connection lifecycle.
"""

import os
import sys
import json
import logging
from typing import Dict, List, Any, Optional, Union

# Try to import DuckConnect from different locations
try:
    from duck_connect.python import DuckConnect
except ImportError:
    try:
        from pipeduck.duck_connect.python import DuckConnect
    except ImportError:
        try:
            from pipeduck.duck_connect.core.facade import DuckConnect
        except ImportError:
            raise ImportError("DuckConnect could not be imported. Please check your installation.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck.duck_context')

class DuckContext:
    """
    Context Manager for pipeline nodes to interact with DuckConnect
    
    This class provides a simplified interface for pipeline nodes to read inputs,
    write outputs, and interact with the shared DuckDB database.
    """
    
    def __init__(self, metadata_file=None, db_path=None):
        """
        Initialize DuckContext with metadata and database connection
        
        Args:
            metadata_file: Path to node metadata JSON file (usually provided by pipeline)
            db_path: Path to DuckDB database file (overrides metadata if provided)
        """
        self.metadata = {}
        self.db_path = db_path
        
        # Load metadata if file is provided
        if metadata_file is not None:
            try:
                with open(metadata_file, 'r') as f:
                    self.metadata = json.load(f)
                logger.info(f"Loaded metadata from {metadata_file}")
            except Exception as e:
                logger.error(f"Error loading metadata: {e}")
                
        # Override db_path from metadata if not manually provided
        if self.db_path is None and 'db_path' in self.metadata:
            self.db_path = self.metadata.get('db_path')
        
        # Default to pipeline.duckdb if still no path
        if self.db_path is None:
            self.db_path = "pipeline.duckdb"
            
        # Create DuckConnect instance
        self.duck_connect = DuckConnect(self.db_path)
        
        # Extract input and output datasets
        self.node_id = self.metadata.get('node_id', 'unknown')
        self.pipeline_id = self.metadata.get('pipeline_id', 'unknown')
        self.inputs = self.metadata.get('inputs', [])
        self.outputs = self.metadata.get('outputs', [])
        self.params = self.metadata.get('params', {})
        
    def __enter__(self):
        """Enter context manager"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and clean up resources"""
        if hasattr(self, 'duck_connect'):
            self.duck_connect.close()
            
    def get_input(self, name=None, as_arrow=False, required=True):
        """
        Get an input dataset from the pipeline
        
        Args:
            name: Name of the input dataset (uses first input if None)
            as_arrow: Whether to return as Arrow Table instead of pandas DataFrame
            required: Whether this input is required (raises error if missing)
            
        Returns:
            Input dataset as pandas DataFrame or Arrow Table
        """
        # Use first input if name is not provided
        if name is None and self.inputs:
            name = self.inputs[0]
            
        if name is None:
            if required:
                raise ValueError("No input name provided and no inputs defined in metadata")
            return None
            
        # Get dataset from DuckConnect
        try:
            return self.duck_connect.get_dataset(name, as_arrow=as_arrow)
        except Exception as e:
            if required:
                raise ValueError(f"Required input '{name}' not found: {e}")
            return None
            
    def set_output(self, data, name=None, description=None):
        """
        Set an output dataset for the pipeline
        
        Args:
            data: Data to register (pandas DataFrame or Arrow Table)
            name: Name of the output dataset (uses first output if None)
            description: Optional description for the dataset
            
        Returns:
            ID of the registered dataset
        """
        # Use first output if name is not provided
        if name is None and self.outputs:
            name = self.outputs[0]
            
        if name is None:
            raise ValueError("No output name provided and no outputs defined in metadata")
            
        # Create description if not provided
        if description is None:
            description = f"Output from {self.node_id} in pipeline {self.pipeline_id}"
            
        # Register dataset
        return self.duck_connect.register_dataset(
            data=data,
            name=name,
            description=description,
            source_language='python',
            available_to_languages=['python', 'r', 'julia']
        )
        
    def execute_query(self, query, params=None, as_arrow=False):
        """
        Execute a SQL query against the DuckDB database
        
        Args:
            query: SQL query to execute
            params: Query parameters (optional)
            as_arrow: Whether to return as Arrow Table
            
        Returns:
            Query result as pandas DataFrame or Arrow Table
        """
        return self.duck_connect.execute_query(query, params=params, as_arrow=as_arrow)
        
    def get_metadata(self):
        """
        Get node metadata
        
        Returns:
            Dictionary with node metadata
        """
        return self.metadata.copy()
        
    def get_param(self, name, default=None):
        """
        Get a parameter from node configuration
        
        Args:
            name: Parameter name
            default: Default value if parameter not found
            
        Returns:
            Parameter value or default
        """
        return self.params.get(name, default) 