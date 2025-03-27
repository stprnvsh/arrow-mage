"""
Node utilities for PipeLink

Helper functions for working with PipeLink in Python node scripts
"""
import os
import sys
import yaml
from typing import Any, Dict, List, Optional, Union
import pandas as pd
from pipelink.crosslink.python.crosslink import CrossLink

class NodeContext:
    """
    Context manager for working with PipeLink nodes
    
    Provides easy access to input/output datasets and parameters
    
    Example:
    ```python
    with NodeContext() as ctx:
        # Get input data
        df = ctx.get_input("input_name")
        
        # Get parameters
        param = ctx.get_param("param_name", default_value)
        
        # Process data...
        result = process_data(df, param)
        
        # Save output
        ctx.save_output("output_name", result)
    ```
    """
    
    def __init__(self, meta_path: Optional[str] = None):
        """
        Initialize NodeContext with metadata
        
        Args:
            meta_path: Path to metadata file. If None, tries to get from command line
        """
        self.meta = self._load_metadata(meta_path)
        self.cl = CrossLink(db_path=self.meta.get("db_path", "crosslink.duckdb"))
        self.input_cache = {}
    
    def _load_metadata(self, meta_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load metadata from file
        
        Args:
            meta_path: Path to metadata file. If None, tries to get from command line
            
        Returns:
            Dict containing node metadata
        """
        # Get metadata file path from command line if not provided
        if meta_path is None:
            if len(sys.argv) < 2:
                raise ValueError("No metadata file provided. This script should be run by PipeLink.")
            meta_path = sys.argv[1]
        
        if not os.path.isfile(meta_path):
            raise FileNotFoundError(f"Metadata file not found: {meta_path}")
        
        # Load metadata from YAML
        with open(meta_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_input(self, name: str, required: bool = True) -> Optional[pd.DataFrame]:
        """
        Get an input dataset by name
        
        Args:
            name: Name of the input dataset
            required: Whether this input is required
            
        Returns:
            DataFrame with the requested data or None if not required and not found
        """
        # Check if this input is defined for this node
        inputs = self.meta.get("inputs", [])
        if name not in inputs:
            if required:
                raise ValueError(f"Input '{name}' is not defined for this node")
            return None
        
        # Check cache
        if name in self.input_cache:
            return self.input_cache[name]
        
        # Pull from CrossLink
        df = self.cl.pull(name)
        self.input_cache[name] = df
        return df
    
    def save_output(self, name: str, df: pd.DataFrame, description: Optional[str] = None) -> str:
        """
        Save an output dataset
        
        Args:
            name: Name of the output dataset
            df: DataFrame to save
            description: Optional description for the dataset
            
        Returns:
            Dataset ID
        """
        # Check if this output is defined for this node
        outputs = self.meta.get("outputs", [])
        if name not in outputs:
            raise ValueError(f"Output '{name}' is not defined for this node")
        
        # Generate a description if not provided
        if description is None:
            node_id = self.meta.get("node_id", "unknown")
            description = f"Output '{name}' from node '{node_id}'"
        
        # Push to CrossLink
        return self.cl.push(df, name=name, description=description)
    
    def get_param(self, name: str, default: Any = None) -> Any:
        """
        Get a parameter value
        
        Args:
            name: Parameter name
            default: Default value if parameter is not found
            
        Returns:
            Parameter value or default
        """
        params = self.meta.get("params", {})
        return params.get(name, default)
    
    def get_node_id(self) -> str:
        """
        Get the node ID
        
        Returns:
            Node ID string
        """
        return self.meta.get("node_id", "unknown")
    
    def get_pipeline_name(self) -> str:
        """
        Get the pipeline name
        
        Returns:
            Pipeline name string
        """
        return self.meta.get("pipeline_name", "unknown")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cl.close() 