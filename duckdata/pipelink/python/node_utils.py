"""
Node utilities for PipeLink

Helper functions for working with PipeLink in Python node scripts
"""
import os
import sys
import yaml
from typing import Any, Dict, List, Optional, Union, Tuple
import pandas as pd
from pipelink.crosslink.python.crosslink import CrossLink

def get_optimized_data_access(identifier: str, crosslink_instance) -> Tuple[str, Any]:
    """
    Get the most efficient data access method for a dataset
    
    Args:
        identifier: Dataset ID or name
        crosslink_instance: CrossLink instance
        
    Returns:
        Tuple of (access_method, data_object) where:
        - access_method is one of: "zero_copy_mmap", "zero_copy_arrow", "direct_reference", "pandas"
        - data_object is the actual data object (Arrow table, DataFrame, table reference)
    """
    # First try to get a direct table reference (most efficient)
    try:
        reference = crosslink_instance.get_table_reference(identifier)
        # Check if we have a memory-mapped file path
        if reference.get("memory_map_path") and os.path.exists(reference["memory_map_path"]):
            # Best approach - direct memory-mapped file access
            try:
                import pyarrow as pa
                with pa.memory_map(reference["memory_map_path"], 'rb') as source:
                    table = pa.ipc.open_file(source).read_all()
                return "zero_copy_mmap", table
            except ImportError:
                # If pyarrow isn't available, return the reference
                return "direct_reference", reference
        else:
            # Return the reference for direct database access
            return "direct_reference", reference
    except Exception as e:
        if os.environ.get("PIPELINK_ENABLE_ZERO_COPY") == "1":
            print(f"Warning: Zero-copy table reference failed: {e}")
    
    # Fall back to regular pull with Arrow if possible
    try:
        arrow_table = crosslink_instance.pull(identifier, to_pandas=False, use_arrow=True, zero_copy=True)
        return "zero_copy_arrow", arrow_table
    except Exception as e:
        if os.environ.get("PIPELINK_ENABLE_ZERO_COPY") == "1":
            print(f"Warning: Zero-copy Arrow access failed: {e}")
    
    # Last resort - regular pandas DataFrame
    df = crosslink_instance.pull(identifier)
    return "pandas", df

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
        
        # Use the optimized data access method
        access_method, data_obj = get_optimized_data_access(name, self.cl)
        
        if access_method == "zero_copy_mmap" or access_method == "zero_copy_arrow":
            # Convert Arrow table to pandas
            import pyarrow as pa
            df = data_obj.to_pandas()
        elif access_method == "direct_reference":
            # Query DuckDB directly using the reference
            import duckdb
            conn = duckdb.connect(data_obj["database_path"])
            query = f"SELECT * FROM {data_obj['table_name']}"
            df = conn.execute(query).fetchdf()
            conn.close()
        else:
            # Already a pandas DataFrame
            df = data_obj
        
        self.input_cache[name] = df
        return df
    
    def get_input_arrow(self, name: str, required: bool = True):
        """
        Get an input dataset as an Arrow table with zero-copy optimization
        
        Args:
            name: Name of the input dataset
            required: Whether this input is required
            
        Returns:
            Arrow table with the requested data or None if not required and not found
        """
        # Check if this input is defined for this node
        inputs = self.meta.get("inputs", [])
        if name not in inputs:
            if required:
                raise ValueError(f"Input '{name}' is not defined for this node")
            return None
        
        # Use the optimized data access method
        access_method, data_obj = get_optimized_data_access(name, self.cl)
        
        if access_method == "zero_copy_mmap" or access_method == "zero_copy_arrow":
            # Return the Arrow table directly
            return data_obj
        elif access_method == "direct_reference":
            # Query DuckDB directly for Arrow table
            import duckdb
            conn = duckdb.connect(data_obj["database_path"])
            query = f"SELECT * FROM {data_obj['table_name']}"
            arrow_table = conn.execute(query).arrow()
            conn.close()
            return arrow_table
        else:
            # Convert pandas DataFrame to Arrow
            import pyarrow as pa
            return pa.Table.from_pandas(data_obj)
    
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
        
        # Push to CrossLink with zero-copy enabled
        return self.cl.push(df, name=name, description=description, enable_zero_copy=True)
    
    def save_output_arrow(self, name: str, arrow_table, description: Optional[str] = None) -> str:
        """
        Save an Arrow table as an output dataset with zero-copy optimization
        
        Args:
            name: Name of the output dataset
            arrow_table: Arrow table to save
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
        
        # Push to CrossLink directly using the Arrow table (most efficient)
        return self.cl.push(arrow_table, name=name, description=description, enable_zero_copy=True)
    
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