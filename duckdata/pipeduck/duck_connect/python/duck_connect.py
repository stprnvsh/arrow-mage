"""
DuckConnect - Python Client Implementation

This module provides a Python-specific implementation of DuckConnect
with additional features optimized for the Python ecosystem.
"""

import os
import uuid
import logging
import pandas as pd
import numpy as np
import pyarrow as pa
from typing import Dict, List, Any, Optional, Union, Tuple

# Import the core DuckConnect implementation
from pipeduck.duck_connect.core.facade import DuckConnect as CoreDuckConnect

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.python')

class DuckConnect(CoreDuckConnect):
    """
    Python-specific implementation of DuckConnect
    
    Extends the core DuckConnect with Python-specific features and optimizations.
    """
    
    def __init__(self, db_path="duck_connect.duckdb", pool_size=5, enable_numpy=True, enable_pandas=True, enable_arrow=True):
        """
        Initialize DuckConnect with Python-specific options
        
        Args:
            db_path: Path to the DuckDB database file
            pool_size: Size of the connection pool
            enable_numpy: Whether to enable NumPy integration
            enable_pandas: Whether to enable pandas integration
            enable_arrow: Whether to enable PyArrow integration
        """
        super().__init__(db_path=db_path, pool_size=pool_size)
        
        self.enable_numpy = enable_numpy
        self.enable_pandas = enable_pandas
        self.enable_arrow = enable_arrow
        
        # Check for NumPy availability
        self.numpy_available = False
        if enable_numpy:
            try:
                import numpy as np
                self.numpy_available = True
            except ImportError:
                logger.warning("NumPy not available, NumPy integration disabled")
        
        # Check for pandas availability
        self.pandas_available = False
        if enable_pandas:
            try:
                import pandas as pd
                self.pandas_available = True
            except ImportError:
                logger.warning("pandas not available, pandas integration disabled")
        
        # Check for PyArrow availability (core class already checks this)
        self.arrow_available = self._check_arrow_availability() and enable_arrow
        
    def __enter__(self):
        """Enter context manager"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close connections"""
        self.close()
        
    def register_dataset(self, 
                        data: Union[pd.DataFrame, pa.Table, np.ndarray, str], 
                        name: str, 
                        description: Optional[str] = None,
                        available_to_languages: Optional[List[str]] = None,
                        overwrite: bool = False,
                        **kwargs) -> str:
        """
        Register a dataset with DuckConnect.
        
        Python-specific version that handles NumPy arrays and other Python data types.
        
        Args:
            data: DataFrame, Arrow Table, NumPy array or SQL query string
            name: Name for the dataset
            description: Optional description
            available_to_languages: List of languages that can access this dataset
            overwrite: Whether to overwrite an existing dataset with the same name
            **kwargs: Additional arguments passed to the core implementation
            
        Returns:
            dataset_id: ID of the registered dataset
        """
        # Handle NumPy arrays by converting to pandas DataFrame
        if isinstance(data, np.ndarray) and self.numpy_available:
            if data.ndim == 1:
                data = pd.DataFrame({f'column_0': data})
            else:
                data_dict = {}
                for i in range(data.shape[1]):
                    data_dict[f'column_{i}'] = data[:, i]
                data = pd.DataFrame(data_dict)
            
        # Set default Python as source language
        if 'source_language' not in kwargs:
            kwargs['source_language'] = 'python'
            
        # If available_to_languages is None, default to all languages
        if available_to_languages is None:
            available_to_languages = ['python', 'r', 'julia']
            
        # Call the parent implementation
        return super().register_dataset(
            data=data,
            name=name,
            description=description,
            available_to_languages=available_to_languages,
            overwrite=overwrite,
            **kwargs
        )
        
    def get_dataset(self, 
                   identifier: str, 
                   as_numpy: bool = False,
                   **kwargs) -> Union[pd.DataFrame, pa.Table, np.ndarray, None]:
        """
        Get a dataset by ID or name.
        
        Python-specific version with option to return as NumPy array.
        
        Args:
            identifier: Dataset ID or name
            as_numpy: Whether to return as NumPy array
            **kwargs: Additional arguments passed to the core implementation
            
        Returns:
            Dataset as pandas DataFrame, Arrow Table, or NumPy array
        """
        # Set default language
        if 'language' not in kwargs:
            kwargs['language'] = 'python'
            
        # Handle NumPy output format
        if as_numpy and self.numpy_available:
            result = super().get_dataset(identifier=identifier, as_arrow=False, **kwargs)
            if result is not None and isinstance(result, pd.DataFrame):
                return result.to_numpy()
            return None
            
        # Call parent implementation
        return super().get_dataset(identifier=identifier, **kwargs)
    
    def to_dict(self, identifier: str) -> Dict:
        """
        Convert a dataset to a Python dictionary.
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            Dictionary representation of the dataset
        """
        df = self.get_dataset(identifier)
        if df is not None and isinstance(df, pd.DataFrame):
            return df.to_dict(orient='records')
        return {}
    
    def from_dict(self, data: List[Dict], name: str, **kwargs) -> str:
        """
        Register a dataset from a Python dictionary.
        
        Args:
            data: List of dictionaries to convert to a dataset
            name: Name for the dataset
            **kwargs: Additional arguments for register_dataset
            
        Returns:
            dataset_id: ID of the registered dataset
        """
        df = pd.DataFrame(data)
        return self.register_dataset(df, name, **kwargs)
    
    def execute_query(self, 
                     query: str, 
                     params: Optional[Dict] = None,
                     as_numpy: bool = False,
                     **kwargs) -> Union[pd.DataFrame, pa.Table, np.ndarray]:
        """
        Execute a SQL query with Python-specific features.
        
        Args:
            query: SQL query to execute
            params: Dictionary of query parameters
            as_numpy: Whether to return the result as a NumPy array
            **kwargs: Additional arguments for the core implementation
            
        Returns:
            Query result as DataFrame, Arrow Table, or NumPy array
        """
        # Handle NumPy output format
        if as_numpy and self.numpy_available:
            result = super().execute_query(query=query, as_arrow=False, **kwargs)
            if result is not None and isinstance(result, pd.DataFrame):
                return result.to_numpy()
            return None
        
        # Execute query with parameters
        if params is not None:
            with self.connection_pool.get_connection() as conn:
                result = conn.execute(query, params).fetchdf()
                return result
                
        # Call parent implementation
        return super().execute_query(query=query, **kwargs)


# Define a context manager for use in pipeline nodes
class DuckContext:
    """
    DuckContext: Context manager for pipeline nodes
    
    Provides a simplified interface for pipeline nodes to interact with DuckConnect.
    """
    
    def __init__(self, db_path=None, metadata=None):
        """
        Initialize a DuckContext for a pipeline node.
        
        Args:
            db_path: Path to the DuckDB database file
            metadata: Node metadata dictionary (from command line)
        """
        self.db_path = db_path
        self.metadata = metadata or {}
        
        # Extract database path from metadata if not provided
        if self.db_path is None and 'db_path' in self.metadata:
            self.db_path = self.metadata['db_path']
        elif self.db_path is None:
            self.db_path = "pipeline.duckdb"
            
        # Create DuckConnect instance
        self.duck_connect = DuckConnect(self.db_path)
        
        # Input/output names from metadata
        self.inputs = self.metadata.get('inputs', [])
        self.outputs = self.metadata.get('outputs', [])
        
    def __enter__(self):
        """Enter context manager"""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context manager and close connections"""
        self.duck_connect.close()
        
    def get_input(self, name=None, required=True):
        """
        Get an input dataset by name.
        
        Args:
            name: Name of the input dataset (if None, returns the first input)
            required: Whether this input is required (raises error if missing)
            
        Returns:
            Input dataset as pandas DataFrame
        """
        # Use the first input if name is not provided
        if name is None and self.inputs:
            name = self.inputs[0]
            
        # Check if the input exists
        try:
            return self.duck_connect.get_dataset(name)
        except Exception as e:
            if required:
                raise ValueError(f"Required input '{name}' not found: {e}")
            return None
            
    def set_output(self, data, name=None):
        """
        Set an output dataset.
        
        Args:
            data: Output data to register
            name: Name of the output dataset (if None, uses the first output)
            
        Returns:
            ID of the registered dataset
        """
        # Use the first output if name is not provided
        if name is None and self.outputs:
            name = self.outputs[0]
            
        if name is None:
            raise ValueError("Output name must be specified")
            
        # Register the output dataset
        return self.duck_connect.register_dataset(
            data=data,
            name=name,
            source_language='python',
            available_to_languages=['python', 'r', 'julia']
        ) 