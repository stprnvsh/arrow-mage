"""
Dask integration for PipeLink

Provides a Dask-enabled context manager for parallel processing in PipeLink nodes
"""
import os
import sys
import multiprocessing
import yaml
from typing import Any, Dict, List, Optional, Union

import pandas as pd

# Use absolute import path so this works when run as a subprocess
try:
    from pipelink.python.node_utils import NodeContext, get_optimized_data_access
except ImportError:
    # Fall back to relative import for development mode
    try:
        # Add the parent directory to the path if being run directly
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from pipelink.python.node_utils import NodeContext, get_optimized_data_access
    except ImportError:
        raise ImportError("Cannot import NodeContext. Make sure the pipelink package is installed.")

class DaskNodeContext(NodeContext):
    """
    Enhanced context manager for working with PipeLink nodes using Dask
    for parallel processing of large datasets.
    
    Example:
    ```python
    with DaskNodeContext() as ctx:
        # Get input data as a Dask DataFrame
        ddf = ctx.get_dask_input("input_name")
        
        # Process in parallel
        result = ddf.map_partitions(process_function)
        
        # Save output
        ctx.save_dask_output("output_name", result)
    ```
    """
    
    def __init__(self, meta_path=None, n_workers=None, memory_limit="4GB", 
                 processes=True, threads_per_worker=1):
        """
        Initialize DaskNodeContext with metadata
        
        Args:
            meta_path: Path to metadata file. If None, tries to get from command line
            n_workers: Number of Dask workers to use (defaults to number of cores - 1)
            memory_limit: Memory limit per worker
            processes: Whether to use processes (True) or threads (False)
            threads_per_worker: Number of threads per worker
        """
        super().__init__(meta_path)
        
        # Import Dask here to avoid requiring it as a dependency for non-Dask users
        try:
            import dask
            import dask.dataframe as dd
            from dask.distributed import Client, LocalCluster
        except ImportError:
            raise ImportError("Dask is required for DaskNodeContext. Install with pip install dask[complete]")
        
        # Set up Dask client
        if n_workers is None:
            n_workers = max(1, multiprocessing.cpu_count() - 1)
        
        # Set environment variable for zero-copy
        os.environ["PIPELINK_ENABLE_ZERO_COPY"] = "1"
        
        # Create cluster and client
        print(f"Creating Dask LocalCluster with {n_workers} workers")
        self.cluster = LocalCluster(
            n_workers=n_workers,
            threads_per_worker=threads_per_worker,
            processes=processes,
            memory_limit=memory_limit
        )
        self.client = Client(self.cluster)
        print(f"Dask dashboard available at: {self.client.dashboard_link}")
    
    def get_dask_input(self, name, required=True, npartitions=None):
        """
        Get an input dataset as a Dask DataFrame
        
        Args:
            name: Name of the input dataset
            required: Whether this input is required
            npartitions: Number of partitions for the Dask DataFrame
            
        Returns:
            Dask DataFrame with the requested data or None if not required and not found
        """
        import dask.dataframe as dd
        
        # Check if this input is defined for this node
        inputs = self.meta.get("inputs", [])
        if name not in inputs:
            if required:
                raise ValueError(f"Input '{name}' is not defined for this node")
            return None
        
        # Use optimized data access
        access_method, data_obj = get_optimized_data_access(name, self.cl)
        
        # Set default number of partitions if not specified
        if npartitions is None:
            npartitions = self.cluster.workers
        
        # Convert to Dask DataFrame based on access method
        if access_method == "zero_copy_mmap" or access_method == "zero_copy_arrow":
            # Convert Arrow table to pandas first
            try:
                pdf = data_obj.to_pandas()
                return dd.from_pandas(pdf, npartitions=npartitions)
            except Exception as e:
                print(f"Warning: Failed to convert Arrow to Dask: {e}")
                # Fall through to other methods
                
        elif access_method == "direct_reference":
            # Use DuckDB to read directly into Dask
            try:
                # Check if dask_gbrpc extension is available for direct DuckDB integration
                try:
                    import dask_gbrpc
                    import duckdb
                    
                    conn = duckdb.connect(data_obj["database_path"])
                    query = f"SELECT * FROM {data_obj['table_name']}"
                    
                    # Use dask_gbrpc to create Dask DataFrame from DuckDB
                    return dask_gbrpc.duckdb.read_query(conn, query, npartitions=npartitions)
                except ImportError:
                    # Fall back to using pandas as intermediate
                    import duckdb
                    conn = duckdb.connect(data_obj["database_path"])
                    query = f"SELECT * FROM {data_obj['table_name']}"
                    df = conn.execute(query).fetchdf()
                    conn.close()
                    return dd.from_pandas(df, npartitions=npartitions)
            except Exception as e:
                print(f"Warning: Failed to use direct reference with Dask: {e}")
                # Fall through to standard method
        
        # Regular pandas method
        try:
            # If data_obj is already a pandas DataFrame, use it directly
            if isinstance(data_obj, pd.DataFrame):
                return dd.from_pandas(data_obj, npartitions=npartitions)
            
            # Otherwise, pull data
            df = self.cl.pull(name)
            return dd.from_pandas(df, npartitions=npartitions)
        except Exception as e:
            raise ValueError(f"Failed to get input '{name}' as Dask DataFrame: {e}")
    
    def save_dask_output(self, name, ddf, description=None):
        """
        Save a Dask DataFrame as an output dataset
        
        Args:
            name: Name of the output dataset
            ddf: Dask DataFrame to save
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
        
        # Compute the Dask DataFrame to a pandas DataFrame
        print(f"Computing Dask DataFrame for output '{name}'...")
        df = ddf.compute()
        
        # Try to push using Arrow conversion if available
        try:
            import pyarrow as pa
            arrow_table = pa.Table.from_pandas(df)
            return self.save_output_arrow(name, arrow_table, description)
        except Exception as e:
            print(f"Warning: Failed to convert to Arrow: {e}")
            # Fall back to pandas method
            return self.save_output(name, df, description)
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close Dask client and cluster when exiting context"""
        if hasattr(self, 'client') and self.client:
            self.client.close()
        
        if hasattr(self, 'cluster') and self.cluster:
            self.cluster.close()
        
        # Call parent's exit method to clean up CrossLink
        super().__exit__(exc_type, exc_val, exc_tb) 