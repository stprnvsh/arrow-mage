"""
Data operations module for CrossLink
"""
import os
import pandas as pd
import tempfile
import warnings
import json
import hashlib
import uuid
import time
from datetime import datetime

# Import from other modules
from ..shared_memory.shared_memory import setup_shared_memory, get_from_shared_memory
from ..arrow_integration.arrow_integration import share_arrow_table, get_arrow_table

def push(cl, data, name=None, description=None, zero_copy=True, use_arrow=True,
        shared_memory=True, memory_mapped=True,
        access_languages=None):
    """Push data to CrossLink with zero-copy optimization
    
    Args:
        cl: CrossLink instance
        data: Data to push (DataFrame, Arrow Table, etc.)
        name: Optional name for the dataset
        description: Optional description
        zero_copy: Enable zero-copy optimization
        use_arrow: Use Arrow for data transfer
        shared_memory: Use shared memory for zero-copy
        memory_mapped: Use memory-mapped files for zero-copy
        access_languages: Languages that can access this data
        
    Returns:
        Dataset ID
    """
    # Default access languages
    if access_languages is None:
        access_languages = ["python", "r", "julia", "cpp"]
    
    # Use C++ implementation if available
    if hasattr(cl, '_cpp_instance') and cl._cpp_instance is not None:
        # Convert to Arrow table if needed
        if hasattr(data, "to_arrow"):
            data = data.to_arrow()
        elif hasattr(data, "__array__"):
            import pyarrow as pa
            data = pa.Table.from_pandas(data)
        
        # Push using C++ implementation
        return cl._cpp_instance.push(data, name or "", description or "")
    
    # Python implementation
    from ..shared_memory.shared_memory import _shared_memory_available
    
    # Generate a name if not provided
    if name is None:
        import uuid
        name = f"dataset_{uuid.uuid4().hex[:8]}"
    
    # Determine appropriate method based on the type of data
    if hasattr(data, "to_arrow"):
        # Convert to Arrow table
        table = data.to_arrow()
        
        # Share Arrow table with zero-copy if enabled
        if zero_copy and _shared_memory_available and cl.arrow_available:
            from ..arrow_integration.arrow_integration import share_arrow_table
            return share_arrow_table(cl, table, name, description, 
                                   shared_memory=shared_memory, 
                                   memory_mapped=memory_mapped,
                                   access_languages=access_languages)
    
    # Get metadata manager
    if not hasattr(cl, 'metadata_manager') or cl.metadata_manager is None:
        from ..metadata.metadata import CrossLinkMetadataManager
        cl.metadata_manager = CrossLinkMetadataManager(cl.conn)
    
    # Use SQL to create a table from the data
    import uuid
    dataset_id = str(uuid.uuid4())
    
    # Create a table name
    table_name = f"data_{name.lower().replace(' ', '_')}"
    
    # Register the data with DuckDB
    if hasattr(cl.conn, "register"):
        cl.conn.register(table_name, data)
        
        # Create a permanent table
        cl.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM {table_name}")
        
        # Unregister the view
        if hasattr(cl.conn, "unregister"):
            cl.conn.unregister(table_name)
    else:
        # Direct parameter execution
        cl.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM data", {"data": data})
    
    # Get schema information
    schema = {}
    if hasattr(data, "dtypes"):
        schema = {
            "columns": list(data.columns),
            "dtypes": {col: str(dtype) for col, dtype in zip(data.columns, data.dtypes)}
        }
    
    # Add metadata
    import json
    cl.metadata_manager.create_dataset_metadata(
        dataset_id=dataset_id,
        name=name,
        source_language="python",
        table_name=table_name,
        schema=json.dumps(schema),
        description=description or "",
        access_languages=access_languages
    )
    
    return dataset_id

def pull(cl, identifier, to_pandas=True, zero_copy=True):
    """Pull data from CrossLink with zero-copy optimization
    
    Args:
        cl: CrossLink instance
        identifier: Dataset ID or name
        to_pandas: Convert to pandas DataFrame
        zero_copy: Use zero-copy optimization
        
    Returns:
        Data (DataFrame or Arrow Table depending on to_pandas)
    """
    # Use C++ implementation if available
    if hasattr(cl, '_cpp_instance') and cl._cpp_instance is not None:
        # Pull using C++ implementation - always returns Arrow table
        arrow_table = cl._cpp_instance.pull(identifier)
        
        # Convert to pandas if requested
        if to_pandas:
            return arrow_table.to_pandas()
        return arrow_table
    
    # Python implementation
    # Get metadata manager
    if not hasattr(cl, 'metadata_manager') or cl.metadata_manager is None:
        from ..metadata.metadata import CrossLinkMetadataManager
        cl.metadata_manager = CrossLinkMetadataManager(cl.conn)
    
    # Get the metadata
    metadata = cl.metadata_manager.get_dataset(identifier)
    if metadata is None:
        raise ValueError(f"Dataset {identifier} not found")
    
    # Check if the dataset has an Arrow table available
    if zero_copy and metadata.get("memory_map_path") and cl.arrow_available:
        # Get Arrow table via memory mapped file
        from ..arrow_integration.arrow_integration import get_arrow_table
        table = get_arrow_table(cl, identifier)
        
        if to_pandas:
            return table.to_pandas()
        return table
    
    # Check if the dataset has shared memory available
    if zero_copy and metadata.get("shared_memory_key"):
        # Try to get from shared memory
        from ..shared_memory.shared_memory import get_from_shared_memory
        table = get_from_shared_memory(cl, metadata["shared_memory_key"], metadata)
        if table is not None:
            if to_pandas:
                return table.to_pandas()
            return table
    
    # Fall back to SQL query
    result = cl.conn.execute(f"SELECT * FROM {metadata['table_name']}")
    if cl.arrow_available and hasattr(result, "fetch_arrow_table"):
        table = result.fetch_arrow_table()
        if to_pandas:
            return table.to_pandas()
        return table
    
    # If all else fails, return pandas DataFrame
    if hasattr(result, "df"):
        return result.df()
    elif hasattr(result, "fetchdf"):
        return result.fetchdf()
    else:
        # Most basic fallback
        return result.fetchall()

def get_table_reference(cl, identifier):
    """Get a reference to a table without copying the data
    
    Args:
        cl: CrossLink instance
        identifier: Dataset ID or name
        
    Returns:
        Dictionary with reference information
    """
    # Get metadata manager
    if not hasattr(cl, 'metadata_manager') or cl.metadata_manager is None:
        from ..metadata.metadata import CrossLinkMetadataManager
        cl.metadata_manager = CrossLinkMetadataManager(cl.conn)
    
    # Get metadata
    metadata = cl.metadata_manager.get_dataset_metadata(identifier)
    if metadata is None:
        raise ValueError(f"Dataset {identifier} not found")
    
    # Return reference information
    return {
        "database_path": cl.db_path,
        "table_name": metadata["table_name"],
        "dataset_id": metadata["id"]
    }

def register_external_table(cl, table_name, data, name=None, description=None):
    """Register an external table with CrossLink
    
    Args:
        cl: CrossLink instance
        table_name: Name for the table
        data: Data to register (can be None if table already exists in DuckDB)
        name: Optional display name for the dataset
        description: Optional description for the dataset
        
    Returns:
        Dataset ID if successful
    """
    # Register the table with DuckDB only if data is provided
    if data is not None:
        if hasattr(cl.conn, "register"):
            cl.conn.register(table_name, data)
        else:
            cl.conn.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM data", {"data": data})
    
    # Create metadata if provided
    if name is not None:
        # Get metadata manager
        if not hasattr(cl, 'metadata_manager') or cl.metadata_manager is None:
            from ..metadata.metadata import CrossLinkMetadataManager
            cl.metadata_manager = CrossLinkMetadataManager(cl.conn)
        
        # Create a unique ID for the dataset
        import uuid
        dataset_id = str(uuid.uuid4())
        
        # Get schema from data if available
        if data is not None and hasattr(data, "dtypes"):
            # Pandas DataFrame
            schema = dict(data.dtypes.astype(str))
        else:
            # Generic schema
            schema = {"columns": "unknown"}
        
        # Create metadata
        cl.metadata_manager.create_dataset_metadata(
            dataset_id=dataset_id,
            name=name,
            table_name=table_name,
            source_language="python",
            schema=schema,
            description=description
        )
        
        return dataset_id
    
    return True

def list_datasets(cl):
    """List all datasets in CrossLink
    
    Args:
        cl: CrossLink instance
        
    Returns:
        List of datasets with metadata
    """
    # Use C++ implementation if available
    if hasattr(cl, '_cpp_instance') and cl._cpp_instance is not None:
        # List datasets using C++ implementation
        return cl._cpp_instance.list_datasets()
    
    # Python implementation
    # Get metadata manager
    if not hasattr(cl, 'metadata_manager') or cl.metadata_manager is None:
        from ..metadata.metadata import CrossLinkMetadataManager
        cl.metadata_manager = CrossLinkMetadataManager(cl.conn)
    
    # Get all datasets
    return cl.metadata_manager.list_datasets()

def query(cl, sql):
    """Execute a SQL query on the CrossLink database
    
    Args:
        cl: CrossLink instance
        sql: SQL query
        
    Returns:
        Query results as pandas DataFrame or Arrow Table
    """
    # Use C++ implementation if available
    if hasattr(cl, '_cpp_instance') and cl._cpp_instance is not None:
        # Execute query using C++ implementation
        return cl._cpp_instance.query(sql)
    
    # Python implementation
    result = cl.conn.execute(sql)
    
    # Return as DataFrame or Arrow Table based on what's available
    if cl.arrow_available and hasattr(result, "fetch_arrow_table"):
        return result.fetch_arrow_table()
    elif hasattr(result, "df"):
        return result.df()
    elif hasattr(result, "fetchdf"):
        return result.fetchdf()
    else:
        return result.fetchall() 