"""
Arrow integration module for CrossLink
"""
import os
import warnings
import json
import uuid
import hashlib
from datetime import datetime

# Import from shared_memory module
from ..shared_memory.shared_memory import setup_shared_memory, get_from_shared_memory

# Global cache for memory-mapped Arrow tables
_mmapped_tables = {}

def share_arrow_table(cl, table, name=None, description=None, shared_memory=True, 
                     memory_mapped=True, register_in_duckdb=True,
                     access_languages=None):
    """Share an Arrow table directly without converting to pandas or other intermediate formats
    
    This is the most efficient way to share data between languages as it avoids any conversion.
    
    Args:
        cl: CrossLink instance
        table: PyArrow Table to share
        name: Optional name for the dataset (auto-generated if None)
        description: Optional description
        shared_memory: Use shared memory for ultra-fast cross-process access
        memory_mapped: Use memory-mapped files for disk-based sharing
        register_in_duckdb: Register the Arrow data for SQL access in DuckDB
        access_languages: Languages that can access this dataset
        
    Returns:
        dataset_id: ID of the shared dataset
    """
    # Lazy import to improve startup time
    import pyarrow as pa
    
    if not isinstance(table, pa.Table):
        raise TypeError("Must provide a PyArrow Table object")
        
    # Ensure Arrow is initialized
    cl._ensure_arrow_extension()
    
    # Generate ID and name
    dataset_id = str(uuid.uuid4())
    if name is None:
        name = f"arrow_direct_{dataset_id[:8]}"
        
    # Initialize storage methods
    memory_map_path = None
    shared_memory_info = None
    table_name = None
    
    # Set up memory-mapped file if requested
    if memory_mapped:
        try:
            # Create directory for memory-mapped files
            mmaps_dir = os.path.join(os.path.dirname(cl.db_path), "crosslink_mmaps")
            os.makedirs(mmaps_dir, exist_ok=True)
            
            # Create file path
            memory_map_path = os.path.join(mmaps_dir, f"{dataset_id}.arrow")
            
            # Write Arrow table directly to file
            with pa.OSFile(memory_map_path, 'wb') as f:
                pa.ipc.write_file(table, f)
            
            # Add to tracking for cleanup
            cl._arrow_files.add(memory_map_path)
            
            cl._log(f"Wrote Arrow table to {memory_map_path}")
        except Exception as e:
            cl._log(f"Failed to create memory-mapped file: {e}", level="warning")
            memory_map_path = None
    
    # Set up shared memory if requested
    if shared_memory:
        shared_memory_info = setup_shared_memory(cl, table, dataset_id)
    
    # Register in DuckDB if requested
    if register_in_duckdb and cl.conn is not None:
        try:
            # Create a DuckDB table from the Arrow data
            table_name = f"arrow_{dataset_id.replace('-', '_')}"
            
            # If we have a memory mapped file, register it through the Arrow scanner
            if memory_map_path:
                cl.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS 
                SELECT * FROM read_parquet('{memory_map_path}')
                """)
            else:
                # Register the Arrow table directly with DuckDB
                cl.conn.register(f"__temp_arrow_{dataset_id}", table)
                cl.conn.execute(f"""
                CREATE OR REPLACE VIEW {table_name} AS 
                SELECT * FROM __temp_arrow_{dataset_id}
                """)
                # Cleanup temp registration
                cl.conn.unregister(f"__temp_arrow_{dataset_id}")
        except Exception as e:
            cl._log(f"Failed to register Arrow table in DuckDB: {e}", level="warning")
            table_name = None
            
    # Create schema dictionary
    schema_dict = {
        "columns": table.column_names,
        "dtypes": {col: str(table.schema.field(col).type) for col in table.column_names}
    }
    
    # Serialize Arrow schema information
    arrow_schema = {
        "schema": str(table.schema),
        "serialized": table.schema.serialize().hex(),
        "metadata": {k.decode(): v.decode() for k, v in table.schema.metadata.items()} if table.schema.metadata else {}
    }
    
    # Calculate schema hash
    schema_hash = hashlib.md5(json.dumps(schema_dict, sort_keys=True).encode()).hexdigest()
    
    # Set default access languages if not provided
    if access_languages is None:
        access_languages = ["python", "r", "julia", "cpp"]
    
    # Store metadata
    cl.metadata_manager.create_dataset_metadata(
        dataset_id=dataset_id,
        name=name,
        table_name=table_name,
        source_language="python",
        schema=schema_dict,
        description=description or "Direct Arrow table",
        arrow_data=True,
        memory_map_path=memory_map_path,
        shared_memory_key=shared_memory_info["shared_memory_key"] if shared_memory_info else None,
        arrow_schema=arrow_schema,
        access_languages=access_languages
    )
    
    # Cache metadata for future lookups
    cl._metadata_cache[dataset_id] = {
        "id": dataset_id,
        "name": name,
        "table_name": table_name,
        "schema": schema_dict,
        "source_language": "python",
        "arrow_data": True,
        "memory_map_path": memory_map_path,
        "shared_memory_key": shared_memory_info["shared_memory_key"] if shared_memory_info else None,
        "arrow_schema": arrow_schema
    }
    cl._metadata_cache[name] = cl._metadata_cache[dataset_id]
    
    # Log access
    cl.metadata_manager.log_access(
        dataset_id=dataset_id,
        language="python",
        operation="write",
        access_method="direct_arrow",
        success=True
    )
    
    return dataset_id

def get_arrow_table(cl, identifier):
    """Get a PyArrow table directly, using the most efficient method available
    
    This method prioritizes:
    1. Shared memory (fastest, if available)
    2. Memory-mapped file (fast file-based access)
    3. DuckDB query with Arrow results (fallback)
    
    Args:
        cl: CrossLink instance
        identifier: Dataset ID or name
        
    Returns:
        PyArrow Table
    """
    # Lazy import to improve startup time
    import pyarrow as pa
    
    # Get metadata
    metadata = cl._metadata_cache.get(identifier)
    if not metadata:
        metadata = cl.metadata_manager.get_dataset_metadata(identifier)
        if not metadata:
            raise ValueError(f"Dataset {identifier} not found")
            
    # Try to access shared memory if available
    shared_memory_key = metadata.get("shared_memory_key")
    if shared_memory_key:
        try:
            # Get Arrow table from shared memory
            arrow_table = get_from_shared_memory(cl, shared_memory_key, metadata)
            if arrow_table is not None:
                # Log access
                cl.metadata_manager.log_access(
                    dataset_id=metadata['id'],
                    language="python",
                    operation="read",
                    access_method="shared_memory",
                    success=True
                )
                return arrow_table
        except Exception as e:
            cl._log(f"Failed to read from shared memory: {e}", level="warning")
            # Fall through to next method
            
    # Try memory-mapped file if available
    memory_map_path = metadata.get("memory_map_path")
    if memory_map_path and os.path.exists(memory_map_path):
        try:
            # Check if we already have this table in our cache
            arrow_table = _mmapped_tables.get(memory_map_path)
            if arrow_table is None:
                # Read from file
                arrow_table = pa.ipc.read_table(memory_map_path)
                # Cache for future use
                _mmapped_tables[memory_map_path] = arrow_table
            
            # Log access
            cl.metadata_manager.log_access(
                dataset_id=metadata['id'],
                language="python",
                operation="read",
                access_method="memory_mapped",
                success=True
            )
            
            return arrow_table
        except Exception as e:
            cl._log(f"Failed to read from memory-mapped file: {e}", level="warning")
            # Fall through to next method
    
    # Try through DuckDB with Arrow results
    table_name = metadata.get("table_name")
    if table_name and cl.conn is not None:
        try:
            # Ensure Arrow extension is loaded
            cl._ensure_arrow_extension()
            
            # Execute query with Arrow results
            arrow_table = cl.conn.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
            
            # Log access
            cl.metadata_manager.log_access(
                dataset_id=metadata['id'],
                language="python",
                operation="read",
                access_method="duckdb_arrow",
                success=True
            )
            
            return arrow_table
        except Exception as e:
            cl._log(f"Failed to fetch Arrow results from DuckDB: {e}", level="warning")
            # Fall through to error
    
    # If all methods failed
    raise RuntimeError(f"Could not retrieve Arrow data for {identifier} using any available method")

def create_duckdb_view_from_arrow(cl, table, view_name=None):
    """Create a SQL view in DuckDB from a PyArrow table
    
    This allows you to query the Arrow table using SQL without copying the data.
    
    Args:
        cl: CrossLink instance
        table: PyArrow table to register as a view
        view_name: Name for the SQL view (auto-generated if None)
        
    Returns:
        view_name: Name of the created SQL view
    """
    # Lazy import to improve startup time
    import pyarrow as pa
    
    if cl.conn is None:
        raise RuntimeError("Database connection is not available")
        
    if not isinstance(table, pa.Table):
        raise TypeError("Must provide a PyArrow Table object")
    
    # Generate a view name if not provided
    if view_name is None:
        view_id = str(uuid.uuid4()).replace('-', '_')
        view_name = f"arrow_view_{view_id}"
    
    # Register the table with DuckDB
    try:
        # First try direct registration
        cl.conn.register(f"__temp_{view_name}", table)
        cl.conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM __temp_{view_name}")
        
        # Clean up the temporary registration
        try:
            cl.conn.unregister(f"__temp_{view_name}")
        except:
            pass
    except Exception as e:
        cl._log(f"Failed to create view via direct registration: {e}", level="warning")
        
        # Try alternative approach with memory-mapped file
        try:
            # Create a temporary file
            import tempfile
            temp_file = tempfile.NamedTemporaryFile(suffix='.arrow', delete=False).name
            
            # Write Arrow data to temp file
            with pa.OSFile(temp_file, 'wb') as f:
                pa.ipc.write_file(table, f)
                
            # Create view from the file
            cl.conn.execute(f"""
            CREATE OR REPLACE VIEW {view_name} AS 
            SELECT * FROM read_parquet('{temp_file}')
            """)
            
            # Add to cleanup list to ensure temp file gets removed later
            cl._arrow_files.add(temp_file)
        except Exception as e2:
            # Both methods failed
            raise RuntimeError(f"Failed to create view: {e2}") from e
    
    return view_name 