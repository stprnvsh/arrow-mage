"""
Dataset registry for DuckConnect

This module provides functions for registering, retrieving, updating, and deleting
datasets in DuckConnect.
"""

import os
import time
import uuid
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Iterator

import pandas as pd
import pyarrow as pa
import duckdb

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.datasets.registry')

def register_dataset(connection_pool, metadata_manager, transaction_manager,
                   data, name, description=None, source_language="python",
                   available_to_languages=None, overwrite=False, partition_by=None,
                   partition_values=None, streaming=False, chunk_size=100000,
                   owner=None, is_encrypted=False, access_control=None) -> str:
    """
    Register a dataset with DuckConnect.
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        transaction_manager: Transaction manager
        data: DataFrame, Arrow Table, or SQL query
        name: Dataset name
        description: Optional description
        source_language: Source language
        available_to_languages: Languages that can access this dataset
        overwrite: Whether to overwrite an existing dataset
        partition_by: Column to partition by
        partition_values: Partition values
        streaming: Whether to process in streaming mode
        chunk_size: Chunk size for streaming
        owner: Dataset owner
        is_encrypted: Whether the dataset is encrypted
        access_control: Access control rules
        
    Returns:
        Dataset ID
    """
    start_time = time.time()
    
    # Use a dedicated connection from the pool
    with connection_pool.get_connection() as conn:
        # Check if dataset with this name already exists
        existing = conn.execute(
            "SELECT id, table_name FROM duck_connect_metadata WHERE name = ?", 
            [name]
        ).fetchone()
        
        if existing and not overwrite:
            raise ValueError(f"Dataset with name '{name}' already exists. Use overwrite=True to replace it.")
        
        # Generate a unique ID for this dataset
        dataset_id = str(uuid.uuid4())
        
        # Set default available languages if not specified
        if available_to_languages is None:
            available_to_languages = ["python", "r", "julia"]
        
        # Generate unique table name
        table_name = f"duck_connect_{dataset_id.replace('-', '_')}"
        
        # Set default owner if not specified
        if owner is None:
            owner = f"{source_language}-{os.getpid()}"
            
        # Handle partitioning
        partition_info = None
        if partition_by:
            partition_info = {
                "partition_column": partition_by,
                "partitions": []
            }
                
            # If we have a DataFrame or Arrow Table with a partition column, 
            # we'll create multiple partitioned tables
            if isinstance(data, (pd.DataFrame, pa.Table)) and partition_by in (
                data.columns if isinstance(data, pd.DataFrame) else data.column_names
            ):
                # Create the main table without data first
                if isinstance(data, pd.DataFrame):
                    # Create an empty table with the same schema
                    sample_data = data.head(0)
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM sample_data", {
                        "sample_data": sample_data
                    })
                else:  # Arrow Table
                    # Create an empty table with the same schema
                    sample_data = data.slice(0, 0)
                    conn.register("arrow_sample", sample_data)
                    conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_sample")
                    conn.unregister("arrow_sample")
                    
                # Now insert data in partitions
                unique_values = partition_values if partition_values else (
                    data[partition_by].unique() if isinstance(data, pd.DataFrame) 
                    else pa.compute.unique(data[partition_by]).to_pylist()
                )
                    
                partition_info["partitions"] = []
                    
                # Process each partition
                for value in unique_values:
                    # Create partition id
                    partition_id = str(uuid.uuid4())
                    partition_table = f"{table_name}_part_{str(value).replace(' ', '_')}"
                        
                    # Filter data for this partition
                    if isinstance(data, pd.DataFrame):
                        partition_data = data[data[partition_by] == value]
                            
                        # Store partition in DuckDB
                        if streaming and len(partition_data) > chunk_size:
                            # Process in chunks for large datasets
                            conn.execute(f"CREATE TABLE {partition_table} AS SELECT * FROM partition_data LIMIT 0", {
                                "partition_data": partition_data.head(0)
                            })
                                
                            for chunk_start in range(0, len(partition_data), chunk_size):
                                chunk = partition_data.iloc[chunk_start:chunk_start + chunk_size]
                                conn.execute(f"INSERT INTO {partition_table} SELECT * FROM chunk", {
                                    "chunk": chunk
                                })
                        else:
                            # Small dataset, load at once
                            conn.execute(f"CREATE TABLE {partition_table} AS SELECT * FROM partition_data", {
                                "partition_data": partition_data
                            })
                    else:  # Arrow Table
                        mask = pa.compute.equal(data[partition_by], value)
                        partition_data = data.filter(mask)
                            
                        # Store partition in DuckDB
                        conn.register("arrow_partition", partition_data)
                        conn.execute(f"CREATE TABLE {partition_table} AS SELECT * FROM arrow_partition")
                        conn.unregister("arrow_partition")
                        
                    # Add partition info
                    partition_info["partitions"].append({
                        "partition_id": partition_id,
                        "partition_value": str(value),
                        "table_name": partition_table,
                        "row_count": len(partition_data) if isinstance(partition_data, pd.DataFrame) else partition_data.num_rows
                    })
                        
                    # Record partition metadata
                    conn.execute("""
                    INSERT INTO duck_connect_partitions (
                        id, dataset_id, partition_key, partition_value, table_name, row_count, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                    """, [
                        partition_id, dataset_id, partition_by, str(value), 
                        partition_table, len(partition_data) if isinstance(partition_data, pd.DataFrame) else partition_data.num_rows
                    ])
                        
                    # Create view on main table
                    conn.execute(f"""
                    INSERT INTO {table_name} SELECT * FROM {partition_table}
                    """)
                
                # Create schema info
                if isinstance(data, pd.DataFrame):
                    schema = {
                        'columns': list(data.columns),
                        'dtypes': {col: str(data[col].dtype) for col in data.columns},
                        'source_type': 'pandas_dataframe',
                        'shape': data.shape,
                        'partitioned': True,
                        'partition_column': partition_by
                    }
                else:  # Arrow Table
                    schema = {
                        'columns': data.column_names,
                        'dtypes': {col: str(data.schema.field(col).type) for col in data.column_names},
                        'source_type': 'arrow_table',
                        'shape': (data.num_rows, data.num_columns),
                        'partitioned': True,
                        'partition_column': partition_by
                    }
            else:
                # Process based on input type if no partitioning is done
                _process_data_input(conn, data, table_name, streaming, chunk_size)
                schema = _get_schema_from_data(conn, data, table_name)
        else:
            # Process based on input type if no partitioning is done
            _process_data_input(conn, data, table_name, streaming, chunk_size)
            schema = _get_schema_from_data(conn, data, table_name)
                
        # Record the metadata
        metadata_manager.create_dataset_metadata(
            dataset_id, name, table_name, source_language, schema,
            description=description,
            available_to_languages=available_to_languages,
            owner=owner,
            is_encrypted=is_encrypted,
            access_control=access_control,
            partition_info=partition_info
        )
            
        # Record transaction
        metadata_manager.record_transaction(
            dataset_id, 
            'create', 
            source_language, 
            details={'name': name, 'partitioned': partition_by is not None}
        )
            
        # Record performance stats
        end_time = time.time()
        duration_ms = (end_time - start_time) * 1000
            
        # Get row and column counts
        row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        column_count = len(schema['columns'])
        
        # Estimate memory usage (rough calculation)
        memory_usage_mb = (row_count * column_count * 8) / (1024 * 1024)  # Assuming 8 bytes per cell average
            
        stats_id = str(uuid.uuid4())
        conn.execute("""
        INSERT INTO duck_connect_stats (
            id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count
        ) VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?)
        """, [
            stats_id, dataset_id, 'create', source_language, 
            duration_ms, memory_usage_mb, row_count, column_count
        ])
            
        return dataset_id

def _process_data_input(conn, data, table_name, streaming=False, chunk_size=100000):
    """Helper method to process different data input types"""
    if isinstance(data, str) and data.upper().startswith("SELECT "):
        # It's a SQL query
        try:
            # Create table from query
            conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS {data}")
        except Exception as e:
            raise ValueError(f"Failed to create table from SQL query: {e}")
                
    elif isinstance(data, pd.DataFrame):
        # It's a pandas DataFrame
        try:
            if streaming and len(data) > chunk_size:
                # Process in chunks for large datasets
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data LIMIT 0", {
                    "data": data.head(0)
                })
                    
                for chunk_start in range(0, len(data), chunk_size):
                    chunk = data.iloc[chunk_start:chunk_start + chunk_size]
                    conn.execute(f"INSERT INTO {table_name} SELECT * FROM chunk", {
                        "chunk": chunk
                    })
            else:
                # Store in DuckDB directly
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM data", {
                    "data": data
                })
        except Exception as e:
            raise ValueError(f"Failed to store DataFrame in DuckDB: {e}")
                
    elif isinstance(data, pa.Table):
        # It's an Arrow Table
        try:
            if streaming and data.num_rows > chunk_size:
                # Process in chunks for large Arrow tables
                sample = data.slice(0, 0)
                conn.register("arrow_sample", sample)
                conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_sample")
                conn.unregister("arrow_sample")
                    
                for chunk_start in range(0, data.num_rows, chunk_size):
                    chunk = data.slice(chunk_start, min(chunk_size, data.num_rows - chunk_start))
                    conn.register("arrow_chunk", chunk)
                    conn.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_chunk")
                    conn.unregister("arrow_chunk")
            else:
                # Register table with DuckDB in one go
                conn.register("arrow_data", data)
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM arrow_data")
                conn.unregister("arrow_data")
        except Exception as e:
            raise ValueError(f"Failed to store Arrow Table in DuckDB: {e}")
                
    elif isinstance(data, str) and data.startswith("duck_table:"):
        # It's a reference to an existing DuckDB table
        ref_table = data.replace("duck_table:", "")
            
        # Verify the table exists
        table_exists = conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            [ref_table]
        ).fetchone()[0]
            
        if not table_exists:
            raise ValueError(f"Referenced table '{ref_table}' does not exist in DuckDB")
                
        # For references, create a view instead of a new table
        conn.execute(f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM {ref_table}")
    else:
        raise ValueError(f"Unsupported data type: {type(data)}")
    
def _get_schema_from_data(conn, data, table_name):
    """Helper method to get schema info from different data types"""
    if isinstance(data, str) and data.upper().startswith("SELECT "):
        # Get schema from created table
        schema_df = conn.execute(f"SELECT * FROM {table_name} LIMIT 0").fetchdf()
        return {
            'columns': list(schema_df.columns),
            'dtypes': {col: str(schema_df[col].dtype) for col in schema_df.columns},
            'source_type': 'sql_query'
        }
                
    elif isinstance(data, pd.DataFrame):
        return {
            'columns': list(data.columns),
            'dtypes': {col: str(data[col].dtype) for col in data.columns},
            'source_type': 'pandas_dataframe',
            'shape': data.shape
        }
                
    elif isinstance(data, pa.Table):
        return {
            'columns': data.column_names,
            'dtypes': {col: str(data.schema.field(col).type) for col in data.column_names},
            'source_type': 'arrow_table',
            'shape': (data.num_rows, data.num_columns)
        }
                
    elif isinstance(data, str) and data.startswith("duck_table:"):
        # Get schema from the table
        ref_table = data.replace("duck_table:", "")
        schema_df = conn.execute(f"SELECT * FROM {ref_table} LIMIT 0").fetchdf()
        return {
            'columns': list(schema_df.columns),
            'dtypes': {col: str(schema_df[col].dtype) for col in schema_df.columns},
            'source_type': 'duck_table_reference'
        }
    
def get_dataset(connection_pool, metadata_manager, identifier, language="python",
              as_arrow=False, columns=None, filters=None, limit=None,
              streaming=False, chunk_size=100000) -> Union[pd.DataFrame, pa.Table, None, Iterator]:
    """
    Get a dataset from DuckConnect.
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        identifier: Dataset ID or name
        language: Language requesting the dataset
        as_arrow: Whether to return as Arrow Table
        columns: Columns to select
        filters: Filters to apply
        limit: Maximum number of rows
        streaming: Whether to stream the data
        chunk_size: Chunk size for streaming
        
    Returns:
        Dataset as DataFrame, Arrow Table, or generator of chunks
    """
    # Use a dedicated connection from the pool
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        metadata = metadata_manager.get_dataset_metadata(identifier)
        
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id = metadata['id']
        name = metadata['name']
        table_name = metadata['table_name']
        avail_langs = metadata.get('available_to_languages', [])
        
        # Check language access
        if language not in avail_langs:
            raise ValueError(f"Dataset '{name}' is not available to language '{language}'")
                
        # Record transaction
        metadata_manager.record_transaction(
            dataset_id, 'read', language, 
            details={
                'streaming': streaming, 
                'as_arrow': as_arrow,
                'filtered': filters is not None
            }
        )
                
        # Build SQL query
        select_clause = "*" if columns is None else ", ".join(columns)
        query = f"SELECT {select_clause} FROM {table_name}"
                
        # Apply filters if specified
        if filters:
            where_clauses = []
            params = []
            for col, value in filters.items():
                if isinstance(value, (list, tuple)):
                    placeholders = ", ".join(["?" for _ in value])
                    where_clauses.append(f"{col} IN ({placeholders})")
                    params.extend(value)
                else:
                    where_clauses.append(f"{col} = ?")
                    params.append(value)
            
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
                        
        # Apply limit if specified
        if limit is not None:
            query += f" LIMIT {limit}"
                
        # Check for partitioning
        partition_info = metadata.get('partition_info')
        if partition_info:
            # Optimize query for partitions if needed
            # For example, if filter matches a specific partition, we can query that partition directly
            if filters and partition_info.get('partition_column') in filters:
                partition_value = filters[partition_info['partition_column']]
                # Find matching partition
                for partition in partition_info.get('partitions', []):
                    if str(partition_value) == str(partition.get('partition_value')):
                        # Use partition table directly for better performance
                        query = query.replace(f"FROM {table_name}", f"FROM {partition['table_name']}")
                        break
                
        # Execute query in streaming or regular mode
        if streaming:
            # Create a generator to stream data in chunks
            def stream_data():
                # Get total row count
                count_query = f"SELECT COUNT(*) FROM ({query}) as q"
                total_rows = conn.execute(count_query, params if filters else []).fetchone()[0]
                        
                # Stream in chunks
                for offset in range(0, total_rows, chunk_size):
                    paginated_query = f"{query} OFFSET {offset} LIMIT {chunk_size}"
                            
                    if as_arrow:
                        chunk = conn.execute(paginated_query, params if filters else []).fetch_arrow_table()
                    else:
                        chunk = conn.execute(paginated_query, params if filters else []).fetchdf()
                                
                    yield chunk
                    
            return stream_data()
        else:
            # Regular mode, return all at once
            if as_arrow:
                return conn.execute(query, params if filters else []).fetch_arrow_table()
            else:
                return conn.execute(query, params if filters else []).fetchdf()
    
def update_dataset(connection_pool, metadata_manager, transaction_manager,
                 identifier, data, language="python", description=None) -> str:
    """
    Update a dataset in DuckConnect.
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        transaction_manager: Transaction manager
        identifier: Dataset ID or name
        data: New data
        language: Language performing the update
        description: Optional updated description
        
    Returns:
        Dataset ID
    """
    start_time = time.time()
    
    # Use transaction for atomicity
    with transaction_manager.transaction():
        # Use a dedicated connection from the pool
        with connection_pool.get_connection() as conn:
            # Get dataset metadata
            metadata = metadata_manager.get_dataset_metadata(identifier)
            
            if not metadata:
                raise ValueError(f"Dataset '{identifier}' not found")
                
            dataset_id = metadata['id']
            table_name = metadata['table_name']
            is_partitioned = 'partition_info' in metadata and metadata['partition_info'] is not None
            
            # Check if partitioned update is needed
            if is_partitioned:
                # For partitioned datasets, we need special handling
                # This is a simplified approach - in a real system, you'd
                # handle partition-aware updates more efficiently
                
                # First, drop all existing data
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Delete all partitions
                partition_tables = conn.execute(
                    "SELECT table_name FROM duck_connect_partitions WHERE dataset_id = ?",
                    [dataset_id]
                ).fetchall()
                
                for table in partition_tables:
                    conn.execute(f"DROP TABLE IF EXISTS {table[0]}")
                
                # Remove partition metadata
                conn.execute(
                    "DELETE FROM duck_connect_partitions WHERE dataset_id = ?",
                    [dataset_id]
                )
                
                # Re-register with same name and ID to create new partitions
                register_dataset(
                    connection_pool, 
                    metadata_manager,
                    transaction_manager,
                    data=data,
                    name=metadata['name'],
                    description=description or metadata['description'],
                    source_language=language,
                    available_to_languages=metadata.get('available_to_languages'),
                    overwrite=True,
                    partition_by=metadata['partition_info'].get('partition_column'),
                    owner=metadata.get('owner'),
                    is_encrypted=metadata.get('is_encrypted', False),
                    access_control=metadata.get('access_control')
                )
            else:
                # For non-partitioned datasets, simple replace the data
                
                # Drop existing table
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Process new data
                _process_data_input(conn, data, table_name)
                
                # Update schema
                schema = _get_schema_from_data(conn, data, table_name)
                
                # Update metadata
                updates = {
                    'schema': schema
                }
                
                if description is not None:
                    updates['description'] = description
                    
                metadata_manager.update_dataset_metadata(dataset_id, **updates)
            
            # Record transaction
            metadata_manager.record_transaction(
                dataset_id, 'update', language, 
                details={'is_partitioned': is_partitioned}
            )
                
            # Record performance stats
            end_time = time.time()
            duration_ms = (end_time - start_time) * 1000
                
            # Get row and column counts
            row_count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            schema = _get_schema_from_data(conn, data, table_name)
            column_count = len(schema['columns'])
            
            # Estimate memory usage (rough calculation)
            memory_usage_mb = (row_count * column_count * 8) / (1024 * 1024)  # Assuming 8 bytes per cell average
                
            stats_id = str(uuid.uuid4())
            conn.execute("""
            INSERT INTO duck_connect_stats (
                id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count
            ) VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?)
            """, [
                stats_id, dataset_id, 'update', language, 
                duration_ms, memory_usage_mb, row_count, column_count
            ])
                
            return dataset_id
    
def delete_dataset(connection_pool, metadata_manager, identifier, language="python") -> bool:
    """
    Delete a dataset from DuckConnect.
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        identifier: Dataset ID or name
        language: Language performing the deletion
        
    Returns:
        True if successful
    """
    # Use a dedicated connection from the pool
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        metadata = metadata_manager.get_dataset_metadata(identifier)
        
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id = metadata['id']
        table_name = metadata['table_name']
        
        # Check if partitioned
        is_partitioned = 'partition_info' in metadata and metadata['partition_info'] is not None
        
        # Delete table
        conn.execute(f"DROP TABLE IF EXISTS {table_name}")
        
        # Delete partitions if needed
        if is_partitioned:
            partition_tables = conn.execute(
                "SELECT table_name FROM duck_connect_partitions WHERE dataset_id = ?",
                [dataset_id]
            ).fetchall()
            
            for table in partition_tables:
                conn.execute(f"DROP TABLE IF EXISTS {table[0]}")
            
            # Remove partition metadata
            conn.execute(
                "DELETE FROM duck_connect_partitions WHERE dataset_id = ?",
                [dataset_id]
            )
        
        # Record transaction before deleting metadata
        metadata_manager.record_transaction(
            dataset_id, 'delete', language, 
            details={'name': metadata.get('name'), 'is_partitioned': is_partitioned}
        )
        
        # Delete metadata
        metadata_manager.delete_dataset_metadata(dataset_id)
        
        return True 