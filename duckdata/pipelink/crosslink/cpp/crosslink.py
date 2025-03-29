"""
CrossLink C++ Core Interface for Python

This module provides a pythonic interface to the C++ core implementation of CrossLink.
"""

import os
import sys
import tempfile
import uuid
import json
import pyarrow as pa
import numpy as np
from pathlib import Path
import warnings

try:
    from .crosslink_cpp import CrossLinkCpp
    _CPP_AVAILABLE = True
except ImportError:
    warnings.warn(
        "CrossLink C++ module not available. Using pure Python implementation. "
        "Zero-copy will not be available across languages."
    )
    _CPP_AVAILABLE = False

# Pure Python implementation of shared memory (fallback)
if not _CPP_AVAILABLE:
    try:
        import multiprocessing as mp
        from multiprocessing import shared_memory
        _SHARED_MEMORY_AVAILABLE = True
    except ImportError:
        _SHARED_MEMORY_AVAILABLE = False
        warnings.warn("Shared memory not available in Python. Using file-based sharing only.")

    try:
        import pandas as pd
        _PANDAS_AVAILABLE = True
    except ImportError:
        _PANDAS_AVAILABLE = False
        warnings.warn("Pandas not available. Some functionality will be limited.")

    try:
        import duckdb
        _DUCKDB_AVAILABLE = True
    except ImportError:
        _DUCKDB_AVAILABLE = False
        warnings.warn("DuckDB not available. Some functionality will be limited.")

class CrossLink:
    """
    CrossLink Python interface to the C++ core implementation.
    
    This class provides a Python-friendly wrapper around the C++ core,
    with additional features specific to Python.
    """
    
    def __init__(self, db_path=None, debug=False):
        """
        Initialize CrossLink with the given database path.
        
        Args:
            db_path (str, optional): Path to the CrossLink database file.
                If None, uses 'crosslink.duckdb' in the current directory.
            debug (bool, optional): Whether to enable debug output.
        """
        if db_path is None:
            db_path = os.path.join(os.getcwd(), "crosslink.duckdb")
        else:
            db_path = str(Path(db_path).resolve())
            
        self.db_path = db_path
        self.debug = debug
        
        if _CPP_AVAILABLE:
            self._cpp = CrossLinkCpp(db_path, debug)
            if debug:
                print(f"Using CrossLink C++ core with database: {db_path}")
        else:
            if debug:
                print(f"Using CrossLink pure Python implementation with database: {db_path}")
            
            # Initialize pure Python implementation
            if _DUCKDB_AVAILABLE:
                self._conn = duckdb.connect(db_path)
                self._init_tables()
            else:
                self._conn = None
                warnings.warn("DuckDB not available. Metadata management disabled.")
            
            # Shared memory regions
            self._shared_regions = {}
            
            # Event subscribers
            self._subscribers = {}
    
    def _init_tables(self):
        """Initialize the database tables for metadata storage"""
        if not _DUCKDB_AVAILABLE or self._conn is None:
            return
        
        try:
            # Create datasets table if it doesn't exist
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS datasets (
                    id VARCHAR PRIMARY KEY,
                    name VARCHAR,
                    source_language VARCHAR,
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    description VARCHAR,
                    schema_json VARCHAR,
                    table_name VARCHAR,
                    arrow_data BOOLEAN,
                    version INTEGER,
                    current_version BOOLEAN,
                    schema_hash VARCHAR,
                    memory_map_path VARCHAR,
                    shared_memory_key VARCHAR,
                    arrow_schema_json VARCHAR,
                    shared_memory_size BIGINT DEFAULT 0,
                    num_rows BIGINT DEFAULT 0,
                    num_columns INTEGER DEFAULT 0,
                    serialized_size BIGINT DEFAULT 0
                )
            """)
            
            # Access log and access rights
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS access_log (
                    id VARCHAR PRIMARY KEY,
                    dataset_id VARCHAR,
                    language VARCHAR,
                    operation VARCHAR,
                    access_method VARCHAR,
                    timestamp TIMESTAMP,
                    success BOOLEAN
                )
            """)
            
            self._conn.execute("""
                CREATE TABLE IF NOT EXISTS dataset_access (
                    dataset_id VARCHAR,
                    language VARCHAR,
                    PRIMARY KEY (dataset_id, language)
                )
            """)
            
        except Exception as e:
            warnings.warn(f"Failed to initialize tables: {e}")
            
    def _log_access(self, dataset_id, operation, access_method, success=True):
        """Log dataset access"""
        if not _DUCKDB_AVAILABLE or self._conn is None:
            return
        
        try:
            log_id = f"log-{dataset_id}-{uuid.uuid4()}"
            self._conn.execute(
                """INSERT INTO access_log VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)""",
                [log_id, dataset_id, "python", operation, access_method, success]
            )
        except Exception as e:
            if self.debug:
                print(f"Failed to log access: {e}")
    
    def push(self, df, name="", description=""):
        """
        Share a pandas DataFrame with other languages.
        
        Args:
            df: A pandas DataFrame or PyArrow Table to share.
            name (str, optional): A name for this dataset.
            description (str, optional): A description of the dataset.
            
        Returns:
            str: The ID of the shared dataset.
        """
        # Convert pandas DataFrame to Arrow Table if needed
        if hasattr(df, 'to_arrow'):
            table = df.to_arrow()
        elif _PANDAS_AVAILABLE and isinstance(df, pd.DataFrame):
            table = pa.Table.from_pandas(df)
        elif not isinstance(df, pa.Table):
            try:
                table = pa.Table.from_pandas(df)
            except:
                raise TypeError("Input must be a pandas DataFrame or PyArrow Table")
        else:
            table = df
            
        if _CPP_AVAILABLE:
            return self._cpp.push(table, name, description)
        else:
            # Fallback to pure Python implementation
            dataset_id = str(uuid.uuid4())
            
            if not name:
                name = f"py_{dataset_id[:8]}"
                
            # Store in shared memory if available
            shm_name = None
            if _SHARED_MEMORY_AVAILABLE:
                try:
                    # Serialize to arrow buffer
                    sink = pa.BufferOutputStream()
                    writer = pa.RecordBatchStreamWriter(sink, table.schema)
                    writer.write_table(table)
                    writer.close()
                    buf = sink.getvalue()
                    
                    # Create shared memory
                    shm = shared_memory.SharedMemory(
                        create=True, 
                        size=buf.size,
                        name=dataset_id
                    )
                    
                    # Copy data to shared memory
                    np.frombuffer(shm.buf, dtype=np.uint8)[:buf.size] = np.frombuffer(
                        buf, dtype=np.uint8
                    )
                    
                    self._shared_regions[dataset_id] = shm
                    shm_name = dataset_id
                    
                    if self.debug:
                        print(f"Table stored in shared memory with name: {shm_name}")
                except Exception as e:
                    if self.debug:
                        print(f"Failed to use shared memory: {e}")
                    shm_name = None
            
            # Always create a memory mapped file as fallback
            arrow_dir = os.path.join(os.path.dirname(self.db_path), "arrow_files")
            os.makedirs(arrow_dir, exist_ok=True)
            
            file_path = os.path.join(arrow_dir, f"{dataset_id}.arrow")
            with pa.OSFile(file_path, 'wb') as f:
                writer = pa.RecordBatchFileWriter(f, table.schema)
                writer.write_table(table)
                writer.close()
                
            if self.debug:
                print(f"Table written to file: {file_path}")
            
            # Store metadata if DuckDB is available
            if _DUCKDB_AVAILABLE and self._conn is not None:
                try:
                    # Convert schema to JSON
                    schema_json = json.dumps({
                        "fields": [
                            {
                                "name": field.name,
                                "type": str(field.type),
                                "nullable": field.nullable
                            }
                            for field in table.schema
                        ]
                    })
                    
                    # Insert metadata
                    current_time = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                    self._conn.execute(
                        """
                        INSERT INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [
                            dataset_id, name, "python", current_time, current_time,
                            description, schema_json, "", True, 1, True, "", 
                            file_path, shm_name, schema_json,
                            buf.size if shm_name else 0,
                            table.num_rows, table.num_columns, buf.size
                        ]
                    )
                    
                    # Set access rights
                    for lang in ["python", "r", "julia", "cpp"]:
                        self._conn.execute(
                            "INSERT INTO dataset_access VALUES (?, ?)",
                            [dataset_id, lang]
                        )
                        
                    # Log the access
                    self._log_access(dataset_id, "push", "arrow")
                    
                except Exception as e:
                    warnings.warn(f"Failed to store metadata: {e}")
            
            # Notify subscribers
            self._notify(dataset_id, "updated")
            
            return dataset_id
    
    def pull(self, identifier):
        """
        Get a dataset by ID or name.
        
        Args:
            identifier (str): The ID or name of the dataset to retrieve.
            
        Returns:
            pyarrow.Table: The retrieved dataset as an Arrow Table.
        """
        if _CPP_AVAILABLE:
            return self._cpp.pull(identifier)
        else:
            # Fallback to pure Python implementation
            
            # Get metadata
            metadata = self._get_metadata(identifier)
            if not metadata:
                raise ValueError(f"Dataset not found: {identifier}")
            
            dataset_id = metadata["id"]
            
            # Try shared memory first
            if metadata["shared_memory_key"]:
                try:
                    shm_name = metadata["shared_memory_key"]
                    
                    # Attach to shared memory
                    shm = shared_memory.SharedMemory(name=shm_name)
                    
                    # Read the arrow buffer from shared memory
                    size = metadata["shared_memory_size"]
                    buffer = pa.py_buffer(np.frombuffer(shm.buf, dtype=np.uint8)[:size].tobytes())
                    
                    # Deserialize arrow buffer
                    reader = pa.RecordBatchStreamReader(buffer)
                    batches = [batch for batch in reader]
                    table = pa.Table.from_batches(batches)
                    
                    # Don't close the shared memory because other processes may be using it
                    # In a real implementation, we'd need reference counting
                    
                    self._log_access(dataset_id, "pull", "shared_memory")
                    return table
                except Exception as e:
                    if self.debug:
                        print(f"Failed to get from shared memory: {e}")
            
            # Fall back to memory-mapped file
            if metadata["memory_map_path"] and os.path.exists(metadata["memory_map_path"]):
                try:
                    with pa.memory_map(metadata["memory_map_path"], "r") as f:
                        reader = pa.RecordBatchFileReader(f)
                        table = reader.read_all()
                        
                        self._log_access(dataset_id, "pull", "memory_mapped")
                        return table
                except Exception as e:
                    raise RuntimeError(f"Failed to read memory-mapped file: {e}")
            
            raise RuntimeError(f"Dataset not accessible: {identifier}")
    
    def query(self, sql):
        """
        Execute a SQL query against the CrossLink database.
        
        Args:
            sql (str): The SQL query to execute.
            
        Returns:
            pyarrow.Table: The query results as an Arrow Table.
        """
        if _CPP_AVAILABLE:
            return self._cpp.query(sql)
        else:
            # Fallback to pure Python implementation
            if not _DUCKDB_AVAILABLE or self._conn is None:
                raise RuntimeError("DuckDB not available for query execution")
            
            try:
                result = self._conn.execute(sql).fetch_arrow_table()
                return result
            except Exception as e:
                raise RuntimeError(f"Query execution failed: {e}")
    
    def list_datasets(self):
        """
        List available datasets.
        
        Returns:
            list: A list of dataset IDs.
        """
        if _CPP_AVAILABLE:
            return self._cpp.list_datasets()
        else:
            # Fallback to pure Python implementation
            if not _DUCKDB_AVAILABLE or self._conn is None:
                return []
            
            try:
                result = self._conn.execute(
                    "SELECT id FROM datasets WHERE current_version = TRUE"
                ).fetchall()
                return [row[0] for row in result]
            except Exception as e:
                warnings.warn(f"Failed to list datasets: {e}")
                return []
    
    def _get_metadata(self, identifier):
        """Get metadata for a dataset"""
        if not _DUCKDB_AVAILABLE or self._conn is None:
            return None
        
        try:
            result = self._conn.execute(
                "SELECT * FROM datasets WHERE id = ? OR name = ?",
                [identifier, identifier]
            ).fetchone()
            
            if not result:
                return None
            
            # Convert to dictionary
            columns = [
                "id", "name", "source_language", "created_at", "updated_at",
                "description", "schema_json", "table_name", "arrow_data",
                "version", "current_version", "schema_hash", "memory_map_path",
                "shared_memory_key", "arrow_schema_json", "shared_memory_size",
                "num_rows", "num_columns", "serialized_size"
            ]
            
            return dict(zip(columns, result))
            
        except Exception as e:
            warnings.warn(f"Failed to get metadata: {e}")
            return None
    
    def register_notification(self, callback):
        """
        Register for notifications about data changes.
        
        Args:
            callback: A function taking (dataset_id, event_type) parameters.
            
        Returns:
            str: A registration ID that can be used to unregister.
        """
        if _CPP_AVAILABLE:
            return self._cpp.register_notification(callback)
        else:
            # Fallback to pure Python implementation
            registration_id = str(uuid.uuid4())
            self._subscribers[registration_id] = callback
            return registration_id
    
    def unregister_notification(self, registration_id):
        """
        Unregister from notifications.
        
        Args:
            registration_id (str): The registration ID to unregister.
        """
        if _CPP_AVAILABLE:
            self._cpp.unregister_notification(registration_id)
        else:
            # Fallback to pure Python implementation
            if registration_id in self._subscribers:
                del self._subscribers[registration_id]
    
    def _notify(self, dataset_id, event_type):
        """Notify subscribers of an event"""
        if not _CPP_AVAILABLE:
            for callback in self._subscribers.values():
                try:
                    callback(dataset_id, event_type)
                except Exception as e:
                    if self.debug:
                        print(f"Error in notification callback: {e}")
    
    def cleanup(self):
        """
        Clean up resources.
        """
        if _CPP_AVAILABLE:
            self._cpp.cleanup()
        else:
            # Fallback to pure Python implementation
            
            # Clean up shared memory regions
            for shm in self._shared_regions.values():
                try:
                    shm.close()
                    shm.unlink()
                except Exception as e:
                    if self.debug:
                        print(f"Error cleaning up shared memory: {e}")
            
            self._shared_regions.clear()
            
            # Close database connection
            if _DUCKDB_AVAILABLE and self._conn is not None:
                try:
                    self._conn.close()
                except:
                    pass
    
    def __del__(self):
        """
        Clean up resources when the object is garbage collected.
        """
        self.cleanup() 