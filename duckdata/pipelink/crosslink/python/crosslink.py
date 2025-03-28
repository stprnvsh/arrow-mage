"""
CrossLink: Simple cross-language data sharing
"""
import os
import pandas as pd
import duckdb
import json
import uuid
import time
import pyarrow as pa
import warnings
import tempfile
import hashlib
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
import psutil

class CrossLinkMetadataManager:
    """Metadata management for cross-language data sharing"""
    
    def __init__(self, conn):
        """Initialize the metadata manager
        
        Args:
            conn: DuckDB connection
        """
        self.conn = conn
        self._setup_metadata_tables()
    
    def _setup_metadata_tables(self):
        """Set up metadata tables for efficient cross-language data sharing"""
        # Main metadata table for datasets
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_metadata (
            id TEXT PRIMARY KEY,
            name TEXT,
            source_language TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            description TEXT,
            schema TEXT,
            table_name TEXT,
            arrow_data BOOLEAN DEFAULT FALSE,
            version INTEGER DEFAULT 1,
            current_version BOOLEAN DEFAULT TRUE,
            lineage TEXT,
            schema_hash TEXT,
            memory_map_path TEXT,         -- Path to memory-mapped file for zero-copy access
            shared_memory_key TEXT,       -- Key for shared memory segment
            arrow_schema TEXT,            -- Serialized Arrow schema for zero-copy
            access_languages TEXT,        -- JSON array of languages that can access this dataset
            memory_layout TEXT            -- Memory layout information for zero-copy access
        )
        """)
        
        # Schema history table for tracking schema evolution
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_schema_history (
            id TEXT,
            version INTEGER,
            schema TEXT,
            schema_hash TEXT,
            changed_at TIMESTAMP,
            change_type TEXT,
            changes TEXT,
            PRIMARY KEY (id, version)
        )
        """)
        
        # Lineage table for tracking data provenance
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_lineage (
            dataset_id TEXT,
            source_dataset_id TEXT,
            source_dataset_version INTEGER,
            transformation TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (dataset_id, source_dataset_id)
        )
        """)
        
        # Cross-language access tracking
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_access_log (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            language TEXT,
            operation TEXT,
            timestamp TIMESTAMP,
            access_method TEXT,       -- 'copy', 'zero-copy', 'view'
            success BOOLEAN,
            error_message TEXT
        )
        """)
        
        # Add missing columns to existing databases for backward compatibility
        self._ensure_columns_exist()
    
    def _ensure_columns_exist(self):
        """Ensure all required columns exist in metadata table"""
        columns_to_check = [
            ("memory_map_path", "TEXT"),
            ("shared_memory_key", "TEXT"),
            ("arrow_schema", "TEXT"),
            ("access_languages", "TEXT"),
            ("memory_layout", "TEXT")
        ]
        
        for col_name, col_type in columns_to_check:
            result = self.conn.execute(f"""
            SELECT count(*) as col_exists FROM pragma_table_info('crosslink_metadata') 
            WHERE name = '{col_name}'
            """).fetchone()
            
            if result[0] == 0:
                self.conn.execute(f"""
                ALTER TABLE crosslink_metadata ADD COLUMN {col_name} {col_type}
                """)
    
    def create_dataset_metadata(self, dataset_id, name, table_name, source_language, schema,
                              description=None, arrow_data=False, memory_map_path=None,
                              shared_memory_key=None, arrow_schema=None, access_languages=None,
                              memory_layout=None):
        """Create metadata for a new dataset
        
        Args:
            dataset_id: Unique ID for the dataset
            name: Human-readable name
            table_name: Table name in DuckDB
            source_language: Language that created the dataset
            schema: Schema as a dictionary
            description: Optional dataset description
            arrow_data: Whether data is in Arrow format
            memory_map_path: Path to memory-mapped file for zero-copy
            shared_memory_key: Key for shared memory segment
            arrow_schema: Serialized Arrow schema
            access_languages: Languages that can access this dataset
            memory_layout: Memory layout information for zero-copy access
            
        Returns:
            True if successful
        """
        # Set defaults
        if access_languages is None:
            access_languages = ["python", "r", "julia", "cpp"]
        
        schema_hash = hashlib.md5(json.dumps(schema, sort_keys=True).encode()).hexdigest()
        
        # Insert metadata
        self.conn.execute("""
        INSERT INTO crosslink_metadata (
            id, name, source_language, created_at, updated_at, description,
            schema, table_name, arrow_data, version, current_version,
            schema_hash, memory_map_path, shared_memory_key, arrow_schema,
            access_languages, memory_layout
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dataset_id, name, source_language, datetime.now(), datetime.now(),
            description or "", json.dumps(schema), table_name, arrow_data, 1, True,
            schema_hash, memory_map_path, shared_memory_key,
            json.dumps(arrow_schema) if arrow_schema else None,
            json.dumps(access_languages), json.dumps(memory_layout) if memory_layout else None
        ])
        
        return True
    
    def get_dataset_metadata(self, identifier):
        """Get metadata for a dataset
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            Dictionary of metadata or None if not found
        """
        # Check if identifier is an ID or name
        result = self.conn.execute("""
        SELECT * FROM crosslink_metadata 
        WHERE id = ? OR name = ?
        """, [identifier, identifier]).fetchone()
        
        if not result:
            return None
            
        # Convert result to dict
        columns = [col[0] for col in self.conn.description]
        metadata = {columns[i]: result[i] for i in range(len(columns))}
        
        # Parse JSON fields
        for key in ['schema', 'lineage', 'arrow_schema', 'access_languages', 'memory_layout']:
            if metadata.get(key):
                try:
                    metadata[key] = json.loads(metadata[key])
                except:
                    pass
                    
        return metadata
    
    def update_dataset_metadata(self, dataset_id, **updates):
        """Update metadata for an existing dataset
        
        Args:
            dataset_id: Dataset ID
            **updates: Fields to update
            
        Returns:
            True if successful
        """
        # Convert dict values to JSON
        for key, value in updates.items():
            if key in ['schema', 'arrow_schema', 'access_languages', 'memory_layout'] and value is not None:
                updates[key] = json.dumps(value)
        
        # Always update the updated_at timestamp
        updates['updated_at'] = datetime.now()
        
        # Build SQL update statement
        set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
        values = list(updates.values()) + [dataset_id]
        
        # Update metadata
        self.conn.execute(f"""
        UPDATE crosslink_metadata 
        SET {set_clause}
        WHERE id = ?
        """, values)
        
        return True
    
    def log_access(self, dataset_id, language, operation, access_method, success=True, error_message=None):
        """Log cross-language access to a dataset
        
        Args:
            dataset_id: Dataset ID
            language: Language accessing the dataset
            operation: Operation (read, write, etc.)
            access_method: Method of access (copy, zero-copy, view)
            success: Whether access was successful
            error_message: Error message if access failed
            
        Returns:
            True if log was created
        """
        log_id = str(uuid.uuid4())
        
        self.conn.execute("""
        INSERT INTO crosslink_access_log (
            id, dataset_id, language, operation, timestamp, 
            access_method, success, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            log_id, dataset_id, language, operation, datetime.now(),
            access_method, success, error_message
        ])
        
        return True

class CrossLink:
    def __init__(self, db_path="crosslink.duckdb"):
        """
        Initialize CrossLink with a database path.
        
        Args:
            db_path: Path to the DuckDB database file
        """
        self.db_path = db_path
        self.conn = duckdb.connect(db_path)
        
        # Install and load Arrow extension
        try:
            self.conn.execute("INSTALL arrow")
            self.conn.execute("LOAD arrow")
            self.arrow_available = True
        except Exception as e:
            warnings.warn(f"Arrow extension installation or loading failed: {e}")
            warnings.warn("Some Arrow functionality may not be available")
            self.arrow_available = False
        
        # Initialize metadata manager
        self.metadata_manager = CrossLinkMetadataManager(self.conn)
        
        # Configure DuckDB for performance
        self._configure_duckdb()
    
    def _configure_duckdb(self):
        """Configure DuckDB for optimal performance"""
        try:
            # Set memory limit to a specific amount instead of percentage
            # DuckDB expects units like MB, GB, etc.
            total_memory = psutil.virtual_memory().total
            memory_limit_mb = int(total_memory * 0.8 / (1024 * 1024))  # 80% of total memory in MB
            self.conn.execute(f"PRAGMA memory_limit='{memory_limit_mb}MB'")
            
            # Set threads to a reasonable number
            import multiprocessing
            num_cores = max(1, multiprocessing.cpu_count() - 1)
            self.conn.execute(f"PRAGMA threads={num_cores}")
            
            # Enable parallelism
            self.conn.execute("PRAGMA force_parallelism")
            
            # Enable object cache for better performance with repeated queries
            self.conn.execute("PRAGMA enable_object_cache")
        except Exception as e:
            warnings.warn(f"Failed to configure DuckDB performance settings: {e}")
    
    def _compute_schema_hash(self, df):
        """Compute a hash of the dataframe schema for detecting changes."""
        schema_dict = {
            'columns': list(df.columns),
            'dtypes': {col: str(df[col].dtype) for col in df.columns}
        }
        schema_str = json.dumps(schema_dict, sort_keys=True)
        return hashlib.md5(schema_str.encode()).hexdigest()
    
    def list_datasets(self):
        """List all available datasets in the CrossLink registry."""
        result = self.conn.execute("""
        SELECT id, name, source_language, created_at, table_name, description, version 
        FROM crosslink_metadata
        WHERE current_version = TRUE
        ORDER BY updated_at DESC
        """).fetchall()
        
        return pd.DataFrame(result, columns=[
            'id', 'name', 'source_language', 'created_at', 'table_name', 'description', 'version'
        ])
    
    def push(self, df, name=None, description=None, use_arrow=True, sources=None, transformation=None,
             enable_zero_copy=True, shared_memory=True, access_languages=None):
        """Push a dataframe to the shared database with metadata for zero-copy access
        
        Args:
            df: Pandas DataFrame or PyArrow Table to push
            name: Name for the dataset (will be auto-generated if None)
            description: Optional description
            use_arrow: Whether to use Arrow for data transfer
            sources: Optional list of source dataset IDs
            transformation: Description of transformation applied
            enable_zero_copy: Enable zero-copy access between languages
            shared_memory: Use shared memory for zero-copy access
            access_languages: Languages that can access this dataset
            
        Returns:
            dataset_id: ID of the registered dataset
        """
        # Generate dataset ID and name if not provided
        dataset_id = str(uuid.uuid4())
        if name is None:
            name = f"dataset_{int(time.time())}"
        
        # Create table name (sanitized from dataset name)
        table_name = f"data_{name.replace(' ', '_').replace('-', '_').lower()}"
        
        # Convert to PyArrow table if using Arrow and not already an Arrow table
        arrow_table = None
        if use_arrow and self.arrow_available:
            if isinstance(df, pa.Table):
                arrow_table = df
            else:
                arrow_table = pa.Table.from_pandas(df)
        
        # Create DuckDB table from DataFrame
        if use_arrow and self.arrow_available and arrow_table is not None:
            # Create table from Arrow data
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM arrow_table")
        else:
            # Create table from pandas DataFrame
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
        
        # Prepare metadata
        schema_dict = {
            'columns': list(df.columns),
            'dtypes': {col: str(df[col].dtype) for col in df.columns}
        }
        
        # Prepare zero-copy access metadata
        memory_map_path = None
        shared_memory_key = None
        arrow_schema = None
        memory_layout = None
        
        if enable_zero_copy and self.arrow_available and arrow_table is not None:
            # Serialize Arrow schema for zero-copy access
            arrow_schema = {
                'schema': arrow_table.schema.to_string(),
                'metadata': {k.decode(): v.decode() for k, v in arrow_table.schema.metadata.items()} if arrow_table.schema.metadata else {}
            }
            
            # Create memory layout information
            memory_layout = self._create_memory_layout(arrow_table)
            
            # If using shared memory, set up shared memory key
            if shared_memory:
                shared_memory_key = f"crosslink_{dataset_id}"
                
                # Store Arrow table in shared memory for zero-copy access
                self._store_in_shared_memory(arrow_table, shared_memory_key)
        
        # Create metadata entry
        self.metadata_manager.create_dataset_metadata(
            dataset_id=dataset_id,
            name=name,
            table_name=table_name,
            source_language="python",
            schema=schema_dict,
            description=description,
            arrow_data=use_arrow and self.arrow_available,
            memory_map_path=memory_map_path,
            shared_memory_key=shared_memory_key,
            arrow_schema=arrow_schema,
            access_languages=access_languages,
            memory_layout=memory_layout
        )
        
        # Log the operation
        self.metadata_manager.log_access(
            dataset_id=dataset_id,
            language="python",
            operation="write",
            access_method="zero-copy" if enable_zero_copy else "copy",
            success=True
        )
        
        return dataset_id
    
    def _create_memory_layout(self, arrow_table):
        """Create memory layout information for zero-copy access
        
        Args:
            arrow_table: PyArrow Table
            
        Returns:
            Memory layout dictionary
        """
        # Extract memory layout information from Arrow table
        layout = {
            'num_rows': arrow_table.num_rows,
            'num_columns': arrow_table.num_columns,
            'columns': []
        }
        
        # Add information about each column
        for i, col in enumerate(arrow_table.columns):
            col_layout = {
                'name': arrow_table.column_names[i],
                'type': str(col.type),
                'null_count': col.null_count,
                'buffers': []
            }
            
            # Handle ChunkedArray - get buffers from chunks if possible
            try:
                # For ChunkedArray, we need to get buffers from each chunk
                if hasattr(col, 'chunks') and len(col.chunks) > 0:
                    # Use the first chunk for layout information
                    chunk = col.chunks[0]
                    if hasattr(chunk, 'buffers'):
                        for j, buf in enumerate(chunk.buffers()):
                            if buf is not None:
                                col_layout['buffers'].append({
                                    'index': j,
                                    'size': buf.size,
                                    'offset': buf.address
                                })
                            else:
                                col_layout['buffers'].append(None)
                # If it's not a ChunkedArray, try to get buffers directly
                elif hasattr(col, 'buffers'):
                    for j, buf in enumerate(col.buffers()):
                        if buf is not None:
                            col_layout['buffers'].append({
                                'index': j,
                                'size': buf.size,
                                'offset': buf.address
                            })
                        else:
                            col_layout['buffers'].append(None)
                else:
                    # If we can't get buffers, just record basic info
                    col_layout['buffers'] = None
            except Exception as e:
                # If there's any error, just skip buffer info
                warnings.warn(f"Could not get buffer information for column {i}: {e}")
                col_layout['buffers'] = None
            
            layout['columns'].append(col_layout)
        
        return layout
    
    def _store_in_shared_memory(self, arrow_table, key):
        """Store Arrow table in shared memory for zero-copy access
        
        Args:
            arrow_table: PyArrow Table
            key: Shared memory key
            
        Returns:
            True if successful
        """
        try:
            import pyarrow.plasma as plasma
            # Create connection to plasma store
            plasma_client = plasma.connect("/tmp/plasma")
            
            # Serialize the table to an object ID
            object_id = plasma.ObjectID.from_random()
            data_size = pa.ipc.get_record_batch_size(pa.RecordBatch.from_pandas(arrow_table.to_pandas()))
            
            # Create buffer in plasma store
            buffer = plasma_client.create(object_id, data_size)
            
            # Write serialized data to buffer
            writer = pa.RecordBatchFileWriter(buffer, arrow_table.schema)
            writer.write_table(arrow_table)
            writer.close()
            
            # Seal the object
            plasma_client.seal(object_id)
            
            # Store the object ID in metadata
            self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS crosslink_plasma_objects (
                key TEXT PRIMARY KEY,
                object_id TEXT,
                created_at TIMESTAMP
            )
            """)
            
            self.conn.execute("""
            INSERT INTO crosslink_plasma_objects (key, object_id, created_at)
            VALUES (?, ?, ?)
            """, [key, str(object_id), datetime.now()])
            
            return True
        except ImportError:
            warnings.warn("PyArrow plasma module not available. Shared memory storage disabled.")
            return False
        except Exception as e:
            warnings.warn(f"Failed to store data in shared memory: {e}")
            return False
    
    def pull(self, identifier, to_pandas=True, use_arrow=True, zero_copy=True):
        """Pull a dataset with zero-copy optimization when possible
        
        Args:
            identifier: Dataset ID or name
            to_pandas: Convert to pandas DataFrame (if False, returns Arrow Table)
            use_arrow: Use Arrow for data transfer
            zero_copy: Attempt zero-copy when possible
            
        Returns:
            DataFrame or Arrow Table
        """
        # Get metadata
        metadata = self.metadata_manager.get_dataset_metadata(identifier)
        if not metadata:
            raise ValueError(f"Dataset with identifier '{identifier}' not found")
        
        # Log the access
        access_method = "copy"
        
        # Try zero-copy access if enabled
        if zero_copy and use_arrow and self.arrow_available and metadata.get('arrow_data'):
            # Check if data is available via shared memory
            if metadata.get('shared_memory_key'):
                try:
                    # Get from shared memory
                    result = self._get_from_shared_memory(metadata['shared_memory_key'])
                    if result is not None:
                        access_method = "zero-copy"
                        
                        # Log successful access
                        self.metadata_manager.log_access(
                            dataset_id=metadata['id'],
                            language="python",
                            operation="read",
                            access_method=access_method,
                            success=True
                        )
                        
                        # Return result
                        return result.to_pandas() if to_pandas else result
                except Exception as e:
                    warnings.warn(f"Zero-copy access failed: {e}. Falling back to copy.")
        
        # Fall back to standard query
        table_name = metadata['table_name']
        
        if use_arrow and self.arrow_available:
            # Get as Arrow
            arrow_result = self.conn.execute(f"SELECT * FROM {table_name}").fetch_arrow_table()
            
            # Log access
            self.metadata_manager.log_access(
                dataset_id=metadata['id'],
                language="python",
                operation="read",
                access_method=access_method,
                success=True
            )
            
            return arrow_result.to_pandas() if to_pandas else arrow_result
        else:
            # Get as pandas
            result = self.conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Log access
            self.metadata_manager.log_access(
                dataset_id=metadata['id'],
                language="python",
                operation="read",
                access_method=access_method,
                success=True
            )
            
            return result
    
    def _get_from_shared_memory(self, key):
        """Get Arrow table from shared memory
        
        Args:
            key: Shared memory key
            
        Returns:
            Arrow Table or None if not found
        """
        try:
            import pyarrow.plasma as plasma
            
            # Get object ID from database
            result = self.conn.execute("""
            SELECT object_id FROM crosslink_plasma_objects
            WHERE key = ?
            """, [key]).fetchone()
            
            if not result:
                return None
                
            object_id = plasma.ObjectID.from_binary(bytes.fromhex(result[0]))
            
            # Connect to plasma store
            plasma_client = plasma.connect("/tmp/plasma")
            
            # Get object
            [data] = plasma_client.get([object_id])
            
            # Deserialize to Arrow table
            reader = pa.RecordBatchFileReader(pa.BufferReader(data))
            batches = [reader.get_record_batch(i) for i in range(reader.num_record_batches)]
            table = pa.Table.from_batches(batches)
            
            return table
        except ImportError:
            warnings.warn("PyArrow plasma module not available. Shared memory access disabled.")
            return None
        except Exception as e:
            warnings.warn(f"Failed to access data from shared memory: {e}")
            return None
    
    def query(self, sql, use_arrow=True):
        """Execute a SQL query on the CrossLink database
        
        Args:
            sql: SQL query string
            use_arrow: Use Arrow for data transfer if available
            
        Returns:
            DataFrame or Arrow Table
        """
        if use_arrow and self.arrow_available:
            return self.conn.execute(sql).fetch_arrow_table().to_pandas()
        else:
            return self.conn.execute(sql).fetchdf()
    
    def get_schema_history(self, identifier):
        """Get schema history for a dataset
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            DataFrame with schema history
        """
        # Get dataset ID if name is provided
        metadata = self.metadata_manager.get_dataset_metadata(identifier)
        if not metadata:
            raise ValueError(f"Dataset with identifier '{identifier}' not found")
            
        dataset_id = metadata['id']
        
        result = self.conn.execute("""
        SELECT * FROM crosslink_schema_history
        WHERE id = ?
        ORDER BY version DESC
        """, [dataset_id]).fetchall()
        
        return pd.DataFrame(result, columns=[
            'id', 'version', 'schema', 'schema_hash', 'changed_at', 'change_type', 'changes'
        ])
    
    def close(self):
        """Close the database connection."""
        self.conn.close()
        
    def __enter__(self):
        """Context manager entry."""
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def get_table_reference(self, identifier):
        """Get a direct reference to a table without copying data
        
        This method returns information needed to directly access the table
        in the DuckDB database from any language, enabling true zero-copy access.
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            Dict with database_path, table_name, and schema information
        """
        # Get metadata
        metadata = self.metadata_manager.get_dataset_metadata(identifier)
        if not metadata:
            raise ValueError(f"Dataset with identifier '{identifier}' not found")
        
        # Log the access as zero-copy
        self.metadata_manager.log_access(
            dataset_id=metadata['id'],
            language="python",
            operation="reference",
            access_method="zero-copy",
            success=True
        )
        
        # Return the information needed to access the table directly
        return {
            "database_path": os.path.abspath(self.db_path),
            "table_name": metadata['table_name'],
            "schema": metadata['schema'],
            "arrow_schema": metadata.get('arrow_schema'),
            "dataset_id": metadata['id'],
            "access_method": "direct_table_reference"
        }
        
    def register_external_table(self, external_db_path, external_table_name, name=None, description=None):
        """Register an external table from another DuckDB database without copying data
        
        This method creates a reference to a table in another DuckDB database
        without copying the data, enabling true zero-copy access across connections.
        
        Args:
            external_db_path: Path to the external DuckDB database
            external_table_name: Name of the table in the external database
            name: Name for the dataset (will be auto-generated if None)
            description: Optional description
            
        Returns:
            dataset_id: ID of the registered reference
        """
        # Generate dataset ID and name if not provided
        dataset_id = str(uuid.uuid4())
        if name is None:
            name = f"ext_{os.path.basename(external_db_path)}_{external_table_name}"
        
        # Use DuckDB's attach feature to access the external database
        external_db_path = os.path.abspath(external_db_path)
        attach_name = f"ext_{dataset_id.replace('-', '_')}"
        
        try:
            # Check if the external database is the same as our current database
            current_db_path = os.path.abspath(self.db_path)
            same_database = (current_db_path == external_db_path)
            
            # If it's not the same database, attach it
            if not same_database:
                try:
                    # Attach the external database
                    self.conn.execute(f"ATTACH DATABASE '{external_db_path}' AS {attach_name}")
                    external_schema_prefix = f"{attach_name}."
                except Exception as e:
                    # Check if error is about database already being attached
                    if "already attached" in str(e):
                        # Get the existing attachment name by querying pragma_database_list
                        result = self.conn.execute("SELECT name FROM pragma_database_list() WHERE file = ?", 
                                              [external_db_path]).fetchone()
                        if result:
                            attach_name = result[0]
                            external_schema_prefix = f"{attach_name}."
                        else:
                            # Can't find attachment, use main
                            external_schema_prefix = ""
                    else:
                        # Some other error occurred
                        raise e
            else:
                # Same database, don't need prefix
                external_schema_prefix = ""
            
            # Get schema information from the external table
            schema_df = self.conn.execute(f"DESCRIBE {external_schema_prefix}{external_table_name}").fetchdf()
            
            # Create a schema dictionary
            schema_dict = {
                'columns': schema_df['column_name'].tolist(),
                'dtypes': {row['column_name']: row['column_type'] for _, row in schema_df.iterrows()}
            }
            
            # Create a view to the external table
            view_name = f"ext_view_{dataset_id.replace('-', '_')}"
            self.conn.execute(f"CREATE VIEW {view_name} AS SELECT * FROM {external_schema_prefix}{external_table_name}")
            
            # Store metadata about this reference
            self.metadata_manager.create_dataset_metadata(
                dataset_id=dataset_id,
                name=name,
                table_name=view_name,
                source_language="external",
                schema=schema_dict,
                description=description or f"External table reference to {external_db_path}:{external_table_name}",
                arrow_data=False,
                memory_map_path=None,
                shared_memory_key=None,
                arrow_schema=None,
                access_languages=["python", "r", "julia", "cpp"],
                memory_layout=None
            )
            
            # Log the operation
            self.metadata_manager.log_access(
                dataset_id=dataset_id,
                language="python",
                operation="reference",
                access_method="external_table",
                success=True
            )
            
            return dataset_id
            
        except Exception as e:
            warnings.warn(f"Failed to register external table: {e}")
            # Detach the database if it was attached and isn't the same as current
            if 'same_database' in locals() and not same_database and 'attach_name' in locals():
                try:
                    self.conn.execute(f"DETACH DATABASE {attach_name}")
                except:
                    pass
            
            raise e