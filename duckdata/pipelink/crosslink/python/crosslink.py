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
            
        self._setup_metadata_tables()
    
    def _setup_metadata_tables(self):
        """Set up the metadata tables if they don't exist and configure DuckDB for large datasets."""
        # Configure DuckDB for handling large datasets
        try:
            # Set memory limit to a percentage of available RAM (80%)
            self.conn.execute("PRAGMA memory_limit='80%'")
            
            # Set threads to a reasonable number (adjust based on system capabilities)
            import multiprocessing
            num_cores = max(1, multiprocessing.cpu_count() - 1)  # Leave 1 core for other processes
            self.conn.execute(f"PRAGMA threads={num_cores}")
            
            # Enable parallelism
            self.conn.execute("PRAGMA force_parallelism")
            
            # Enable progress bar for long-running queries (useful for debugging)
            self.conn.execute("PRAGMA enable_progress_bar")
            
            # Enable object cache for better performance with repeated queries
            self.conn.execute("PRAGMA enable_object_cache")
        except Exception as e:
            warnings.warn(f"Failed to configure DuckDB performance settings: {e}")
        
        # Enable external IO if possible
        try:
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")
            self._httpfs_available = True
        except Exception:
            warnings.warn("httpfs extension not available. Remote file access will be limited.")
            self._httpfs_available = False
        
        # Main metadata table for current versions
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
            schema_hash TEXT
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
        
        # Performance metrics table
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_performance_metrics (
            id TEXT,
            operation TEXT,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            dataset_rows INTEGER,
            dataset_columns INTEGER,
            memory_used_mb DOUBLE,
            execution_time_ms INTEGER,
            success BOOLEAN,
            error_message TEXT,
            PRIMARY KEY (id, started_at)
        )
        """)
        
        # Add missing columns to existing databases for backward compatibility
        self._ensure_columns_exist()
    
    def _ensure_columns_exist(self):
        """Ensure all required columns exist in metadata table for backwards compatibility."""
        columns_to_check = [
            ("arrow_data", "BOOLEAN DEFAULT FALSE"),
            ("version", "INTEGER DEFAULT 1"),
            ("current_version", "BOOLEAN DEFAULT TRUE"),
            ("lineage", "TEXT"),
            ("schema_hash", "TEXT")
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
    
    def _compute_schema_hash(self, df):
        """Compute a hash of the dataframe schema for detecting changes."""
        schema_dict = {
            'columns': list(df.columns),
            'dtypes': {col: str(df[col].dtype) for col in df.columns}
        }
        schema_str = json.dumps(schema_dict, sort_keys=True)
        return hashlib.md5(schema_str.encode()).hexdigest()
    
    def _detect_schema_changes(self, old_schema, new_df):
        """
        Detect schema changes between old schema and new dataframe.
        
        Returns:
            dict: Description of changes
        """
        if not old_schema:
            return {"change_type": "initial_schema"}
            
        old_schema_dict = json.loads(old_schema)
        old_columns = set(old_schema_dict.get('columns', []))
        old_dtypes = old_schema_dict.get('dtypes', {})
        
        new_columns = set(new_df.columns)
        new_dtypes = {col: str(new_df[col].dtype) for col in new_df.columns}
        
        added_columns = new_columns - old_columns
        removed_columns = old_columns - new_columns
        common_columns = old_columns.intersection(new_columns)
        
        changed_dtypes = {
            col: (old_dtypes.get(col), new_dtypes.get(col))
            for col in common_columns
            if old_dtypes.get(col) != new_dtypes.get(col)
        }
        
        changes = {
            "added_columns": list(added_columns),
            "removed_columns": list(removed_columns),
            "changed_dtypes": changed_dtypes
        }
        
        if added_columns or removed_columns or changed_dtypes:
            if removed_columns:
                change_type = "breaking"
            elif added_columns and not changed_dtypes:
                change_type = "non_breaking_addition"
            else:
                change_type = "potentially_breaking"
        else:
            change_type = "no_change"
            
        return {
            "change_type": change_type,
            "changes": changes
        }
    
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
    
    def list_dataset_versions(self, identifier):
        """
        List all versions of a dataset.
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            DataFrame containing version information
        """
        result = self.conn.execute("""
        SELECT id, name, version, created_at, updated_at, schema_hash, current_version
        FROM crosslink_metadata 
        WHERE id = ? OR name = ?
        ORDER BY version DESC
        """, (identifier, identifier)).fetchall()
        
        if not result:
            raise ValueError(f"Dataset with identifier '{identifier}' not found")
        
        return pd.DataFrame(result, columns=[
            'id', 'name', 'version', 'created_at', 'updated_at', 'schema_hash', 'current_version'
        ])
    
    def get_schema_history(self, identifier):
        """
        Get schema evolution history for a dataset.
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            DataFrame containing schema history
        """
        # First get the ID if name was provided
        dataset_id = self.conn.execute("""
        SELECT id FROM crosslink_metadata
        WHERE id = ? OR name = ?
        LIMIT 1
        """, (identifier, identifier)).fetchone()
        
        if not dataset_id:
            raise ValueError(f"Dataset with identifier '{identifier}' not found")
        
        dataset_id = dataset_id[0]
        
        result = self.conn.execute("""
        SELECT id, version, schema, schema_hash, changed_at, change_type, changes
        FROM crosslink_schema_history
        WHERE id = ?
        ORDER BY version
        """, (dataset_id,)).fetchall()
        
        return pd.DataFrame(result, columns=[
            'id', 'version', 'schema', 'schema_hash', 'changed_at', 'change_type', 'changes'
        ])
    
    def get_lineage(self, identifier):
        """
        Get data lineage information for a dataset.
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            DataFrame containing lineage information
        """
        # First get the ID if name was provided
        dataset_id = self.conn.execute("""
        SELECT id FROM crosslink_metadata
        WHERE id = ? OR name = ?
        LIMIT 1
        """, (identifier, identifier)).fetchone()
        
        if not dataset_id:
            raise ValueError(f"Dataset with identifier '{identifier}' not found")
        
        dataset_id = dataset_id[0]
        
        result = self.conn.execute("""
        SELECT dataset_id, source_dataset_id, source_dataset_version, transformation, created_at
        FROM crosslink_lineage
        WHERE dataset_id = ?
        """, (dataset_id,)).fetchall()
        
        return pd.DataFrame(result, columns=[
            'dataset_id', 'source_dataset_id', 'source_dataset_version', 'transformation', 'created_at'
        ])
    
    def push(self, df, name=None, description=None, use_arrow=True, sources=None, transformation=None, force_new_version=False,
             chunk_size=None, use_disk_spilling=True):
        """
        Push a pandas DataFrame to the CrossLink registry.
        
        Args:
            df: pandas DataFrame to share
            name: Optional name for the dataset
            description: Optional description of the dataset
            use_arrow: Whether to use Arrow for storage (default: True)
            sources: List of source dataset IDs or names that were used to create this dataset
            transformation: Description of the transformation applied to create this dataset
            force_new_version: Whether to create a new version even if schema hasn't changed
            chunk_size: Size of chunks when handling large datasets (None = auto-detect based on data size)
            use_disk_spilling: Whether to allow DuckDB to spill to disk for large datasets
            
        Returns:
            dataset_id: The ID of the registered dataset
        """
        # Use the provided name or generate one
        if name is None:
            name = f"python_data_{int(time.time())}"
            
        # Check if dataset with this name already exists
        existing = self.conn.execute("""
        SELECT id, schema, version FROM crosslink_metadata 
        WHERE name = ? AND current_version = TRUE
        """, (name,)).fetchone()
        
        dataset_id = None
        create_new_version = True
        old_version = 1
        
        if existing:
            dataset_id, old_schema, old_version = existing
            
            # Detect schema changes
            schema_hash = self._compute_schema_hash(df)
            schema_changes = self._detect_schema_changes(old_schema, df)
            
            # Only create new version if schema changed or forced
            if schema_changes["change_type"] == "no_change" and not force_new_version:
                create_new_version = False
        else:
            # New dataset
            dataset_id = str(uuid.uuid4())
            old_schema = None
            schema_changes = {"change_type": "initial_schema"}
        
        # Generate unique table name for this version
        new_version = old_version + 1 if create_new_version else old_version
        table_name = f"crosslink_data_{dataset_id.replace('-', '_')}_{new_version}"
        
        # Calculate schema hash
        schema_json = json.dumps({
            'columns': list(df.columns),
            'dtypes': {col: str(df[col].dtype) for col in df.columns},
            'is_arrow': True if use_arrow and self.arrow_available else False
        })
        schema_hash = self._compute_schema_hash(df)
        
        # Enable disk spilling if requested
        if use_disk_spilling:
            try:
                # Enable memory spilling to disk for large datasets
                self.conn.execute("PRAGMA memory_limit='80%'")
                self.conn.execute("PRAGMA threads=4")  # Adjust based on system capabilities
                self.conn.execute("PRAGMA force_parallelism")
            except Exception as e:
                warnings.warn(f"Failed to configure DuckDB memory settings: {e}")
        
        # Determine if we should use chunking
        use_chunking = False
        estimated_size_bytes = df.memory_usage(deep=True).sum()
        
        if chunk_size is None:
            # Auto-detect: if dataset is larger than 1GB, use chunking
            use_chunking = estimated_size_bytes > 1_000_000_000  # 1GB
            chunk_size = 500_000 if use_chunking else None  # Default chunk size of 500k rows
        else:
            use_chunking = chunk_size > 0
        
        # Store data using Arrow if possible, otherwise fall back to direct table creation
        arrow_stored = False
        
        if use_arrow and self.arrow_available:
            try:
                if use_chunking:
                    # Stream data in chunks to avoid memory issues
                    # First create empty table with correct schema
                    arrow_table_sample = pa.Table.from_pandas(df.head(1))
                    self.conn.register("arrow_schema", arrow_table_sample)
                    
                    if create_new_version:
                        # Create a new table with the schema
                        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_schema WHERE 1=0")
                        self.conn.unregister("arrow_schema")
                        
                        # Insert data in chunks
                        total_rows = len(df)
                        for i in range(0, total_rows, chunk_size):
                            chunk = df.iloc[i:min(i+chunk_size, total_rows)]
                            arrow_chunk = pa.Table.from_pandas(chunk)
                            self.conn.register("arrow_chunk", arrow_chunk)
                            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM arrow_chunk")
                            self.conn.unregister("arrow_chunk")
                    else:
                        # Update existing table
                        old_table_name = f"crosslink_data_{dataset_id.replace('-', '_')}_{old_version}"
                        self.conn.execute(f"DROP TABLE IF EXISTS {old_table_name}")
                        self.conn.execute(f"CREATE TABLE {old_table_name} AS SELECT * FROM arrow_schema WHERE 1=0")
                        self.conn.unregister("arrow_schema")
                        
                        # Insert data in chunks
                        total_rows = len(df)
                        for i in range(0, total_rows, chunk_size):
                            chunk = df.iloc[i:min(i+chunk_size, total_rows)]
                            arrow_chunk = pa.Table.from_pandas(chunk)
                            self.conn.register("arrow_chunk", arrow_chunk)
                            self.conn.execute(f"INSERT INTO {old_table_name} SELECT * FROM arrow_chunk")
                            self.conn.unregister("arrow_chunk")
                        table_name = old_table_name
                else:
                    # Non-chunked approach for smaller datasets
                    arrow_table = pa.Table.from_pandas(df)
                    
                    if create_new_version:
                        # Create a new table for the new version
                        self.conn.register("arrow_table", arrow_table)
                        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_table")
                        self.conn.unregister("arrow_table")
                    else:
                        # Update existing table
                        old_table_name = f"crosslink_data_{dataset_id.replace('-', '_')}_{old_version}"
                        self.conn.execute(f"DROP TABLE IF EXISTS {old_table_name}")
                        self.conn.register("arrow_table", arrow_table)
                        self.conn.execute(f"CREATE TABLE {old_table_name} AS SELECT * FROM arrow_table")
                        self.conn.unregister("arrow_table")
                        table_name = old_table_name
                
                # Mark as successfully stored using Arrow
                arrow_stored = True
            except Exception as e:
                warnings.warn(f"Failed to use Arrow for data storage: {e}")
                warnings.warn("Falling back to direct DuckDB table creation")
                arrow_stored = False
        
        if not arrow_stored:
            # Direct table creation without Arrow
            try:
                if use_chunking:
                    # Create empty table with schema
                    if create_new_version:
                        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df WHERE 1=0", {"df": df.head(1)})
                        
                        # Insert data in chunks
                        total_rows = len(df)
                        for i in range(0, total_rows, chunk_size):
                            chunk = df.iloc[i:min(i+chunk_size, total_rows)]
                            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM chunk", {"chunk": chunk})
                    else:
                        # Update existing table
                        old_table_name = f"crosslink_data_{dataset_id.replace('-', '_')}_{old_version}"
                        self.conn.execute(f"DROP TABLE IF EXISTS {old_table_name}")
                        self.conn.execute(f"CREATE TABLE {old_table_name} AS SELECT * FROM df WHERE 1=0", {"df": df.head(1)})
                        
                        # Insert data in chunks
                        total_rows = len(df)
                        for i in range(0, total_rows, chunk_size):
                            chunk = df.iloc[i:min(i+chunk_size, total_rows)]
                            self.conn.execute(f"INSERT INTO {old_table_name} SELECT * FROM chunk", {"chunk": chunk})
                        table_name = old_table_name
                else:
                    # Non-chunked approach for smaller datasets
                    if create_new_version:
                        self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df", {"df": df})
                    else:
                        old_table_name = f"crosslink_data_{dataset_id.replace('-', '_')}_{old_version}"
                        self.conn.execute(f"DROP TABLE IF EXISTS {old_table_name}")
                        self.conn.execute(f"CREATE TABLE {old_table_name} AS SELECT * FROM df", {"df": df})
                        table_name = old_table_name
            except Exception as e:
                # If we still have memory issues, try with a smaller chunk size
                if not use_chunking or chunk_size > 10000:
                    smaller_chunk = 10000 if not use_chunking else chunk_size // 2
                    warnings.warn(f"Retrying with smaller chunk size ({smaller_chunk})")
                    return self.push(df, name, description, use_arrow, sources, transformation, 
                                   force_new_version, smaller_chunk, use_disk_spilling)
                else:
                    raise e
        
        # If creating a new version, mark old versions as not current
        if create_new_version and existing:
            self.conn.execute("""
            UPDATE crosslink_metadata SET current_version = FALSE
            WHERE id = ?
            """, (dataset_id,))
            
            # Record schema history
            self.conn.execute("""
            INSERT INTO crosslink_schema_history 
            (id, version, schema, schema_hash, changed_at, change_type, changes)
            VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
            """, (
                dataset_id, 
                new_version, 
                schema_json, 
                schema_hash, 
                schema_changes["change_type"], 
                json.dumps(schema_changes.get("changes", {}))
            ))
        
        # Record lineage information
        if sources and create_new_version:
            if not isinstance(sources, list):
                sources = [sources]
                
            for source in sources:
                # Get source dataset ID and version
                source_metadata = self.conn.execute("""
                SELECT id, version FROM crosslink_metadata 
                WHERE (id = ? OR name = ?) AND current_version = TRUE
                """, (source, source)).fetchone()
                
                if source_metadata:
                    source_id, source_version = source_metadata
                    
                    # Record lineage
                    self.conn.execute("""
                    INSERT OR REPLACE INTO crosslink_lineage
                    (dataset_id, source_dataset_id, source_dataset_version, transformation, created_at)
                    VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (dataset_id, source_id, source_version, transformation or "unknown"))
        
        # Insert or update metadata
        if create_new_version:
            # Insert new metadata record
            self.conn.execute("""
            INSERT INTO crosslink_metadata 
            (id, name, source_language, created_at, updated_at, description, schema, table_name, arrow_data, version, current_version, schema_hash)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, TRUE, ?)
            """, (
                dataset_id, name, "python", description, schema_json, table_name, arrow_stored, new_version, schema_hash
            ))
        else:
            # Update existing metadata record
            self.conn.execute("""
            UPDATE crosslink_metadata SET
            updated_at = CURRENT_TIMESTAMP,
            description = COALESCE(?, description),
            arrow_data = ?
            WHERE id = ? AND version = ?
            """, (description, arrow_stored, dataset_id, old_version))
        
        return dataset_id
    
    def pull(self, identifier, to_pandas=True, use_arrow=True, version=None, chunk_size=None, 
             stream=False, use_disk_spilling=True):
        """
        Pull a dataset from the CrossLink registry.
        
        Args:
            identifier: Dataset ID or name
            to_pandas: Whether to return as pandas DataFrame (default: True)
            use_arrow: Whether to use Arrow for data retrieval when available (default: True)
            version: Specific version to retrieve (default: latest version)
            chunk_size: Size of chunks when retrieving large datasets (only used when stream=True)
            stream: Whether to return an iterator over chunks instead of the full dataset
            use_disk_spilling: Whether to allow DuckDB to spill to disk for large operations
            
        Returns:
            If stream=False (default): DataFrame or Arrow Table with the dataset
            If stream=True: Iterator over DataFrame or Arrow Table chunks
        """
        # Enable disk spilling if requested
        if use_disk_spilling:
            try:
                # Enable memory spilling to disk for large datasets
                self.conn.execute("PRAGMA memory_limit='80%'")
                self.conn.execute("PRAGMA threads=4")  # Adjust based on system capabilities
                self.conn.execute("PRAGMA force_parallelism")
            except Exception as e:
                warnings.warn(f"Failed to configure DuckDB memory settings: {e}")
        
        # Try to find by ID/name with version condition
        version_condition = ""
        params = [identifier, identifier]
        
        if version is not None:
            version_condition = "AND version = ?"
            params.append(version)
        else:
            version_condition = "AND current_version = TRUE"
        
        query = f"""
        SELECT table_name, schema, arrow_data, 
               (SELECT COUNT(*) FROM crosslink_metadata cm 
                WHERE (id = ? OR name = ?) {version_condition}) as row_count
        FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) {version_condition}
        """
        
        metadata = self.conn.execute(query, [identifier, identifier] + params).fetchone()
        
        if not metadata:
            raise ValueError(f"Dataset with identifier '{identifier}' and specified version not found")
        
        table_name, schema_json, arrow_data, dataset_size = metadata
        
        # Parse schema to check if it's Arrow data
        schema = None
        if schema_json:
            schema = json.loads(schema_json)
        else:
            schema = {}
        
        is_arrow = schema.get('is_arrow', False) or arrow_data
        
        # Get row count to determine if we should use chunking
        row_count_query = f"SELECT COUNT(*) FROM {table_name}"
        row_count = self.conn.execute(row_count_query).fetchone()[0]
        
        # Auto-determine chunk size if not specified and streaming
        if stream and chunk_size is None:
            # Default chunk size: 100k rows for regular tables, 10k rows for wide tables
            col_count = len(schema.get('columns', []))
            if col_count > 100:  # Wide table
                chunk_size = 10000
            else:
                chunk_size = 100000
        
        # Define a generator for streaming data
        def data_stream_generator():
            # Calculate number of chunks
            num_chunks = (row_count + chunk_size - 1) // chunk_size
            
            for chunk_idx in range(num_chunks):
                offset = chunk_idx * chunk_size
                limit = chunk_size
                
                # Query for this chunk
                chunk_query = f"SELECT * FROM {table_name} LIMIT {limit} OFFSET {offset}"
                
                # Use Arrow for retrieval if available and requested
                if use_arrow and self.arrow_available and is_arrow:
                    chunk_result = self.conn.execute(chunk_query).arrow()
                    
                    if to_pandas:
                        # Convert to pandas DataFrame
                        yield chunk_result.to_pandas()
                    else:
                        # Return as Arrow Table
                        yield chunk_result
                else:
                    # Standard DuckDB retrieval
                    chunk_result = self.conn.execute(chunk_query).fetchdf()
                    
                    if not to_pandas and self.arrow_available:
                        # Convert to Arrow Table if requested
                        yield pa.Table.from_pandas(chunk_result)
                    else:
                        # Return as pandas DataFrame
                        yield chunk_result
        
        # Return a streaming iterator if requested
        if stream:
            return data_stream_generator()
        
        # For non-streaming mode, we still use chunking internally for large datasets
        if row_count > 1_000_000:  # For datasets with more than 1M rows
            # Use chunking internally for memory efficiency
            internal_chunk_size = 500_000  # 500k rows per chunk
            
            # Decide on container based on output type
            if to_pandas:
                all_chunks = []
                
                # Stream data in chunks
                for i in range(0, row_count, internal_chunk_size):
                    chunk_query = f"SELECT * FROM {table_name} LIMIT {internal_chunk_size} OFFSET {i}"
                    if use_arrow and self.arrow_available:
                        chunk = self.conn.execute(chunk_query).arrow().to_pandas()
                    else:
                        chunk = self.conn.execute(chunk_query).fetchdf()
                    all_chunks.append(chunk)
                
                # Combine all chunks
                if all_chunks:
                    return pd.concat(all_chunks, ignore_index=True)
                return pd.DataFrame()
            else:
                # For Arrow tables, we'll collect RecordBatches
                batches = []
                
                # Stream data in chunks
                for i in range(0, row_count, internal_chunk_size):
                    chunk_query = f"SELECT * FROM {table_name} LIMIT {internal_chunk_size} OFFSET {i}"
                    if use_arrow and self.arrow_available:
                        chunk = self.conn.execute(chunk_query).arrow()
                    else:
                        chunk = pa.Table.from_pandas(self.conn.execute(chunk_query).fetchdf())
                    
                    # Add all record batches
                    for batch in chunk.to_batches():
                        batches.append(batch)
                
                # Combine all batches
                if batches:
                    return pa.Table.from_batches(batches)
                return pa.Table.from_arrays([], [])
        
        # Standard retrieval for smaller datasets
        query = f"SELECT * FROM {table_name}"
        
        # Use Arrow for retrieval if available and requested
        if use_arrow and self.arrow_available and is_arrow:
            result = self.conn.execute(query).arrow()
            
            if to_pandas:
                # Convert to pandas DataFrame
                return result.to_pandas()
            else:
                # Return as Arrow Table
                return result
        else:
            # Standard DuckDB retrieval
            result = self.conn.execute(query).fetchdf()
            
            if not to_pandas and self.arrow_available:
                # Convert to Arrow Table if requested
                return pa.Table.from_pandas(result)
            else:
                # Return as pandas DataFrame
                return result
    
    def delete(self, identifier, version=None):
        """
        Delete a dataset from the CrossLink registry.
        
        Args:
            identifier: Dataset ID or name
            version: Specific version to delete (default: all versions)
        """
        # Get metadata for the dataset
        version_condition = ""
        params = [identifier, identifier]
        
        if version is not None:
            version_condition = "AND version = ?"
            params.append(version)
        
        metadata_results = self.conn.execute(f"""
        SELECT id, table_name, version FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) {version_condition}
        """, params).fetchall()
        
        if not metadata_results:
            raise ValueError(f"Dataset with identifier '{identifier}' not found")
        
        dataset_id = metadata_results[0][0]
        
        for _, table_name, version in metadata_results:
            # Delete the data table
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
            # Delete schema history
            self.conn.execute("""
            DELETE FROM crosslink_schema_history WHERE id = ? AND version = ?
            """, (dataset_id, version))
            
            # Delete the metadata
            self.conn.execute("""
            DELETE FROM crosslink_metadata WHERE id = ? AND version = ?
            """, (dataset_id, version))
        
        # If deleting all versions, also clean up lineage
        if version is None:
            self.conn.execute("""
            DELETE FROM crosslink_lineage WHERE dataset_id = ? OR source_dataset_id = ?
            """, (dataset_id, dataset_id))
    
    def check_compatibility(self, source_id, target_id):
        """
        Check if two datasets have compatible schemas.
        
        Args:
            source_id: Source dataset ID or name
            target_id: Target dataset ID or name
            
        Returns:
            dict: Compatibility information
        """
        source_metadata = self.conn.execute("""
        SELECT schema FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) AND current_version = TRUE
        """, (source_id, source_id)).fetchone()
        
        target_metadata = self.conn.execute("""
        SELECT schema FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) AND current_version = TRUE
        """, (target_id, target_id)).fetchone()
        
        if not source_metadata or not target_metadata:
            missing = []
            if not source_metadata:
                missing.append(f"Source dataset '{source_id}' not found")
            if not target_metadata:
                missing.append(f"Target dataset '{target_id}' not found")
            return {"compatible": False, "reason": "; ".join(missing)}
        
        source_schema = json.loads(source_metadata[0])
        target_schema = json.loads(target_metadata[0])
        
        source_cols = set(source_schema.get('columns', []))
        target_cols = set(target_schema.get('columns', []))
        
        missing_cols = source_cols - target_cols
        if missing_cols:
            return {
                "compatible": False,
                "reason": f"Target schema missing columns: {', '.join(missing_cols)}"
            }
        
        common_cols = source_cols.intersection(target_cols)
        source_dtypes = source_schema.get('dtypes', {})
        target_dtypes = target_schema.get('dtypes', {})
        
        dtype_mismatches = []
        for col in common_cols:
            source_type = source_dtypes.get(col)
            target_type = target_dtypes.get(col)
            if source_type != target_type:
                dtype_mismatches.append(f"{col}: {source_type} vs {target_type}")
        
        if dtype_mismatches:
            return {
                "compatible": False,
                "reason": f"Type mismatches: {'; '.join(dtype_mismatches)}"
            }
        
        return {"compatible": True}
    
    def close(self):
        """Close the connection to the database."""
        self.conn.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def monitor_resources(self, active=True):
        """
        Enable or disable resource monitoring for CrossLink operations.
        
        Args:
            active: Whether to enable resource monitoring
            
        Returns:
            None
        """
        try:
            import psutil
            self._has_psutil = True
        except ImportError:
            warnings.warn("psutil is not installed. Resource monitoring will be limited.")
            self._has_psutil = False
        
        self._resource_monitoring = active
        
        if active:
            # Initialize resource monitoring
            self._resource_data = {
                'timestamps': [],
                'memory_usage': [],
                'disk_reads': [],
                'disk_writes': [],
                'cpu_percent': []
            }
            
            # Start monitoring thread if psutil is available
            if self._has_psutil:
                import threading
                import time
                
                def monitor_loop():
                    process = psutil.Process()
                    disk_io_start = psutil.disk_io_counters()
                    
                    while self._resource_monitoring:
                        try:
                            # Collect data
                            mem_info = process.memory_info()
                            cpu_percent = process.cpu_percent()
                            disk_io_current = psutil.disk_io_counters()
                            
                            # Calculate disk I/O deltas
                            read_delta = disk_io_current.read_bytes - disk_io_start.read_bytes
                            write_delta = disk_io_current.write_bytes - disk_io_start.write_bytes
                            
                            # Update disk I/O baseline
                            disk_io_start = disk_io_current
                            
                            # Record data
                            self._resource_data['timestamps'].append(time.time())
                            self._resource_data['memory_usage'].append(mem_info.rss)
                            self._resource_data['disk_reads'].append(read_delta)
                            self._resource_data['disk_writes'].append(write_delta)
                            self._resource_data['cpu_percent'].append(cpu_percent)
                            
                            # Sleep for a short interval
                            time.sleep(0.5)
                        except Exception as e:
                            warnings.warn(f"Error in resource monitoring: {e}")
                            time.sleep(1)
                
                # Start monitoring in a separate thread
                self._monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
                self._monitor_thread.start()
        else:
            # Stop monitoring
            self._resource_monitoring = False
            self._resource_data = None

    def get_resource_stats(self):
        """
        Get resource usage statistics for CrossLink operations.
        
        Returns:
            DataFrame with resource usage data if monitoring is active,
            None otherwise
        """
        if not hasattr(self, '_resource_monitoring') or not self._resource_monitoring:
            warnings.warn("Resource monitoring is not enabled. Call monitor_resources(True) to enable it.")
            return None
        
        if not self._resource_data or len(self._resource_data['timestamps']) == 0:
            return pd.DataFrame()
        
        # Convert to DataFrame
        df = pd.DataFrame({
            'timestamp': self._resource_data['timestamps'],
            'memory_mb': [m / (1024 * 1024) for m in self._resource_data['memory_usage']],
            'disk_reads_mb': [r / (1024 * 1024) for r in self._resource_data['disk_reads']],
            'disk_writes_mb': [w / (1024 * 1024) for w in self._resource_data['disk_writes']],
            'cpu_percent': self._resource_data['cpu_percent']
        })
        
        # Calculate summary statistics
        summary = pd.DataFrame({
            'metric': ['memory_mb', 'disk_reads_mb', 'disk_writes_mb', 'cpu_percent'],
            'mean': [df['memory_mb'].mean(), df['disk_reads_mb'].mean(), 
                    df['disk_writes_mb'].mean(), df['cpu_percent'].mean()],
            'max': [df['memory_mb'].max(), df['disk_reads_mb'].max(), 
                   df['disk_writes_mb'].max(), df['cpu_percent'].max()],
            'total': [None, df['disk_reads_mb'].sum(), df['disk_writes_mb'].sum(), None]
        })
        
        # Get DuckDB stats if available
        try:
            duckdb_stats = self.conn.execute("PRAGMA database_size").fetchdf()
            self._resource_data['duckdb_stats'] = duckdb_stats
        except Exception as e:
            warnings.warn(f"Failed to get DuckDB database stats: {e}")
        
        return {
            'time_series': df,
            'summary': summary,
            'duckdb_stats': self._resource_data.get('duckdb_stats')
        }