import duckdb
import pandas as pd
import pyarrow as pa
import json
import os
from typing import Dict, List, Optional, Any, Tuple
import tempfile
import threading
import time
import logging
import uuid

logger = logging.getLogger(__name__)

class MetadataStore:
    """
    Manages metadata for cache entries using DuckDB.
    """
    def __init__(self, db_path: Optional[str] = None, lazy_registration: bool = False):
        """
        Initialize the metadata store.
        
        Args:
            db_path: Path to the DuckDB database file. If None, uses an in-memory database.
            lazy_registration: If True, tables are only registered with DuckDB when queried.
        """
        self.db_path = db_path or ':memory:'
        self.con = duckdb.connect(self.db_path)
        self.con_lock = threading.RLock()
        self.lazy_registration = lazy_registration
        self._initialize_extensions()
        self._initialize_tables()
    
    def _initialize_extensions(self) -> None:
        """Install and load required DuckDB extensions.
        
        This is called during MetadataStore initialization.
        """
        # Attempt to install and load the spatial extension
        try:
            logger.info("Attempting to install DuckDB spatial extension...")
            self.con.execute("INSTALL spatial;")
            logger.info("Spatial extension installed (or already present).")
            
            logger.info("Attempting to load DuckDB spatial extension...")
            self.con.execute("LOAD spatial;")
            logger.info("Spatial extension loaded successfully.")
        except Exception as e:
            logger.warning(f"Failed to install or load DuckDB spatial extension: {e}. "
                           f"Spatial functions may not be available.")
            # Continue without the extension if it fails
            
    def _initialize_tables(self) -> None:
        """Create metadata tables if they don't exist."""
        # Cache entries table stores basic information about each cached item
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS cache_entries (
                key VARCHAR PRIMARY KEY,
                created_at DOUBLE,
                last_accessed_at DOUBLE,
                access_count INTEGER,
                expires_at DOUBLE,
                size_bytes BIGINT,
                schema_json VARCHAR,
                num_rows BIGINT,
                metadata_json VARCHAR,
                geometry_column VARCHAR,
                geometry_type VARCHAR,
                crs_info VARCHAR,
                storage_format VARCHAR,
                column_types_json VARCHAR,
                column_duckdb_types_json VARCHAR,
                column_null_counts_json VARCHAR,
                column_chunk_counts_json VARCHAR,
                column_dictionary_encoded_json VARCHAR,
                column_min_max_json VARCHAR
            )
        """)
        
        # Register tables lookup tracks what tables are registered with DuckDB
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS registered_tables (
                key VARCHAR PRIMARY KEY,
                is_registered BOOLEAN,
                last_registered_at DOUBLE,
                FOREIGN KEY (key) REFERENCES cache_entries(key)
            )
        """)
        
        # Create table for persistence metadata
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS persistence_metadata (
                key VARCHAR PRIMARY KEY,
                storage_path VARCHAR,
                is_partitioned BOOLEAN,
                partition_count INTEGER,
                created_at DOUBLE,
                metadata JSON
            )
        """)
        
        # Make sure we can use pragma_database_size to track metadata DB size
        self.con.execute("PRAGMA enable_progress_bar")
        self.con.execute("PRAGMA enable_object_cache")
    
    def add_entry(
        self,
        key: str,
        created_at: float,
        last_accessed_at: float,
        access_count: int,
        expires_at: Optional[float],
        size_bytes: int,
        schema: pa.Schema,
        num_rows: int,
        metadata: Dict[str, Any]
    ) -> None:
        """
        Add a new cache entry to the metadata store.
        
        Args:
            key: Unique identifier for the cache entry
            created_at: Timestamp when the entry was created
            last_accessed_at: Timestamp when the entry was last accessed
            access_count: Number of times the entry has been accessed
            expires_at: Timestamp when the entry expires (None for no expiration)
            size_bytes: Size of the entry in bytes
            schema: Arrow schema of the cached table
            num_rows: Number of rows in the cached table
            metadata: Additional metadata for the entry (including geometry info and types)
        """
        schema_json = schema.to_string()
        # Ensure metadata is stored as JSON string, handle None
        metadata_json = json.dumps(metadata) if metadata else 'null'

        # Extract values from metadata, providing defaults
        geometry_column = metadata.get('geometry_column')
        geometry_type = metadata.get('geometry_type') # This might be added elsewhere or already present
        crs_info = metadata.get('crs_info') # This might be added elsewhere or already present
        storage_format = metadata.get('storage_format')
        column_types_json = metadata.get('column_types_json')
        column_duckdb_types_json = metadata.get('column_duckdb_types_json')
        column_null_counts_json = metadata.get('column_null_counts_json')
        column_chunk_counts_json = metadata.get('column_chunk_counts_json')
        column_dictionary_encoded_json = metadata.get('column_dictionary_encoded_json')
        column_min_max_json = metadata.get('column_min_max_json')

        self.con.execute("""
            INSERT OR REPLACE INTO cache_entries (
                key, created_at, last_accessed_at, access_count, expires_at,
                size_bytes, schema_json, num_rows, metadata_json,
                geometry_column, geometry_type, crs_info, storage_format,
                column_types_json, column_duckdb_types_json,
                column_null_counts_json, column_chunk_counts_json,
                column_dictionary_encoded_json, column_min_max_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            key, created_at, last_accessed_at, access_count,
            expires_at, size_bytes, schema_json, num_rows, metadata_json,
            geometry_column, geometry_type, crs_info, storage_format, # Added storage_format
            column_types_json, column_duckdb_types_json, # Added duckdb types
            column_null_counts_json, column_chunk_counts_json,
            column_dictionary_encoded_json, column_min_max_json
        ))
    
    def update_access_stats(self, key: str, last_accessed_at: float, access_count: int) -> None:
        """
        Update access statistics for a cache entry.
        
        Args:
            key: The cache entry key
            last_accessed_at: New last accessed timestamp
            access_count: New access count
        """
        self.con.execute("""
            UPDATE cache_entries
            SET last_accessed_at = ?, access_count = ?
            WHERE key = ?
        """, (last_accessed_at, access_count, key))
    
    def get_entry_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get metadata for a specific cache entry.
        
        Args:
            key: The cache entry key
            
        Returns:
            Entry metadata or None if the entry doesn't exist
        """
        result = self.con.execute("""
            SELECT * FROM cache_entries WHERE key = ?
        """, (key,)).fetchone()
        
        if not result:
            return None
            
        columns = [col[0] for col in self.con.description]
        result_dict = {columns[i]: result[i] for i in range(len(columns))}
        
        # Parse JSON fields
        if result_dict.get('metadata_json'):
            result_dict['metadata'] = json.loads(result_dict['metadata_json'])
            del result_dict['metadata_json']
            
        return result_dict
    
    def get_all_entries(self) -> pd.DataFrame:
        """
        Get metadata for all cache entries.
        
        Returns:
            DataFrame containing metadata for all entries
        """
        return self.con.execute("""
            SELECT
                e.key, e.created_at, e.last_accessed_at, e.access_count,
                e.expires_at, e.size_bytes, e.schema_json, e.num_rows,
                e.metadata_json,
                e.geometry_column, e.geometry_type, e.crs_info, e.storage_format,
                e.column_types_json, e.column_duckdb_types_json,
                e.column_null_counts_json, e.column_chunk_counts_json,
                e.column_dictionary_encoded_json, e.column_min_max_json,
                COALESCE(r.is_registered, FALSE) as is_registered,
                r.last_registered_at
            FROM cache_entries e
            LEFT JOIN registered_tables r ON e.key = r.key
        """).fetchdf()
    
    def get_expired_entries(self, current_time: float) -> List[str]:
        """
        Get keys of all expired cache entries.
        
        Args:
            current_time: Current timestamp to compare against expiration time
            
        Returns:
            List of expired entry keys
        """
        return [
            row[0] for row in self.con.execute("""
                SELECT key FROM cache_entries
                WHERE expires_at IS NOT NULL AND expires_at < ?
            """, (current_time,)).fetchall()
        ]
    
    def remove_entry(self, key: str) -> bool:
        """
        Remove a cache entry from the metadata store.
        
        Args:
            key: The cache entry key
            
        Returns:
            True if the entry was removed, False if it didn't exist
        """
        # First check if the entry exists
        exists = self.con.execute("""
            SELECT COUNT(*) FROM cache_entries WHERE key = ?
        """, (key,)).fetchone()[0] > 0
        
        if not exists:
            return False
        
        # First delete from registered_tables to avoid foreign key constraint
        self.con.execute("""
            DELETE FROM registered_tables WHERE key = ?
        """, (key,))
        
        # Then delete from cache_entries
        self.con.execute("""
            DELETE FROM cache_entries WHERE key = ?
        """, (key,))
        
        return True
    
    def clear_all_entries(self) -> None:
        """Remove all cache entries from the metadata store."""
        try:
            # First check if the tables exist
            registered_exists = self.con.execute("""
                SELECT count(*) FROM sqlite_master 
                WHERE type='table' AND name='registered_tables'
            """).fetchone()[0] > 0
            
            entries_exists = self.con.execute("""
                SELECT count(*) FROM sqlite_master 
                WHERE type='table' AND name='cache_entries'
            """).fetchone()[0] > 0
            
            # Only attempt to delete if tables exist
            if registered_exists:
                self.con.execute("DELETE FROM registered_tables")
            else:
                logger.warning("registered_tables table does not exist, nothing to clear")
                
            if entries_exists:
                self.con.execute("DELETE FROM cache_entries")
            else:
                logger.warning("cache_entries table does not exist, nothing to clear")
        except Exception as e:
            logger.error(f"Failed to clear cache entries: {e}")
    
    def register_table(self, key: str, table: pa.Table) -> None:
        """
        Register a table with DuckDB for querying.
        
        Args:
            key: Table key
            table: Arrow table to register
        """
        with self.con_lock:
            try:
                # First unregister if it already exists
                self.unregister_table(key)
                
                # Check if the key contains special characters that need escaping
                needs_quoting = '(' in key or ')' in key or ' ' in key or '-' in key or key.isdigit()
                
                # Register the table with DuckDB
                # For tables with special characters, we need to create a view
                if needs_quoting:
                    # First register the arrow table with a temporary name
                    temp_name = f"temp_{uuid.uuid4().hex[:8]}"
                    self.con.register(temp_name, table)
                    
                    # Then create a view with the proper quoted name
                    # Note: We need to quote the entire view name including the _cache_ prefix
                    view_name = f"_cache_{key}"
                    quoted_view_name = f'"{view_name}"'
                    self.con.execute(f'CREATE OR REPLACE VIEW {quoted_view_name} AS SELECT * FROM {temp_name}')
                    
                    # Create an alias without parentheses if needed
                    if '(' in key and ')' in key:
                        # Create an alias without the parentheses part
                        base_key = key.split('(')[0].rstrip('_')
                        if base_key != key:
                            try:
                                alias_view_name = f'_cache_{base_key}'
                                quoted_alias_name = f'"{alias_view_name}"'
                                self.con.execute(f'CREATE OR REPLACE VIEW {quoted_alias_name} AS SELECT * FROM {quoted_view_name}')
                                logger.info(f"Created alias view {alias_view_name} for table with parentheses {view_name}")
                            except Exception as alias_error:
                                logger.warning(f"Failed to create alias for table with parentheses: {alias_error}")
                    
                    # Unregister the temporary table to clean up
                    try:
                        self.con.unregister(temp_name)
                    except:
                        pass
                else:
                    # For regular table names, use the standard registration
                    table_name = f"_cache_{key}"
                    self.con.register(table_name, table)
                    
                # Initialize registered_tables if it doesn't exist
                if not hasattr(self, 'registered_tables'):
                    self.registered_tables = {}
                
                # Store in our collection
                self.registered_tables[key] = table
                logger.info(f"Successfully registered table for key: {key}")
                
            except Exception as e:
                logger.error(f"Failed to register table with DuckDB: {e}")
                raise
    
    def register_alias(self, orig_key: str, alias_key: str) -> None:
        """
        Register an alias for a table.
        
        Args:
            orig_key: Original table key
            alias_key: Alias key
        """
        with self.con_lock:
            # Check if the keys need quoting
            orig_needs_quoting = '(' in orig_key or ')' in orig_key or ' ' in orig_key or '-' in orig_key or orig_key.isdigit()
            alias_needs_quoting = '(' in alias_key or ')' in alias_key or ' ' in alias_key or '-' in alias_key or alias_key.isdigit()
            
            orig_duckdb_key = f'"{orig_key}"' if orig_needs_quoting else orig_key
            alias_duckdb_key = f'"{alias_key}"' if alias_needs_quoting else alias_key
            
            try:
                # Create a view pointing to the original table
                self.con.execute(f'CREATE OR REPLACE VIEW _cache_{alias_duckdb_key} AS SELECT * FROM _cache_{orig_duckdb_key}')
                
                # Remember the alias
                self.table_aliases[alias_key] = orig_key
            except Exception as e:
                logger.error(f"Failed to register alias: {e}")
                raise
    
    def ensure_registered(self, key: str, table: pa.Table) -> None:
        """
        Ensure that a table is registered with DuckDB for querying.
        If the table is not already registered, register it.
        
        Args:
            key: The cache entry key
            table: The Arrow table to register
        """
        with self.con_lock:
            # First check if already registered
            is_registered = False
            try:
                # Test if the table is accessible with a simple COUNT query
                # Use double quotes around the table name to handle special characters
                quoted_table_name = f'"_cache_{key}"'
                test_query = f"SELECT COUNT(*) FROM {quoted_table_name} LIMIT 1"
                self.con.execute(test_query)
                is_registered = True
            except Exception:
                # If this fails, the table is not properly registered
                is_registered = False
            
            if not is_registered:
                # Table is not accessible, try to register it now
                try:
                    # First check if we have tracking info
                    tracking_info = self.con.execute("""
                        SELECT is_registered FROM registered_tables
                        WHERE key = ?
                    """, (key,)).fetchone()
                    
                    # If tracking says registered but we can't access it, there's an issue
                    if tracking_info and tracking_info[0]:
                        logger.warning(f"Table '_cache_{key}' was marked as registered but not accessible. Re-registering.")
                    
                    # Register the table with force=True to ensure it's registered
                    self.register_table(key, table)
                except Exception as e:
                    logger.error(f"Failed to register table '_cache_{key}': {e}")
                    raise
    
    def unregister_table(self, key: str) -> None:
        """
        Unregister a table from DuckDB.
        
        Args:
            key: Table key
        """
        with self.con_lock:
            try:
                # Check if the key contains special characters that need escaping
                needs_quoting = '(' in key or ')' in key or ' ' in key or '-' in key or key.isdigit()
                
                # Remove the DuckDB view or registration
                if needs_quoting:
                    # Need to quote the entire view name
                    view_name = f"_cache_{key}"
                    quoted_view_name = f'"{view_name}"'
                    try:
                        self.con.execute(f'DROP VIEW IF EXISTS {quoted_view_name}')
                        logger.debug(f"Dropped view {quoted_view_name}")
                    except Exception as e:
                        logger.warning(f"Failed to drop view {quoted_view_name}: {e}")
                    
                    # Also try to remove any alias created for tables with parentheses
                    if '(' in key and ')' in key:
                        base_key = key.split('(')[0].rstrip('_')
                        if base_key != key:
                            try:
                                alias_view_name = f'_cache_{base_key}'
                                quoted_alias_name = f'"{alias_view_name}"'
                                self.con.execute(f'DROP VIEW IF EXISTS {quoted_alias_name}')
                                logger.debug(f"Dropped alias view {quoted_alias_name}")
                            except Exception as alias_error:
                                logger.warning(f"Failed to drop alias view: {alias_error}")
                else:
                    # For regular tables, use standard unregistration
                    table_name = f"_cache_{key}"
                    try:
                        self.con.unregister(table_name)
                        logger.debug(f"Unregistered table {table_name}")
                    except Exception as e:
                        # If unregister fails, try to drop view as a fallback
                        try:
                            self.con.execute(f'DROP VIEW IF EXISTS {table_name}')
                            logger.debug(f"Dropped view {table_name} as fallback")
                        except Exception as view_error:
                            logger.warning(f"Failed to unregister table or drop view {table_name}: {view_error}")
                
                # Initialize registered_tables if it doesn't exist
                if not hasattr(self, 'registered_tables'):
                    self.registered_tables = {}
                    
                # Remove from our internal collection
                if key in self.registered_tables:
                    del self.registered_tables[key]
                    logger.debug(f"Removed table {key} from registered_tables collection")
                    
            except Exception as e:
                logger.warning(f"Error unregistering table: {e}")
                # Continue execution despite errors - best effort cleanup
    
    def query(self, sql: str) -> pa.Table:
        """
        Execute a SQL query against registered tables.
        
        Args:
            sql: SQL query to execute
            
        Returns:
            Arrow table with the query results
        """
        # Execute the query and convert to Arrow table
        result = self.con.execute(sql)
        return result.arrow()
    
    def get_database_size(self) -> int:
        """
        Get the size of the metadata database in bytes.
        
        Returns:
            Size in bytes
        """
        if self.db_path == ':memory:':
            # For memory DBs, use an estimate
            size_info = self.con.execute("PRAGMA database_size").fetchone()
            return size_info[0] if size_info else 0
        else:
            # For file DBs, get the actual file size
            return os.path.getsize(self.db_path)
    
    @property
    def is_closed(self) -> bool:
        """
        Check if the DuckDB connection is closed
        
        Returns:
            True if the connection is closed, False otherwise
        """
        # This is a safer way to check if the connection is closed
        try:
            # Try to execute a simple query
            self.con.execute("SELECT 1")
            return False
        except Exception:
            # If an error occurs, the connection is likely closed
            return True
    
    def close(self) -> None:
        """Close the DuckDB connection."""
        if not self.is_closed:
            self.con.close()
        
    def __del__(self) -> None:
        """Ensure the DuckDB connection is closed on deletion."""
        try:
            self.close()
        except:
            pass

    def add_persistence_metadata(self, key: str, storage_path: str, is_partitioned: bool, 
                               partition_count: int, metadata: Dict[str, Any]) -> None:
        """
        Store persistence metadata in DuckDB
        
        Args:
            key: Cache entry key
            storage_path: Path where data is stored
            is_partitioned: Whether this is a partitioned table
            partition_count: Number of partitions (0 for regular entries)
            metadata: Additional metadata
        """
        with self.con_lock:
            try:
                # Convert metadata to JSON string
                metadata_json = json.dumps(metadata)
                
                # Insert or replace
                self.con.execute("""
                    INSERT OR REPLACE INTO persistence_metadata (
                        key, storage_path, is_partitioned, partition_count, created_at, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?)
                """, (key, storage_path, is_partitioned, partition_count, time.time(), metadata_json))
            except Exception as e:
                logger.error(f"Failed to store persistence metadata: {e}")
                raise
                
    def get_persistence_metadata(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Get persistence metadata for a cache entry
        
        Args:
            key: Cache entry key
            
        Returns:
            Dictionary with persistence metadata or None if not found
        """
        with self.con_lock:
            try:
                result = self.con.execute("""
                    SELECT key, storage_path, is_partitioned, partition_count, created_at, metadata
                    FROM persistence_metadata
                    WHERE key = ?
                """, (key,)).fetchone()
                
                if result:
                    metadata_dict = json.loads(result[5]) if result[5] else {}
                    return {
                        "key": result[0],
                        "storage_path": result[1],
                        "is_partitioned": result[2],
                        "partition_count": result[3],
                        "created_at": result[4],
                        "metadata": metadata_dict
                    }
                return None
            except Exception as e:
                logger.error(f"Failed to retrieve persistence metadata: {e}")
                return None
                
    def remove_persistence_metadata(self, key: str) -> None:
        """
        Remove persistence metadata for a cache entry
        
        Args:
            key: Cache entry key
        """
        with self.con_lock:
            try:
                self.con.execute("DELETE FROM persistence_metadata WHERE key = ?", (key,))
            except Exception as e:
                logger.error(f"Failed to remove persistence metadata: {e}")
                
    def get_all_persistence_metadata(self) -> List[Dict[str, Any]]:
        """
        Get all persistence metadata
        
        Returns:
            List of dictionaries with persistence metadata
        """
        with self.con_lock:
            try:
                # First check if the table exists
                table_exists = self.con.execute("""
                    SELECT count(*) FROM sqlite_master 
                    WHERE type='table' AND name='persistence_metadata'
                """).fetchone()[0] > 0
                
                if not table_exists:
                    logger.warning("persistence_metadata table does not exist, returning empty list")
                    return []
                    
                result = self.con.execute("""
                    SELECT key, storage_path, is_partitioned, partition_count, created_at, metadata
                    FROM persistence_metadata
                """).fetchall()
                
                metadata_list = []
                for row in result:
                    metadata_dict = json.loads(row[5]) if row[5] else {}
                    metadata_list.append({
                        "key": row[0],
                        "storage_path": row[1],
                        "is_partitioned": row[2],
                        "partition_count": row[3],
                        "created_at": row[4],
                        "metadata": metadata_dict
                    })
                return metadata_list
            except Exception as e:
                logger.error(f"Failed to retrieve all persistence metadata: {e}")
                return []
                
    def clear_all_persistence_metadata(self) -> None:
        """
        Clear all persistence metadata
        """
        with self.con_lock:
            try:
                # First check if the table exists
                table_exists = self.con.execute("""
                    SELECT count(*) FROM sqlite_master 
                    WHERE type='table' AND name='persistence_metadata'
                """).fetchone()[0] > 0
                
                if not table_exists:
                    logger.warning("persistence_metadata table does not exist, nothing to clear")
                    return
                    
                self.con.execute("DELETE FROM persistence_metadata")
            except Exception as e:
                logger.error(f"Failed to clear persistence metadata: {e}")
