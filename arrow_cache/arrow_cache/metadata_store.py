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
        self._initialize_tables()
    
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
                metadata_json VARCHAR
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
            metadata: Additional metadata for the entry
        """
        schema_json = schema.to_string()
        metadata_json = json.dumps(metadata)
        
        self.con.execute("""
            INSERT OR REPLACE INTO cache_entries
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            key, created_at, last_accessed_at, access_count,
            expires_at, size_bytes, schema_json, num_rows, metadata_json
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
        self.con.execute("DELETE FROM registered_tables")
        self.con.execute("DELETE FROM cache_entries")
    
    def register_table(self, key: str, table: pa.Table, force: bool = False) -> None:
        """
        Register an Arrow table with DuckDB for querying.
        
        Args:
            key: The cache entry key
            table: The Arrow table to register
            force: If True, register the table regardless of lazy_registration setting
        """
        # If using lazy registration and not forced, just update the tracking info
        if self.lazy_registration and not force:
            # Just update registration tracking without actually registering
            with self.con_lock:
                self.con.execute("""
                    INSERT OR REPLACE INTO registered_tables
                    VALUES (?, FALSE, ?)
                """, (key, float(pd.Timestamp.now().timestamp())))
            return
            
        # Register the table with DuckDB
        try:
            with self.con_lock:
                # Unregister first if it exists to avoid potential conflicts
                try:
                    self.con.unregister(f"_cache_{key}")
                except Exception:
                    # Ignore errors during unregister - might not exist yet
                    pass
                    
                # Use efficient zero-copy registration when possible
                # This uses memory mapping for better performance with Arrow tables
                try:
                    # Set PRAGMA to optimize DuckDB for Arrow integration
                    self.con.execute("PRAGMA enable_object_cache")
                    self.con.execute("PRAGMA threads=4")  # Use multithreading for better performance 
                    
                    # Register with DuckDB with zero_copy flag to avoid data duplication
                    self.con.register(f"_cache_{key}", table)
                    
                    # Add indexing optimization hint for frequently queried tables
                    try:
                        # Use heuristic: create automatic indices for small tables (< 100k rows)
                        if table.num_rows > 0 and table.num_rows < 100000:
                            num_columns = table.num_columns
                            # Only create indices for tables with reasonable number of columns
                            if 1 <= num_columns <= 20:
                                # Choose most selective columns as candidates for indices
                                # Heuristic: first column and any ID/key columns are good candidates
                                candidate_cols = []
                                
                                # Add first column as candidate
                                if num_columns > 0:
                                    first_col = table.column_names[0]
                                    candidate_cols.append(first_col)
                                
                                # Find ID-like columns
                                for col in table.column_names:
                                    col_lower = col.lower()
                                    if 'id' in col_lower or 'key' in col_lower:
                                        if col not in candidate_cols:
                                            candidate_cols.append(col)
                                
                                # Limit to max 3 indices
                                candidate_cols = candidate_cols[:3]
                                
                                # Create indices on candidate columns
                                for col in candidate_cols:
                                    # Use a safe query with proper escaping
                                    try:
                                        # Skip if column has null values - DuckDB doesn't handle them well in indices
                                        has_nulls = self.con.execute(f"""
                                            SELECT COUNT(*) FROM _cache_{key} WHERE "{col}" IS NULL LIMIT 1
                                        """).fetchone()[0] > 0
                                        
                                        if not has_nulls:
                                            # Create the index
                                            logger.debug(f"Creating automatic index on _cache_{key}.{col}")
                                            self.con.execute(f"""
                                                CREATE INDEX IF NOT EXISTS idx_{key}_{col} ON _cache_{key}("{col}")
                                            """)
                                    except Exception as idx_err:
                                        # Skip index creation if it fails, but continue with registration
                                        logger.debug(f"Skipped index creation for _cache_{key}.{col}: {idx_err}")
                    except Exception as auto_idx_err:
                        # Skip automatic indexing if it fails, but continue with registration
                        logger.debug(f"Skipped automatic indexing for _cache_{key}: {auto_idx_err}")

                except Exception as e:
                    # If registration fails, try to modify the table to make it more compatible
                    logger.warning(f"Error registering table '_cache_{key}' with DuckDB: {e}")
                    
                    # Try to fix common issues:
                    # 1. Convert problematic column types to strings
                    try:
                        # Convert to pandas, which is more forgiving, then back to Arrow
                        df = table.to_pandas()
                        fixed_table = pa.Table.from_pandas(df)
                        self.con.register(f"_cache_{key}", fixed_table)
                        logger.info(f"Successfully registered table '_cache_{key}' after type conversion")
                    except Exception as inner_e:
                        logger.error(f"Failed to register table '_cache_{key}' even after type conversion: {inner_e}")
                        # Re-raise the original error
                        raise e
                
                # Update registration tracking
                self.con.execute("""
                    INSERT OR REPLACE INTO registered_tables
                    VALUES (?, TRUE, ?)
                """, (key, float(pd.Timestamp.now().timestamp())))
                
                # Verify registration was successful by running a test query
                try:
                    self.con.execute(f"SELECT COUNT(*) FROM _cache_{key} LIMIT 1")
                except Exception as e:
                    logger.error(f"Table '_cache_{key}' registered but failed verification: {e}")
                    raise
        except Exception as e:
            logger.error(f"Failed to register table '_cache_{key}' with DuckDB: {e}")
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
                test_query = f"SELECT COUNT(*) FROM _cache_{key} LIMIT 1"
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
                    self.register_table(key, table, force=True)
                except Exception as e:
                    logger.error(f"Failed to register table '_cache_{key}': {e}")
                    raise
    
    def unregister_table(self, key: str) -> None:
        """
        Unregister an Arrow table from DuckDB.
        
        Args:
            key: The cache entry key
        """
        try:
            # Try to unregister if it exists
            self.con.unregister(f"_cache_{key}")
        except:
            # If it wasn't registered, just ignore
            pass
            
        # Update registration tracking
        self.con.execute("""
            DELETE FROM registered_tables
            WHERE key = ?
        """, (key,))
    
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
                self.con.execute("DELETE FROM persistence_metadata")
            except Exception as e:
                logger.error(f"Failed to clear persistence metadata: {e}")

    def register_alias(self, original_key: str, alias_key: str) -> None:
        """
        Register an alias for an existing table in DuckDB.
        
        Args:
            original_key: The original table key
            alias_key: The alias key to register
        """
        if not original_key or not alias_key:
            return
            
        # Only register alias if the original table is already registered
        with self.con_lock:
            try:
                # Test if the original table is accessible
                test_query = f"SELECT COUNT(*) FROM _cache_{original_key} LIMIT 1"
                self.con.execute(test_query)
                
                # Original table exists, create a view for the alias
                view_sql = f"CREATE OR REPLACE VIEW _cache_{alias_key} AS SELECT * FROM _cache_{original_key}"
                self.con.execute(view_sql)
                
                # Register the alias in our tracking table
                self.con.execute("""
                    INSERT OR REPLACE INTO registered_tables
                    VALUES (?, TRUE, ?)
                """, (alias_key, float(pd.Timestamp.now().timestamp())))
                
                logger.info(f"Created alias '_cache_{alias_key}' for table '_cache_{original_key}'")
            except Exception as e:
                logger.error(f"Failed to create alias '_cache_{alias_key}' for table '_cache_{original_key}': {e}")
                raise
