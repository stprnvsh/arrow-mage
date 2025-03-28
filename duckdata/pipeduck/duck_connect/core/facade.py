"""
DuckConnect facade class

This module provides the main DuckConnect class that integrates all the modules and
presents a unified interface to the user.
"""

import os
import time
import uuid
import json
import logging
import pandas as pd
import pyarrow as pa
from typing import Dict, List, Any, Optional, Union

from ..core.connection import ConnectionPool
from ..core.transactions import TransactionManager, LockManager
from ..core.metadata import MetadataManager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect')

class DuckConnect:
    """
    DuckConnect: Optimized cross-language data sharing with DuckDB
    
    This class provides a way to share data between Python, R, and Julia
    by leveraging DuckDB as a shared memory database with metadata.
    """
    
    def __init__(self, db_path="duck_connect.duckdb", pool_size=5):
        """
        Initialize DuckConnect with a database path.
        
        Args:
            db_path: Path to the DuckDB database file
            pool_size: Size of the connection pool for concurrent access
        """
        self.db_path = db_path
        
        # Create connection pool for thread safety
        self.connection_pool = ConnectionPool(db_path, max_connections=pool_size)
        
        # For backward compatibility
        self.conn = self._get_main_connection()
        
        # Initialize managers
        self.transaction_manager = TransactionManager(self.connection_pool)
        self.lock_manager = LockManager(self.connection_pool)
        self.metadata_manager = MetadataManager(self.connection_pool)
        
        # Check Arrow availability
        self.arrow_available = self._check_arrow_availability()
    
    def _check_arrow_availability(self):
        """Check if Arrow extension is available"""
        with self.connection_pool.get_connection() as conn:
            try:
                conn.execute("SELECT arrow_version()")
                return True
            except:
                return False
    
    def _get_main_connection(self):
        """Get a dedicated connection for backward compatibility"""
        conn = None
        with self.connection_pool.get_connection() as c:
            # Clone the connection
            conn = c
        return conn
    
    @property
    def transaction(self):
        """Get the transaction context manager"""
        return self.transaction_manager.transaction
    
    def register_dataset(self, 
                        data: Union[pd.DataFrame, pa.Table, str], 
                        name: str, 
                        description: Optional[str] = None,
                        source_language: str = "python",
                        available_to_languages: Optional[List[str]] = None,
                        overwrite: bool = False,
                        partition_by: Optional[str] = None,
                        partition_values: Optional[List[Any]] = None,
                        streaming: bool = False,
                        chunk_size: int = 100000,
                        owner: Optional[str] = None,
                        is_encrypted: bool = False,
                        access_control: Optional[Dict] = None) -> str:
        """
        Register a dataset with DuckConnect.
        
        Instead of copying data, this creates or references a table in DuckDB and creates metadata.
        
        Args:
            data: DataFrame or Arrow Table to register, or a SQL query string
            name: Name for the dataset
            description: Optional description
            source_language: Source language (python, r, julia)
            available_to_languages: List of languages that can access this dataset
            overwrite: Whether to overwrite an existing dataset with the same name
            partition_by: Column to partition the data by
            partition_values: List of partition values for the partition_by column
            streaming: Whether to process in streaming mode for large datasets
            chunk_size: Number of rows to process at once in streaming mode
            owner: Owner of the dataset for access control
            is_encrypted: Whether the data should be encrypted
            access_control: Dictionary of access control rules
            
        Returns:
            dataset_id: ID of the registered dataset
        """
        from ..datasets.registry import register_dataset
        # Import the dataset registry module here to avoid circular imports
        
        # Call the dataset registration function
        return register_dataset(
            self.connection_pool,
            self.metadata_manager,
            self.transaction_manager,
            data=data,
            name=name,
            description=description,
            source_language=source_language,
            available_to_languages=available_to_languages,
            overwrite=overwrite,
            partition_by=partition_by,
            partition_values=partition_values,
            streaming=streaming,
            chunk_size=chunk_size,
            owner=owner,
            is_encrypted=is_encrypted,
            access_control=access_control
        )
    
    def get_dataset(self, 
                   identifier: str, 
                   language: str = "python",
                   as_arrow: bool = False,
                   columns: Optional[List[str]] = None,
                   filters: Optional[Dict] = None,
                   limit: Optional[int] = None,
                   streaming: bool = False,
                   chunk_size: int = 100000) -> Union[pd.DataFrame, pa.Table, None, Any]:
        """
        Get a dataset by ID or name.
        
        Args:
            identifier: Dataset ID or name
            language: Language requesting the dataset
            as_arrow: Whether to return as Arrow Table (True) or pandas DataFrame (False)
            columns: List of columns to select (None for all)
            filters: Dictionary of filters to apply
            limit: Maximum number of rows to return
            streaming: Whether to return a generator for large datasets
            chunk_size: Size of chunks for streaming
            
        Returns:
            Dataset as pandas DataFrame, Arrow Table, or generator of chunks
        """
        from ..datasets.registry import get_dataset
        # Import the dataset registry module here to avoid circular imports
        
        # Acquire read lock
        self.lock_manager.acquire_lock(identifier, lock_type='read')
        
        try:
            # Call the dataset retrieval function
            return get_dataset(
                self.connection_pool,
                self.metadata_manager,
                identifier=identifier,
                language=language,
                as_arrow=as_arrow,
                columns=columns,
                filters=filters,
                limit=limit,
                streaming=streaming,
                chunk_size=chunk_size
            )
        finally:
            # Release read lock
            self.lock_manager.release_lock(identifier)
    
    def update_dataset(self, 
                      identifier: str,
                      data: Union[pd.DataFrame, pa.Table, str],
                      language: str = "python",
                      description: Optional[str] = None) -> str:
        """
        Update an existing dataset.
        
        Args:
            identifier: Dataset ID or name
            data: New data to replace the existing data
            language: Language performing the update
            description: Optional updated description
            
        Returns:
            Dataset ID of the updated dataset
        """
        from ..datasets.registry import update_dataset
        # Import the dataset registry module here to avoid circular imports
        
        # Acquire write lock
        self.lock_manager.acquire_lock(identifier, lock_type='write')
        
        try:
            # Call the dataset update function
            return update_dataset(
                self.connection_pool,
                self.metadata_manager,
                self.transaction_manager,
                identifier=identifier,
                data=data,
                language=language,
                description=description
            )
        finally:
            # Release write lock
            self.lock_manager.release_lock(identifier)
    
    def delete_dataset(self, identifier: str, language: str = "python") -> bool:
        """
        Delete a dataset.
        
        Args:
            identifier: Dataset ID or name
            language: Language performing the deletion
            
        Returns:
            True if successful
        """
        from ..datasets.registry import delete_dataset
        # Import the dataset registry module here to avoid circular imports
        
        # Acquire exclusive lock
        self.lock_manager.acquire_lock(identifier, lock_type='exclusive')
        
        try:
            # Call the dataset deletion function
            return delete_dataset(
                self.connection_pool,
                self.metadata_manager,
                identifier=identifier,
                language=language
            )
        finally:
            # Release exclusive lock
            self.lock_manager.release_lock(identifier)
    
    def list_datasets(self, language: str = "python") -> pd.DataFrame:
        """
        List all datasets available to a language.
        
        Args:
            language: Language requesting the list
            
        Returns:
            DataFrame with dataset information
        """
        return self.metadata_manager.list_datasets(language=language)
    
    def execute_query(self, 
                     query: str, 
                     language: str = "python",
                     as_arrow: bool = False) -> Union[pd.DataFrame, pa.Table]:
        """
        Execute a SQL query on the DuckDB database.
        
        Args:
            query: SQL query to execute
            language: Language executing the query
            as_arrow: Whether to return as Arrow Table
            
        Returns:
            DataFrame or Arrow Table with query results
        """
        start_time = time.time()
        
        try:
            # Execute query
            with self.connection_pool.get_connection() as conn:
                if as_arrow and self.arrow_available:
                    result = conn.execute(query).fetch_arrow_table()
                else:
                    result = conn.execute(query).fetchdf()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise
        
        # Record transaction
        self.metadata_manager.record_transaction(
            dataset_id='N/A', 
            operation='query', 
            language=language, 
            details={'query': query[:1000], 'as_arrow': as_arrow}
        )
        
        # Record statistics
        duration_ms = (time.time() - start_time) * 1000
        row_count = len(result) if isinstance(result, pd.DataFrame) else result.num_rows
        column_count = len(result.columns) if isinstance(result, pd.DataFrame) else result.num_columns
        
        stats_id = str(uuid.uuid4())
        with self.connection_pool.get_connection() as conn:
            conn.execute("""
            INSERT INTO duck_connect_stats (
                id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count
            ) VALUES (?, ?, ?, ?, datetime('now'), ?, ?, ?, ?)
            """, [
                stats_id, 'N/A', 'query', language, 
                duration_ms, 0.0, row_count, column_count
            ])
        
        return result
    
    def get_dataset_lineage(self, identifier: str) -> Dict:
        """
        Get lineage information for a dataset.
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            Dictionary with upstream and downstream lineage
        """
        # Get dataset ID if name is provided
        metadata = self.metadata_manager.get_dataset_metadata(identifier)
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
        
        # Get lineage information
        return self.metadata_manager.get_dataset_lineage(metadata['id'])
    
    def close(self):
        """Close all connections"""
        if self.connection_pool:
            self.connection_pool.close_all()
            
    # Security features
    def add_user(self, username: str, password: str, role: str = "user", permissions: Optional[Dict] = None):
        """
        Add a user for authentication and authorization
        
        Args:
            username: Username
            password: Password (will be hashed)
            role: Role (admin, user, etc.)
            permissions: Dictionary of permissions
        """
        from ..security.auth import add_user
        # Import the auth module here to avoid circular imports
        
        return add_user(
            self.connection_pool,
            username=username,
            password=password,
            role=role,
            permissions=permissions
        )
    
    def authenticate(self, username: str, password: str) -> bool:
        """
        Authenticate a user
        
        Args:
            username: Username
            password: Password
            
        Returns:
            True if authentication successful, False otherwise
        """
        from ..security.auth import authenticate
        # Import the auth module here to avoid circular imports
        
        return authenticate(
            self.connection_pool,
            username=username,
            password=password
        )
        
    def encrypt_dataset(self, identifier: str, encryption_key: str, language: str = "python") -> str:
        """
        Encrypt a dataset
        
        Args:
            identifier: Dataset ID or name
            encryption_key: Encryption key
            language: Language performing the operation
            
        Returns:
            ID of the encrypted dataset
        """
        from ..security.encryption import encrypt_dataset
        # Import the encryption module here to avoid circular imports
        
        return encrypt_dataset(
            self.connection_pool,
            self.metadata_manager,
            self.lock_manager,
            identifier=identifier,
            encryption_key=encryption_key,
            language=language
        )
        
    def decrypt_dataset(self, identifier: str, encryption_key: str, language: str = "python") -> Union[pd.DataFrame, None]:
        """
        Decrypt a dataset
        
        Args:
            identifier: Dataset ID or name
            encryption_key: Encryption key
            language: Language performing the operation
            
        Returns:
            Decrypted DataFrame or None if not encrypted
        """
        from ..security.encryption import decrypt_dataset
        # Import the encryption module here to avoid circular imports
        
        return decrypt_dataset(
            self.connection_pool,
            self.metadata_manager,
            self.lock_manager,
            identifier=identifier,
            encryption_key=encryption_key,
            language=language
        )
        
    def set_access_control(self, identifier: str, access_rules: Dict, language: str = "python") -> bool:
        """
        Set access control rules for a dataset
        
        Args:
            identifier: Dataset ID or name
            access_rules: Dictionary of access control rules
            language: Language performing the operation
            
        Returns:
            True if successful
        """
        from ..security.access_control import set_access_control
        # Import the access_control module here to avoid circular imports
        
        return set_access_control(
            self.connection_pool,
            self.metadata_manager,
            self.lock_manager,
            identifier=identifier,
            access_rules=access_rules,
            language=language
        )
        
    def can_access_dataset(self, identifier: str, username: str, operation: str = 'read') -> bool:
        """
        Check if a user can access a dataset
        
        Args:
            identifier: Dataset ID or name
            username: Username
            operation: Operation (read, write, delete)
            
        Returns:
            True if access is allowed
        """
        from ..security.access_control import can_access_dataset
        # Import the access_control module here to avoid circular imports
        
        return can_access_dataset(
            self.connection_pool,
            identifier=identifier,
            username=username,
            operation=operation
        ) 