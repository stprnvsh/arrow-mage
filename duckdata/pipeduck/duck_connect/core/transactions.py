"""
Transaction management for DuckConnect

This module provides transaction and locking functionality for DuckDB.
"""

import time
import os
import uuid
import threading
import logging
from contextlib import contextmanager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.transactions')

class TransactionManager:
    """
    Manages transactions and locking for DuckDB
    """
    
    def __init__(self, connection_pool):
        """
        Initialize the transaction manager
        
        Args:
            connection_pool: Connection pool to use for transactions
        """
        self.connection_pool = connection_pool
        self.transaction_lock = threading.RLock()
        self.in_transaction = False
    
    @contextmanager
    def transaction(self):
        """
        Start a transaction for multiple operations that should be atomic.
        
        Usage:
            with transaction_manager.transaction():
                # Perform operations
        """
        with self.transaction_lock:
            if self.in_transaction:
                # Nested transaction, just yield
                yield
                return
                
            self.in_transaction = True
            
        conn = None
        try:
            with self.connection_pool.get_connection() as conn:
                # Start transaction
                conn.execute("BEGIN TRANSACTION")
                
                # Provide the connection
                yield
                
                # Commit if no exceptions
                conn.execute("COMMIT")
        except Exception as e:
            # Rollback on exception
            if conn:
                try:
                    conn.execute("ROLLBACK")
                except:
                    pass
            raise e
        finally:
            with self.transaction_lock:
                self.in_transaction = False

class LockManager:
    """
    Manages locks for datasets to ensure data integrity
    """
    
    def __init__(self, connection_pool):
        """
        Initialize the lock manager
        
        Args:
            connection_pool: Connection pool to use for locking
        """
        self.connection_pool = connection_pool
        
        # Create the locks table if it doesn't exist
        with self.connection_pool.get_connection() as conn:
            conn.execute("""
            CREATE TABLE IF NOT EXISTS duck_connect_locks (
                dataset_id TEXT PRIMARY KEY,
                lock_type TEXT, -- 'read', 'write', 'exclusive'
                owner TEXT,     -- Language or process ID
                acquired_at TIMESTAMP,
                expires_at TIMESTAMP
            )
            """)
    
    def acquire_lock(self, dataset_id, lock_type='write', timeout=10):
        """
        Acquire a lock on a dataset
        
        Args:
            dataset_id: Dataset ID or name
            lock_type: 'read', 'write', or 'exclusive'
            timeout: Maximum time to wait for lock in seconds
            
        Returns:
            True if lock acquired, False otherwise
        """
        start_time = time.time()
        owner = f"python-{os.getpid()}-{threading.get_ident()}"
        
        # Resolve name to ID if necessary
        if not dataset_id.startswith('duck_'):
            with self.connection_pool.get_connection() as conn:
                result = conn.execute(
                    "SELECT id FROM duck_connect_metadata WHERE name = ?", 
                    [dataset_id]
                ).fetchone()
                if result:
                    dataset_id = result[0]
                    
        # Try to acquire lock
        while (time.time() - start_time) < timeout:
            with self.connection_pool.get_connection() as conn:
                # Check existing locks
                if lock_type == 'read':
                    # For read locks, only check for exclusive locks
                    result = conn.execute(
                        "SELECT lock_type FROM duck_connect_locks WHERE dataset_id = ? AND lock_type = 'exclusive'",
                        [dataset_id]
                    ).fetchone()
                    
                    if not result:
                        # No exclusive lock, we can acquire a read lock
                        conn.execute(
                            "INSERT OR REPLACE INTO duck_connect_locks VALUES (?, ?, ?, datetime('now'), datetime('now', '+5 minutes'))",
                            [dataset_id, lock_type, owner]
                        )
                        return True
                else:
                    # For write/exclusive locks, check for any locks
                    result = conn.execute(
                        "SELECT lock_type FROM duck_connect_locks WHERE dataset_id = ?",
                        [dataset_id]
                    ).fetchone()
                    
                    if not result:
                        # No locks, we can acquire a write/exclusive lock
                        conn.execute(
                            "INSERT OR REPLACE INTO duck_connect_locks VALUES (?, ?, ?, datetime('now'), datetime('now', '+5 minutes'))",
                            [dataset_id, lock_type, owner]
                        )
                        return True
            
            # Wait before retrying
            time.sleep(0.1)
            
        # Timeout
        return False
        
    def release_lock(self, dataset_id):
        """
        Release a lock on a dataset
        
        Args:
            dataset_id: Dataset ID or name
        """
        # Resolve name to ID if necessary
        if not dataset_id.startswith('duck_'):
            with self.connection_pool.get_connection() as conn:
                result = conn.execute(
                    "SELECT id FROM duck_connect_metadata WHERE name = ?", 
                    [dataset_id]
                ).fetchone()
                if result:
                    dataset_id = result[0]
        
        # Release lock
        with self.connection_pool.get_connection() as conn:
            conn.execute(
                "DELETE FROM duck_connect_locks WHERE dataset_id = ?",
                [dataset_id]
            )

    @contextmanager
    def dataset_lock(self, dataset_id, lock_type='write', timeout=10):
        """
        Context manager for locking a dataset
        
        Args:
            dataset_id: Dataset ID or name
            lock_type: 'read', 'write', or 'exclusive'
            timeout: Maximum time to wait for lock in seconds
            
        Usage:
            with lock_manager.dataset_lock('my_dataset', lock_type='write'):
                # Perform operations
        """
        if not self.acquire_lock(dataset_id, lock_type, timeout):
            raise TimeoutError(f"Failed to acquire {lock_type} lock on dataset {dataset_id}")
            
        try:
            yield
        finally:
            self.release_lock(dataset_id) 