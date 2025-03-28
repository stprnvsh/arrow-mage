"""
Connection management for DuckConnect

This module provides connection pooling and management for DuckDB connections.
"""
import os
import atexit
import logging
import threading
import queue
import duckdb
from contextlib import contextmanager

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.connection')

# Define connection lock for thread safety
_connection_lock = threading.RLock()

class ConnectionPool:
    """
    A connection pool for DuckDB connections to enable thread-safe concurrent access
    """
    
    def __init__(self, db_path, max_connections=10, init_connections=2):
        """
        Initialize a connection pool for DuckDB
        
        Args:
            db_path: Path to the DuckDB database
            max_connections: Maximum number of connections in the pool
            init_connections: Initial number of connections to create
        """
        self.db_path = db_path
        self.max_connections = max_connections
        self.connection_queue = queue.Queue(maxsize=max_connections)
        self.active_connections = set()
        self.lock = threading.RLock()
        
        # Create initial connections
        for _ in range(min(init_connections, max_connections)):
            conn = self._create_new_connection()
            self.connection_queue.put(conn)
            
        # Register cleanup for program exit
        atexit.register(self.close_all)
    
    def _create_new_connection(self):
        """Create a new DuckDB connection with optimized settings"""
        conn = duckdb.connect(self.db_path)
        
        # Configure DuckDB for handling large datasets
        try:
            # Set memory limit (80% of available RAM)
            conn.execute("PRAGMA memory_limit='80%'")
            
            # Enable parallelism
            conn.execute("PRAGMA force_parallelism")
            
            # Enable object cache for better performance
            conn.execute("PRAGMA enable_object_cache")
            
            # Install and load Arrow extension
            conn.execute("INSTALL arrow")
            conn.execute("LOAD arrow")
        except Exception as e:
            logger.warning(f"Failed to configure DuckDB performance settings: {e}")
            
        return conn
    
    @contextmanager
    def get_connection(self):
        """
        Get a connection from the pool (context manager)
        
        Returns:
            A DuckDB connection
        """
        conn = None
        try:
            # Try to get a connection from the queue
            try:
                conn = self.connection_queue.get(block=False)
                logger.debug("Obtained existing connection from pool")
            except queue.Empty:
                # If the queue is empty, create a new connection if allowed
                with self.lock:
                    if len(self.active_connections) < self.max_connections:
                        conn = self._create_new_connection()
                        logger.debug("Created new connection for pool")
                    else:
                        # Wait for a connection to become available
                        logger.debug("Waiting for connection from pool")
                        conn = self.connection_queue.get(block=True)
            
            # Add to active connections
            with self.lock:
                self.active_connections.add(conn)
                
            yield conn
            
        finally:
            # Return the connection to the pool
            if conn:
                with self.lock:
                    if conn in self.active_connections:
                        self.active_connections.remove(conn)
                self.connection_queue.put(conn)
                
    def close_all(self):
        """Close all connections in the pool"""
        with self.lock:
            # Close active connections
            for conn in self.active_connections:
                try:
                    conn.close()
                except:
                    pass
            self.active_connections.clear()
            
            # Close queued connections
            while not self.connection_queue.empty():
                try:
                    conn = self.connection_queue.get(block=False)
                    conn.close()
                except:
                    pass

def get_optimized_connection(db_path):
    """
    Create an optimized standalone DuckDB connection
    
    Args:
        db_path: Path to the DuckDB database
        
    Returns:
        An optimized DuckDB connection
    """
    conn = duckdb.connect(db_path)
    
    # Configure DuckDB for handling large datasets
    try:
        # Set memory limit (80% of available RAM)
        conn.execute("PRAGMA memory_limit='80%'")
        
        # Enable parallelism
        conn.execute("PRAGMA force_parallelism")
        
        # Enable object cache for better performance
        conn.execute("PRAGMA enable_object_cache")
        
        # Install and load Arrow extension
        conn.execute("INSTALL arrow")
        conn.execute("LOAD arrow")
    except Exception as e:
        logger.warning(f"Failed to configure DuckDB performance settings: {e}")
        
    return conn 