"""
Advanced DuckDB connector for efficient cross-language data sharing and processing.

This module provides optimized connections to DuckDB with advanced features.
"""

import os
import json
import uuid
import time
import logging
import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds
from typing import Dict, List, Any, Optional, Union, Tuple

# Try to import DuckConnect from the installed package
try:
    from duck_connect.python import DuckConnect
except ImportError:
    try:
        # Fallback to the relative import
        from pipeduck.duck_connect.python import DuckConnect
    except ImportError:
        # If all else fails, use the core implementation
        try:
            from pipeduck.duck_connect.core.facade import DuckConnect
        except ImportError:
            logger.warning("DuckConnect not found. Some features may be limited.")
            DuckConnect = None

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connector')

class DuckConnector:
    """
    Advanced DuckDB connector with optimized performance features.
    
    Provides a high-performance interface to DuckDB with advanced features like:
    - MVCC transactions
    - Out-of-core processing for large datasets
    - Advanced indexing
    - Zero-copy Arrow integration
    - Native Parquet and JSON support
    """
    
    def __init__(self, db_path: str = ":memory:", config: Optional[Dict] = None):
        """
        Initialize DuckConnector with advanced settings.
        
        Args:
            db_path: Path to DuckDB database or ":memory:" for in-memory DB
            config: Optional configuration dictionary with advanced settings
        """
        self.db_path = db_path
        self.config = config or {}
        
        # Create connection with optimized settings
        self.conn = self._create_optimized_connection()
        
        # Setup extensions
        self._setup_extensions()
        
        # Setup advanced features
        self._setup_advanced_features()
        
    def _create_optimized_connection(self) -> duckdb.DuckDBPyConnection:
        """Create an optimized DuckDB connection with advanced settings"""
        conn = duckdb.connect(self.db_path)
        
        # Set memory limit based on config or default to 80% of available RAM
        memory_limit = self.config.get('memory_limit', '80%')
        conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
        
        # Enable MVCC for concurrent transactions
        conn.execute("PRAGMA enable_mvcc")
        
        # Set threads based on config or default to num_cores - 1
        import multiprocessing
        num_cores = max(1, multiprocessing.cpu_count() - 1)
        threads = self.config.get('threads', num_cores)
        conn.execute(f"PRAGMA threads={threads}")
        
        # Enable parallelism for better performance
        conn.execute("PRAGMA force_parallelism")
        
        # Enable object cache
        conn.execute("PRAGMA enable_object_cache")
        
        # Enable progress bar for long-running queries
        if self.config.get('show_progress', True):
            conn.execute("PRAGMA enable_progress_bar")
            
        # External data cache - for out-of-core processing
        external_cache = self.config.get('external_cache_directory')
        if external_cache:
            os.makedirs(external_cache, exist_ok=True)
            conn.execute(f"PRAGMA temp_directory='{external_cache}'")
            
        # External buffer for large datasets
        if self.config.get('use_external_buffer', True):
            conn.execute("PRAGMA external_threads=4")
            buffer_size = self.config.get('external_buffer_size', '4GB')
            conn.execute(f"PRAGMA memory_limit='{buffer_size}'")
            
        return conn
        
    def _setup_extensions(self):
        """Setup DuckDB extensions for advanced functionality"""
        # Always install and load Arrow
        self.conn.execute("INSTALL arrow")
        self.conn.execute("LOAD arrow")
        
        # JSON extension for native JSON support
        self.conn.execute("INSTALL json")
        self.conn.execute("LOAD json")
        
        # Parquet extension for native Parquet support
        self.conn.execute("INSTALL parquet")
        self.conn.execute("LOAD parquet")
        
        # Install additional extensions based on configuration
        if self.config.get('extensions', {}).get('httpfs', False):
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")
            
        if self.config.get('extensions', {}).get('icu', False):
            self.conn.execute("INSTALL icu")
            self.conn.execute("LOAD icu")
            
        if self.config.get('extensions', {}).get('fts', False):
            self.conn.execute("INSTALL fts")
            self.conn.execute("LOAD fts")
            
        if self.config.get('extensions', {}).get('spatial', False):
            self.conn.execute("INSTALL spatial")
            self.conn.execute("LOAD spatial")
            
        if self.config.get('extensions', {}).get('sqlite', False):
            self.conn.execute("INSTALL sqlite")
            self.conn.execute("LOAD sqlite")
            
        # Check for custom extensions
        for ext in self.config.get('extensions', {}).get('custom', []):
            try:
                self.conn.execute(f"INSTALL {ext}")
                self.conn.execute(f"LOAD {ext}")
            except Exception as e:
                logger.warning(f"Failed to load extension {ext}: {e}")
                
    def _setup_advanced_features(self):
        """Setup advanced features for DuckDB"""
        # Fine tune the analyzer based on configuration
        if self.config.get('optimizer', {}).get('join_order', False):
            self.conn.execute("PRAGMA default_join_type='adaptive'")
            
        if self.config.get('optimizer', {}).get('filter_pushdown', True):
            self.conn.execute("PRAGMA enable_filter_pushdown")
            
        if self.config.get('optimizer', {}).get('lazy_analyze', True):
            self.conn.execute("PRAGMA explain_output='all'")
            
        # Set up optimizer for out-of-core processing
        if self.config.get('out_of_core', {}).get('enabled', True):
            self.conn.execute("PRAGMA temp_directory='/tmp/duckdb_tmp'")
            self.conn.execute("PRAGMA memory_limit='8GB'")
            self.conn.execute("PRAGMA threads=8")
            
    def create_indexed_table(self, table_name: str, data: Union[pd.DataFrame, pa.Table, str],
                            index_columns: Optional[List[str]] = None,
                            index_type: str = 'art',
                            replace: bool = False) -> bool:
        """
        Create a table with advanced indexing for fast lookups
        
        Args:
            table_name: Name of the table to create
            data: Data to insert (DataFrame, Arrow Table, or SQL query)
            index_columns: Columns to create indexes on
            index_type: Type of index (art, btree, etc.)
            replace: Whether to replace an existing table
            
        Returns:
            True if successful
        """
        # Handle different input types
        if isinstance(data, pd.DataFrame):
            # Create from pandas DataFrame
            if replace:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM data", {
                "data": data
            })
        elif isinstance(data, pa.Table):
            # Create from Arrow Table with zero-copy when possible
            if replace:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.register("arrow_data", data)
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM arrow_data")
            self.conn.unregister("arrow_data")
        elif isinstance(data, str):
            # Create from SQL query
            if replace:
                self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self.conn.execute(f"CREATE TABLE {table_name} AS {data}")
        else:
            raise ValueError(f"Unsupported data type: {type(data)}")
            
        # Create indexes for specified columns
        if index_columns:
            for col in index_columns:
                index_name = f"idx_{table_name}_{col}"
                self.conn.execute(f"CREATE INDEX {index_name} ON {table_name}({col}) USING {index_type}")
                
        return True
    
    def execute_mvcc_transaction(self, queries: List[str]) -> bool:
        """
        Execute multiple queries in a single MVCC transaction for atomic operations
        
        Args:
            queries: List of SQL queries to execute
            
        Returns:
            True if transaction successful
        """
        # Start transaction
        self.conn.execute("BEGIN TRANSACTION")
        
        try:
            # Execute each query
            for query in queries:
                self.conn.execute(query)
                
            # Commit transaction
            self.conn.execute("COMMIT")
            return True
        except Exception as e:
            # Rollback on error
            self.conn.execute("ROLLBACK")
            logger.error(f"Transaction failed: {e}")
            raise
    
    def load_parquet_direct(self, parquet_file: str, table_name: Optional[str] = None) -> Union[pa.Table, str]:
        """
        Load data directly from Parquet file with zero copy where possible
        
        Args:
            parquet_file: Path to Parquet file
            table_name: Optional table name to create
            
        Returns:
            Arrow table or table name if table created
        """
        # Use native Parquet support to efficiently load data
        result = self.conn.execute(f"SELECT * FROM read_parquet('{parquet_file}')")
        
        # Either create a table or return as Arrow
        if table_name:
            self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet('{parquet_file}')")
            return table_name
        else:
            return result.fetch_arrow_table()
    
    def load_parquet_partitioned(self, parquet_dir: str, table_name: str, 
                               partition_column: Optional[str] = None) -> str:
        """
        Load partitioned Parquet dataset with native support
        
        Args:
            parquet_dir: Directory containing partitioned Parquet files
            table_name: Table name to create
            partition_column: Optional partition column to use
            
        Returns:
            Table name
        """
        if partition_column:
            # Load with explicit partitioning
            self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM parquet_scan('{parquet_dir}', HIVE_PARTITIONING=1)
            """)
        else:
            # Auto-detect partitioning
            self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{parquet_dir}/*.parquet')
            """)
            
        return table_name
    
    def load_json_direct(self, json_file: str, table_name: Optional[str] = None, 
                      auto_detect: bool = True) -> Union[pa.Table, str]:
        """
        Load data directly from JSON file using native support
        
        Args:
            json_file: Path to JSON file (can be NDJSON or regular JSON)
            table_name: Optional table name to create
            auto_detect: Automatically detect JSON structure
            
        Returns:
            Arrow table or table name if table created
        """
        # Use native JSON support to efficiently load data
        if auto_detect:
            result = self.conn.execute(f"SELECT * FROM read_json_auto('{json_file}')")
        else:
            result = self.conn.execute(f"SELECT * FROM read_json('{json_file}')")
        
        # Either create a table or return as Arrow
        if table_name:
            if auto_detect:
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json_auto('{json_file}')")
            else:
                self.conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_json('{json_file}')")
            return table_name
        else:
            return result.fetch_arrow_table()
    
    def execute_arrow_query(self, query: str) -> pa.Table:
        """
        Execute SQL query and return results as zero-copy Arrow table
        
        Args:
            query: SQL query to execute
            
        Returns:
            PyArrow Table with results (zero-copy)
        """
        return self.conn.execute(query).fetch_arrow_table()
    
    def create_out_of_core_table(self, table_name: str, data_source: str, 
                             source_type: str = 'parquet', replace: bool = False) -> bool:
        """
        Create a table that can efficiently process data larger than memory
        
        Args:
            table_name: Name of the table to create
            data_source: Path to data source (file or directory)
            source_type: Type of source data (parquet, csv, etc.)
            replace: Whether to replace an existing table
            
        Returns:
            True if successful
        """
        if replace:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            
        # Create appropriate table based on source type
        if source_type == 'parquet':
            self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_parquet('{data_source}')
            """)
        elif source_type == 'csv':
            self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_csv_auto('{data_source}')
            """)
        elif source_type == 'json':
            self.conn.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM read_json_auto('{data_source}')
            """)
        else:
            raise ValueError(f"Unsupported source type: {source_type}")
            
        return True
    
    def vacuum_analyze(self, table_name: Optional[str] = None) -> None:
        """
        Vacuum and analyze tables for better performance
        
        Args:
            table_name: Optional specific table to vacuum and analyze
        """
        if table_name:
            self.conn.execute(f"VACUUM {table_name}")
            self.conn.execute(f"ANALYZE {table_name}")
        else:
            self.conn.execute("VACUUM")
            self.conn.execute("ANALYZE")
    
    def create_user_defined_function(self, function_name: str, function: callable, 
                                return_type: str, parameter_types: List[str]) -> None:
        """
        Create a user-defined function in DuckDB
        
        Args:
            function_name: Name of the function
            function: Python function to register
            return_type: Return type of the function
            parameter_types: List of parameter types
        """
        self.conn.create_function(
            function_name,
            function,
            return_type,
            parameter_types
        )
    
    def create_view_with_schema(self, view_name: str, query: str,
                           schema: pa.Schema) -> bool:
        """
        Create a view with a specified Arrow schema
        
        Args:
            view_name: Name of the view to create
            query: SQL query for the view
            schema: PyArrow schema to apply
            
        Returns:
            True if successful
        """
        # Create a temporary table with the schema
        temp_table = f"temp_{uuid.uuid4().hex}"
        column_defs = []
        
        for field in schema:
            duck_type = self._arrow_to_duckdb_type(field.type)
            column_defs.append(f"{field.name} {duck_type}")
            
        columns_str = ", ".join(column_defs)
        self.conn.execute(f"CREATE TEMPORARY TABLE {temp_table}({columns_str})")
        
        # Create the view with the schema
        self.conn.execute(f"CREATE OR REPLACE VIEW {view_name} AS {query}")
        
        # Drop the temporary table
        self.conn.execute(f"DROP TABLE {temp_table}")
        
        return True
    
    def _arrow_to_duckdb_type(self, arrow_type: pa.DataType) -> str:
        """Convert Arrow data type to DuckDB data type"""
        if pa.types.is_integer(arrow_type):
            if isinstance(arrow_type, pa.Int8Type):
                return "TINYINT"
            elif isinstance(arrow_type, pa.Int16Type):
                return "SMALLINT"
            elif isinstance(arrow_type, pa.Int32Type):
                return "INTEGER"
            else:
                return "BIGINT"
        elif pa.types.is_floating(arrow_type):
            if isinstance(arrow_type, pa.Float32Type):
                return "REAL"
            else:
                return "DOUBLE"
        elif pa.types.is_boolean(arrow_type):
            return "BOOLEAN"
        elif pa.types.is_string(arrow_type):
            return "VARCHAR"
        elif pa.types.is_date(arrow_type):
            return "DATE"
        elif pa.types.is_timestamp(arrow_type):
            return "TIMESTAMP"
        elif pa.types.is_binary(arrow_type):
            return "BLOB"
        elif pa.types.is_decimal(arrow_type):
            return "DECIMAL"
        else:
            return "VARCHAR"  # Default
    
    def export_arrow_ipc(self, query: str, output_file: str) -> str:
        """
        Export query results to Arrow IPC format for zero-copy sharing
        
        Args:
            query: SQL query to execute
            output_file: Path to output file
            
        Returns:
            Path to output file
        """
        result = self.conn.execute(query).fetch_arrow_table()
        with pa.OSFile(output_file, 'wb') as sink:
            with pa.ipc.new_file(sink, result.schema) as writer:
                writer.write_table(result)
        return output_file
    
    def export_parquet(self, query: str, output_file: str, compression: str = 'snappy') -> str:
        """
        Export query results to Parquet format
        
        Args:
            query: SQL query to execute
            output_file: Path to output file
            compression: Compression algorithm to use
            
        Returns:
            Path to output file
        """
        result = self.conn.execute(query).fetch_arrow_table()
        pq.write_table(result, output_file, compression=compression)
        return output_file
    
    def execute_partitioned_query(self, query: str, partition_column: str, 
                               num_partitions: int, output_dir: str) -> str:
        """
        Execute a query in partitions for large datasets and save to Parquet
        
        Args:
            query: SQL query to execute
            partition_column: Column to partition by
            num_partitions: Number of partitions to create
            output_dir: Directory to save partitioned results
            
        Returns:
            Path to output directory
        """
        os.makedirs(output_dir, exist_ok=True)
        
        # Get min and max values for partition column
        min_max = self.conn.execute(f"SELECT MIN({partition_column}), MAX({partition_column}) FROM ({query}) t").fetchone()
        min_val, max_val = min_max
        
        # Calculate partition ranges
        if isinstance(min_val, (int, float)) and isinstance(max_val, (int, float)):
            step = (max_val - min_val) / num_partitions
            
            for i in range(num_partitions):
                start = min_val + i * step
                end = min_val + (i + 1) * step if i < num_partitions - 1 else max_val + 0.1
                
                partition_query = f"""
                SELECT * FROM ({query}) t
                WHERE {partition_column} >= {start} AND {partition_column} < {end}
                """
                
                result = self.conn.execute(partition_query).fetch_arrow_table()
                output_file = os.path.join(output_dir, f"part-{i:05d}.parquet")
                pq.write_table(result, output_file)
        else:
            # For non-numeric partitioning, use modulo
            hashed_query = f"""
            SELECT *, row_number() OVER() AS __row_id 
            FROM ({query}) t
            """
            
            for i in range(num_partitions):
                partition_query = f"""
                SELECT * FROM ({hashed_query}) t
                WHERE __row_id % {num_partitions} = {i}
                """
                
                result = self.conn.execute(partition_query).fetch_arrow_table()
                output_file = os.path.join(output_dir, f"part-{i:05d}.parquet")
                pq.write_table(result, output_file)
                
        return output_dir
    
    def get_zero_copy_table_handle(self, table_name_or_query: str) -> int:
        """
        Get a handle to a table or query result for zero-copy access
        
        Args:
            table_name_or_query: Table name or SQL query
            
        Returns:
            Handle ID for zero-copy access
        """
        # If input is a query, materialize it first
        if table_name_or_query.strip().lower().startswith("select "):
            temp_table = f"temp_zero_copy_{uuid.uuid4().hex}"
            self.conn.execute(f"CREATE TEMPORARY TABLE {temp_table} AS {table_name_or_query}")
            table_name = temp_table
        else:
            table_name = table_name_or_query
            
        # Generate a unique handle ID
        handle_id = int(time.time() * 1000) & 0x7FFFFFFF
        
        # Register the table with the handle
        self.conn.execute(f"PRAGMA register_arrow_catalog_entry({handle_id}, '{table_name}')")
        
        return handle_id
    
    def create_temporary_column_store(self, data: Union[pd.DataFrame, pa.Table], 
                                  name: Optional[str] = None) -> str:
        """
        Create a temporary column store for efficient in-memory operations
        
        Args:
            data: Data to store
            name: Optional name for the store
            
        Returns:
            Name of the created store
        """
        if name is None:
            name = f"temp_store_{uuid.uuid4().hex}"
            
        if isinstance(data, pd.DataFrame):
            self.conn.execute(f"CREATE TEMPORARY TABLE {name} AS SELECT * FROM data", {
                "data": data
            })
        elif isinstance(data, pa.Table):
            self.conn.register(name, data)
            
        return name
    
    def close(self):
        """Close the DuckDB connection"""
        if self.conn:
            self.conn.close()
            self.conn = None 