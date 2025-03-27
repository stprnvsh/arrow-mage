"""
Data Connector: Connect to various data sources for PipeLink

This module provides functionality to connect to various data sources 
including local files, S3, DuckDB, PostgreSQL, and Apache Flight servers.
It leverages Arrow for efficient data transfer and conversion.
"""

import os
import pandas as pd
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import duckdb
import json
import uuid
import warnings
import tempfile
import logging
from typing import Dict, List, Any, Optional, Union
from pathlib import Path

# Set up logging
logger = logging.getLogger('pipelink.data_connector')

class DataConnector:
    """
    Connect to various data sources and return data as Arrow tables or Pandas DataFrames.
    Supports local files, S3, Apache Flight, DuckDB, PostgreSQL, and more.
    """
    
    def __init__(self, connection_params: Optional[Dict[str, Any]] = None):
        """
        Initialize the data connector with optional connection parameters.
        
        Args:
            connection_params: Dictionary of connection parameters
        """
        self.connection_params = connection_params or {}
        self.conn = None
        self.flight_client = None
        
        # Initialize DuckDB connection if needed
        if self.connection_params.get('type') == 'duckdb':
            self._init_duckdb()
    
    def _init_duckdb(self):
        """Initialize DuckDB connection."""
        db_path = self.connection_params.get('db_path', ':memory:')
        self.conn = duckdb.connect(db_path)
        
        # Install and load Arrow extension if not already loaded
        try:
            self.conn.execute("INSTALL arrow")
            self.conn.execute("LOAD arrow")
            self.arrow_available = True
        except Exception as e:
            warnings.warn(f"Arrow extension installation or loading failed: {e}")
            warnings.warn("Some Arrow functionality may not be available")
            self.arrow_available = False
            
        # Install other extensions if specified
        extensions = self.connection_params.get('extensions', [])
        for ext in extensions:
            try:
                self.conn.execute(f"INSTALL {ext}")
                self.conn.execute(f"LOAD {ext}")
                logger.info(f"Loaded extension: {ext}")
            except Exception as e:
                warnings.warn(f"Extension {ext} installation or loading failed: {e}")
    
    def connect_to_flight(self, host: str, port: int, options: Optional[Dict[str, Any]] = None):
        """
        Connect to an Apache Flight server.
        
        Args:
            host: Server hostname
            port: Server port
            options: Additional connection options
        """
        options = options or {}
        location = f"grpc://{host}:{port}"
        
        try:
            # Set up client
            client = flight.FlightClient(location)
            
            # Handle authentication if provided
            if 'username' in options and 'password' in options:
                auth = flight.BasicAuth(options['username'], options['password'])
                try:
                    token = client.authenticate_basic_token(auth)
                    headers = [token]
                    self.flight_client = (client, headers)
                except Exception as e:
                    raise ValueError(f"Authentication failed: {e}")
            else:
                self.flight_client = (client, [])
                
            logger.info(f"Connected to Flight server at {location}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Flight server at {location}: {e}")
            return False
    
    def query_flight(self, query: str) -> Optional[pa.Table]:
        """
        Execute a query on the connected Flight server.
        
        Args:
            query: SQL query to execute
            
        Returns:
            Arrow Table with results or None if failed
        """
        if not self.flight_client:
            raise ValueError("Not connected to a Flight server. Call connect_to_flight first.")
            
        client, headers = self.flight_client
        
        try:
            # Create a FlightDescriptor for the query
            descriptor = flight.FlightDescriptor.for_command(query)
            
            # Get FlightInfo
            flight_info = client.get_flight_info(descriptor, headers)
            
            # Retrieve the data
            reader = client.do_get(flight_info.endpoints[0].ticket, headers)
            
            # Read all batches and combine into a single table
            batches = []
            while True:
                try:
                    batch, _ = reader.read_chunk()
                    if batch is not None:
                        batches.append(batch)
                except StopIteration:
                    break
            
            if not batches:
                return None
                
            return pa.Table.from_batches(batches)
        except Exception as e:
            logger.error(f"Error executing Flight query: {e}")
            return None
    
    def load_file(self, file_path: str, file_format: str = None) -> Optional[Union[pa.Table, pd.DataFrame]]:
        """
        Load data from a file.
        
        Args:
            file_path: Path to the file
            file_format: Format of the file (parquet, csv, json, etc.)
                         If None, will try to infer from extension
                         
        Returns:
            Arrow Table or pandas DataFrame with data
        """
        # Determine file format if not specified
        if file_format is None:
            _, ext = os.path.splitext(file_path)
            file_format = ext.lstrip('.').lower()
            
        try:
            if file_format in ['parquet', 'pq']:
                return pq.read_table(file_path)
            elif file_format == 'csv':
                if not self.conn:
                    self._init_duckdb()
                # Use DuckDB for CSV parsing (preferred method)
                try:
                    return self.conn.execute(f"SELECT * FROM read_csv_auto('{file_path}')").arrow()
                except Exception as e:
                    logger.warning(f"DuckDB CSV loading failed, falling back to pandas: {e}")
                    # Fallback to pandas
                    return pd.read_csv(file_path)
            elif file_format == 'json':
                if self.conn:
                    # Use DuckDB for more efficient JSON parsing
                    return self.conn.execute(f"SELECT * FROM read_json_auto('{file_path}')").arrow()
                else:
                    # Fallback to pandas for JSON
                    return pd.read_json(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        except Exception as e:
            logger.error(f"Error loading file {file_path}: {e}")
            return None
    
    def load_s3(self, 
                bucket: str, 
                key: str, 
                region: str = 'us-east-1', 
                file_format: str = None,
                aws_access_key_id: str = None,
                aws_secret_access_key: str = None) -> Optional[Union[pa.Table, pd.DataFrame]]:
        """
        Load data from an S3 bucket.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            region: AWS region
            file_format: Format of the file (parquet, csv, json, etc.)
                        If None, will try to infer from extension
            aws_access_key_id: AWS access key ID (optional)
            aws_secret_access_key: AWS secret access key (optional)
                        
        Returns:
            Arrow Table or pandas DataFrame with data
        """
        if not self.conn:
            self._init_duckdb()
            
        # Determine file format if not specified
        if file_format is None:
            _, ext = os.path.splitext(key)
            file_format = ext.lstrip('.').lower()
        
        try:
            # Install httpfs extension if not already installed
            self.conn.execute("INSTALL httpfs")
            self.conn.execute("LOAD httpfs")
            
            # Set AWS credentials if provided
            if aws_access_key_id and aws_secret_access_key:
                self.conn.execute(f"""
                SET s3_region='{region}';
                SET s3_access_key_id='{aws_access_key_id}';
                SET s3_secret_access_key='{aws_secret_access_key}';
                """)
            
            s3_path = f"s3://{bucket}/{key}"
            
            if file_format in ['parquet', 'pq']:
                return self.conn.execute(f"SELECT * FROM read_parquet('{s3_path}')").arrow()
            elif file_format == 'csv':
                return self.conn.execute(f"SELECT * FROM read_csv_auto('{s3_path}')").arrow()
            elif file_format == 'json':
                return self.conn.execute(f"SELECT * FROM read_json_auto('{s3_path}')").arrow()
            else:
                raise ValueError(f"Unsupported file format: {file_format}")
        except Exception as e:
            logger.error(f"Error loading S3 object s3://{bucket}/{key}: {e}")
            return None
    
    def execute_sql(self, query: str, to_arrow: bool = True) -> Optional[Union[pa.Table, pd.DataFrame]]:
        """
        Execute a SQL query using DuckDB.
        
        Args:
            query: SQL query to execute
            to_arrow: If True, return an Arrow table, otherwise return a pandas DataFrame
            
        Returns:
            Arrow Table or pandas DataFrame with results
        """
        if not self.conn:
            self._init_duckdb()
            
        try:
            result = self.conn.execute(query)
            if to_arrow and self.arrow_available:
                return result.arrow()
            else:
                return result.fetchdf()
        except Exception as e:
            logger.error(f"Error executing SQL query: {e}")
            return None
    
    def register_postgres(self, 
                         host: str, 
                         port: int = 5432, 
                         database: str = 'postgres',
                         user: str = 'postgres',
                         password: str = None,
                         schema: str = 'public',
                         tables: Optional[List[str]] = None):
        """
        Register PostgreSQL database tables in DuckDB for querying.
        
        Args:
            host: PostgreSQL server hostname
            port: PostgreSQL server port
            database: Database name
            user: Username
            password: Password
            schema: Schema name
            tables: List of tables to register (if None, register all)
            
        Returns:
            List of registered tables
        """
        if not self.conn:
            self._init_duckdb()
            
        try:
            # Install postgres extension if not already installed
            self.conn.execute("INSTALL postgres")
            self.conn.execute("LOAD postgres")
            
            # Create connection string
            conn_string = f"host={host} port={port} dbname={database} user={user}"
            if password:
                conn_string += f" password={password}"
                
            # Attach PostgreSQL database
            self.conn.execute(f"ATTACH '{conn_string}' AS postgres (TYPE postgres)")
            
            # Register tables
            if tables:
                registered_tables = []
                for table in tables:
                    table_name = f"postgres_{table}"
                    self.conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM postgres.{schema}.{table}")
                    registered_tables.append(table_name)
                return registered_tables
            else:
                # Get list of tables
                tables_query = f"SELECT table_name FROM postgres.information_schema.tables WHERE table_schema = '{schema}'"
                tables_df = self.conn.execute(tables_query).fetchdf()
                
                registered_tables = []
                for table in tables_df['table_name']:
                    table_name = f"postgres_{table}"
                    self.conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM postgres.{schema}.{table}")
                    registered_tables.append(table_name)
                return registered_tables
        except Exception as e:
            logger.error(f"Error registering PostgreSQL database: {e}")
            return []
    
    def register_sqlite(self, db_path: str, tables: Optional[List[str]] = None):
        """
        Register SQLite database tables in DuckDB for querying.
        
        Args:
            db_path: Path to SQLite database file
            tables: List of tables to register (if None, register all)
            
        Returns:
            List of registered tables
        """
        if not self.conn:
            self._init_duckdb()
            
        try:
            # Attach SQLite database
            self.conn.execute(f"ATTACH '{db_path}' AS sqlite_db (TYPE sqlite)")
            
            # Register tables
            if tables:
                registered_tables = []
                for table in tables:
                    table_name = f"sqlite_{table}"
                    self.conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM sqlite_db.{table}")
                    registered_tables.append(table_name)
                return registered_tables
            else:
                # Get list of tables
                tables_query = "SELECT name FROM sqlite_db.sqlite_master WHERE type='table'"
                tables_df = self.conn.execute(tables_query).fetchdf()
                
                registered_tables = []
                for table in tables_df['name']:
                    table_name = f"sqlite_{table}"
                    self.conn.execute(f"CREATE VIEW {table_name} AS SELECT * FROM sqlite_db.{table}")
                    registered_tables.append(table_name)
                return registered_tables
        except Exception as e:
            logger.error(f"Error registering SQLite database: {e}")
            return []
    
    def to_pandas(self, data: pa.Table) -> pd.DataFrame:
        """
        Convert Arrow table to pandas DataFrame.
        
        Args:
            data: Arrow table
            
        Returns:
            pandas DataFrame
        """
        return data.to_pandas()
    
    def to_arrow(self, data: pd.DataFrame) -> pa.Table:
        """
        Convert pandas DataFrame to Arrow table.
        
        Args:
            data: pandas DataFrame
            
        Returns:
            Arrow table
        """
        return pa.Table.from_pandas(data)
    
    def close(self):
        """Close all connections."""
        if self.conn:
            self.conn.close()
            self.conn = None
            
        if self.flight_client:
            self.flight_client = None
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close() 