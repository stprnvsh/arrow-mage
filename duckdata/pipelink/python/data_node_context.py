"""
DataNodeContext: Context manager for working with data connector nodes in PipeLink
"""
import os
import sys
import yaml
import logging
from typing import Any, Dict, List, Optional, Union

import pandas as pd
import pyarrow as pa

# Use absolute import path so this works when run as a subprocess
try:
    from pipelink.crosslink.python.crosslink import CrossLink
    from pipelink.python.data_connector import DataConnector
except ImportError:
    # Fall back to relative import for development mode
    try:
        # Add the parent directory to the path if being run directly
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from pipelink.crosslink.python.crosslink import CrossLink
        from pipelink.python.data_connector import DataConnector
    except ImportError:
        raise ImportError("Cannot import required modules. Make sure the pipelink package is installed.")

# Set up logging
logger = logging.getLogger('pipelink.data_node')

class DataNodeContext:
    """
    Context manager for working with PipeLink data connector nodes
    
    Provides easy access to configure data sources and save output datasets
    
    Example:
    ```python
    with DataNodeContext() as ctx:
        # Configure data source
        ctx.connect_to_postgres(host='localhost', database='mydb', user='user', password='pass')
        
        # Execute SQL query
        df = ctx.execute_sql("SELECT * FROM table WHERE col > 100")
        
        # Save output
        ctx.save_output("filtered_data", df)
    ```
    """
    
    def __init__(self, meta_path: Optional[str] = None):
        """
        Initialize DataNodeContext with metadata
        
        Args:
            meta_path: Path to metadata file. If None, tries to get from command line
        """
        self.meta = self._load_metadata(meta_path)
        self.cl = CrossLink(db_path=self.meta.get("db_path", "crosslink.duckdb"))
        self.data_connector = DataConnector()
    
    def _load_metadata(self, meta_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Load metadata from file
        
        Args:
            meta_path: Path to metadata file. If None, tries to get from command line
            
        Returns:
            Dict containing node metadata
        """
        # Get metadata file path from command line if not provided
        if meta_path is None:
            if len(sys.argv) < 2:
                raise ValueError("No metadata file provided. This script should be run by PipeLink.")
            meta_path = sys.argv[1]
        
        if not os.path.isfile(meta_path):
            raise FileNotFoundError(f"Metadata file not found: {meta_path}")
        
        # Load metadata from YAML
        with open(meta_path, 'r') as f:
            return yaml.safe_load(f)
    
    def get_input(self, name: str, required: bool = True) -> Optional[pd.DataFrame]:
        """
        Get an input dataset by name
        
        Args:
            name: Name of the input dataset
            required: Whether this input is required
            
        Returns:
            DataFrame with the requested data or None if not required and not found
        """
        # Check if this input is defined for this node
        inputs = self.meta.get("inputs", [])
        if name not in inputs:
            if required:
                raise ValueError(f"Input '{name}' is not defined for this node")
            return None
        
        # Pull from CrossLink
        return self.cl.pull(name)
    
    def save_output(self, name: str, data: Union[pd.DataFrame, pa.Table], description: Optional[str] = None) -> str:
        """
        Save an output dataset
        
        Args:
            name: Name of the output dataset
            data: DataFrame or Arrow Table to save
            description: Optional description for the dataset
            
        Returns:
            Dataset ID
        """
        # Check if this output is defined for this node
        outputs = self.meta.get("outputs", [])
        if name not in outputs:
            raise ValueError(f"Output '{name}' is not defined for this node")
        
        # Generate a description if not provided
        if description is None:
            node_id = self.meta.get("node_id", "unknown")
            description = f"Output '{name}' from data node '{node_id}'"
        
        # Convert Arrow Table to DataFrame if needed
        if isinstance(data, pa.Table):
            data = data.to_pandas()
        
        # Push to CrossLink
        return self.cl.push(data, name=name, description=description)
    
    def get_param(self, name: str, default: Any = None) -> Any:
        """
        Get a parameter value
        
        Args:
            name: Parameter name
            default: Default value if parameter is not found
            
        Returns:
            Parameter value or default
        """
        params = self.meta.get("params", {})
        return params.get(name, default)
    
    def get_connection_params(self) -> Dict[str, Any]:
        """
        Get connection parameters from metadata
        
        Returns:
            Dictionary of connection parameters
        """
        return self.meta.get("connection", {})
    
    def get_node_id(self) -> str:
        """
        Get the node ID
        
        Returns:
            Node ID string
        """
        return self.meta.get("node_id", "unknown")
    
    def get_pipeline_name(self) -> str:
        """
        Get the pipeline name
        
        Returns:
            Pipeline name string
        """
        return self.meta.get("pipeline_name", "unknown")
    
    def setup_connection(self) -> bool:
        """
        Set up data connection based on connection parameters in metadata
        
        Returns:
            True if connection was successful, False otherwise
        """
        connection = self.meta.get("connection", {})
        if not connection:
            logger.warning("No connection parameters specified in metadata")
            return False
        
        conn_type = connection.get("type")
        if not conn_type:
            logger.warning("Connection type not specified in metadata")
            return False
        
        try:
            if conn_type == "postgres":
                return self.connect_to_postgres(
                    host=connection.get("host", "localhost"),
                    port=connection.get("port", 5432),
                    database=connection.get("database", "postgres"),
                    user=connection.get("user", "postgres"),
                    password=connection.get("password"),
                    schema=connection.get("schema", "public"),
                    tables=connection.get("tables")
                )
            elif conn_type == "sqlite":
                return bool(self.connect_to_sqlite(
                    db_path=connection.get("db_path"),
                    tables=connection.get("tables")
                ))
            elif conn_type == "duckdb":
                return bool(self.connect_to_duckdb(
                    db_path=connection.get("db_path", ":memory:"),
                    extensions=connection.get("extensions", [])
                ))
            elif conn_type == "s3":
                # S3 connections are handled per-request
                return True
            elif conn_type == "flight":
                return self.connect_to_flight(
                    host=connection.get("host", "localhost"),
                    port=connection.get("port", 8815),
                    username=connection.get("username"),
                    password=connection.get("password")
                )
            else:
                logger.warning(f"Unsupported connection type: {conn_type}")
                return False
        except Exception as e:
            logger.error(f"Error setting up connection: {e}")
            return False
    
    def connect_to_postgres(self, host: str, port: int = 5432, database: str = "postgres",
                           user: str = "postgres", password: str = None, 
                           schema: str = "public", tables: Optional[List[str]] = None) -> bool:
        """
        Connect to PostgreSQL database
        
        Args:
            host: PostgreSQL server hostname
            port: PostgreSQL server port
            database: Database name
            user: Username
            password: Password
            schema: Schema name
            tables: List of tables to register (if None, register all)
            
        Returns:
            True if connection was successful, False otherwise
        """
        try:
            tables = self.data_connector.register_postgres(
                host=host,
                port=port,
                database=database,
                user=user,
                password=password,
                schema=schema,
                tables=tables
            )
            logger.info(f"Connected to PostgreSQL database, registered tables: {tables}")
            return True
        except Exception as e:
            logger.error(f"Error connecting to PostgreSQL database: {e}")
            return False
    
    def connect_to_sqlite(self, db_path: str, tables: Optional[List[str]] = None) -> List[str]:
        """
        Connect to SQLite database
        
        Args:
            db_path: Path to SQLite database file
            tables: List of tables to register (if None, register all)
            
        Returns:
            List of registered tables
        """
        try:
            tables = self.data_connector.register_sqlite(db_path, tables)
            logger.info(f"Connected to SQLite database, registered tables: {tables}")
            return tables
        except Exception as e:
            logger.error(f"Error connecting to SQLite database: {e}")
            return []
    
    def connect_to_duckdb(self, db_path: str = ":memory:", extensions: List[str] = None) -> DataConnector:
        """
        Connect to DuckDB database
        
        Args:
            db_path: Path to DuckDB database file
            extensions: List of extensions to load
            
        Returns:
            DataConnector instance
        """
        try:
            connection_params = {
                "type": "duckdb",
                "db_path": db_path,
                "extensions": extensions or []
            }
            self.data_connector = DataConnector(connection_params)
            logger.info(f"Connected to DuckDB database at {db_path}")
            return self.data_connector
        except Exception as e:
            logger.error(f"Error connecting to DuckDB database: {e}")
            return None
    
    def connect_to_flight(self, host: str, port: int, 
                         username: Optional[str] = None, 
                         password: Optional[str] = None) -> bool:
        """
        Connect to Apache Flight server
        
        Args:
            host: Server hostname
            port: Server port
            username: Username for authentication (optional)
            password: Password for authentication (optional)
            
        Returns:
            True if connection was successful, False otherwise
        """
        options = {}
        if username and password:
            options["username"] = username
            options["password"] = password
            
        return self.data_connector.connect_to_flight(host, port, options)
    
    def execute_sql(self, query: str, to_arrow: bool = True) -> Union[pd.DataFrame, pa.Table]:
        """
        Execute a SQL query
        
        Args:
            query: SQL query to execute
            to_arrow: If True, return an Arrow table, otherwise return a pandas DataFrame
            
        Returns:
            Query results as DataFrame or Arrow Table
        """
        return self.data_connector.execute_sql(query, to_arrow)
    
    def query_flight(self, query: str) -> Optional[pa.Table]:
        """
        Execute a query on the connected Flight server
        
        Args:
            query: SQL query to execute
            
        Returns:
            Arrow Table with results or None if failed
        """
        return self.data_connector.query_flight(query)
    
    def load_file(self, file_path: str, file_format: str = None) -> Union[pd.DataFrame, pa.Table]:
        """
        Load data from a file
        
        Args:
            file_path: Path to the file
            file_format: Format of the file (parquet, csv, json, etc.)
                         If None, will try to infer from extension
                         
        Returns:
            DataFrame or Arrow Table with data
        """
        return self.data_connector.load_file(file_path, file_format)
    
    def load_s3(self, bucket: str, key: str, region: str = 'us-east-1',
              file_format: str = None, aws_access_key_id: str = None,
              aws_secret_access_key: str = None) -> Union[pd.DataFrame, pa.Table]:
        """
        Load data from an S3 bucket
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
            region: AWS region
            file_format: Format of the file (parquet, csv, json, etc.)
                        If None, will try to infer from extension
            aws_access_key_id: AWS access key ID (optional)
            aws_secret_access_key: AWS secret access key (optional)
                        
        Returns:
            DataFrame or Arrow Table with data
        """
        return self.data_connector.load_s3(
            bucket=bucket,
            key=key,
            region=region,
            file_format=file_format,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key
        )
    
    def __enter__(self):
        # Try to set up connection based on metadata
        self.setup_connection()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cl.close()
        self.data_connector.close() 