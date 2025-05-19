"""
DuckDB ingestion module for Arrow Cache MCP.

This module provides functions to directly leverage DuckDB's connectors
for importing data from various sources.
"""

import os
import json
from typing import Optional, Dict, Any, List, Tuple, Union
import pyarrow as pa
import duckdb
import logging

# Configure module logger
logger = logging.getLogger(__name__)

def import_from_duckdb(
    source: str,
    source_type: str = "auto",
    connection_options: Optional[Dict[str, Any]] = None,
    query: Optional[str] = None,
    table_name: Optional[str] = None,
    schema: Optional[str] = None,
    options: Optional[Dict[str, Any]] = None
) -> pa.Table:
    """
    Import data from various sources using DuckDB.
    
    Args:
        source: Source URI, path, or connection string
        source_type: Source type ('auto', 'parquet', 'csv', 'json', 'ndjson', 'postgres', etc.)
        connection_options: Connection options for database sources
        query: SQL query for database sources
        table_name: Table name for database sources
        schema: Schema name for database sources (PostgreSQL)
        options: Additional source-specific options
        
    Returns:
        Arrow Table with imported data
    """
    # Auto-detect source type if not specified
    if source_type == "auto":
        source_type = _detect_source_type(source)
    
    logger.info(f"Importing data from {source} (type: {source_type})")
    
    # Create a DuckDB connection
    con = duckdb.connect(":memory:")
    
    # Route to appropriate import method based on source type
    if source_type in ["parquet", "csv", "json", "ndjson", "geoparquet", "geojson"]:
        # File-based sources
        return _import_file(con, source, source_type, options)
    
    elif source_type in ["postgres", "postgresql"]:
        # PostgreSQL
        return _import_postgres(con, source, connection_options, query, table_name, schema, options)
    
    elif source_type == "iceberg":
        # Apache Iceberg
        return _import_iceberg(con, source, connection_options, table_name, options)
    
    elif source_type == "s3":
        # S3 files (auto-detect format based on extension)
        return _import_s3(con, source, options)
    
    elif source_type == "http" or source_type == "https":
        # HTTP/HTTPS files (auto-detect format based on extension)
        return _import_http(con, source, options)
    
    elif source_type == "mysql":
        # MySQL
        return _import_mysql(con, source, connection_options, query, table_name, schema, options)
    
    elif source_type == "sqlserver" or source_type == "mssql":
        # SQL Server
        return _import_sqlserver(con, source, connection_options, query, table_name, schema, options)
    
    elif source_type == "sqlite":
        # SQLite
        return _import_sqlite(con, source, query, table_name, options)
    
    elif source_type == "duckdb":
        # Another DuckDB database
        return _import_duckdb_db(con, source, query, table_name, options)
    
    else:
        raise ValueError(f"Unsupported source type: {source_type}")


def _detect_source_type(source: str) -> str:
    """
    Auto-detect the source type based on the source string.
    
    Args:
        source: Source URI or path
        
    Returns:
        Detected source type
    """
    # Check URL schemes
    if source.startswith("s3://"):
        return "s3"
    elif source.startswith("http://") or source.startswith("https://"):
        return "http"
    elif source.startswith("postgresql://") or source.startswith("postgres://"):
        return "postgres"
    elif source.startswith("mysql://"):
        return "mysql"
    elif source.startswith("sqlserver://") or source.startswith("mssql://"):
        return "sqlserver"
    elif source.startswith("iceberg://"):
        return "iceberg"
    
    # Check file extensions
    if source.endswith(".parquet"):
        return "parquet"
    elif source.endswith(".geoparquet"):
        return "geoparquet"
    elif source.endswith(".csv"):
        return "csv"
    elif source.endswith(".json"):
        return "json"
    elif source.endswith(".ndjson") or source.endswith(".jsonl"):
        return "ndjson"
    elif source.endswith(".db") or source.endswith(".sqlite"):
        return "sqlite"
    elif source.endswith(".duckdb"):
        return "duckdb"
    
    # Default to CSV if can't determine
    return "csv"


def _import_file(
    con: duckdb.DuckDBPyConnection,
    source: str,
    file_type: str,
    options: Optional[Dict[str, Any]] = None
) -> pa.Table:
    """
    Import data from a file using DuckDB.
    
    Args:
        con: DuckDB connection
        source: File path
        file_type: File type ('parquet', 'csv', 'json', 'ndjson', 'geoparquet', 'geojson')
        options: Additional options for the specific file type
    
    Returns:
        Arrow Table containing the imported data
    """
    # Prepare options string if provided
    options_str = ""
    if options:
        if file_type == "csv" and "csv_options" in options:
            csv_opts = []
            for k, v in options["csv_options"].items():
                if isinstance(v, str):
                    csv_opts.append(f"{k}='{v}'")
                else:
                    csv_opts.append(f"{k}={v}")
            if csv_opts:
                options_str = f"({', '.join(csv_opts)})"
        
        elif file_type in ["json", "ndjson"] and "json_options" in options:
            json_opts = []
            for k, v in options["json_options"].items():
                if isinstance(v, str):
                    json_opts.append(f"{k}='{v}'")
                else:
                    json_opts.append(f"{k}={v}")
            if json_opts:
                options_str = f"({', '.join(json_opts)})"
    
    # Import based on file type
    if file_type == "parquet" or file_type == "geoparquet":
        # GeoParquet is a specialized Parquet format, but DuckDB reads it the same way
        logger.info(f"Reading {file_type} file using read_parquet: {source}")
        query = f"SELECT * FROM read_parquet('{source}')"
    elif file_type == "csv":
        query = f"SELECT * FROM read_csv('{source}'{options_str})"
    elif file_type == "json":
        query = f"SELECT * FROM read_json('{source}'{options_str})"
    elif file_type == "ndjson":
        query = f"SELECT * FROM read_ndjson('{source}'{options_str})"
    elif file_type == "geojson":
        logger.info(f"Reading GeoJSON file using ST_Read: {source}")
        # Load spatial extension
        con.execute("INSTALL spatial; LOAD spatial;")
        query = f"SELECT * FROM ST_Read('{source}')"
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    # Execute query and convert to Arrow
    return con.execute(query).arrow()


def _import_postgres(
    con: duckdb.DuckDBPyConnection,
    source: str,
    connection_options: Optional[Dict[str, Any]],
    query: Optional[str],
    table_name: Optional[str],
    schema: Optional[str],
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from PostgreSQL using DuckDB's postgres extension.
    
    Args:
        con: DuckDB connection
        source: PostgreSQL connection string
        connection_options: Connection options
        query: SQL query to execute
        table_name: Table name to load
        schema: Schema name
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load PostgreSQL extension
    logger.info("Installing and loading PostgreSQL extension for DuckDB")
    con.execute("INSTALL postgres; LOAD postgres;")
    
    # Build connection string if not provided directly
    if not source.startswith("postgresql://") and not source.startswith("postgres://"):
        if connection_options:
            host = connection_options.get("host", "localhost")
            port = connection_options.get("port", 5432)
            dbname = connection_options.get("dbname", "")
            user = connection_options.get("user", "")
            password = connection_options.get("password", "")
            
            # Construct connection string
            source = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            logger.info(f"Built connection string: postgresql://{user}:[redacted]@{host}:{port}/{dbname}")
    
    # Create temporary connection to PostgreSQL
    connection_name = f"pg_conn_{abs(hash(source)) % 1000000}"
    logger.info(f"Creating temporary connection '{connection_name}'")
    
    try:
        # First try attaching the PostgreSQL database directly
        con.execute(f"ATTACH '{source}' AS {connection_name} (TYPE POSTGRES)")
        logger.info(f"Successfully attached PostgreSQL database as '{connection_name}'")
        
        # Import data directly using the attached connection
        if query:
            # Using a custom query
            logger.info(f"Executing custom query on attached database")
            sql = f"SELECT * FROM {connection_name}.({query})"
            logger.info(f"Query SQL: {sql}")
        else:
            # Using a table name
            if not table_name:
                raise ValueError("Either query or table_name must be provided for PostgreSQL import")
            
            logger.info(f"Reading table: {table_name} from schema: {schema or 'public'}")
            
            # Build fully qualified table name
            if schema:
                full_table_name = f"{schema}.{table_name}"
            else:
                full_table_name = table_name
            
            # Query the table directly from the attached database
            sql = f"SELECT * FROM {connection_name}.{full_table_name}"
            logger.info(f"Table SQL: {sql}")
        
        # Execute the query
        logger.info(f"Executing query: {sql}")
        result = con.execute(sql).arrow()
        logger.info(f"Query successful, returned Arrow table with {len(result)} rows and {len(result.column_names)} columns")
        
        # Detach the database connection
        con.execute(f"DETACH {connection_name}")
        
        return result
        
    except Exception as attach_error:
        logger.error(f"Error using ATTACH method: {str(attach_error)}")
        logger.info("Falling back to postgres_scan method")
        
        # If ATTACH failed, try the postgres_scan approach with multiple methods
        if query:
            # Using a custom query
            logger.info(f"Executing custom query using postgres_scan")
            methods_to_try = [
                {
                    "name": "Direct SQL query",
                    "sql": f"SELECT * FROM postgres_scan('{source}', $${query}$$)"
                },
                {
                    "name": "Three-parameter SQL query with empty schema",
                    "sql": f"SELECT * FROM postgres_scan('{source}', $${query}$$, '')"
                }
            ]
        else:
            # Using a table name
            if not table_name:
                raise ValueError("Either query or table_name must be provided for PostgreSQL import")
            
            logger.info(f"Importing table: {table_name} from schema: {schema or 'default'}")
            
            # Try multiple methods to reference the table, in case one fails
            methods_to_try = []
            
            # For schema-qualified tables
            if schema:
                # Method 1: Schema as third parameter (connection_string, table, schema)
                methods_to_try.append({
                    "name": "Connection, table, schema",
                    "sql": f"SELECT * FROM postgres_scan('{source}', '{table_name}', '{schema}')"
                })
                
                # Method 2: Using schema-qualified table name (connection_string, schema.table)
                methods_to_try.append({
                    "name": "Connection, schema.table",
                    "sql": f"SELECT * FROM postgres_scan('{source}', '{schema}.{table_name}')"
                })
                
                # Method 3: Using SQL query with schema-qualified name
                methods_to_try.append({
                    "name": "SQL query for schema.table",
                    "sql": f"SELECT * FROM postgres_scan('{source}', 'SELECT * FROM {schema}.{table_name}')"
                })
                
                # Method 4: Table as third parameter (connection_string, schema, table) - reversed order
                methods_to_try.append({
                    "name": "Connection, schema, table (reversed)",
                    "sql": f"SELECT * FROM postgres_scan('{source}', '{schema}', '{table_name}')"
                })
            else:
                # No schema provided, try with default/public schema
                
                # Method 1: Standard 2-parameter version (connection_string, table)
                methods_to_try.append({
                    "name": "Connection, table",
                    "sql": f"SELECT * FROM postgres_scan('{source}', '{table_name}')"
                })
                
                # Method 2: With explicit public schema (connection_string, table, 'public')
                methods_to_try.append({
                    "name": "Connection, table, public",
                    "sql": f"SELECT * FROM postgres_scan('{source}', '{table_name}', 'public')"
                })
                
                # Method 3: With explicit public schema in table name
                methods_to_try.append({
                    "name": "Connection, public.table",
                    "sql": f"SELECT * FROM postgres_scan('{source}', 'public.{table_name}')"
                })
                
                # Method 4: Using SQL query with explicit table
                methods_to_try.append({
                    "name": "SQL query for table",
                    "sql": f"SELECT * FROM postgres_scan('{source}', 'SELECT * FROM {table_name}')"
                })
            
            # Try with different quotes around table names
            methods_to_try.append({
                "name": "Double quoted identifiers",
                "sql": f"SELECT * FROM postgres_scan('{source}', 'SELECT * FROM \"{schema or 'public'}\".\"{table_name}\"')"
            })
        
        # Try each method until one works
        last_exception = None
        for method in methods_to_try:
            try:
                logger.info(f"Trying method: {method['name']}")
                logger.info(f"SQL: {method['sql']}")
                result = con.execute(method['sql']).arrow()
                logger.info(f"Method {method['name']} succeeded! Got {len(result)} rows.")
                return result
            except Exception as e:
                logger.error(f"Method {method['name']} failed: {str(e)}")
                last_exception = e
        
        # If we get here, all methods failed
        if last_exception:
            # Try direct connection to see what tables are actually available
            try:
                logger.info("Attempting direct query to list available tables")
                # Attach the database directly to see what's actually there
                con.execute(f"ATTACH '{source}' AS pg_debug (TYPE POSTGRES)")
                tables = con.execute("""
                    SELECT table_schema, table_name 
                    FROM pg_debug.information_schema.tables 
                    WHERE table_type = 'BASE TABLE'
                    LIMIT 10
                """).fetchall()
                logger.info(f"Available tables: {tables}")
                con.execute("DETACH pg_debug")
            except Exception as debug_error:
                logger.error(f"Debug query failed: {str(debug_error)}")
            
            raise last_exception
        else:
            raise ValueError(f"Could not import table {schema + '.' if schema else ''}{table_name} using any method")


def _import_iceberg(
    con: duckdb.DuckDBPyConnection,
    source: str,
    connection_options: Optional[Dict[str, Any]],
    table_name: Optional[str],
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from Apache Iceberg using DuckDB's iceberg extension.
    
    Args:
        con: DuckDB connection
        source: Iceberg catalog URI
        connection_options: Connection options
        table_name: Iceberg table name
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load Iceberg extension
    con.execute("INSTALL iceberg; LOAD iceberg;")
    
    # Configure Iceberg catalog
    catalog_name = options.get("catalog_name", "my_catalog") if options else "my_catalog"
    
    if source.startswith("iceberg://"):
        # Use the provided catalog URI
        catalog_uri = source
    else:
        # Use source as a path to the catalog
        catalog_uri = source
    
    # Create catalog
    con.execute(f"CALL iceberg_create_catalog('{catalog_name}', 'uri', '{catalog_uri}')")
    
    # Import data
    if not table_name:
        raise ValueError("table_name must be provided for Iceberg import")
    
    # Execute query and return as Arrow table
    return con.execute(f"SELECT * FROM iceberg_scan('{catalog_name}.{table_name}')").arrow()


def _import_s3(
    con: duckdb.DuckDBPyConnection,
    source: str,
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from S3 using DuckDB.
    
    Args:
        con: DuckDB connection
        source: S3 URI (s3://bucket/path)
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load httpfs extension
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Configure S3 credentials if provided
    if options and "s3_options" in options:
        s3_opts = options["s3_options"]
        if "aws_access_key_id" in s3_opts and "aws_secret_access_key" in s3_opts:
            con.execute(f"SET s3_access_key_id='{s3_opts['aws_access_key_id']}'")
            con.execute(f"SET s3_secret_access_key='{s3_opts['aws_secret_access_key']}'")
        if "region" in s3_opts:
            con.execute(f"SET s3_region='{s3_opts['region']}'")
        if "endpoint" in s3_opts:
            con.execute(f"SET s3_endpoint='{s3_opts['endpoint']}'")
    
    # Detect file type from URI
    if source.endswith(".parquet"):
        query = f"SELECT * FROM read_parquet('{source}')"
    elif source.endswith(".csv"):
        query = f"SELECT * FROM read_csv('{source}')"
    elif source.endswith(".json"):
        query = f"SELECT * FROM read_json('{source}')"
    else:
        # Default to parquet
        query = f"SELECT * FROM read_parquet('{source}')"
    
    # Execute query and return as Arrow table
    return con.execute(query).arrow()


def _import_http(
    con: duckdb.DuckDBPyConnection,
    source: str,
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from HTTP/HTTPS using DuckDB.
    
    Args:
        con: DuckDB connection
        source: HTTP/HTTPS URL
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load httpfs extension
    con.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Detect file type from URL
    if source.endswith(".parquet"):
        query = f"SELECT * FROM read_parquet('{source}')"
    elif source.endswith(".csv"):
        query = f"SELECT * FROM read_csv('{source}')"
    elif source.endswith(".json"):
        query = f"SELECT * FROM read_json('{source}')"
    else:
        # Default to CSV
        query = f"SELECT * FROM read_csv('{source}')"
    
    # Execute query and return as Arrow table
    return con.execute(query).arrow()


def _import_mysql(
    con: duckdb.DuckDBPyConnection,
    source: str,
    connection_options: Optional[Dict[str, Any]],
    query: Optional[str],
    table_name: Optional[str],
    schema: Optional[str],
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from MySQL using DuckDB's mysql extension.
    
    Args:
        con: DuckDB connection
        source: MySQL connection string
        connection_options: Connection options
        query: SQL query to execute
        table_name: Table name to load
        schema: Schema name
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load MySQL extension
    con.execute("INSTALL mysql; LOAD mysql;")
    
    # Build connection string if not provided directly
    if not source.startswith("mysql://"):
        if connection_options:
            host = connection_options.get("host", "localhost")
            port = connection_options.get("port", 3306)
            dbname = connection_options.get("dbname", "")
            user = connection_options.get("user", "")
            password = connection_options.get("password", "")
            
            # Construct connection string
            source = f"mysql://{user}:{password}@{host}:{port}/{dbname}"
    
    # Import data
    if query:
        # Using a custom query
        sql = f"SELECT * FROM mysql_scan('{source}', $${query}$$)"
    else:
        # Using a table name
        if not table_name:
            raise ValueError("Either query or table_name must be provided for MySQL import")
        
        # Add schema if provided
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        sql = f"SELECT * FROM mysql_scan('{source}', '{full_table_name}')"
    
    # Execute query and return as Arrow table
    return con.execute(sql).arrow()


def _import_sqlserver(
    con: duckdb.DuckDBPyConnection,
    source: str,
    connection_options: Optional[Dict[str, Any]],
    query: Optional[str],
    table_name: Optional[str],
    schema: Optional[str],
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from SQL Server using DuckDB's sqlserver extension.
    
    Args:
        con: DuckDB connection
        source: SQL Server connection string
        connection_options: Connection options
        query: SQL query to execute
        table_name: Table name to load
        schema: Schema name
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load SQL Server extension
    con.execute("INSTALL sqlserver; LOAD sqlserver;")
    
    # Build connection string if not provided directly
    if not source.startswith("sqlserver://") and not source.startswith("mssql://"):
        if connection_options:
            host = connection_options.get("host", "localhost")
            port = connection_options.get("port", 1433)
            dbname = connection_options.get("dbname", "")
            user = connection_options.get("user", "")
            password = connection_options.get("password", "")
            
            # Construct connection string
            source = f"sqlserver://{user}:{password}@{host}:{port}/{dbname}"
    
    # Import data
    if query:
        # Using a custom query
        sql = f"SELECT * FROM sqlserver_scan('{source}', $${query}$$)"
    else:
        # Using a table name
        if not table_name:
            raise ValueError("Either query or table_name must be provided for SQL Server import")
        
        # Add schema if provided
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        sql = f"SELECT * FROM sqlserver_scan('{source}', '{full_table_name}')"
    
    # Execute query and return as Arrow table
    return con.execute(sql).arrow()


def _import_sqlite(
    con: duckdb.DuckDBPyConnection,
    source: str,
    query: Optional[str],
    table_name: Optional[str],
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from SQLite using DuckDB's sqlite extension.
    
    Args:
        con: DuckDB connection
        source: SQLite database file path
        query: SQL query to execute
        table_name: Table name to load
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load SQLite extension
    con.execute("INSTALL sqlite; LOAD sqlite;")
    
    # Import data
    if query:
        # Using a custom query
        sql = f"SELECT * FROM sqlite_scan('{source}', $${query}$$)"
    else:
        # Using a table name
        if not table_name:
            raise ValueError("Either query or table_name must be provided for SQLite import")
        
        sql = f"SELECT * FROM sqlite_scan('{source}', '{table_name}')"
    
    # Execute query and return as Arrow table
    return con.execute(sql).arrow()


def _import_duckdb_db(
    con: duckdb.DuckDBPyConnection,
    source: str,
    query: Optional[str],
    table_name: Optional[str],
    options: Optional[Dict[str, Any]]
) -> pa.Table:
    """
    Import data from another DuckDB database.
    
    Args:
        con: DuckDB connection
        source: DuckDB database file path
        query: SQL query to execute
        table_name: Table name to load
        options: Additional options
    
    Returns:
        Arrow Table containing the imported data
    """
    # Load another DuckDB database
    if query:
        # Using a custom query, need to attach the database
        attach_name = f"db_{abs(hash(source)) % 1000000}"  # Generate a unique attach name
        con.execute(f"ATTACH '{source}' AS {attach_name}")
        
        # Execute the query in the context of the attached database
        result = con.execute(f"SELECT * FROM ({query})").arrow()
        
        # Detach the database
        con.execute(f"DETACH {attach_name}")
        
        return result
    else:
        # Using a table name
        if not table_name:
            raise ValueError("Either query or table_name must be provided for DuckDB import")
        
        # Directly read from the other database
        return con.execute(f"SELECT * FROM '{source}'.{table_name}").arrow()


def import_to_cache(
    cache_instance: Any,
    key: str,
    source: str,
    source_type: str = "auto",
    connection_options: Optional[Dict[str, Any]] = None,
    query: Optional[str] = None,
    table_name: Optional[str] = None,
    schema: Optional[str] = None,
    ttl: Optional[float] = None,
    metadata: Optional[Dict[str, Any]] = None,
    options: Optional[Dict[str, Any]] = None
) -> str:
    """
    Import data from a source directly into the Arrow Cache.
    
    Args:
        cache_instance: Arrow Cache instance
        key: Key to store the data under
        source: Source URI or path
        source_type: Source type
        connection_options: Connection options
        query: SQL query to execute
        table_name: Table name to load
        schema: Schema name
        ttl: Time-to-live for the cached data
        metadata: Additional metadata to store with the cached data
        options: Additional options for the specific connector
    
    Returns:
        Key of the cached data
    """
    # Import the data using DuckDB
    arrow_table = import_from_duckdb(
        source=source,
        source_type=source_type,
        connection_options=connection_options,
        query=query,
        table_name=table_name,
        schema=schema,
        options=options
    )
    
    # Store metadata about the import
    if metadata is None:
        metadata = {}
    
    # Add source information to metadata
    metadata.update({
        "source": source,
        "source_type": source_type,
        "import_method": "duckdb",
        "row_count": len(arrow_table),
        "column_count": len(arrow_table.column_names),
        "columns": arrow_table.column_names
    })
    
    # Calculate table size using Arrow's methods
    size_bytes = estimate_arrow_table_size(arrow_table)
    metadata["size_bytes"] = size_bytes
    metadata["size_mb"] = size_bytes / (1024 * 1024)
    
    # Add table structure information to help diagnose issues
    try:
        # Collect column types
        column_types = {}
        column_dtypes = {}
        
        for col_name in arrow_table.column_names:
            col = arrow_table.column(col_name)
            column_types[col_name] = str(col.type)
            # Extract dtype info for easier querying
            dtype_str = str(col.type).replace("DataType", "").strip("()")
            column_dtypes[col_name] = dtype_str
            
        # Store type information
        metadata["column_types"] = column_types
        metadata["dtypes"] = column_dtypes
    except Exception as e:
        logger.warning(f"Error collecting column type information: {e}")
    
    if table_name:
        metadata["table_name"] = table_name
    if schema:
        metadata["schema"] = schema
    
    # Add the data to the cache
    return cache_instance.put(
        key=key,
        data=arrow_table,
        ttl=ttl,
        metadata=metadata,
        overwrite=True
    )

def estimate_arrow_table_size(table: pa.Table) -> int:
    """
    Estimate the memory usage of an Arrow table using PyArrow's native methods.
    
    Args:
        table: PyArrow Table
        
    Returns:
        Size in bytes
    """
    # Method 1: Use table's nbytes attribute if available
    try:
        if hasattr(table, 'nbytes'):
            return table.nbytes
    except Exception as e:
        logger.debug(f"Error getting table.nbytes: {e}")
    
    # Method 2: Sum up the size of each column using Arrow's buffer tracking
    try:
        total_size = 0
        for col_name in table.column_names:
            col = table.column(col_name)
            
            # Track unique buffers to avoid double counting
            seen_buffers = set()
            
            # Add size of each buffer in the column
            for chunk in col.chunks:
                for buf in chunk.buffers():
                    if buf is not None:
                        # Use the buffer's address as identifier to avoid duplicates
                        buf_id = id(buf)
                        if buf_id not in seen_buffers:
                            seen_buffers.add(buf_id)
                            total_size += buf.size
        
        # Add schema overhead (rough estimate)
        schema_overhead = len(table.schema.to_string()) * 2
        total_size += schema_overhead
        
        return total_size
    except Exception as e:
        logger.debug(f"Error in buffer-based size estimation: {e}")
    
    # Method 3: Use memory pool delta measurement
    try:
        import gc
        # Get Arrow's memory pool
        memory_pool = pa.default_memory_pool()
        
        # Force garbage collection to stabilize memory
        gc.collect()
        
        # Measure memory before deep materialization
        bytes_before = memory_pool.bytes_allocated()
        
        # Force deep materialization of the table
        for col in table.columns:
            for chunk in col.chunks:
                for _ in range(min(10, len(chunk))):
                    # Just materialize a few values from each chunk
                    chunk[0]
                    break
                    
        # Measure memory after materialization
        bytes_after = memory_pool.bytes_allocated()
        
        # If we got a meaningful difference, use it
        if bytes_after > bytes_before:
            return bytes_after - bytes_before
    except Exception as e:
        logger.debug(f"Error in memory pool measurement: {e}")
    
    # Method 4: Fallback to basic row*column*estimate approach
    try:
        # Estimate based on typical sizes for different types
        row_count = len(table)
        size_per_row = 0
        
        for col_name in table.column_names:
            col = table.column(col_name)
            col_type = str(col.type)
            
            # Estimate bytes per value based on type
            bytes_per_value = 8  # Default to 8 bytes
            
            if 'string' in col_type.lower():
                # Sample some string values to estimate average length
                sample = col.slice(0, min(100, len(col)))
                try:
                    sample_values = [str(x) if x is not None else '' for x in sample.to_pylist()]
                    if sample_values:
                        avg_len = sum(len(s) for s in sample_values) / len(sample_values)
                        bytes_per_value = max(8, avg_len)
                except:
                    bytes_per_value = 32  # Default string size
            elif 'int8' in col_type or 'bool' in col_type:
                bytes_per_value = 1
            elif 'int16' in col_type or 'uint16' in col_type:
                bytes_per_value = 2
            elif 'int32' in col_type or 'uint32' in col_type or 'float' in col_type:
                bytes_per_value = 4
            elif 'int64' in col_type or 'uint64' in col_type or 'double' in col_type:
                bytes_per_value = 8
            elif 'dictionary' in col_type:
                # For dictionary encoded columns, calculate dictionary size + index size
                bytes_per_value = 12  # Higher overhead for dictionary encoding
            
            size_per_row += bytes_per_value
        
        # Calculate total size
        total_size = row_count * size_per_row
        
        # Add overhead for table structure (~5%)
        total_size = int(total_size * 1.05)
        
        return total_size
    except Exception as e:
        logger.warning(f"All size estimation methods failed: {e}")
        
        # Absolute fallback: use a very conservative estimate
        return len(table) * len(table.column_names) * 16  # 16 bytes per cell 