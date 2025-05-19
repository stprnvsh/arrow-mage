"""
Data loading functions for Arrow Cache MCP.

This module provides functions to load datasets from various sources using DuckDB's connectors.
"""

import os
import time
import logging
import pyarrow as pa
from typing import Dict, List, Optional, Tuple, Any, Union
from urllib.parse import urlparse
import pandas as pd
from .core import get_arrow_cache
from .utils import clean_dataset_name

# Import the DuckDB ingest module from local package
from .duckdb_ingest import import_from_duckdb, import_to_cache

# Configure logging
logger = logging.getLogger(__name__)

# Sample datasets (placeholder)
SAMPLE_DATASETS = {}

# Supported file formats
SUPPORTED_FORMATS = {
    'csv': {'extensions': ['.csv']},
    'parquet': {'extensions': ['.parquet', '.pq']},
    'arrow': {'extensions': ['.arrow', '.feather']},
    'feather': {'extensions': ['.feather']},
    'json': {'extensions': ['.json']},
    'ndjson': {'extensions': ['.ndjson', '.jsonl']},
    'postgres': {'extensions': []},
    'mysql': {'extensions': []},
    'sqlserver': {'extensions': []},
    'sqlite': {'extensions': ['.db', '.sqlite']},
    'duckdb': {'extensions': ['.duckdb']},
    'iceberg': {'extensions': []},
    's3': {'extensions': []},
    'http': {'extensions': []},
    'https': {'extensions': []},
}

# Try to import optional geospatial dependencies
try:
    import pyarrow.dataset as ds
    SUPPORTED_FORMATS['geojson'] = {'extensions': ['.geojson']}
    SUPPORTED_FORMATS['geoparquet'] = {'extensions': ['.geoparquet']}
    HAS_GEOSPATIAL = True
except ImportError:
    HAS_GEOSPATIAL = False
    logger.warning("Geospatial features will be limited.")

def guess_format_from_path(file_path: str) -> Optional[str]:
    """
    Guess the format of a file based on its extension or URL scheme.
    
    Args:
        file_path: Path or URL to the file
        
    Returns:
        Format name or None if unknown
    """
    # Check URL schemes
    if file_path.startswith("s3://"):
        return "s3"
    elif file_path.startswith("http://"):
        return "http"
    elif file_path.startswith("https://"):
        return "https"
    elif file_path.startswith("postgresql://") or file_path.startswith("postgres://"):
        return "postgres"
    elif file_path.startswith("mysql://"):
        return "mysql"
    elif file_path.startswith("sqlserver://") or file_path.startswith("mssql://"):
        return "sqlserver"
    elif file_path.startswith("iceberg://"):
        return "iceberg"
    
    # Extract just the filename from path or URL
    if '://' in file_path:  # It's a URL
        parsed_url = urlparse(file_path)
        file_path = os.path.basename(parsed_url.path)
    
    ext = os.path.splitext(file_path.lower())[1]
    if not ext:
        return None
        
    for format_name, format_info in SUPPORTED_FORMATS.items():
        if ext in format_info['extensions']:
            return format_name
            
    # If we get here, the extension wasn't recognized
    return None

def load_dataset_from_url(url: str, dataset_name: Optional[str] = None, 
                         format: Optional[str] = None, **kwargs) -> Tuple[bool, Union[Dict[str, Any], str]]:
    """
    Load a dataset from a URL into the cache using DuckDB's connectors.
    
    Args:
        url: URL of the dataset file
        dataset_name: Name to assign to the dataset (defaults to filename)
        format: Format of the dataset (autodetected if None)
        **kwargs: Additional arguments to pass to the reader
        
    Returns:
        Tuple of (success, result_or_error_message)
    """
    if not dataset_name:
        # Extract name from URL
        path = urlparse(url).path
        base_name = os.path.basename(path).split('.')[0]
        # Clean up name
        dataset_name = clean_dataset_name(base_name)
    
    # Check if dataset already exists
    cache = get_arrow_cache()
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    try:
        # Determine format from URL if not specified
        if not format:
            format = guess_format_from_path(url)
            if not format:
                return False, "Could not determine format from URL. Please specify format explicitly."
            logger.info(f"Detected format from URL: {format}")
        
        # Prepare options based on kwargs
        options = {}
        if 'csv_options' in kwargs:
            options['csv_options'] = kwargs.get('csv_options')
        if 'json_options' in kwargs:
            options['json_options'] = kwargs.get('json_options')
        if 's3_options' in kwargs:
            options['s3_options'] = kwargs.get('s3_options')
        
        # Add any DuckDB configuration options
        duckdb_config = {}
        for key, value in kwargs.items():
            if key.startswith('duckdb_'):
                config_key = key.replace('duckdb_', '')
                duckdb_config[config_key] = value
        
        if duckdb_config:
            options['duckdb_config'] = duckdb_config
        
        logger.info(f"Loading dataset from URL {url} with format {format}")
        start_time = time.time()
        
        # Use the new DuckDB import_to_cache function to directly load into the cache
        try:
            # Extract connection options if provided
            connection_options = kwargs.get('connection_options', None)
            query = kwargs.get('query', None)
            table_name = kwargs.get('table_name', None)
            schema = kwargs.get('schema', None)
            ttl = kwargs.get('ttl', None)
            
            # Metadata to store with the dataset
            metadata = {
                'source': url,
                'format': format,
                'loaded_at': time.time(),
            }
            
            # Add any additional metadata from kwargs
            if 'metadata' in kwargs and isinstance(kwargs['metadata'], dict):
                metadata.update(kwargs['metadata'])
            
            # Import directly to cache using DuckDB's connectors
            key = import_to_cache(
                cache_instance=cache,
                key=dataset_name,
                source=url,
                source_type=format,
                connection_options=connection_options,
                query=query,
                table_name=table_name,
                schema=schema,
                ttl=ttl,
                metadata=metadata,
                options=options
            )
            
            # Get imported table metadata
            table_metadata = cache.get_metadata(key)
            if table_metadata:
                metadata.update(table_metadata)
            
            # Calculate memory usage and update metadata
            try:
                from arrow_cache.memory import estimate_table_memory_usage
                table = cache.get(key, limit=1)  # Get minimal sample to access schema
                size_bytes = estimate_table_memory_usage(table)
                metadata['size_bytes'] = size_bytes
                metadata['size_mb'] = size_bytes / (1024 * 1024)
            except Exception as size_err:
                logger.warning(f"Could not estimate size: {size_err}")
            
            # Update additional metadata
            load_time = time.time() - start_time
            metadata['load_time_seconds'] = load_time
            metadata['total_time_seconds'] = load_time
            metadata['name'] = dataset_name
            
            return True, metadata
            
        except Exception as e:
            logger.error(f"Error loading dataset with DuckDB: {e}")
            raise
        
    except Exception as e:
        import traceback
        logger.error(f"Error loading from URL: {e}")
        logger.debug(traceback.format_exc())
        return False, f"Error loading from URL: {str(e)}"

def load_dataset_from_path(path: str, dataset_name: Optional[str] = None, 
                           format: Optional[str] = None, **kwargs) -> Tuple[bool, Union[Dict[str, Any], str]]:
    """
    Load a dataset from a local path using DuckDB's connectors.
    
    Args:
        path: Path to the dataset file
        dataset_name: Name to assign to the dataset (defaults to filename)
        format: Format of the dataset (autodetected if None)
        **kwargs: Additional arguments to pass to the reader
        
    Returns:
        Tuple of (success, result_or_error_message)
    """
    # For HTTP/HTTPS URLs, redirect to load_dataset_from_url
    if path.startswith("http://") or path.startswith("https://"):
        return load_dataset_from_url(path, dataset_name, format, **kwargs)
    
    if not dataset_name:
        # Extract name from path (remove extension)
        base_name = os.path.basename(path).split('.')[0]
        # Clean up name
        dataset_name = clean_dataset_name(base_name)
    
    # Check if dataset with this name already exists
    cache = get_arrow_cache()
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    # Determine format if not provided
    if not format:
        format = guess_format_from_path(path)
        if not format:
            return False, f"Could not determine format from file extension. Please specify it explicitly."
    
    logger.info(f"Loading file {path} with format: {format}")
    
    try:
        # Prepare options based on kwargs
        options = {}
        if 'csv_options' in kwargs:
            options['csv_options'] = kwargs.get('csv_options')
        if 'json_options' in kwargs:
            options['json_options'] = kwargs.get('json_options')
        
        # Add any DuckDB configuration options
        duckdb_config = {}
        for key, value in kwargs.items():
            if key.startswith('duckdb_'):
                config_key = key.replace('duckdb_', '')
                duckdb_config[config_key] = value
        
        if duckdb_config:
            options['duckdb_config'] = duckdb_config
        
        start_time = time.time()
        
        # Extract connection options and other parameters if provided
        connection_options = kwargs.get('connection_options', None)
        query = kwargs.get('query', None)
        table_name = kwargs.get('table_name', None)
        schema = kwargs.get('schema', None)
        ttl = kwargs.get('ttl', None)
        
        # Metadata to store with the dataset
        metadata = {
            'source': path,
            'format': format,
            'loaded_at': time.time(),
        }
        
        # Add any additional metadata from kwargs
        if 'metadata' in kwargs and isinstance(kwargs['metadata'], dict):
            metadata.update(kwargs['metadata'])
        
        # Check if we should use optimized loading for large files
        if os.path.exists(path):  # Only for local files
            file_size = os.path.getsize(path)
            is_large_file = file_size > 500 * 1024 * 1024  # 500MB+
            
            if is_large_file:
                # For large files, add specific DuckDB config options
                logger.info(f"Using optimized settings for large file: {file_size/(1024*1024*1024):.2f} GB")
                if 'duckdb_config' not in options:
                    options['duckdb_config'] = {}
                
                # Add optimization settings for large files
                options['duckdb_config'].update({
                    'memory_limit': '8GB',
                    'threads': str(min(8, os.cpu_count() or 4)),
                })
        
        # Import directly to cache using DuckDB's connectors
        key = import_to_cache(
            cache_instance=cache,
            key=dataset_name,
            source=path,
            source_type=format,
            connection_options=connection_options,
            query=query,
            table_name=table_name,
            schema=schema,
            ttl=ttl,
            metadata=metadata,
            options=options
        )
        
        # Get imported table metadata
        table_metadata = cache.get_metadata(key)
        if table_metadata:
            metadata.update(table_metadata)
        
        # Calculate memory usage and update metadata
        try:
            from arrow_cache.memory import estimate_table_memory_usage
            table = cache.get(key, limit=1)  # Get minimal sample to access schema
            size_bytes = estimate_table_memory_usage(table)
            metadata['size_bytes'] = size_bytes
            metadata['size_mb'] = size_bytes / (1024 * 1024)
        except Exception as size_err:
            logger.warning(f"Could not estimate size: {size_err}")
        
        # Update timing information
        load_time = time.time() - start_time
        metadata['load_time_seconds'] = load_time
        metadata['total_time_seconds'] = load_time
        metadata['name'] = dataset_name
        
        return True, metadata
    
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        return False, f"Error loading dataset: {str(e)}"

def load_dataset_from_upload(uploaded_file: Any, dataset_name: Optional[str] = None, 
                            format: Optional[str] = None, **kwargs) -> Tuple[bool, Union[Dict[str, Any], str]]:
    """
    Load a dataset from an uploaded file into the cache using DuckDB ingest.
    
    Args:
        uploaded_file: File-like object with getvalue() method
        dataset_name: Name to assign to the dataset (defaults to filename)
        format: Format of the dataset (autodetected if None)
        **kwargs: Additional arguments to pass to the reader
        
    Returns:
        Tuple of (success, result_or_error_message)
    """
    if not dataset_name:
        base_name = uploaded_file.name.split('.')[0]
        # Clean up name
        dataset_name = clean_dataset_name(base_name)
    
    # Check if dataset already exists
    cache = get_arrow_cache()
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    # Determine format if not provided
    if not format:
        format = guess_format_from_path(uploaded_file.name)
        if not format:
            return False, f"Could not determine format from file extension. Please specify it explicitly."
    
    try:
        # Create a temporary file to save the uploaded content
        import tempfile
        
        with tempfile.NamedTemporaryFile(delete=False, suffix=f".{format}") as temp_file:
            temp_path = temp_file.name
            # Write the uploaded file content to the temporary file
            temp_file.write(uploaded_file.getvalue())
        
        logger.info(f"Saved uploaded file to temporary location: {temp_path}")
        
        try:
            # Use load_dataset_from_path with the temporary file
            success, result = load_dataset_from_path(
                path=temp_path,
                dataset_name=dataset_name,
                format=format,
                **kwargs
            )
            
            # Update source information in metadata
            if success and isinstance(result, dict):
                result['source'] = f"uploaded_{uploaded_file.name}"
            
            return success, result
        finally:
            # Clean up the temporary file
            try:
                os.unlink(temp_path)
                logger.info(f"Removed temporary file: {temp_path}")
            except Exception as cleanup_err:
                logger.warning(f"Failed to remove temporary file {temp_path}: {cleanup_err}")
    
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        return False, f"Error loading dataset from upload: {str(e)}"

def load_database_table(connection_string: str, table_name: str, dataset_name: Optional[str] = None,
                      database_type: str = "postgres", schema: Optional[str] = None, 
                      **kwargs) -> Tuple[bool, Union[Dict[str, Any], str]]:
    """
    Load a table from a database into the cache using DuckDB's database extensions.
    
    Args:
        connection_string: Database connection string
        table_name: Table name to load
        dataset_name: Name to assign to the dataset (defaults to table_name)
        database_type: Type of database ('postgres', 'mysql', 'sqlserver', 'sqlite')
        schema: Database schema name
        **kwargs: Additional arguments to pass to the reader
        
    Returns:
        Tuple of (success, result_or_error_message)
    """
    if not dataset_name:
        # Use table name as dataset name if not provided
        dataset_name = clean_dataset_name(table_name)
    
    # Check if dataset already exists
    cache = get_arrow_cache()
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    try:
        # Prepare options based on kwargs
        options = {}
        
        # Add any DuckDB configuration options
        duckdb_config = {}
        for key, value in kwargs.items():
            if key.startswith('duckdb_'):
                config_key = key.replace('duckdb_', '')
                duckdb_config[config_key] = value
        
        if duckdb_config:
            options['duckdb_config'] = duckdb_config
        
        start_time = time.time()
        
        # Extract connection options if provided
        connection_options = kwargs.get('connection_options', None)
        ttl = kwargs.get('ttl', None)
        
        # Metadata to store with the dataset
        metadata = {
            'source': connection_string,
            'format': database_type,
            'table_name': table_name,
            'loaded_at': time.time(),
        }
        
        if schema:
            metadata['schema'] = schema
        
        # Add any additional metadata from kwargs
        if 'metadata' in kwargs and isinstance(kwargs['metadata'], dict):
            metadata.update(kwargs['metadata'])
        
        # Import directly to cache using DuckDB's connectors
        # This will calculate and add size information in import_to_cache function
        key = import_to_cache(
            cache_instance=cache,
            key=dataset_name,
            source=connection_string,
            source_type=database_type,
            connection_options=connection_options,
            table_name=table_name,
            schema=schema,
            ttl=ttl,
            metadata=metadata,
            options=options
        )
        
        # Get the metadata that was created by import_to_cache
        imported_metadata = cache.get_metadata(key)
        
        # Create final metadata, starting with our original metadata
        final_metadata = metadata.copy()
        
        # Merge metadata returned by import_to_cache
        if imported_metadata:
            # Selectively update metadata to keep size and other fields calculated in import_to_cache
            for k, v in imported_metadata.items():
                # Keep these fields from the imported metadata
                if k in ['size_bytes', 'size_mb', 'row_count', 'column_count', 'columns', 
                         'column_types', 'dtypes']:
                    final_metadata[k] = v
                # For other keys, maintain our original values if they exist
                elif k not in final_metadata:
                    final_metadata[k] = v
        
        # Update timing information
        load_time = time.time() - start_time
        final_metadata['load_time_seconds'] = load_time
        final_metadata['total_time_seconds'] = load_time
        final_metadata['name'] = dataset_name
        
        # Log the size information for debugging
        if 'size_bytes' in final_metadata:
            logger.info(f"Table size: {final_metadata['size_bytes']} bytes ({final_metadata.get('size_mb', 0):.2f} MB)")
        
        return True, final_metadata
    
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        return False, f"Error loading database table: {str(e)}"

def load_database_query(connection_string: str, query: str, dataset_name: Optional[str] = None,
                      database_type: str = "postgres", **kwargs) -> Tuple[bool, Union[Dict[str, Any], str]]:
    """
    Load the results of a database query into the cache using DuckDB's database extensions.
    
    Args:
        connection_string: Database connection string
        query: SQL query to execute
        dataset_name: Name to assign to the dataset
        database_type: Type of database ('postgres', 'mysql', 'sqlserver', 'sqlite')
        **kwargs: Additional arguments to pass to the reader
        
    Returns:
        Tuple of (success, result_or_error_message)
    """
    if not dataset_name:
        # Generate a name based on a hash of the query
        import hashlib
        query_hash = hashlib.md5(query.encode()).hexdigest()[:8]
        dataset_name = f"{database_type}_query_{query_hash}"
    
    # Check if dataset already exists
    cache = get_arrow_cache()
    if cache.contains(dataset_name):
        return False, f"Dataset '{dataset_name}' already exists. Please use a different name or remove it first."
    
    try:
        # Prepare options based on kwargs
        options = {}
        
        # Add any DuckDB configuration options
        duckdb_config = {}
        for key, value in kwargs.items():
            if key.startswith('duckdb_'):
                config_key = key.replace('duckdb_', '')
                duckdb_config[config_key] = value
        
        if duckdb_config:
            options['duckdb_config'] = duckdb_config
        
        start_time = time.time()
        
        # Extract connection options if provided
        connection_options = kwargs.get('connection_options', None)
        ttl = kwargs.get('ttl', None)
        
        # Metadata to store with the dataset
        metadata = {
            'source': connection_string,
            'format': database_type,
            'query': query,
            'loaded_at': time.time(),
        }
        
        # Add any additional metadata from kwargs
        if 'metadata' in kwargs and isinstance(kwargs['metadata'], dict):
            metadata.update(kwargs['metadata'])
        
        # Import directly to cache using DuckDB's connectors
        key = import_to_cache(
            cache_instance=cache,
            key=dataset_name,
            source=connection_string,
            source_type=database_type,
            connection_options=connection_options,
            query=query,
            ttl=ttl,
            metadata=metadata,
            options=options
        )
        
        # Get imported table metadata
        table_metadata = cache.get_metadata(key)
        if table_metadata:
            metadata.update(table_metadata)
        
        # Update timing information
        load_time = time.time() - start_time
        metadata['load_time_seconds'] = load_time
        metadata['total_time_seconds'] = load_time
        metadata['name'] = dataset_name
        
        return True, metadata
    
    except Exception as e:
        import traceback
        logger.error(traceback.format_exc())
        return False, f"Error loading database query: {str(e)}" 