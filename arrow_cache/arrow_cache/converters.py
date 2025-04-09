import pyarrow as pa
import pandas as pd
import numpy as np
from typing import Any, Dict, Optional, Union, Tuple
import importlib.util
import io
import os
from pathlib import Path

# Check for optional dependencies
HAVE_GEOPANDAS = importlib.util.find_spec("geopandas") is not None
HAVE_PARQUET = importlib.util.find_spec("pyarrow.parquet") is not None

if HAVE_GEOPANDAS:
    import geopandas as gpd
    
if HAVE_PARQUET:
    import pyarrow.parquet as pq


def to_arrow_table(data: Any, preserve_index: bool = True) -> pa.Table:
    """
    Convert various data types to an Arrow table.
    
    Args:
        data: The data to convert (DataFrame, GeoDataFrame, file path, etc.)
        preserve_index: Whether to preserve the index as a column
        
    Returns:
        Arrow table
    """
    metadata = {"source_type": type(data).__name__}
    
    # Case 1: Already an Arrow table
    if isinstance(data, pa.Table):
        return data
    
    # Case 2: Pandas DataFrame
    if isinstance(data, pd.DataFrame):
        if HAVE_GEOPANDAS and isinstance(data, gpd.GeoDataFrame):
            table, _ = _convert_geodataframe(data, preserve_index, metadata)
            return table
        else:
            # For large dataframes, use a more memory-efficient approach
            if len(data) > 1000000:  # 1M+ rows
                return _convert_large_dataframe(data, preserve_index, metadata)
            else:
                table, _ = _convert_dataframe(data, preserve_index, metadata)
                return table
    
    # Case 3: Path to a file
    if isinstance(data, (str, Path)):
        path = str(data)
        ext = os.path.splitext(path)[1].lower()
        metadata["source_path"] = path
        
        # For large files, use more efficient loading methods
        file_size = os.path.getsize(path) if os.path.exists(path) else 0
        is_large_file = file_size > 1024 * 1024 * 1024  # 1GB+
        
        if ext == '.parquet' and HAVE_PARQUET:
            # For very large parquet files, don't load everything at once
            if is_large_file:
                return _load_large_parquet(path, metadata)
            else:
                table, _ = _load_parquet(path, metadata)
                return table
        elif ext == '.feather' or ext == '.arrow':
            table, _ = _load_feather(path, metadata)
            return table
        elif ext == '.csv':
            # For large CSV files, use chunked reader
            if is_large_file:
                return _load_large_csv(path, metadata)
            else:
                table, _ = _load_csv(path, metadata)
                return table
        elif ext in ['.geojson', '.shp', '.gpkg'] and HAVE_GEOPANDAS:
            table, _ = _load_geofile(path, metadata)
            return table
        else:
            raise ValueError(f"Unsupported file extension: {ext}")
    
    # Case 4: Convert NumPy array to Arrow array
    if isinstance(data, np.ndarray):
        table, _ = _convert_numpy(data, metadata)
        return table
    
    # Case 5: Convert dict of arrays or list of dicts
    if isinstance(data, dict) and all(isinstance(v, (list, np.ndarray)) for v in data.values()):
        return pa.Table.from_pydict(data)
    
    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        table, _ = _convert_list_of_dicts(data, metadata)
        return table
    
    raise ValueError(f"Unsupported data type: {type(data).__name__}")


def from_arrow_table(table: pa.Table, target_type: str, metadata: Optional[Dict[str, Any]] = None) -> Any:
    """
    Convert an Arrow table back to the original data type.
    
    Args:
        table: The Arrow table to convert
        target_type: The target type ('pandas', 'geopandas', etc.)
        metadata: Additional metadata about the original data
    
    Returns:
        Converted data in the target format
    """
    metadata = metadata or {}
    
    if target_type == 'arrow' or target_type == 'pyarrow.Table':
        return table
    
    if target_type == 'pandas' or target_type == 'pd.DataFrame':
        # Use efficient conversion to pandas
        return table.to_pandas(
            split_blocks=True,  # Split result blocks by column for better memory usage
            self_destruct=True,  # Allow Arrow to reuse memory from the Table
            date_as_object=False,  # Keep dates as native types
        )
    
    if target_type == 'geopandas' or target_type == 'gpd.GeoDataFrame':
        if not HAVE_GEOPANDAS:
            raise ValueError("GeoPandas is not installed")
        return _arrow_to_geodataframe(table, metadata)
    
    if target_type == 'dict':
        # Convert directly to dict without going through pandas
        return {col: table[col].to_numpy() for col in table.column_names}
    
    if target_type == 'numpy':
        # Try to convert directly to numpy when possible
        # For simple tables with homogeneous types
        if table.num_columns == 1:
            # Single column table can be converted directly
            return table.column(0).to_numpy()
        else:
            # For multiple columns, try to use Arrow's RecordBatch for efficient conversion
            batch = table.to_batches()[0] if table.num_rows > 0 else None
            if batch:
                try:
                    # Try direct conversion if all columns have compatible types
                    return batch.to_numpy()
                except:
                    # Fall back to pandas conversion if direct conversion fails
                    return table.to_pandas().to_numpy()
            else:
                # Empty table
                return np.array([])
    
    if target_type == 'parquet' and HAVE_PARQUET:
        # Return as in-memory parquet bytes
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)
        return buffer
    
    if target_type == 'feather':
        # Return as in-memory feather bytes
        buffer = io.BytesIO()
        with pa.ipc.new_file(buffer) as writer:
            writer.write(table)
        buffer.seek(0)
        return buffer
    
    raise ValueError(f"Unsupported target type: {target_type}")


def estimate_size_bytes(table: pa.Table) -> int:
    """
    Estimate the size of an Arrow table in bytes accurately.
    
    Args:
        table: The Arrow table to measure
        
    Returns:
        Estimated size in bytes
    """
    # Use a set to track unique buffer addresses to avoid double-counting
    # shared memory in sliced tables
    unique_buffers = set()
    size = 0
    
    for column in table.columns:
        # Handle ChunkedArray by iterating through its chunks
        if isinstance(column, pa.ChunkedArray):
            for chunk in column.chunks:
                # Check if the chunk is a slice/view of another array
                is_slice = hasattr(chunk, '_is_slice') and chunk._is_slice
                
                for buf in chunk.buffers():
                    if buf is not None:
                        # Use buffer address as a unique identifier
                        buffer_id = id(buf)
                        if buffer_id not in unique_buffers:
                            unique_buffers.add(buffer_id)
                            size += buf.size
        else:
            # Handle Array directly
            # Check if the array is a slice/view
            is_slice = hasattr(column, '_is_slice') and column._is_slice
            
            for buf in column.buffers():
                if buf is not None:
                    # Use buffer address as a unique identifier
                    buffer_id = id(buf)
                    if buffer_id not in unique_buffers:
                        unique_buffers.add(buffer_id)
                        size += buf.size
    
    # Add schema overhead
    schema_size = len(table.schema.to_string()) * 2  # Rough estimate for schema
    
    # Add metadata size if present
    metadata_size = 0
    if table.schema.metadata:
        for k, v in table.schema.metadata.items():
            metadata_size += len(k) + len(v)
    
    # Add buffer management overhead (conservative estimate)
    overhead = table.num_columns * 16  # Reduced from 24 to 16 bytes per column pointer
    
    # Safety check to prevent unrealistically large values
    # This helps catch potential miscalculations
    total_size = size + schema_size + metadata_size + overhead
    if total_size > 100 * 1024 * 1024 * 1024:  # > 100GB
        # Sanity check against system memory
        import psutil
        system_memory = psutil.virtual_memory().total
        if total_size > system_memory * 2:
            # This is clearly an error - cap at a reasonable percentage of system memory
            return int(system_memory * 0.8)
    
    return total_size


# Private helper functions for conversion

def _convert_dataframe(df: pd.DataFrame, preserve_index: bool, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Convert a pandas DataFrame to an Arrow table."""
    metadata["pandas_version"] = pd.__version__
    metadata["columns"] = list(df.columns)
    
    if preserve_index and not df.index.equals(pd.RangeIndex(len(df))):
        df = df.reset_index()
        metadata["had_index"] = True
    
    return pa.Table.from_pandas(df), metadata


def _convert_geodataframe(gdf: 'gpd.GeoDataFrame', preserve_index: bool, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Convert a GeoPandas GeoDataFrame to an Arrow table."""
    metadata["pandas_version"] = pd.__version__
    metadata["geopandas_version"] = gpd.__version__
    metadata["crs"] = str(gdf.crs)
    metadata["geometry_column"] = gdf._geometry_column_name
    metadata["columns"] = list(gdf.columns)
    
    if preserve_index and not gdf.index.equals(pd.RangeIndex(len(gdf))):
        gdf = gdf.reset_index()
        metadata["had_index"] = True
    
    # Convert to WKB for storage
    wkb_series = gdf.geometry.apply(lambda geom: geom.wkb if geom else None)
    
    # Replace geometry column with WKB
    result = gdf.copy()
    result[gdf._geometry_column_name] = wkb_series
    
    return pa.Table.from_pandas(result), metadata


def _arrow_to_geodataframe(table: pa.Table, metadata: Dict[str, Any]) -> 'gpd.GeoDataFrame':
    """Convert an Arrow table back to a GeoPandas GeoDataFrame."""
    import geopandas as gpd
    from shapely import wkb
    
    # Convert to pandas first
    df = table.to_pandas()
    
    # Get the geometry column name
    geometry_column = metadata.get("geometry_column", "geometry")
    
    # Convert WKB back to geometry
    df[geometry_column] = df[geometry_column].apply(
        lambda x: wkb.loads(x) if x is not None else None
    )
    
    # Create GeoDataFrame
    gdf = gpd.GeoDataFrame(df, geometry=geometry_column)
    
    # Set CRS if available
    if "crs" in metadata:
        gdf.set_crs(metadata["crs"], inplace=True)
    
    # Restore index if needed
    if metadata.get("had_index", False) and "index" in df.columns:
        gdf.set_index("index", inplace=True)
    
    return gdf


def _convert_numpy(arr: np.ndarray, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Convert a NumPy array to an Arrow table."""
    metadata["numpy_version"] = np.__version__
    metadata["shape"] = arr.shape
    metadata["dtype"] = str(arr.dtype)
    
    # For 1D arrays
    if arr.ndim == 1:
        return pa.Table.from_arrays([pa.array(arr)], names=["values"]), metadata
    
    # For 2D arrays (matrix)
    if arr.ndim == 2:
        arrays = [pa.array(arr[:, i]) for i in range(arr.shape[1])]
        names = [f"col_{i}" for i in range(arr.shape[1])]
        return pa.Table.from_arrays(arrays, names=names), metadata
    
    # For higher dimensions, flatten to 1D
    flattened = arr.flatten()
    metadata["original_shape"] = arr.shape
    return pa.Table.from_arrays([pa.array(flattened)], names=["values"]), metadata


def _convert_list_of_dicts(data: list, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Convert a list of dictionaries to an Arrow table."""
    metadata["source_records"] = len(data)
    
    if not data:
        # Return empty table
        return pa.table({}), metadata
        
    # Convert directly to Arrow table without pandas
    # First, collect all unique keys to determine schema
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())
    
    # Create a dictionary of lists for each key
    columns = {key: [] for key in all_keys}
    
    # Fill the dictionaries
    for item in data:
        for key in all_keys:
            columns[key].append(item.get(key, None))
            
    # Convert to Arrow arrays
    arrays = {key: pa.array(values) for key, values in columns.items()}
    
    # Create Arrow table
    return pa.Table.from_pydict(arrays), metadata


def _load_parquet(path: str, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Load a Parquet file as an Arrow table."""
    metadata["file_format"] = "parquet"
    table = pq.read_table(path)
    
    # Extract parquet metadata
    parquet_metadata = pq.read_metadata(path)
    metadata["num_row_groups"] = parquet_metadata.num_row_groups
    metadata["num_rows"] = parquet_metadata.num_rows
    metadata["created_by"] = parquet_metadata.created_by
    
    return table, metadata


def _load_feather(path: str, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Load a Feather file as an Arrow table."""
    metadata["file_format"] = "feather"
    return pa.ipc.open_file(path).read_all(), metadata


def _load_csv(path: str, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Load a CSV file as an Arrow table."""
    metadata["file_format"] = "csv"
    
    # Use Arrow's CSV reader directly instead of going through pandas
    import pyarrow.csv as csv
    
    # Configure CSV reading options for best performance
    read_options = csv.ReadOptions(use_threads=True, block_size=4*1024*1024)  # 4MB blocks
    parse_options = csv.ParseOptions(delimiter=',')
    convert_options = csv.ConvertOptions(
        strings_can_be_null=True,
        auto_dict_encode=True,  # Use dictionary encoding for strings
        include_columns=None,
        include_missing_columns=False
    )
    
    # Read directly to Arrow table
    table = csv.read_csv(
        path, 
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options
    )
    
    # Add file info to metadata
    metadata["num_rows"] = table.num_rows
    metadata["num_columns"] = table.num_columns
    metadata["column_names"] = table.column_names
    
    return table, metadata


def _load_geofile(path: str, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Load a geographic file as an Arrow table."""
    import geopandas as gpd
    
    ext = os.path.splitext(path)[1].lower()
    metadata["file_format"] = ext.lstrip('.')
    
    gdf = gpd.read_file(path)
    return _convert_geodataframe(gdf, True, metadata)


def _convert_large_dataframe(df: pd.DataFrame, preserve_index: bool, metadata: Dict[str, Any]) -> pa.Table:
    """
    Convert a large pandas DataFrame to an Arrow table with lower memory usage.
    
    Args:
        df: DataFrame to convert
        preserve_index: Whether to preserve the index
        metadata: Conversion metadata
        
    Returns:
        Arrow table
    """
    import gc
    
    metadata["pandas_version"] = pd.__version__
    metadata["columns"] = list(df.columns)
    
    # Handle the index directly with Arrow
    if preserve_index and not df.index.equals(pd.RangeIndex(len(df))):
        # Create index array directly instead of using reset_index
        index_array = pa.array(df.index.values)
        index_name = df.index.name or 'index'
        metadata["had_index"] = True
        
        # Create arrays for existing columns
        arrays = [index_array]
        field_names = [index_name]
    else:
        arrays = []
        field_names = []
    
    # Process columns in batches to reduce memory usage
    # Process columns in batches of 20 to reduce peak memory usage
    column_batches = [list(df.columns)[i:i+20] for i in range(0, len(df.columns), 20)]
    
    for batch in column_batches:
        # Process each column batch
        batch_arrays = []
        
        for col in batch:
            # Convert column to Arrow array
            try:
                # Try to optimize numeric columns
                if pd.api.types.is_numeric_dtype(df[col]):
                    # For numeric columns, convert directly to numpy then Arrow
                    arr = pa.array(df[col].values, type=None)
                else:
                    # For other types, use direct Arrow conversion
                    # Avoid going through Python objects when possible
                    if pd.api.types.is_string_dtype(df[col]):
                        arr = pa.array(df[col].values, type=pa.string())
                    elif pd.api.types.is_bool_dtype(df[col]):
                        arr = pa.array(df[col].values, type=pa.bool_())
                    elif pd.api.types.is_datetime64_dtype(df[col]):
                        arr = pa.array(df[col].values, type=pa.timestamp('ns'))
                    else:
                        arr = pa.array(df[col])
                
                batch_arrays.append(arr)
                field_names.append(col)
            except Exception as e:
                # If conversion fails, try a more robust approach
                try:
                    # Try to convert through numpy when possible
                    arr = pa.array(df[col].values)
                except:
                    # Last resort - use Python objects
                    arr = pa.array(df[col].tolist())
                batch_arrays.append(arr)
                field_names.append(col)
        
        arrays.extend(batch_arrays)
        
        # Explicitly clean up batch references
        del batch_arrays
        gc.collect()
    
    # Create the table from arrays
    table = pa.Table.from_arrays(arrays, names=field_names)
    
    # Clean up references to large objects
    del arrays
    gc.collect()
    
    return table


def _load_large_parquet(path: str, metadata: Dict[str, Any]) -> pa.Table:
    """
    Load a large parquet file efficiently.
    
    Args:
        path: Path to the parquet file
        metadata: Conversion metadata
        
    Returns:
        Arrow table
    """
    import pyarrow.parquet as pq
    import pyarrow as pa
    import os
    import gc
    
    metadata["file_format"] = "parquet"
    
    # Read metadata without loading the whole file
    parquet_metadata = pq.read_metadata(path)
    metadata["num_row_groups"] = parquet_metadata.num_row_groups
    metadata["num_rows"] = parquet_metadata.num_rows
    metadata["created_by"] = parquet_metadata.created_by
    
    # Load the schema
    schema = pq.read_schema(path)
    
    # Get system info for better parallelism
    import psutil
    cpu_count = os.cpu_count() or 4
    available_memory = psutil.virtual_memory().available
    
    # For extremely large files, use row group-level reading
    if parquet_metadata.num_rows > 100000000 or os.path.getsize(path) > 4 * 1024 * 1024 * 1024:  # 100M+ rows or 4GB+
        # Calculate optimal number of row groups to read at once based on file size and available memory
        file_size = os.path.getsize(path)
        
        # Target using at most 25% of available memory at once
        target_memory = min(available_memory * 0.25, 2 * 1024 * 1024 * 1024)  # Max 2GB chunks
        
        # Estimate row groups per batch
        avg_row_group_size = file_size / parquet_metadata.num_row_groups
        row_groups_per_batch = max(1, int(target_memory / avg_row_group_size))
        
        # Read in parallel by processing multiple batches of row groups simultaneously
        from concurrent.futures import ThreadPoolExecutor
        
        # Function to read a batch of row groups
        def read_row_group_batch(batch_indices):
            return pq.read_table(
                path, 
                use_threads=True,
                memory_map=True,  # Use memory mapping for better performance
                row_groups=batch_indices
            )
        
        # Create batches of row group indices
        batches = []
        for i in range(0, parquet_metadata.num_row_groups, row_groups_per_batch):
            end_idx = min(i + row_groups_per_batch, parquet_metadata.num_row_groups)
            batches.append(list(range(i, end_idx)))
        
        # Use thread pool to read batches in parallel, but limit threads to avoid oversubscription
        tables = []
        max_threads = min(len(batches), max(2, cpu_count - 1))
        with ThreadPoolExecutor(max_workers=max_threads) as executor:
            for batch_table in executor.map(read_row_group_batch, batches):
                tables.append(batch_table)
                # Force garbage collection after each batch to prevent memory buildup
                gc.collect()
        
        # Concatenate the tables
        result = pa.concat_tables(tables)
        
        # Force garbage collection again
        del tables
        gc.collect()
        
        return result
    
    # For moderately large files, use memory mapping and parallel reading
    try:
        # Use the maximum number of threads (leave one for system)
        max_threads = max(2, cpu_count - 1)
        table = pq.read_table(
            path, 
            use_threads=True,
            memory_map=True,  # Use memory mapping for better performance
            use_pandas_metadata=False  # Skip pandas metadata for faster loading
        )
        return table
    except pa.lib.ArrowMemoryError:
        # If we run out of memory, fall back to row group reading
        tables = []
        for i in range(parquet_metadata.num_row_groups):
            # Read one row group at a time
            batch = pq.read_table(path, row_groups=[i], use_threads=True)
            tables.append(batch)
            
            # Force garbage collection after each batch
            if i % 10 == 0:
                gc.collect()
        
        # Concatenate the tables
        result = pa.concat_tables(tables)
        
        # Force garbage collection
        del tables
        gc.collect()
        
        return result


def _load_large_csv(path: str, metadata: Dict[str, Any]) -> pa.Table:
    """
    Load a large CSV file efficiently.
    
    Args:
        path: Path to the CSV file
        metadata: Conversion metadata
        
    Returns:
        Arrow table
    """
    import pyarrow.csv as csv
    
    metadata["file_format"] = "csv"
    
    # For large CSV files, read a sample to get the schema
    read_options = csv.ReadOptions(block_size=1024*1024)  # 1MB blocks
    parse_options = csv.ParseOptions(delimiter=',')
    convert_options = csv.ConvertOptions(
        strings_can_be_null=True,
        include_columns=None,
        include_missing_columns=False,
        auto_dict_encode=True  # Use dictionary encoding for strings
    )
    
    # Read the schema and a small sample
    with open(path, 'rb') as f:
        # Read a small sample to infer schema
        sample_table = csv.read_csv(
            f, 
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )
    
    # For extremely large files, return just the schema
    file_size = os.path.getsize(path)
    if file_size > 5 * 1024 * 1024 * 1024:  # 5GB+
        # Create an empty table with the schema
        empty_arrays = []
        for field in sample_table.schema:
            empty_arrays.append(pa.array([], type=field.type))
        
        # We return an empty table with correct schema
        # This is a placeholder - actual data will be loaded via streaming
        return pa.Table.from_arrays(empty_arrays, schema=sample_table.schema)
    
    # For moderately large files, use better options
    read_options = csv.ReadOptions(use_threads=True, block_size=8*1024*1024)  # 8MB blocks
    
    # Read the full file
    return csv.read_csv(
        path, 
        read_options=read_options,
        parse_options=parse_options,
        convert_options=convert_options
    )


def guess_format(file_path: str) -> Optional[str]:
    """
    Guess the format of a file based on its extension.
    
    Args:
        file_path: Path to the file or URL
        
    Returns:
        String format identifier or None if format cannot be determined
    """
    # Handle URLs by extracting just the filename
    if '://' in file_path:
        from urllib.parse import urlparse
        parsed_url = urlparse(file_path)
        file_path = os.path.basename(parsed_url.path)
    
    # Get the file extension
    _, ext = os.path.splitext(file_path.lower())
    if not ext:
        return None
    
    # Match extension to format
    if ext in ['.parquet', '.pq']:
        return 'parquet'
    elif ext in ['.feather', '.arrow']:
        return 'arrow'
    elif ext == '.csv':
        return 'csv'
    elif ext == '.json':
        return 'json'
    elif ext in ['.xls', '.xlsx']:
        return 'excel'
    elif ext == '.geojson' and HAVE_GEOPANDAS:
        return 'geojson'
    elif ext == '.geoparquet' and HAVE_GEOPANDAS:
        return 'geoparquet'
    
    # Unknown extension
    return None
