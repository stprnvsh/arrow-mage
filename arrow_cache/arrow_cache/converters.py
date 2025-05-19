import pyarrow as pa
import pandas as pd
import numpy as np
from typing import Any, Dict, Optional, Union, Tuple
import importlib.util
import io
import os
from pathlib import Path
import logging
import sys
import re # Added for regex in geometry info

# Check for optional dependencies
HAVE_GEOPANDAS = importlib.util.find_spec("geopandas") is not None
HAVE_PARQUET = importlib.util.find_spec("pyarrow.parquet") is not None

if HAVE_GEOPANDAS:
    import geopandas as gpd
    
if HAVE_PARQUET:
    import pyarrow.parquet as pq

# Get the module's logger
logger = logging.getLogger(__name__)

# Try to detect GeoArrow extension types
try:
    # Check if pyarrow has geo support integrated or via extension type check
    HAS_GEOARROW = hasattr(pa, 'has_geo') and pa.has_geo()
    # Alternatively, check for specific types if has_geo() isn't present
    if not HAS_GEOARROW:
         # Import specific types if available (adjust based on actual geoarrow lib structure)
         # This is a placeholder, actual import might differ
         # from pyarrow.compute import geoarrow # Example
         # HAS_GEOARROW = True 
         pass # Keep HAS_GEOARROW false if specific types aren't found/checked
except ImportError:
    HAS_GEOARROW = False
logger.info(f"GeoArrow detection: {HAS_GEOARROW}")

def safe_dictionary_encode(column: pa.Array) -> pa.Array:
    """
    Safely apply dictionary encoding to a column.
    
    Args:
        column: PyArrow array to encode
        
    Returns:
        Dictionary-encoded array or original array if encoding fails
    """
    # Skip if column is already dictionary encoded
    if pa.types.is_dictionary(column.type):
        return column
        
    # Skip null columns
    if column.null_count == len(column):
        return column
        
    import pyarrow.compute as pc
    
    # Apply dictionary encoding
    try:
        return pc.dictionary_encode(column)
    except:
        # Return original if encoding fails
        return column


def to_arrow_table(data: Any, preserve_index: bool = True, format: Optional[str] = None) -> pa.Table:
    """
    Convert various data types to an Arrow table.
    
    Args:
        data: The data to convert (DataFrame, GeoDataFrame, file path, etc.)
        preserve_index: Whether to preserve the index as a column
        format: Optional file format hint when data is a path
        
    Returns:
        Arrow table
    """
    # Import pandas here to ensure it's available in all code paths
    import pandas as pd
    
    metadata = {"source_type": type(data).__name__}
    
    # Case 1: Already an Arrow table
    if isinstance(data, pa.Table):
        return data
    
    # Case 2: Pandas DataFrame
    if isinstance(data, pd.DataFrame):
        if HAVE_GEOPANDAS and isinstance(data, gpd.GeoDataFrame):
            table, geo_meta = _convert_geodataframe(data, preserve_index)
            metadata.update(geo_meta) # Merge geospatial metadata
            return table
        else:
            table, _ = _convert_dataframe(data, preserve_index, metadata)
            return table
    
    # Case 3: NumPy array to Arrow array
    if isinstance(data, np.ndarray):
        table, _ = _convert_numpy(data, metadata)
        return table
    
    # Case 4: Convert dict of arrays or list of dicts
    if isinstance(data, dict) and all(isinstance(v, (list, np.ndarray)) for v in data.values()):
        return pa.Table.from_pydict(data)
    
    if isinstance(data, list) and all(isinstance(item, dict) for item in data):
        table, _ = _convert_list_of_dicts(data, metadata)
        return table
    
    # Case 5: Handle BytesIO or StringIO objects (in-memory buffers)
    if isinstance(data, io.BytesIO) or isinstance(data, io.StringIO):
        if not format:
            raise ValueError("Format must be specified when using in-memory buffers")
        
        # Reset the buffer position to the beginning
        data.seek(0)
        
        if format == 'parquet':
            if not HAVE_PARQUET:
                raise ImportError("pyarrow.parquet is required for reading Parquet files")
            import pyarrow.parquet as pq
            return pq.read_table(data)
        
        elif format == 'feather' or format == 'arrow':
            reader = pa.ipc.open_file(data)
            return reader.read_all()
        
        elif format == 'csv':
            import pyarrow.csv as csv
            read_options = csv.ReadOptions(use_threads=True)
            parse_options = csv.ParseOptions(delimiter=',')
            convert_options = csv.ConvertOptions(strings_can_be_null=True)
            return csv.read_csv(data, read_options=read_options, 
                             parse_options=parse_options,
                             convert_options=convert_options)
        
        elif format == 'json':
            # For JSON, use pandas as a bridge
            # Determine if we have text or binary data
            if isinstance(data, io.BytesIO):
                # Convert binary to string
                content = data.getvalue().decode('utf-8')
                json_data = io.StringIO(content)
            else:
                json_data = data
            
            # Use pandas to parse JSON
            df = pd.read_json(json_data)
            return pa.Table.from_pandas(df, preserve_index=preserve_index)
        
        else:
            raise ValueError(f"Unsupported in-memory format: {format}")
    
    # Case 6: Path to a file
    if isinstance(data, (str, Path)):
        path = str(data)
        metadata["source_path"] = path
        
        # Use format parameter if provided, otherwise infer from extension
        ext = os.path.splitext(path)[1].lower()
        file_format = format or ext[1:] if ext else None
        
        if file_format == 'parquet' or ext == '.parquet' or ext == '.pq':
            if not HAVE_PARQUET:
                raise ImportError("pyarrow.parquet is required for reading Parquet files")
            table, _ = _load_parquet(path, metadata)
            return table
        elif file_format == 'geoparquet' or ext == '.geoparquet':
            if not HAVE_PARQUET or not HAVE_GEOPANDAS:
                raise ImportError("pyarrow.parquet and geopandas are required for reading GeoParquet files")
            # For GeoParquet, we use GeoPandas since it knows how to handle the geo metadata
            if HAVE_GEOPANDAS:
                import geopandas as gpd
                gdf = gpd.read_parquet(path)
                # Extract geospatial metadata
                geo_metadata = {
                    "geometry_column": gdf.geometry.name,
                    "crs_info": gdf.crs.to_string() if gdf.crs else None,
                    "geometry_type": gdf.geom_type.mode()[0] if not gdf.geom_type.empty else None,
                    "storage_format": "WKB",
                    "source_format": "geoparquet"
                }
                metadata.update(geo_metadata)
                return pa.Table.from_pandas(gdf)
            else:
                # Fallback to regular parquet if geopandas is not available
                logger.warning("GeoPandas not available, reading GeoParquet file as regular Parquet")
                table, _ = _load_parquet(path, metadata)
                return table
        elif file_format == 'feather' or file_format == 'arrow' or ext == '.feather' or ext == '.arrow':
            table, _ = _load_feather(path, metadata)
            return table
        elif file_format == 'csv' or ext == '.csv':
            table, _ = _load_csv(path, metadata)
            return table
        elif file_format == 'json' or ext == '.json':
            # Handle JSON files directly without temporary files
            with open(path, 'r', encoding='utf-8') as f:
                df = pd.read_json(f)
            return pa.Table.from_pandas(df, preserve_index=preserve_index)
        elif (file_format == 'geojson' or ext == '.geojson' or 
              file_format == 'shp' or ext == '.shp' or 
              file_format == 'gpkg' or ext == '.gpkg') and HAVE_GEOPANDAS:
            table, geo_meta = _load_geofile(path)
            metadata.update(geo_meta) # Merge geospatial metadata
            metadata["source_path"] = path # Ensure path is added
            return table
        else:
            # Raise a specific error if the format derived from path/param is unsupported
            available_formats = [
                'parquet', 'feather', 'arrow', 'csv', 'json',
                'geojson' if HAVE_GEOPANDAS else None,
                'shp' if HAVE_GEOPANDAS else None,
                'gpkg' if HAVE_GEOPANDAS else None
            ]
            available_formats = [f for f in available_formats if f]  # Filter out None values
            error_format = file_format or ext or "(unknown)"
            raise ValueError(
                f"Unsupported file format '{error_format}' for path '{path}'. "
                f"Ensure the format is one of {available_formats} and required dependencies are installed."
            )

    # This error is now only raised if the initial data type is not Table, DataFrame, ndarray, dict, list, or path string
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
        return table.to_pandas()
    
    if target_type == 'geopandas' or target_type == 'gpd.GeoDataFrame':
        if not HAVE_GEOPANDAS:
            raise ValueError("GeoPandas is not installed")
        return _arrow_to_geodataframe(table, metadata)
    
    if target_type == 'dict':
        # Convert directly to dict using pyarrow
        return table.to_pydict()
    
    if target_type == 'numpy':
        # Use Arrow's RecordBatch for efficient conversion to numpy
        if table.num_rows == 0:
            return np.array([])
        
        # Convert to structured numpy array
        return table.to_pandas().to_numpy()
    
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
    # Use Arrow's memory pool to get an accurate measurement
    import gc
    
    # Force a GC cycle first to clean up any lingering objects
    gc.collect()
    
    # Get the default memory pool
    memory_pool = pa.default_memory_pool()
    
    # Capture memory before accessing the table
    memory_before = memory_pool.bytes_allocated()
    
    # Access the data to ensure it's materialized
    for col in table.columns:
        # Just access one buffer per chunk to force materialization
        for chunk in col.chunks:
            if len(chunk) > 0:
                chunk[0]  # Access first element to materialize
                break
    
    # Measure memory after access
    memory_after = memory_pool.bytes_allocated()
    
    # Calculate the difference
    table_size = memory_after - memory_before
    
    # Add schema overhead
    schema_size = len(table.schema.to_string()) * 2
    
    # Add metadata size if present
    metadata_size = 0
    if table.schema.metadata:
        for k, v in table.schema.metadata.items():
            metadata_size += len(k) + len(v)
    
    return table_size + schema_size + metadata_size


# Private helper functions for conversion

def _convert_dataframe(df: pd.DataFrame, preserve_index: bool, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Convert a pandas DataFrame to an Arrow table."""
    # Import pandas to ensure it's available in this scope
    import pandas as pd
    
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
    
    # Convert GeoDataFrame to Arrow, store geometry as WKB
    # Extract geospatial metadata
    geo_metadata = {
        "geometry_column": gdf.geometry.name,
        "crs_info": gdf.crs.to_string() if gdf.crs else None,
        "geometry_type": gdf.geom_type.mode()[0] if not gdf.geom_type.empty else None, # Dominant type
        "storage_format": "WKB" # Assume WKB for Arrow storage
    }
    
    # Convert GeoDataFrame to Arrow Table, handling geometry
    table = pa.Table.from_pandas(result, preserve_index=preserve_index)
    
    # Potentially update metadata with source type info
    metadata["source_type"] = "GeoDataFrame"
    
    return table, geo_metadata


def _arrow_to_geodataframe(table: pa.Table, metadata: Dict[str, Any]) -> 'gpd.GeoDataFrame':
    """Convert an Arrow table back to a GeoPandas GeoDataFrame."""
    # Import pandas and geopandas to ensure they're available in this scope
    import pandas as pd
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
    read_options = csv.ReadOptions(use_threads=True, block_size=8*1024*1024)  # 8MB blocks
    parse_options = csv.ParseOptions(delimiter=',')
    convert_options = csv.ConvertOptions(
        strings_can_be_null=True,
        auto_dict_encode=True  # Use dictionary encoding for strings
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


def _load_geofile(path: str) -> Tuple[pa.Table, Dict[str, Any]]:
    """
    Load a geospatial file using GeoPandas and return an Arrow Table and metadata.
    
    Args:
        path: Path to the geospatial file
        
    Returns:
        Tuple of (Arrow table, geospatial metadata dictionary)
    """
    if not HAVE_GEOPANDAS:
        raise ImportError("GeoPandas is required for reading geospatial files.")
    
    gdf = gpd.read_file(path)
    
    # Extract geospatial metadata
    geo_metadata = {
        "geometry_column": gdf.geometry.name,
        "crs_info": gdf.crs.to_string() if gdf.crs else None,
        "geometry_type": gdf.geom_type.mode()[0] if not gdf.geom_type.empty else None, # Dominant type
        "storage_format": "WKB" # Assume WKB for Arrow storage
    }
    
    # Convert to Arrow Table
    table = pa.Table.from_pandas(gdf)
    
    return table, geo_metadata


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

def _map_arrow_type_to_duckdb(arrow_type: pa.DataType) -> str:
    """Maps PyArrow DataType to DuckDB SQL type string."""
    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"
    elif pa.types.is_int8(arrow_type):
        return "TINYINT"
    elif pa.types.is_int16(arrow_type):
        return "SMALLINT"
    elif pa.types.is_int32(arrow_type):
        return "INTEGER"
    elif pa.types.is_int64(arrow_type):
        return "BIGINT"
    elif pa.types.is_uint8(arrow_type):
        return "UTINYINT"
    elif pa.types.is_uint16(arrow_type):
        return "USMALLINT"
    elif pa.types.is_uint32(arrow_type):
        return "UINTEGER"
    elif pa.types.is_uint64(arrow_type):
        return "UBIGINT"
    elif pa.types.is_float16(arrow_type):
        return "FLOAT" # DuckDB might map half-float to FLOAT
    elif pa.types.is_float32(arrow_type):
        return "FLOAT"
    elif pa.types.is_float64(arrow_type):
        return "DOUBLE"
    elif pa.types.is_decimal(arrow_type):
        # DuckDB DECIMAL(width, scale)
        return f"DECIMAL({arrow_type.precision}, {arrow_type.scale})"
    elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return "DATE"
    elif pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        return "TIME"
    elif pa.types.is_timestamp(arrow_type):
        # DuckDB TIMESTAMP supports nanoseconds if compiled with it
        # Otherwise defaults to microseconds. Let DuckDB handle precision.
        return "TIMESTAMP"
    elif pa.types.is_duration(arrow_type):
        return "INTERVAL"
    elif pa.types.is_binary(arrow_type) or pa.types.is_large_binary(arrow_type):
        return "BLOB"
    elif pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return "VARCHAR"
    elif pa.types.is_list(arrow_type) or pa.types.is_large_list(arrow_type):
        # Map Arrow list to DuckDB list
        value_type = _map_arrow_type_to_duckdb(arrow_type.value_type)
        return f"{value_type}[]"
    elif pa.types.is_struct(arrow_type):
        # Map Arrow struct to DuckDB struct
        fields = [f"{field.name} { _map_arrow_type_to_duckdb(field.type)}" for field in arrow_type]
        return f"STRUCT({', '.join(fields)})"
    elif pa.types.is_map(arrow_type):
         key_type = _map_arrow_type_to_duckdb(arrow_type.key_type)
         item_type = _map_arrow_type_to_duckdb(arrow_type.item_type)
         return f"MAP({key_type}, {item_type})"
    elif pa.types.is_dictionary(arrow_type):
        # Map dictionary to its index type in DuckDB
        return _map_arrow_type_to_duckdb(arrow_type.index_type)
    elif pa.types.is_null(arrow_type):
        return "NULL" # Or handle appropriately
    else:
        # Fallback for unknown types
        logger.warning(f"Unknown Arrow type '{arrow_type}'. Mapping to VARCHAR.")
        return "VARCHAR"

def _get_geometry_info(table: pa.Table) -> Tuple[Optional[str], Optional[str]]:
    """
    Inspects an Arrow Table's schema to identify the geometry column and its format.

    Checks for common names ('geometry', 'geom', 'wkb') and binary type for WKB.
    If GeoArrow is available, checks for specific GeoArrow extension types.

    Returns:
        Tuple of (geometry_column_name, storage_format)
        storage_format can be 'WKB', 'GeoArrow', or None.
    """
    potential_geom_cols = {} # Store potential candidates: {col_name: format}

    # Common geometry column names (case-insensitive)
    common_names = ['geometry', 'geom', 'shape', 'wkb', 'wkt']
    name_pattern = re.compile(f"^({'|'.join(common_names)})$", re.IGNORECASE)

    for field in table.schema:
        col_name = field.name
        col_type = field.type

        # 1. Check using GeoArrow Extension Types (if available)
        if HAS_GEOARROW:
            # This requires knowing the specific GeoArrow type names or how to check them
            # Example placeholder check:
            # if isinstance(col_type, pa.ExtensionType) and 'geoarrow' in col_type.extension_name:
            #    storage_format = "GeoArrow" # Could refine based on specific type (point, wkb, etc.)
            #    potential_geom_cols[col_name] = storage_format
            #    continue # Found via GeoArrow type, move to next field
            pass # Add actual GeoArrow type checks here if library/spec known

        # 2. Check by Name and Type (fallback or primary if no GeoArrow)
        if name_pattern.match(col_name):
            if pa.types.is_binary(col_type) or pa.types.is_large_binary(col_type):
                # Strong indicator of WKB if name matches and type is binary
                potential_geom_cols[col_name] = 'WKB'
            elif pa.types.is_string(col_type) or pa.types.is_large_string(col_type):
                 # Could be WKT, but we'll prioritize binary WKB if found
                 if col_name not in potential_geom_cols: # Don't overwrite WKB detection
                      potential_geom_cols[col_name] = 'WKT' # Less common for storage, but possible

    # Prioritize detection results
    if not potential_geom_cols:
        return None, None # No likely geometry column found

    # Priority: 'geometry' name with WKB/GeoArrow, then other names
    if 'geometry' in potential_geom_cols and potential_geom_cols['geometry'] in ('WKB', 'GeoArrow'):
        return 'geometry', potential_geom_cols['geometry']

    # Check other common names with WKB/GeoArrow format
    for name in common_names:
        if name in potential_geom_cols and potential_geom_cols[name] in ('WKB', 'GeoArrow'):
            return name, potential_geom_cols[name]

    # Fallback: return the first detected potential column (could be WKT)
    first_col = next(iter(potential_geom_cols))
    return first_col, potential_geom_cols[first_col]


def estimate_size_bytes(data: Any) -> int:
    # ... existing code ...
    # Fallback: Pandas memory usage
    if isinstance(data, pd.DataFrame):
        try:
            # Deep estimate for object types
            return data.memory_usage(deep=True).sum()
        except Exception:
            # Simple estimate if deep fails
            return data.memory_usage().sum()
    # Fallback: GeoPandas memory usage
    elif HAVE_GEOPANDAS and isinstance(data, gpd.GeoDataFrame):
         try:
             return data.memory_usage(deep=True).sum()
         except Exception:
             return data.memory_usage().sum()

    # Last resort: Basic Python object size
    return sys.getsizeof(data)