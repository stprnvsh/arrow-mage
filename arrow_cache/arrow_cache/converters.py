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
            table, _ = _convert_dataframe(data, preserve_index, metadata)
            return table
    
    # Case 3: Path to a file
    if isinstance(data, (str, Path)):
        path = str(data)
        ext = os.path.splitext(path)[1].lower()
        metadata["source_path"] = path
        
        if ext == '.parquet' and HAVE_PARQUET:
            table, _ = _load_parquet(path, metadata)
            return table
        elif ext == '.feather' or ext == '.arrow':
            table, _ = _load_feather(path, metadata)
            return table
        elif ext == '.csv':
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
        return table.to_pandas()
    
    if target_type == 'geopandas' or target_type == 'gpd.GeoDataFrame':
        if not HAVE_GEOPANDAS:
            raise ValueError("GeoPandas is not installed")
        return _arrow_to_geodataframe(table, metadata)
    
    if target_type == 'dict':
        return {col: table[col].to_numpy() for col in table.column_names}
    
    if target_type == 'numpy':
        # Convert to a structured numpy array
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
    Estimate the size of an Arrow table in bytes.
    
    Args:
        table: The Arrow table to measure
        
    Returns:
        Estimated size in bytes
    """
    # Get total size from Arrow's buffer sizes
    size = 0
    for column in table.columns:
        # Handle ChunkedArray by iterating through its chunks
        if isinstance(column, pa.ChunkedArray):
            for chunk in column.chunks:
                for buf in chunk.buffers():
                    if buf is not None:
                        size += buf.size
        else:
            # Handle Array directly
            for buf in column.buffers():
                if buf is not None:
                    size += buf.size
    
    # Add schema overhead
    schema_size = len(table.schema.to_string()) * 2  # Rough estimate
    
    # Add buffer management overhead (conservative estimate)
    overhead = table.num_columns * 24  # Roughly 24 bytes per column pointer
    
    return size + schema_size + overhead


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
    df = pd.DataFrame(data)
    metadata["source_records"] = len(data)
    return pa.Table.from_pandas(df), metadata


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
    df = pd.read_csv(path)
    return pa.Table.from_pandas(df), metadata


def _load_geofile(path: str, metadata: Dict[str, Any]) -> Tuple[pa.Table, Dict[str, Any]]:
    """Load a geographic file as an Arrow table."""
    import geopandas as gpd
    
    ext = os.path.splitext(path)[1].lower()
    metadata["file_format"] = ext.lstrip('.')
    
    gdf = gpd.read_file(path)
    return _convert_geodataframe(gdf, True, metadata)
