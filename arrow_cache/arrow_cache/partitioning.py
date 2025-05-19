"""
Table partitioning module for ArrowCache

Handles automatic partitioning of large datasets into manageable chunks
both in memory and on disk.
"""
import os
import uuid
import time
import logging
import threading
from typing import Dict, List, Optional, Tuple, Union, Set, Any, Iterator
import json
import re
import traceback

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import numpy as np
import psutil
from arrow_cache.memory import estimate_table_memory_usage

logger = logging.getLogger(__name__)

def _parse_type_str(type_str):
    """Parse a string representation of an Arrow type into an actual type instance."""
    import pyarrow as pa
    
    # Clean up the type string
    type_str = type_str.strip().lower()
    
    # Map common type strings to Arrow type instances
    if 'string' in type_str:
        return pa.string()
    elif 'int64' in type_str or 'int32' in type_str:
        return pa.int64()
    elif 'int' in type_str:
        return pa.int32()
    elif 'float' in type_str or 'double' in type_str:
        return pa.float64()
    elif 'bool' in type_str:
        return pa.bool_()
    elif 'date' in type_str:
        return pa.date32()
    elif 'timestamp' in type_str:
        return pa.timestamp('ns')
    elif 'binary' in type_str:
        return pa.binary()
    else:
        # Default to string for unknown types
        return pa.string()

class TablePartition:
    """
    Represents a single partition of an Arrow table
    """
    def __init__(
        self,
        table: Optional[pa.Table] = None,
        partition_id: str = None,
        size_bytes: int = 0,
        row_count: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
        path: Optional[str] = None,
        spill_path: Optional[str] = None
    ):
        """
        Initialize a table partition
        
        Args:
            table: Arrow table data for this partition (None if spilled to disk)
            partition_id: Unique identifier for this partition
            size_bytes: Size of this partition in bytes
            row_count: Number of rows in this partition
            metadata: Additional metadata for this partition
            path: Path where this partition is stored (for persistent partitions)
            spill_path: Path where this partition is spilled (for temporary spills)
        """
        self.partition_id = partition_id or str(uuid.uuid4())
        self._table = table
        self.size_bytes = size_bytes
        self.row_count = row_count
        self.metadata = metadata or {}
        self.path = path
        self.spill_path = spill_path
        self.created_at = time.time()
        self.last_accessed_at = self.created_at
        self.access_count = 0
        self.is_pinned = False
        self.is_loaded = table is not None
        self.lock = threading.RLock()
    
    def get_table(self) -> pa.Table:
        """
        Get the Arrow table for this partition, loading it from disk if necessary
        
        Returns:
            The Arrow table
        """
        with self.lock:
            self.last_accessed_at = time.time()
            self.access_count += 1
            
            if self._table is not None:
                return self._table
            
            # Table is not in memory, load it
            error_messages = []
            
            # First try loading from spill path if available
            if self.spill_path and os.path.exists(self.spill_path):
                try:
                    # Check for metadata first for better loading
                    meta_path = os.path.join(os.path.dirname(self.spill_path), 
                                           f"{self.partition_id}.meta.json")
                    
                    import pyarrow.parquet as pq
                    
                    # Use optimized reading options
                    read_options = {
                        "use_threads": True,
                        "use_pandas_metadata": True
                    }
                    
                    # Try to read table
                    self._table = pq.read_table(self.spill_path, **read_options)
                    self.is_loaded = True
                    logger.debug(f"Successfully loaded partition {self.partition_id} from spill path")
                    return self._table
                except Exception as e:
                    error_msg = f"Failed to load partition from spill path: {e}"
                    logger.warning(error_msg)
                    error_messages.append(error_msg)
            
            # If spill path failed, try persistent path
            if self.path and os.path.exists(self.path):
                try:
                    import pyarrow.parquet as pq
                    
                    # Use optimized reading options
                    read_options = {
                        "use_threads": True,
                        "use_pandas_metadata": True
                    }
                    
                    self._table = pq.read_table(self.path, **read_options)
                    self.is_loaded = True
                    return self._table
                except Exception as e:
                    error_msg = f"Failed to load partition from persistent path: {e}"
                    logger.error(error_msg)
                    error_messages.append(error_msg)
            
            # If we get here, both paths failed
            error_details = "\n".join(error_messages)
            raise ValueError(f"Partition {self.partition_id} data not available. Errors:\n{error_details}")
    
    def spill(self, spill_dir: str) -> bool:
        """
        Spill this partition to disk to free memory using Arrow's I/O
        
        Args:
            spill_dir: Directory to spill to
            
        Returns:
            True if spilled successfully, False otherwise
        """
        if self.is_pinned:
            return False
            
        with self.lock:
            if self._table is None:
                # Already spilled or not loaded
                return True
                
            if not os.path.exists(spill_dir):
                os.makedirs(spill_dir, exist_ok=True)
                
            try:
                # Set destination path
                self.spill_path = os.path.join(spill_dir, f"{self.partition_id}.parquet")
                
                # Write table metadata to JSON for better recovery capabilities
                meta_path = os.path.join(spill_dir, f"{self.partition_id}.meta.json")
                try:
                    spill_metadata = {
                        "partition_id": self.partition_id,
                        "size_bytes": self.size_bytes,
                        "row_count": self.row_count,
                        "created_at": self.created_at,
                        "last_accessed_at": self.last_accessed_at,
                        "access_count": self.access_count,
                        "format": "parquet",
                        "schema": str(self._table.schema),
                        "metadata": self.metadata
                    }
                    with open(meta_path, 'w') as f:
                        json.dump(spill_metadata, f)
                except Exception as e:
                    logger.warning(f"Failed to save partition metadata for {self.partition_id}: {e}")
                
                # Write directly with Arrow Parquet writer with optimized settings for spill files
                import pyarrow.parquet as pq
                
                # Preserve schema metadata by ensuring it's included in the write
                writer_options = {
                    "compression": "zstd",  # Use fast compression for spill files
                    "compression_level": 1,  # Low compression level for better speed
                    "version": "2.6",  # Latest version for best compatibility
                    "store_schema": True,  # Ensure schema is stored with the file
                }
                
                pq.write_table(self._table, self.spill_path, **writer_options)
                
                # Release memory
                self._table = None
                self.is_loaded = False
                
                # Record the time we spilled
                self.last_spilled_at = time.time()
                
                return True
            except Exception as e:
                logger.error(f"Failed to spill partition to disk: {e}")
                logger.error(traceback.format_exc())
                self.spill_path = None
                return False
    
    def persist(self, storage_dir: str) -> bool:
        """
        Persist this partition to permanent storage using Arrow's I/O
        
        Args:
            storage_dir: Directory to persist to
            
        Returns:
            True if persisted successfully, False otherwise
        """
        with self.lock:
            table = self.get_table()  # Load if necessary
            
            if not os.path.exists(storage_dir):
                os.makedirs(storage_dir, exist_ok=True)
                
            try:
                # Write table metadata
                meta_path = os.path.join(storage_dir, f"{self.partition_id}.json")
                metadata = {
                    "partition_id": self.partition_id,
                    "size_bytes": self.size_bytes,
                    "row_count": self.row_count,
                    "created_at": self.created_at,
                    "metadata": self.metadata
                }
                
                with open(meta_path, 'w') as f:
                    json.dump(metadata, f)
                
                # Set the final path
                self.path = os.path.join(storage_dir, f"{self.partition_id}.parquet")
                
                # Write to parquet with compression using Arrow directly
                pq.write_table(
                    table, 
                    self.path,
                    compression='zstd',
                    compression_level=3
                )
                
                return True
            except Exception as e:
                logger.error(f"Failed to persist partition: {e}")
                # Clean up metadata file if it was created
                if 'meta_path' in locals() and os.path.exists(meta_path):
                    try:
                        os.unlink(meta_path)
                    except:
                        pass
                self.path = None
                return False
    
    def pin(self) -> None:
        """Pin this partition in memory to prevent spilling"""
        with self.lock:
            self.is_pinned = True
            # Ensure it's loaded
            if not self.is_loaded:
                try:
                    self.get_table()
                    self.is_loaded = True
                except Exception as e:
                    logger.error(f"Failed to load partition {self.partition_id} during pin: {e}")
                    # Don't clear pin status - we want to keep it pinned even if load failed
    
    def unpin(self) -> None:
        """Unpin this partition, allowing it to be spilled"""
        with self.lock:
            self.is_pinned = False
    
    def free(self) -> None:
        """Free this partition's memory"""
        with self.lock:
            if not self.is_pinned:
                self._table = None
                self.is_loaded = False


class PartitionedTable:
    """
    Manages a collection of table partitions
    """
    def __init__(
        self,
        key: str,
        schema: pa.Schema,
        config: Any,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a partitioned table
        
        Args:
            key: Unique identifier for this table
            schema: Arrow schema for the table
            config: ArrowCache configuration
            metadata: Additional metadata
        """
        self.key = key
        self.schema = schema
        self.config = config
        self.metadata = metadata or {}
        self.partition_metadata = {}
        self.partitions = {}
        self.partition_order = []  # Maintain order for queries
        self.total_rows = 0
        self.total_size_bytes = 0
        self.lock = threading.RLock()
        self.partition_counter = 0
    
    def add_partition(
        self,
        table: pa.Table,
        partition_id: Optional[str] = None
    ) -> str:
        """
        Add a partition to this table
        
        Args:
            table: Arrow table data for the partition
            partition_id: Optional partition ID (will be generated if not provided)
            
        Returns:
            Partition ID
        """
        with self.lock:
            # Generate partition ID if not provided
            if partition_id is None:
                partition_id = f"{self.key}_{self.partition_counter}"
                self.partition_counter += 1
            
            # Calculate size, checking for slices to avoid double-counting
            size_bytes = 0
            # Check different ways a table might be a slice
            is_slice = (hasattr(table, '_is_slice') and table._is_slice) or (hasattr(table, '_table'))
            # Only fully count the first partition, or non-slice partitions
            if (len(self.partition_order) == 0 or not is_slice):
                for column in table.columns:
                    for chunk in column.chunks:
                        for buf in chunk.buffers():
                            if buf is not None:
                                size_bytes += buf.size
            else:
                # For subsequent slices, estimate a more accurate size
                # based on row count proportion rather than a fixed divisor
                
                # Get size of the first partition as a reference
                if len(self.partition_order) > 0:
                    first_partition_id = self.partition_order[0]
                    first_partition_meta = self.partition_metadata[first_partition_id]
                    first_partition_rows = first_partition_meta.get("row_count", 1)
                    first_partition_size = first_partition_meta.get("size_bytes", 1000)
                    
                    # Calculate size based on row proportion with minimum floor
                    row_proportion = max(0.01, table.num_rows / max(1, first_partition_rows))
                    size_bytes = int(first_partition_size * row_proportion)
                    
                    # Apply adjustment factor for common compression scenarios
                    if hasattr(table, '_is_slice') and table._is_slice:
                        # Sliced tables often share memory buffers, apply reduction factor
                        size_bytes = int(size_bytes * 0.7)  # 30% reduction for memory sharing
                else:
                    # Fallback if we don't have a first partition
                    size_bytes = sum(column.nbytes for column in table.columns)
                
                # Ensure a minimum size to avoid tracking issues
                size_bytes = max(1024, size_bytes)  # At least 1KB per partition
            
            # Create partition
            partition = TablePartition(
                table=table,
                partition_id=partition_id,
                size_bytes=size_bytes,
                row_count=table.num_rows,
                metadata={"parent_key": self.key}
            )
            
            # Add to our collections
            self.partitions[partition_id] = partition
            self.partition_order.append(partition_id)
            self.partition_metadata[partition_id] = {
                "row_count": table.num_rows,
                "size_bytes": size_bytes,
                "schema": table.schema,
                "row_offset": self.total_rows
            }
            
            # Update totals
            self.total_rows += table.num_rows
            self.total_size_bytes += size_bytes
            
            return partition_id
    
    def get_partition(self, partition_id: str) -> Optional[TablePartition]:
        """
        Get a specific partition by ID
        
        Args:
            partition_id: ID of the partition to get
            
        Returns:
            The partition or None if not found
        """
        return self.partitions.get(partition_id)
    
    def get_partitions(self) -> List[TablePartition]:
        """
        Get all partitions in order
        
        Returns:
            List of table partitions
        """
        return [self.partitions[pid] for pid in self.partition_order]
    
    def get_partition_for_row(self, row_idx: int) -> Optional[TablePartition]:
        """
        Get the partition containing a specific row
        
        Args:
            row_idx: Global row index
            
        Returns:
            The partition containing the row or None if out of bounds
        """
        if row_idx < 0 or row_idx >= self.total_rows:
            return None
            
        # Find the partition containing this row
        current_offset = 0
        for partition_id in self.partition_order:
            metadata = self.partition_metadata[partition_id]
            next_offset = current_offset + metadata["row_count"]
            
            if row_idx < next_offset:
                # This partition contains the row
                return self.partitions[partition_id]
                
            current_offset = next_offset
            
        return None
    
    def get_table(self) -> pa.Table:
        """
        Get the full table by concatenating all partitions
        
        Returns:
            The concatenated Arrow table
        """
        with self.lock:
            tables = []
            for partition_id in self.partition_order:
                partition = self.partitions[partition_id]
                tables.append(partition.get_table())
            
            if not tables:
                # Return empty table with schema
                return pa.Table.from_arrays([], schema=self.schema)
            
            # Check for schema inconsistencies before concatenating
            if len(tables) > 1:
                # Compare schemas
                reference_schema = tables[0].schema
                for i, table in enumerate(tables[1:], 1):
                    if table.schema != reference_schema:
                        logger.warning(f"Schema mismatch detected in partition {self.partition_order[i]}")
                        logger.warning(f"Reference schema: {reference_schema}")
                        logger.warning(f"Partition schema: {table.schema}")
                        
                        # Try to harmonize schemas automatically
                        try:
                            self.standardize_partition_schemas()
                            # Reload tables with standardized schemas
                            tables = []
                            for pid in self.partition_order:
                                tables.append(self.partitions[pid].get_table())
                            break
                        except Exception as e:
                            logger.error(f"Failed to standardize schemas: {e}")
                            
            try:
                # Attempt to concatenate tables
                return pa.concat_tables(tables)
            except pa.ArrowInvalid as e:
                # If we get here, there's still a schema mismatch
                error_msg = str(e)
                if "Schema" in error_msg and "different" in error_msg:
                    logger.error(f"Schema mismatch during table concatenation: {e}")
                    # Print schema details for each partition to help diagnose
                    for i, table in enumerate(tables):
                        logger.error(f"Partition {i} ({self.partition_order[i]}) schema: {table.schema}")
                    
                    # Try one more standardization with more aggressive settings
                    try:
                        # Force conversion of all partitions to match first partition
                        reference_table = tables[0]
                        for i in range(1, len(tables)):
                            # Convert each table to pandas and back to enforce schema
                            df = tables[i].to_pandas()
                            tables[i] = pa.Table.from_pandas(df, schema=reference_table.schema)
                        return pa.concat_tables(tables)
                    except Exception as recovery_e:
                        logger.error(f"Failed to recover from schema mismatch: {recovery_e}")
                        raise
                else:
                    # Some other Arrow error
                    raise
    
    def filter_partitions(self, filter_expr: str) -> Set[str]:
        """
        Identify partitions that could match a filter expression
        
        Args:
            filter_expr: DuckDB filter expression
            
        Returns:
            Set of partition IDs that might match the filter
        """
        # This is a simple implementation - a more advanced one would parse the
        # filter expression and use partition statistics to determine which
        # partitions could match
        return set(self.partition_order)
    
    def get_slice(self, offset: int, length: int) -> pa.Table:
        """
        Get a slice of the table
        
        Args:
            offset: Starting row index
            length: Number of rows to get
            
        Returns:
            The sliced Arrow table
        """
        if offset < 0:
            raise ValueError("Offset must be non-negative")
            
        if length <= 0:
            raise ValueError("Length must be positive")
            
        with self.lock:
            result_tables = []
            row_count = 0
            
            # Find the partitions that contain the requested rows
            current_offset = 0
            for partition_id in self.partition_order:
                metadata = self.partition_metadata[partition_id]
                partition_rows = metadata["row_count"]
                next_offset = current_offset + partition_rows
                
                # Check if this partition contains any of the requested rows
                if next_offset <= offset:
                    # This partition is entirely before the requested range
                    current_offset = next_offset
                    continue
                    
                if current_offset >= offset + length:
                    # This partition is entirely after the requested range
                    break
                    
                # This partition contains some of the requested rows
                partition = self.partitions[partition_id]
                table = partition.get_table()
                
                # Calculate the slice of this partition to include
                partition_start = max(0, offset - current_offset)
                partition_end = min(partition_rows, offset + length - current_offset)
                partition_length = partition_end - partition_start
                
                if partition_start > 0 or partition_length < partition_rows:
                    # Need to slice this partition
                    table = table.slice(partition_start, partition_length)
                
                result_tables.append(table)
                row_count += table.num_rows
                
                if row_count >= length:
                    break
                    
                current_offset = next_offset
            
            if not result_tables:
                # Return empty table with schema
                return pa.Table.from_arrays([], schema=self.schema)
                
            return pa.concat_tables(result_tables)
    
    def spill_partitions(self, spill_dir: str, target_bytes: int) -> int:
        """
        Spill partitions to disk to free memory
        
        Args:
            spill_dir: Directory to spill to
            target_bytes: Target number of bytes to free
            
        Returns:
            Number of bytes freed
        """
        with self.lock:
            # Get partitions that can be spilled
            spillable = [
                (pid, self.partitions[pid]) 
                for pid in self.partition_order 
                if not self.partitions[pid].is_pinned and self.partitions[pid].is_loaded
            ]
            
            # Sort by last accessed time (oldest first)
            spillable.sort(key=lambda x: x[1].last_accessed_at)
            
            bytes_freed = 0
            for pid, partition in spillable:
                if bytes_freed >= target_bytes:
                    break
                    
                if partition.spill(spill_dir):
                    bytes_freed += partition.size_bytes
            
            return bytes_freed
    
    def persist_partitions(self, storage_dir: str) -> bool:
        """
        Persist all partitions to storage
        
        Args:
            storage_dir: Directory to persist to
            
        Returns:
            True if all partitions were persisted successfully
        """
        with self.lock:
            # Create table directory
            os.makedirs(storage_dir, exist_ok=True)
            
            # Persist each partition
            success = True
            for partition in self.get_partitions():
                if not partition.persist(storage_dir):
                    success = False
            
            return success
    
    @classmethod
    def load(cls, key: str, storage_dir: str, config: Any) -> 'PartitionedTable':
        """
        Load a partitioned table from storage
        
        Args:
            key: Table key
            storage_dir: Storage directory
            config: ArrowCache configuration
            
        Returns:
            The loaded partitioned table
        """
        table_dir = os.path.join(storage_dir, key)
        meta_path = os.path.join(table_dir, "_metadata.json")
        
        with open(meta_path, 'r') as f:
            metadata = json.load(f)
        
        # Create schema from field information in metadata
        # First check if we have a full schema string we can interpret
        schema = None
        if "schema" in metadata:
            try:
                # Try to get the schema from first partition on disk
                if "partitions" in metadata and metadata["partitions"]:
                    first_partition = metadata["partitions"][0]
                    partition_path = os.path.join(table_dir, f"{first_partition}.parquet")
                    if os.path.exists(partition_path):
                        # Read just the schema using PyArrow directly
                        schema = pq.read_schema(partition_path)
                        logger.info(f"Loaded schema from first partition: {first_partition}")
            except Exception as e:
                logger.warning(f"Could not read schema from partition: {e}")
        
        # If we couldn't get schema from partition, create from metadata
        if schema is None and "schema" in metadata:
            try:
                # If metadata includes detailed column info, use it
                if "columns" in metadata:
                    fields = []
                    for col in metadata["columns"]:
                        name = col["name"]
                        type_str = col["type"]
                        nullable = col.get("nullable", True)
                        field_type = _parse_type_str(type_str)
                        fields.append(pa.field(name, field_type, nullable=nullable))
                    schema = pa.schema(fields)
                else:
                    # Try to get basic field names from schema string
                    schema_str = metadata["schema"]
                    # Safe parsing of schema string
                    field_names = []
                    if isinstance(schema_str, str):
                        # Extract field names using simpler approach
                        for name in schema_str.replace("schema:", "").split(","):
                            match = re.search(r'field_name: (\w+)|name: (\w+)|"(\w+)"', name)
                            if match:
                                field_name = next(g for g in match.groups() if g is not None)
                                field_names.append(field_name)
                
                    # Create default schema if we found field names
                    if field_names:
                        schema = pa.schema([pa.field(name, pa.string()) for name in field_names])
            except Exception as e:
                logger.warning(f"Could not parse schema from metadata: {e}")
        
        # If we still don't have a schema, create a minimal default one
        if schema is None:
            schema = pa.schema([pa.field("value", pa.string())])
            logger.warning("Using default schema with single string column")
        
        # Create table instance
        table = cls(
            key=key,
            schema=schema,
            config=config,
            metadata=metadata.get("metadata", {})
        )
        
        # Set table properties
        table.total_rows = metadata["total_rows"]
        table.total_size_bytes = metadata["total_size_bytes"]
        table.partition_order = metadata["partitions"]
        table.partition_metadata = metadata["partition_metadata"]
        
        # Load partition metadata (but not the actual data)
        for partition_id in table.partition_order:
            partition_path = os.path.join(table_dir, f"{partition_id}.parquet")
            partition_meta = table.partition_metadata[partition_id]
            
            table.partitions[partition_id] = TablePartition(
                partition_id=partition_id,
                size_bytes=partition_meta["size_bytes"],
                row_count=partition_meta["row_count"],
                metadata={"parent_key": key},
                path=partition_path
            )
        
        return table
    
    def standardize_partition_schemas(self) -> None:
        """
        Standardize schemas across all partitions to ensure compatibility using native Arrow operations.
        
        This method ensures all partitions have the same schema by:
        1. Determining the most complete schema across all partitions
        2. Converting each partition to match that schema using Arrow's type system
        """
        if not self.partition_order:
            return  # No partitions to standardize
        
        with self.lock:
            # Collect schemas from all partitions
            partition_schemas = []
            for part_id in self.partition_order:
                if part_id in self.partitions:
                    partition = self.partitions[part_id]
                    try:
                        table = partition.get_table()
                        partition_schemas.append(table.schema)
                    except Exception as e:
                        logger.warning(f"Couldn't get schema for partition {part_id}: {e}")
            
            if not partition_schemas:
                logger.error("No valid partition schemas found")
                return
            
            # Build a unified schema that includes all fields from all partitions
            # First, collect all fields by name
            all_fields = {}
            for schema in partition_schemas:
                for field in schema:
                    name = field.name
                    if name not in all_fields:
                        all_fields[name] = field
                    elif pa.types.is_null(all_fields[name].type) and not pa.types.is_null(field.type):
                        # Prefer non-null types
                        all_fields[name] = field
                    elif (pa.types.is_integer(all_fields[name].type) and pa.types.is_floating(field.type)):
                        # Prefer float over int for numeric fields
                        all_fields[name] = field
                    elif (field.nullable and not all_fields[name].nullable):
                        # Prefer nullable fields
                        all_fields[name] = pa.field(name, all_fields[name].type, nullable=True)
            
            # Create unified schema with all fields
            unified_schema = pa.schema(list(all_fields.values()))
            
            # Update each partition to match the unified schema
            for part_id in self.partition_order:
                if part_id in self.partitions:
                    partition = self.partitions[part_id]
                    try:
                        table = partition.get_table()
                        
                        # Check if schema needs standardization
                        if table.schema != unified_schema:
                            # Create a new table with the unified schema
                            columns = []
                            
                            # Process each field in the unified schema
                            for field in unified_schema:
                                if field.name in table.column_names:
                                    # Use existing column data - cast if types don't match
                                    src_field = table.schema.field(field.name)
                                    if src_field.type != field.type:
                                        # Try to cast the data to the target type
                                        try:
                                            columns.append(table[field.name].cast(field.type))
                                        except pa.ArrowInvalid:
                                            # If casting fails, use null values
                                            columns.append(pa.nulls(table.num_rows, type=field.type))
                                    else:
                                        columns.append(table[field.name])
                                else:
                                    # Add a null array for missing columns
                                    columns.append(pa.nulls(table.num_rows, type=field.type))
                            
                            # Create the standardized table
                            standardized_table = pa.Table.from_arrays(columns, schema=unified_schema)
                            
                            # Update the partition
                            self.partitions[part_id]._table = standardized_table
                    except Exception as e:
                        logger.error(f"Failed to standardize schema for partition {part_id}: {e}")
            
            # Update the schema for the partitioned table
            self.schema = unified_schema

    def recalculate_total_size(self) -> None:
        """
        Recalculate the total size of this partitioned table using Arrow's memory estimation.
        
        Uses Arrow's native memory tracking to get accurate memory size estimates for
        partitioned tables, avoiding the need for manual tracking or double-counting.
        """
        with self.lock:
            from arrow_cache.memory import estimate_table_memory_usage
            old_size = self.total_size_bytes
            
            # Measure each partition directly with Arrow
            total_size = 0
            partition_sizes = {}
            
            for partition_id in self.partition_order:
                partition = self.partitions[partition_id]
                try:
                    table = partition.get_table()
                    # Use Arrow's native memory estimation
                    size = estimate_table_memory_usage(table)
                    partition_sizes[partition_id] = size
                    total_size += size
                except Exception as e:
                    logger.warning(f"Error measuring partition {partition_id}: {e}")
                    # Use existing size if available
                    if hasattr(partition, 'size_bytes') and partition.size_bytes > 0:
                        partition_sizes[partition_id] = partition.size_bytes
                        total_size += partition.size_bytes
            
            # Sanity check: cap at system memory
            system_memory = psutil.virtual_memory().total
            if total_size > system_memory * 0.8:
                logger.info(f"Memory size ({total_size/(1024*1024*1024):.2f} GB) exceeds 80% of system memory "
                          f"({(system_memory * 0.8)/(1024*1024*1024):.2f} GB), adjusting to realistic value.")
                total_size = int(system_memory * 0.8)
            
            # Update the total size
            self.total_size_bytes = total_size
            
            # Update each partition with its measured size
            for partition_id in self.partition_order:
                if partition_id in partition_sizes:
                    self.partitions[partition_id].size_bytes = partition_sizes[partition_id]
                else:
                    # If we couldn't measure a partition, estimate based on row proportion
                    partition = self.partitions[partition_id]
                    row_proportion = partition.row_count / max(1, self.total_rows)
                    partition.size_bytes = int(self.total_size_bytes * row_proportion)
            
            # Log significant size changes
            size_diff = (old_size - self.total_size_bytes) / (1024 * 1024)  # Convert to MB
            if abs(size_diff) > 100:  # Only log if difference is more than 100MB
                logger.debug(f"Recalculated size from {old_size/(1024*1024):.2f} MB "
                          f"to {self.total_size_bytes/(1024*1024):.2f} MB "
                          f"(difference: {size_diff:.2f} MB)")


def partition_table(
    table: pa.Table, 
    config: Any,
    max_rows_per_partition: int = 100_000,
    max_bytes_per_partition: int = 100_000_000
) -> List[pa.Table]:
    """
    Partition a large Arrow table into multiple smaller tables using pure Arrow operations
    
    Args:
        table: Arrow table to partition
        config: ArrowCache configuration
        max_rows_per_partition: Maximum rows per partition
        max_bytes_per_partition: Maximum bytes per partition
        
    Returns:
        List of Arrow tables (partitions)
    """
    total_rows = table.num_rows
    
    if total_rows == 0 or total_rows <= max_rows_per_partition:
        # Table is small enough - no need to partition
        return [table]
    
    # Use Arrow's memory pool for accurate size estimation
    total_size_estimate = estimate_table_memory_usage(table)
    
    # Get system memory information
    system_memory = psutil.virtual_memory().total
    available_memory = psutil.virtual_memory().available
    
    # Cap size estimate at a reasonable value
    if total_size_estimate > system_memory:
        logger.warning(f"Size estimate ({total_size_estimate/(1024*1024*1024):.2f} GB) exceeds system memory " 
                       f"({system_memory/(1024*1024*1024):.2f} GB), capping at 75% of system memory")
        total_size_estimate = int(system_memory * 0.75)

    # Log the size estimate
    logger.info(f"Estimated table size: {total_size_estimate/(1024*1024):.2f} MB from {total_rows} rows")
    
    # Calculate bytes per row for more accurate partitioning
    bytes_per_row = total_size_estimate / max(1, total_rows)
    
    # Adjust partition size based on table size and available memory
    if total_size_estimate > available_memory * 0.5:  # If table is more than 50% of available memory
        # Use smaller partitions for very large tables
        scaling_factor = min(1.0, (available_memory * 0.1) / total_size_estimate)
        adjusted_max_rows = int(max_rows_per_partition * scaling_factor)
        adjusted_max_rows = max(10000, adjusted_max_rows)  # Don't go below 10k rows
        max_rows_per_partition = adjusted_max_rows
        
        # Also adjust max_bytes to match
        max_bytes_per_partition = int(max_bytes_per_partition * scaling_factor)
        max_bytes_per_partition = max(10*1024*1024, max_bytes_per_partition)  # At least 10MB
    
    # Calculate partition count based on rows and size
    num_partitions_by_rows = (total_rows + max_rows_per_partition - 1) // max_rows_per_partition
    num_partitions_by_size = (int(total_size_estimate) + max_bytes_per_partition - 1) // max_bytes_per_partition
    
    # Determine appropriate partition count based on table size
    if total_size_estimate < 1 * 1024 * 1024 * 1024:  # < 1GB
        max_partitions = 5
        logger.info(f"Small table detected ({total_size_estimate/(1024*1024):.2f} MB). Using at most {max_partitions} partitions.")
    elif total_size_estimate < 5 * 1024 * 1024 * 1024:  # < 5GB
        max_partitions = 20
        logger.info(f"Medium table detected ({total_size_estimate/(1024*1024*1024):.2f} GB). Using at most {max_partitions} partitions.")
    elif total_size_estimate < 20 * 1024 * 1024 * 1024:  # < 20GB
        max_partitions = 50
        logger.info(f"Large table detected ({total_size_estimate/(1024*1024*1024):.2f} GB). Using at most {max_partitions} partitions.")
    else:
        max_partitions = 100
        logger.info(f"Very large table detected ({total_size_estimate/(1024*1024*1024):.2f} GB). Using at most {max_partitions} partitions.")
    
    # Choose partition count, bounded by max_partitions
    num_partitions = min(max(num_partitions_by_rows, num_partitions_by_size), max_partitions)
    num_partitions = max(num_partitions, 1)  # Ensure at least one partition
    
    logger.info(f"Final partition count: {num_partitions} (rows-based: {num_partitions_by_rows}, bytes-based: {num_partitions_by_size})")
    
    # Use Arrow's chunking capabilities to partition the table efficiently
    rows_per_partition = (total_rows + num_partitions - 1) // num_partitions
    
    # For large tables, use Arrow's record batching for efficient memory management
    if num_partitions > 10 or total_size_estimate > 1 * 1024 * 1024 * 1024:  # > 1GB
        # Use Arrow's record batch iterators to conserve memory
        logger.info(f"Using Arrow's record batch approach for large table with {num_partitions} partitions")
        
        # Convert to record batches with controlled size
        batch_size = min(100000, rows_per_partition // 2)
        batches = table.to_batches(max_chunksize=batch_size)
        
        # Group batches into partitions
        partitions = []
        current_batches = []
        current_rows = 0
        
        for batch in batches:
            current_batches.append(batch)
            current_rows += batch.num_rows
            
            if current_rows >= rows_per_partition:
                # Create a table from the current set of batches
                partition = pa.Table.from_batches(current_batches)
                partitions.append(partition)
                
                # Reset for next partition
                current_batches = []
                current_rows = 0
        
        # Handle any remaining batches
        if current_batches:
            partition = pa.Table.from_batches(current_batches)
            partitions.append(partition)
        
        return partitions
    else:
        # For smaller tables, use Arrow's built-in slicing which is zero-copy
        logger.info(f"Using Arrow's slice operation for table with {num_partitions} partitions")
        
        partitions = []
        for i in range(0, total_rows, rows_per_partition):
            end = min(i + rows_per_partition, total_rows)
            partition = table.slice(i, end - i)
            partitions.append(partition)
        
        return partitions 