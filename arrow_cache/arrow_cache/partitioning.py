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
            if self.spill_path and os.path.exists(self.spill_path):
                try:
                    self._table = pq.read_table(self.spill_path)
                    self.is_loaded = True
                    return self._table
                except Exception as e:
                    logger.error(f"Failed to load partition from spill path: {e}")
            
            if self.path and os.path.exists(self.path):
                try:
                    self._table = pq.read_table(self.path)
                    self.is_loaded = True
                    return self._table
                except Exception as e:
                    logger.error(f"Failed to load partition from path: {e}")
            
            raise ValueError(f"Partition {self.partition_id} data not available")
    
    def spill(self, spill_dir: str) -> bool:
        """
        Spill this partition to disk to free memory
        
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
                import tempfile
                import shutil
                
                # Create a temporary file for writing
                temp_file = tempfile.NamedTemporaryFile(delete=False, dir=spill_dir, suffix='.parquet.tmp')
                temp_file.close()  # Close the file to allow writing to it
                
                # Write to temporary file
                pq.write_table(self._table, temp_file.name)
                
                # Set destination path
                self.spill_path = os.path.join(spill_dir, f"{self.partition_id}.parquet")
                
                # Rename temp file to destination (atomic operation)
                shutil.move(temp_file.name, self.spill_path)
                
                # Release memory
                self._table = None
                self.is_loaded = False
                
                # Record the time we spilled
                self.last_spilled_at = time.time()
                
                return True
            except IOError as e:
                logger.error(f"I/O error spilling partition to disk: {e}")
                if 'temp_file' in locals() and os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
                self.spill_path = None
                return False
            except Exception as e:
                logger.error(f"Failed to spill partition to disk: {e}")
                if 'temp_file' in locals() and os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
                self.spill_path = None
                return False
    
    def persist(self, storage_dir: str) -> bool:
        """
        Persist this partition to permanent storage
        
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
                import tempfile
                import shutil
                
                # Write table metadata to a temporary file first
                meta_path = os.path.join(storage_dir, f"{self.partition_id}.json")
                meta_temp = tempfile.NamedTemporaryFile(delete=False, dir=storage_dir, suffix='.json.tmp')
                meta_temp.close()
                
                with open(meta_temp.name, 'w') as f:
                    json.dump({
                        "partition_id": self.partition_id,
                        "size_bytes": self.size_bytes,
                        "row_count": self.row_count,
                        "created_at": self.created_at,
                        "metadata": self.metadata
                    }, f)
                
                # Move metadata to final location (atomic)
                shutil.move(meta_temp.name, meta_path)
                
                # Write table data to a temporary file
                temp_file = tempfile.NamedTemporaryFile(delete=False, dir=storage_dir, suffix='.parquet.tmp')
                temp_file.close()
                
                # Write to parquet with compression
                pq.write_table(
                    table, 
                    temp_file.name,
                    compression='zstd',
                    compression_level=3
                )
                
                # Set the final path
                self.path = os.path.join(storage_dir, f"{self.partition_id}.parquet")
                
                # Move to final location (atomic)
                shutil.move(temp_file.name, self.path)
                
                return True
            except IOError as e:
                logger.error(f"I/O error persisting partition: {e}")
                # Clean up temporary files
                if 'meta_temp' in locals() and os.path.exists(meta_temp.name):
                    os.unlink(meta_temp.name)
                if 'temp_file' in locals() and os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
                self.path = None
                return False
            except Exception as e:
                logger.error(f"Failed to persist partition: {e}")
                # Clean up temporary files
                if 'meta_temp' in locals() and os.path.exists(meta_temp.name):
                    os.unlink(meta_temp.name)
                if 'temp_file' in locals() and os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)
                self.path = None
                return False
    
    def pin(self) -> None:
        """Pin this partition in memory to prevent spilling"""
        with self.lock:
            self.is_pinned = True
            # Ensure it's loaded
            if not self.is_loaded:
                self.get_table()
    
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
        
        # Create table instance - Fix Schema.from_string issue
        schema_str = metadata["schema"]
        
        # Extract field definitions using regex - safe alternative to Schema.from_string
        field_matches = re.findall(r'Field\((.*?):', schema_str) or re.findall(r'field_name: (.*?)[,\)]', schema_str)
        field_types = re.findall(r'type: (.*?)[\),]', schema_str)
        
        # Create fields from extracted information
        fields = []
        for i, name in enumerate(field_matches):
            name = name.strip().strip("'\"")
            # Default to string type if type extraction fails
            field_type = pa.string() if i >= len(field_types) else _parse_type_str(field_types[i])
            fields.append(pa.field(name, field_type))
        
        # Create schema
        schema = pa.schema(fields)
        
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
        Standardize schemas across all partitions to ensure compatibility
        
        This method ensures all partitions have the same schema by:
        1. Determining the most complete schema across all partitions
        2. Converting each partition to match that schema
        """
        if not self.partition_order:
            return  # No partitions to standardize
            
        with self.lock:
            # Collect schema information from all partitions
            schemas = []
            for part_id in self.partition_order:
                if part_id in self.partitions:
                    partition = self.partitions[part_id]
                    try:
                        table = partition.get_table()
                        schemas.append((part_id, table.schema))
                    except Exception as e:
                        logger.warning(f"Couldn't get schema for partition {part_id}: {e}")
            
            if not schemas:
                logger.error("No valid partition schemas found")
                return
                
            # Find the most complete schema (one with most fields)
            target_schema = max(schemas, key=lambda s: len(s[1]))[1]
            
            # Create a reference schema that includes all unique fields with consistent types
            field_types = {}
            for _, schema in schemas:
                for field in schema:
                    name = field.name
                    if name not in field_types:
                        field_types[name] = field.type
                    elif pa.types.is_null(field_types[name]) and not pa.types.is_null(field.type):
                        # Upgrade from null type if a more specific type is found
                        field_types[name] = field.type
                    elif (pa.types.is_integer(field_types[name]) and pa.types.is_floating(field.type)):
                        # Upgrade from int to float if needed
                        field_types[name] = field.type
            
            # Create a unified schema with all fields
            unified_fields = [pa.field(name, dtype) for name, dtype in field_types.items()]
            unified_schema = pa.schema(unified_fields)
            
            # Reprocess each partition to match the unified schema
            for part_id in self.partition_order:
                if part_id in self.partitions:
                    partition = self.partitions[part_id]
                    try:
                        table = partition.get_table()
                        
                        # Check if schema needs standardization
                        if table.schema != unified_schema:
                            # Use Arrow compute functions to transform the table
                            # Create arrays for the new schema
                            new_arrays = []
                            
                            # Add each field from the unified schema
                            for field in unified_schema:
                                if field.name in table.column_names:
                                    # Use existing column
                                    new_arrays.append(table[field.name])
                                else:
                                    # Create a null array of the appropriate type and length
                                    null_array = pa.nulls(table.num_rows, type=field.type)
                                    new_arrays.append(null_array)
                            
                            # Create new table with the unified schema
                            standardized_table = pa.Table.from_arrays(new_arrays, schema=unified_schema)
                            
                            # Update the partition with standardized table
                            self.partitions[part_id]._table = standardized_table
                    except Exception as e:
                        logger.error(f"Failed to standardize schema for partition {part_id}: {e}")
            
            # Update the schema for this partitioned table
            self.schema = unified_schema

    def recalculate_total_size(self) -> None:
        """
        Recalculate the total size of this partitioned table accurately.
        
        This helps repair incorrect size calculations that may have occurred
        due to double-counting of slice memory and ensures consistent size
        tracking across the codebase.
        """
        with self.lock:
            from arrow_cache.memory import estimate_table_memory_usage
            old_size = self.total_size_bytes
            
            # We'll use two approaches and take the more conservative estimate
            
            # Approach 1: Use estimate_table_memory_usage on each partition
            total_size = 0
            partition_sizes = {}
            
            for partition_id in self.partition_order:
                partition = self.partitions[partition_id]
                table = partition.get_table()
                size = estimate_table_memory_usage(table)
                partition_sizes[partition_id] = size
                total_size += size
                
            # Approach 2: If number of partitions is reasonable, try getting the combined table
            # and measuring it directly (this catches some reference sharing between partitions)
            combined_size = None
            if len(self.partition_order) <= 5:  # Only for small numbers of partitions
                try:
                    # Get the full table and measure it
                    full_table = self.get_table()  
                    combined_size = estimate_table_memory_usage(full_table)
                    
                    # Use the smaller of the two estimates (to account for shared memory)
                    if combined_size and combined_size < total_size:
                        logger.debug(f"Using combined table size estimate ({combined_size/(1024*1024):.2f} MB) "
                                    f"instead of sum of partitions ({total_size/(1024*1024):.2f} MB)")
                        total_size = combined_size
                except Exception as e:
                    logger.debug(f"Error getting combined size estimate: {e}")
            
            # Set the new size
            self.total_size_bytes = total_size
            
            # Sanity check: cap at system memory
            import psutil
            system_memory = psutil.virtual_memory().total
            if self.total_size_bytes > system_memory * 0.8:
                logger.info(f"Memory size ({self.total_size_bytes/(1024*1024*1024):.2f} GB) exceeds 80% of system memory "
                          f"({(system_memory * 0.8)/(1024*1024*1024):.2f} GB), adjusting to realistic value.")
                self.total_size_bytes = int(system_memory * 0.8)
            
            # Update each partition based on its proportion of rows or directly from measurement
            total_rows = max(1, self.total_rows)
            for partition_id in self.partition_order:
                partition = self.partitions[partition_id]
                if partition_id in partition_sizes:
                    # Use directly measured size if we have it
                    partition.size_bytes = partition_sizes[partition_id]
                else:
                    # Fall back to proportional allocation
                    row_proportion = partition.row_count / total_rows
                    partition.size_bytes = int(self.total_size_bytes * row_proportion)
            
            # Log the change in size if significant, but only at debug level to avoid alarm
            size_diff = (old_size - self.total_size_bytes) / (1024 * 1024 * 1024)  # Convert to GB
            if abs(size_diff) > 1:  # Only log if difference is more than 1GB
                # Use debug instead of warning for size recalculation
                logger.debug(f"Recalculated size from {old_size/(1024*1024*1024):.2f} GB "
                          f"to {self.total_size_bytes/(1024*1024*1024):.2f} GB "
                          f"(difference: {size_diff:.2f} GB)")
            else:
                logger.debug(f"Recalculated size from {old_size/(1024*1024)} MB "
                        f"to {self.total_size_bytes/(1024*1024)} MB "
                        f"(difference: {(old_size - self.total_size_bytes)/(1024*1024):.2f} MB)")
                    
            # Use debug level for large discrepancy logging
            if old_size > self.total_size_bytes * 2:
                logger.debug(f"Memory calculation refined: Previous size was "
                          f"{old_size/(1024*1024*1024):.2f} GB, now {self.total_size_bytes/(1024*1024*1024):.2f} GB")
            elif self.total_size_bytes > old_size * 2:
                logger.debug(f"Memory calculation refined: Previous size was "
                          f"{old_size/(1024*1024*1024):.2f} GB, now {self.total_size_bytes/(1024*1024*1024):.2f} GB")


def partition_table(
    table: pa.Table, 
    config: Any,
    max_rows_per_partition: int = 100_000,
    max_bytes_per_partition: int = 100_000_000
) -> List[pa.Table]:
    """
    Partition a large Arrow table into multiple smaller tables
    
    Args:
        table: Arrow table to partition
        config: ArrowCache configuration
        max_rows_per_partition: Maximum rows per partition
        max_bytes_per_partition: Maximum bytes per partition
        
    Returns:
        List of Arrow tables (partitions)
    """
    import gc
    import pyarrow.compute as pc
    
    total_rows = table.num_rows
    
    if total_rows == 0:
        return [table]
        
    if total_rows <= max_rows_per_partition:
        # Table is small enough - no need to partition
        return [table]
    
    # Use Arrow's memory pool for accurate size estimation
    # Instead of the inaccurate sampling-based approach
    total_size_estimate = estimate_table_memory_usage(table)
    
    # Add a reasonable upper bound based on system memory to prevent ridiculous estimates
    system_memory = psutil.virtual_memory().total
    available_memory = psutil.virtual_memory().available
    
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
    
    # Use a more balanced approach to partitioning based on table size
    if total_size_estimate < 1 * 1024 * 1024 * 1024:  # < 1GB
        # Small tables: fewer partitions
        max_partitions = 5
        logger.info(f"Small table detected ({total_size_estimate/(1024*1024):.2f} MB). Using at most {max_partitions} partitions.")
    elif total_size_estimate < 5 * 1024 * 1024 * 1024:  # < 5GB
        # Medium tables: moderate partitioning
        max_partitions = 20
        logger.info(f"Medium table detected ({total_size_estimate/(1024*1024*1024):.2f} GB). Using at most {max_partitions} partitions.")
    elif total_size_estimate < 20 * 1024 * 1024 * 1024:  # < 20GB
        # Large tables: more partitions
        max_partitions = 50
        logger.info(f"Large table detected ({total_size_estimate/(1024*1024*1024):.2f} GB). Using at most {max_partitions} partitions.")
    else:
        # Very large tables: most partitions, but still with a reasonable limit
        max_partitions = 100
        logger.info(f"Very large table detected ({total_size_estimate/(1024*1024*1024):.2f} GB). Using at most {max_partitions} partitions.")
    
    # Choose partition count, bounded by max_partitions
    num_partitions = min(max(num_partitions_by_rows, num_partitions_by_size), max_partitions)
    num_partitions = max(num_partitions, 1)  # Ensure at least one partition
    
    logger.info(f"Final partition count: {num_partitions} (rows-based: {num_partitions_by_rows}, bytes-based: {num_partitions_by_size})")
    
    rows_per_partition = (total_rows + num_partitions - 1) // num_partitions
    
    # Try to use zero-copy slicing for better memory efficiency
    try:
        # Create partitions using zero-copy slicing
        partitions = []
        for i in range(0, total_rows, rows_per_partition):
            end = min(i + rows_per_partition, total_rows)
            partition = table.slice(i, end - i)
            partitions.append(partition)
            
            # For extremely large tables, force garbage collection occasionally
            if num_partitions > 100 and i % (10 * rows_per_partition) == 0:
                gc.collect()
        
        return partitions
        
    except Exception as e:
        # If zero-copy slicing fails for some reason, try an Arrow-based fallback
        # that avoids going through Python objects when possible
        logger.warning(f"Zero-copy slicing failed: {e}. Using Arrow take-based partitioning.")
        
        try:
            # Use Arrow's take function which is more memory-efficient than Python lists
            partitions = []
            schema = table.schema
            
            for i in range(0, total_rows, rows_per_partition):
                end = min(i + rows_per_partition, total_rows)
                rows_in_partition = end - i
                
                # Create indices for this partition
                indices = list(range(i, end))
                indices_array = pa.array(indices)
                
                # Use Arrow's take function to extract rows by index
                try:
                    # Try to use faster batch processing with Arrow compute take function
                    arrays = []
                    for col in table.columns:
                        arrays.append(pc.take(col, indices_array))
                    
                    # Create new table from these arrays
                    partition = pa.Table.from_arrays(arrays, schema=schema)
                    partitions.append(partition)
                except:
                    # If take fails (sometimes happens with complex types), 
                    # fall back to per-row slice + concatenate
                    partition = table.slice(i, rows_in_partition)
                    partitions.append(partition)
                
                # Force garbage collection for very large tables
                if num_partitions > 50 and i % (5 * rows_per_partition) == 0:
                    gc.collect()
                    
            return partitions
            
        except Exception as compute_error:
            # If Arrow-based approach also fails, fall back to most compatible method
            # but still try to avoid Python objects when possible
            logger.warning(f"Arrow compute-based partitioning failed: {compute_error}. Using most compatible method.")
            
            # Record batch processing can be more memory-efficient
            partitions = []
            
            # Convert table to record batches with appropriate size
            batch_size = min(100000, rows_per_partition)
            batches = table.to_batches(max_chunksize=batch_size)
            
            # Group batches into partitions
            current_partition_batches = []
            current_row_count = 0
            
            for batch in batches:
                current_partition_batches.append(batch)
                current_row_count += len(batch)
                
                if current_row_count >= rows_per_partition:
                    # We have enough rows for a partition, create table from batches
                    partition = pa.Table.from_batches(current_partition_batches)
                    partitions.append(partition)
                    
                    # Reset for next partition
                    current_partition_batches = []
                    current_row_count = 0
                    gc.collect()  # Force garbage collection
            
            # Add any remaining batches as the last partition
            if current_partition_batches:
                partition = pa.Table.from_batches(current_partition_batches)
                partitions.append(partition)
            
            return partitions 