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

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import numpy as np

logger = logging.getLogger(__name__)

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
            
            # Calculate size
            size_bytes = 0
            for column in table.columns:
                for chunk in column.chunks:
                    for buf in chunk.buffers():
                        if buf is not None:
                            size_bytes += buf.size
            
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
                
            return pa.concat_tables(tables)
    
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
        
        # Create table instance
        schema = pa.schema(pa.Schema.from_string(metadata["schema"]))
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
    total_rows = table.num_rows
    
    if total_rows == 0:
        return [table]
        
    if total_rows <= max_rows_per_partition:
        # Table is small enough - no need to partition
        return [table]
    
    # Calculate partition size based on row count
    num_partitions = (total_rows + max_rows_per_partition - 1) // max_rows_per_partition
    rows_per_partition = (total_rows + num_partitions - 1) // num_partitions
    
    # Create partitions
    partitions = []
    for i in range(0, total_rows, rows_per_partition):
        end = min(i + rows_per_partition, total_rows)
        partition = table.slice(i, end - i)
        partitions.append(partition)
    
    return partitions 