import time
from typing import Optional, Any, Dict, List
import pyarrow as pa
import sys

class CacheEntry:
    """
    Represents a single entry in the cache with its metadata.
    """
    def __init__(
        self,
        key: str,
        table: pa.Table,
        size_bytes: Optional[int] = None,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a new cache entry.
        
        Args:
            key: Unique identifier for this cache entry
            table: Arrow table containing the data
            size_bytes: Size of the table in bytes (calculated if not provided)
            ttl: Time-to-live in seconds (None means no expiration)
            metadata: Additional metadata about this entry
        """
        self.key = key
        self.table = table
        # Calculate size bytes if not provided
        self.size_bytes = size_bytes if size_bytes is not None else self._calculate_table_size(table)
        self.metadata = metadata or {}
        
        # Track access patterns for eviction policies
        self.created_at = time.time()
        self.last_accessed_at = self.created_at
        self.access_count = 0
        
        # Set expiration time if TTL is provided
        self.expires_at = (self.created_at + ttl) if ttl is not None else None
    
    @staticmethod
    def _calculate_table_size(table: pa.Table) -> int:
        """
        Calculate the size of an Arrow table in bytes accurately using Arrow's native capabilities.
        
        Args:
            table: Arrow table
            
        Returns:
            Size in bytes
        """
        # First try to use Arrow's native memory pool metrics if available
        try:
            import gc
            # Get the default memory pool
            memory_pool = pa.default_memory_pool()
            
            # Try to estimate by checking allocated memory before and after
            gc_enabled = gc.isenabled()
            if gc_enabled:
                gc.disable()  # Temporarily disable GC for accurate measurement
            
            try:
                # Force a GC cycle first
                gc.collect()
                
                # Capture memory before accessing the table
                memory_before = memory_pool.bytes_allocated()
                
                # Access the data to ensure it's materialized
                # Use a lightweight access method
                for col in table.columns:
                    # Just access one buffer per chunk to force materialization
                    for chunk in col.chunks:
                        if len(chunk) > 0:
                            chunk[0]  # Access first element to materialize
                            break
                
                # Measure memory after access
                memory_after = memory_pool.bytes_allocated()
                
                # If we got a meaningful difference, use it
                if memory_after > memory_before:
                    return memory_after - memory_before
            finally:
                if gc_enabled:
                    gc.enable()  # Restore GC state
        except (ImportError, AttributeError, Exception) as e:
            # Continue to fallback methods
            pass
        
        # Fallback method: use set-based buffer tracking to avoid double-counting
        try:
            # Use a set to track unique buffer addresses
            unique_buffers = set()
            size = 0
            
            for column in table.columns:
                # Handle ChunkedArray by iterating through its chunks
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
            
            # Add schema overhead
            schema_size = len(table.schema.to_string()) * 2  # Rough estimate for schema
            
            # Add metadata size if present
            metadata_size = 0
            if table.schema.metadata:
                for k, v in table.schema.metadata.items():
                    metadata_size += len(k) + len(v)
            
            # Add buffer management overhead (conservative estimate)
            overhead = table.num_columns * 16  # Reduced from 24 to 16 bytes per column pointer
            
            # Final safety check against system memory
            total_size = size + schema_size + metadata_size + overhead
            if total_size > 100 * 1024 * 1024 * 1024:  # > 100GB
                import psutil
                system_memory = psutil.virtual_memory().total
                if total_size > system_memory * 2:
                    # This is clearly an error - cap at a reasonable percentage of system memory
                    return int(system_memory * 0.8)
                
            return total_size
        except Exception as e:
            # Last resort: use a simple estimate based on rows and columns
            # Use a very conservative estimate based on rows and columns
            avg_bytes_per_value = 8  # Assume 8 bytes per value average
            estimated_size = table.num_rows * table.num_columns * avg_bytes_per_value
            # Add overhead
            return int(estimated_size * 1.2)  # Add 20% for overhead
    
    def is_expired(self) -> bool:
        """Check if the entry has expired."""
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at
    
    def access(self) -> None:
        """Update access statistics when the entry is accessed."""
        self.last_accessed_at = time.time()
        self.access_count += 1
    
    def get_age(self) -> float:
        """Get the age of this entry in seconds."""
        return time.time() - self.created_at
    
    def get_table(self) -> pa.Table:
        """Get the Arrow table, updating access statistics."""
        self.access()
        return self.table
    
    def get_column(self, column_name: str) -> pa.ChunkedArray:
        """
        Get a specific column from the table, updating access statistics.
        
        Args:
            column_name: Name of the column to retrieve
            
        Returns:
            Arrow ChunkedArray representing the column
        
        Raises:
            KeyError: If the column doesn't exist
        """
        self.access()
        if column_name not in self.table.column_names:
            raise KeyError(f"Column '{column_name}' not found in table")
        return self.table[column_name]
    
    def get_columns(self, column_names: List[str]) -> pa.Table:
        """
        Get a subset of columns from the table, updating access statistics.
        
        Args:
            column_names: Names of columns to retrieve
            
        Returns:
            Arrow Table with only the requested columns
        """
        self.access()
        # Verify all columns exist
        missing = [col for col in column_names if col not in self.table.column_names]
        if missing:
            raise KeyError(f"Columns not found: {missing}")
        return self.table.select(column_names)
    
    def update_ttl(self, ttl: Optional[float]) -> None:
        """Update the TTL for this entry."""
        if ttl is None:
            self.expires_at = None
        else:
            self.expires_at = time.time() + ttl
    
    def estimate_memory_usage(self) -> int:
        """
        Estimate the total memory usage of this entry including the Arrow table.
        
        Returns:
            Estimated size in bytes
        """
        # Use more detailed memory estimation through Arrow
        try:
            return self._calculate_table_size(self.table)
        except Exception:
            # If detailed calculation fails, fall back to simple estimate
            return self.size_bytes
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert the entry metadata to a dictionary."""
        return {
            "key": self.key,
            "size_bytes": self.size_bytes,
            "created_at": self.created_at,
            "last_accessed_at": self.last_accessed_at,
            "access_count": self.access_count,
            "expires_at": self.expires_at,
            "is_expired": self.is_expired(),
            "schema": self.table.schema.to_string(),
            "num_rows": self.table.num_rows,
            "num_columns": self.table.num_columns,
            "column_names": self.table.column_names,
            "metadata": self.metadata
        }
