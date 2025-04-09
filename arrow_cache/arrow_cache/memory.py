"""
Memory management module for Arrow Cache

Provides functions for efficient memory management including:
- Memory pools
- Memory tracking and limits
- Automatic spilling to disk
"""
import os
import sys
import threading
import logging
import time
import tracemalloc
import psutil
from typing import Dict, List, Optional, Union, Callable, Any
import weakref

import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as pq
import pyarrow.compute as pc
import numpy as np

logger = logging.getLogger(__name__)

# Try to import optional memory allocators
try:
    import pyarrow.jemalloc as jemalloc
    HAVE_JEMALLOC = True
except ImportError:
    HAVE_JEMALLOC = False

try:
    # Mimalloc is a hypothetical future Arrow memory allocator
    import pyarrow.mimalloc as mimalloc
    HAVE_MIMALLOC = True
except ImportError:
    HAVE_MIMALLOC = False


class MemoryManager:
    """
    Memory manager for Arrow Cache
    
    Manages memory pools, tracks memory usage, and handles spilling to disk
    when memory pressure is high.
    """
    def __init__(
        self,
        config: Any,
        spill_callback: Optional[Callable[[int], int]] = None
    ):
        """
        Initialize the memory manager
        
        Args:
            config: ArrowCache configuration
            spill_callback: Callback to spill data when memory pressure is high
        """
        self.config = config
        self.spill_callback = spill_callback
        self.memory_tracker = MemoryTracker(config["memory_limit"])
        self.lock = threading.RLock()
        self._stop_monitor_event = threading.Event()
        self._monitor_thread = None
        
        # Set up memory pool
        self.pool = self._create_memory_pool()
        
        # Start memory monitor if enabled
        if config["memory_spill_threshold"] > 0:
            self._start_memory_monitor()
    
    def _create_memory_pool(self) -> pa.MemoryPool:
        """
        Create an Arrow memory pool based on configuration
        
        Returns:
            Arrow memory pool
        """
        pool_type = self.config["memory_pool_type"].lower()
        
        if pool_type == "jemalloc" and HAVE_JEMALLOC:
            logger.info("Using jemalloc memory pool")
            return jemalloc.default_memory_pool()
        elif pool_type == "mimalloc" and HAVE_MIMALLOC:
            logger.info("Using mimalloc memory pool")
            return mimalloc.default_memory_pool()
        else:
            logger.info("Using system memory pool")
            return pa.system_memory_pool()
    
    def allocate(self, nbytes: int) -> Optional[pa.Buffer]:
        """
        Allocate memory from the pool
        
        Args:
            nbytes: Number of bytes to allocate
            
        Returns:
            Arrow buffer or None if allocation failed
        """
        with self.lock:
            # Check if allocation would exceed limit
            if not self.memory_tracker.can_allocate(nbytes):
                # Try to free some memory through spilling
                if self.spill_callback:
                    freed = self.spill_callback(nbytes)
                    if freed < nbytes:
                        logger.warning(f"Could not free enough memory for allocation of {nbytes} bytes")
                        return None
                else:
                    logger.warning(f"Memory allocation of {nbytes} bytes would exceed limit")
                    return None
            
            try:
                buffer = pa.allocate_buffer(nbytes, memory_pool=self.pool)
                self.memory_tracker.track_allocation(nbytes)
                return buffer
            except Exception as e:
                logger.error(f"Failed to allocate memory: {e}")
                return None
    
    def release(self, buffer: pa.Buffer) -> None:
        """
        Explicitly release memory (buffer will be deleted)
        
        Args:
            buffer: Arrow buffer to release
        """
        with self.lock:
            size = buffer.size
            # Delete reference to the buffer
            del buffer
            self.memory_tracker.track_deallocation(size)
    
    def _monitor_memory(self) -> None:
        """Background thread to monitor memory usage"""
        while not self._stop_monitor_event.is_set():
            try:
                # Check memory pressure
                usage_ratio = self.memory_tracker.get_usage_ratio()
                
                if usage_ratio > self.config["memory_spill_threshold"] and self.spill_callback:
                    # Calculate how much to free
                    target_ratio = self.config["memory_spill_threshold"] * 0.8  # Target 80% of threshold
                    current_usage = self.memory_tracker.get_allocated_bytes()
                    target_usage = target_ratio * self.memory_tracker.get_limit_bytes()
                    bytes_to_free = int(current_usage - target_usage)
                    
                    if bytes_to_free > 0:
                        logger.info(f"Memory usage at {usage_ratio:.2f}, triggering spill of {bytes_to_free} bytes")
                        freed = self.spill_callback(bytes_to_free)
                        logger.info(f"Freed {freed} bytes through spilling")
            except Exception as e:
                logger.error(f"Error in memory monitor: {e}")
            
            # Sleep for a bit
            self._stop_monitor_event.wait(5.0)  # Check every 5 seconds
    
    def _start_memory_monitor(self) -> None:
        """Start the memory monitoring thread"""
        if self._monitor_thread is not None:
            return
            
        self._stop_monitor_event.clear()
        self._monitor_thread = threading.Thread(
            target=self._monitor_memory,
            daemon=True
        )
        self._monitor_thread.start()
    
    def stop_memory_monitor(self) -> None:
        """Stop the memory monitoring thread"""
        if self._monitor_thread is None:
            return
            
        self._stop_monitor_event.set()
        self._monitor_thread.join(timeout=1.0)
        self._monitor_thread = None
    
    def get_memory_info(self) -> Dict[str, Any]:
        """
        Get memory usage information
        
        Returns:
            Dictionary with memory usage information
        """
        return {
            "allocated_bytes": self.memory_tracker.get_allocated_bytes(),
            "limit_bytes": self.memory_tracker.get_limit_bytes(),
            "usage_ratio": self.memory_tracker.get_usage_ratio(),
            "pool_bytes_allocated": self.pool.bytes_allocated(),
            "pool_max_memory": self.pool.max_memory(),
            "system_memory_used": psutil.Process(os.getpid()).memory_info().rss,
            "system_memory_total": psutil.virtual_memory().total,
            "system_memory_available": psutil.virtual_memory().available
        }
    
    def close(self) -> None:
        """Close the memory manager and release resources"""
        self.stop_memory_monitor()
        # Clear pool - in real implementation we would need a way to release
        # the pool, but Arrow doesn't provide this directly


class MemoryTracker:
    """
    Tracks memory allocations and enforces limits
    """
    def __init__(self, memory_limit: Optional[int] = None):
        """
        Initialize the memory tracker
        
        Args:
            memory_limit: Maximum memory usage in bytes (None for no limit)
        """
        self.memory_limit = memory_limit
        self.allocated_bytes = 0
        self.lock = threading.RLock()
        
        # If no explicit limit is provided, use a percentage of system memory
        if memory_limit is None:
            system_memory = psutil.virtual_memory().total
            self.memory_limit = int(system_memory * 0.8)  # Use 80% of system memory
    
    def track_allocation(self, nbytes: int) -> None:
        """
        Track memory allocation
        
        Args:
            nbytes: Number of bytes allocated
        """
        with self.lock:
            self.allocated_bytes += nbytes
    
    def track_deallocation(self, nbytes: int) -> None:
        """
        Track memory deallocation
        
        Args:
            nbytes: Number of bytes deallocated
        """
        with self.lock:
            self.allocated_bytes -= nbytes
            if self.allocated_bytes < 0:
                logger.warning("Memory tracking error: allocated_bytes < 0, resetting to 0")
                self.allocated_bytes = 0
    
    def can_allocate(self, nbytes: int) -> bool:
        """
        Check if an allocation of nbytes would exceed the memory limit
        
        Args:
            nbytes: Number of bytes to allocate
            
        Returns:
            True if allocation is allowed, False otherwise
        """
        with self.lock:
            if self.memory_limit is None:
                return True
            return (self.allocated_bytes + nbytes) <= self.memory_limit
    
    def get_allocated_bytes(self) -> int:
        """
        Get the current allocated bytes
        
        Returns:
            Number of bytes currently allocated
        """
        with self.lock:
            return self.allocated_bytes
    
    def get_limit_bytes(self) -> int:
        """
        Get the memory limit in bytes
        
        Returns:
            Memory limit in bytes
        """
        return self.memory_limit or sys.maxsize
    
    def get_usage_ratio(self) -> float:
        """
        Get the ratio of allocated memory to limit
        
        Returns:
            Ratio of allocated memory to limit (0.0 to 1.0)
        """
        with self.lock:
            if self.memory_limit is None or self.memory_limit == 0:
                return 0.0
            return self.allocated_bytes / self.memory_limit


def zero_copy_slice(table: pa.Table, offset: int, length: int) -> pa.Table:
    """
    Create a zero-copy slice of an Arrow table
    
    Args:
        table: Arrow table to slice
        offset: Starting row index
        length: Number of rows
        
    Returns:
        Sliced Arrow table (zero-copy)
    """
    return table.slice(offset, length)


def estimate_table_memory_usage(table: pa.Table) -> int:
    """
    Estimate the memory usage of an Arrow table
    
    Args:
        table: Arrow table
        
    Returns:
        Estimated memory usage in bytes
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


def get_optimal_compression(column: pa.Array) -> str:
    """
    Get the optimal compression type for a column based on its data type
    
    Args:
        column: Arrow array
        
    Returns:
        Compression type (lz4, zstd, dictionary, etc.)
    """
    # For string/binary columns over a certain size, dictionary encoding is usually best
    if pa.types.is_string(column.type) or pa.types.is_binary(column.type):
        # Check value sizes and cardinality (unique value count)
        if column.nbytes > 1000000:  # > 1MB
            # For large columns, zstd usually works well
            return "zstd"
        else:
            # For smaller string columns, dictionary encoding often helps
            return "dictionary"
    
    # For numeric data, LZ4 is usually a good balance of speed and compression
    if pa.types.is_integer(column.type) or pa.types.is_floating(column.type):
        return "lz4"
    
    # Default to LZ4 for other types
    return "lz4"


def apply_compression(
    table: pa.Table, 
    compression_type: str = "lz4", 
    compression_level: int = 1,
    use_dictionary: bool = True
) -> pa.Table:
    """
    Apply compression to an Arrow table based on configuration and column types
    
    Args:
        table: Arrow table to compress
        compression_type: Type of compression to use (lz4, zstd, etc.)
        compression_level: Compression level
        use_dictionary: Whether to apply dictionary encoding to string columns
        
    Returns:
        Compressed Arrow table
    """
    # Apply dictionary encoding to string columns if enabled
    if use_dictionary:
        arrays = []
        fields = []
        
        for i, column in enumerate(table.columns):
            field = table.schema.field(i)
            
            if (pa.types.is_string(field.type) or pa.types.is_binary(field.type)) and column.nbytes > 1024:
                # Apply dictionary encoding to this column
                dict_array = pc.dictionary_encode(column)
                arrays.append(dict_array)
                
                # Create a new field with the dictionary-encoded type
                new_field = pa.field(field.name, dict_array.type, field.nullable)
                fields.append(new_field)
            else:
                arrays.append(column)
                fields.append(field)
        
        # Create new table with compressed columns
        return pa.Table.from_arrays(arrays, schema=pa.schema(fields))
    
    return table


def get_system_memory_info() -> Dict[str, int]:
    """
    Get system memory information
    
    Returns:
        Dictionary with system memory information
    """
    vm = psutil.virtual_memory()
    return {
        "total": vm.total,
        "available": vm.available,
        "used": vm.used,
        "free": vm.free,
        "percent": vm.percent
    } 