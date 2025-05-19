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
import gc
import pandas as pd

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
        
        # Create directories
        if config["spill_to_disk"] and config["spill_directory"]:
            os.makedirs(config["spill_directory"], exist_ok=True)
        
        # Track allocations for leak detection
        self.enable_leak_detection = config.get("enable_leak_detection", False)
        self.allocation_tracker = {}  # object_id -> (size, stack_trace)
        
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
            # Check if we're approaching memory limit
            usage_ratio = self.memory_tracker.get_usage_ratio()
            high_memory_pressure = usage_ratio >= self.config["memory_spill_threshold"] * 0.9
            
            # If we're under high memory pressure, try to free some memory first
            if high_memory_pressure and self.spill_callback:
                # Calculate how much to free - target getting back to 80% of threshold
                target_ratio = self.config["memory_spill_threshold"] * 0.8
                current_usage = self.memory_tracker.get_allocated_bytes()
                target_usage = target_ratio * self.memory_tracker.get_limit_bytes()
                bytes_to_free = max(nbytes, int(current_usage - target_usage))
                
                if bytes_to_free > 0:
                    freed = self.spill_callback(bytes_to_free)
                    logger.info(f"Pre-emptively freed {freed} bytes under memory pressure")
            
            # Check if allocation would exceed limit
            if not self.memory_tracker.can_allocate(nbytes):
                # We still need to free memory - try more aggressive spilling
                if self.spill_callback:
                    # Try to free twice what we need to create some buffer
                    freed = self.spill_callback(nbytes * 2)
                    if freed < nbytes:
                        logger.warning(f"Could not free enough memory for allocation of {nbytes} bytes")
                        return None
                else:
                    logger.warning(f"Memory allocation of {nbytes} bytes would exceed limit")
                    return None
            
            try:
                buffer = pa.allocate_buffer(nbytes, memory_pool=self.pool)
                self.memory_tracker.track_allocation(nbytes)
                
                # Track allocation for leak detection if enabled
                if self.enable_leak_detection:
                    import traceback
                    buffer_id = id(buffer)
                    self.allocation_tracker[buffer_id] = (nbytes, traceback.extract_stack())
                
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
            
            # Remove from allocation tracker if present
            if self.enable_leak_detection:
                buffer_id = id(buffer)
                if buffer_id in self.allocation_tracker:
                    del self.allocation_tracker[buffer_id]
            
            # Delete reference to the buffer
            del buffer
            self.memory_tracker.track_deallocation(size)
    
    def _monitor_memory(self) -> None:
        """Background thread to monitor memory usage"""
        last_check_time = time.time()
        check_for_leaks_interval = 300  # Check for leaks every 5 minutes
        
        backoff_time = 5.0  # Base check interval in seconds
        
        while not self._stop_monitor_event.is_set():
            try:
                current_time = time.time()
                
                # Check memory pressure based on tracked memory
                usage_ratio = self.memory_tracker.get_usage_ratio()
                
                # Only trigger spilling if our tracked memory exceeds threshold
                if usage_ratio > self.config["memory_spill_threshold"] and self.spill_callback:
                    # Calculate how much to free
                    target_ratio = self.config["memory_spill_threshold"] * 0.8  # Target 80% of threshold
                    current_usage = self.memory_tracker.get_allocated_bytes()
                    target_usage = target_ratio * self.memory_tracker.get_limit_bytes()
                    bytes_to_free = int(current_usage - target_usage)
                    
                    # Only perform spilling if we need to free significant memory
                    if bytes_to_free > 1024 * 1024:  # Only if more than 1MB
                        logger.info(f"Cache memory usage at {usage_ratio:.2f}, triggering spill of {bytes_to_free} bytes")
                        self.spill_callback(bytes_to_free)
                
                # Periodically check for memory leaks
                if self.enable_leak_detection and current_time - last_check_time > check_for_leaks_interval:
                    last_check_time = current_time
                    self._check_for_leaks()
                    
            except Exception as e:
                logger.error(f"Error in memory monitor: {e}")
            
            # Sleep using the current backoff time
            self._stop_monitor_event.wait(backoff_time)
    
    def _check_for_leaks(self) -> None:
        """Check for potential memory leaks by analyzing long-lived allocations"""
        if not self.allocation_tracker:
            return
            
        # Count allocations by stack trace to find patterns
        stack_counts = {}
        total_leaked = 0
        
        for buffer_id, (size, stack) in self.allocation_tracker.items():
            # Convert stack trace to string for grouping
            stack_str = ''.join(str(frame) for frame in stack[-5:])  # Use last 5 frames
            if stack_str in stack_counts:
                stack_counts[stack_str][0] += 1
                stack_counts[stack_str][1] += size
            else:
                stack_counts[stack_str] = [1, size, stack]
            
            total_leaked += size
        
        # Log potential leaks
        if stack_counts:
            logger.warning(f"Potential memory leak detected: {len(self.allocation_tracker)} allocations not freed, total {total_leaked} bytes")
            
            # Log the top 5 sources of leaks
            top_leaks = sorted(stack_counts.items(), key=lambda x: x[1][1], reverse=True)[:5]
            for i, (stack_str, (count, size, stack)) in enumerate(top_leaks):
                logger.warning(f"Leak source #{i+1}: {count} allocations, {size} bytes")
                for frame in stack[-5:]:  # Log last 5 frames
                    logger.warning(f"  {frame}")
    
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
        # Collect memory info
        process = psutil.Process(os.getpid())
        process_memory = process.memory_info()
        
        return {
            "allocated_bytes": self.memory_tracker.get_allocated_bytes(),
            "limit_bytes": self.memory_tracker.get_limit_bytes(),
            "usage_ratio": self.memory_tracker.get_usage_ratio(),
            "pool_bytes_allocated": self.pool.bytes_allocated(),
            "pool_max_memory": self.pool.max_memory(),
            "system_memory_used": psutil.virtual_memory().used,
            "system_memory_total": psutil.virtual_memory().total,
            "system_memory_available": psutil.virtual_memory().available,
            "process_rss": process_memory.rss,
            "process_vms": process_memory.vms,
            "tracked_allocations": len(self.allocation_tracker) if self.enable_leak_detection else 0
        }
    
    def release_unused_memory(self) -> None:
        """
        Attempt to release any unused memory back to the system.
        """
        # Run Python garbage collection
        import gc
        gc.collect()
        
        # If using jemalloc, we can release memory
        if HAVE_JEMALLOC and self.config["memory_pool_type"].lower() == "jemalloc":
            try:
                # Try to release memory back to the OS
                jemalloc.release_unused_memory()
                logger.debug("Released unused jemalloc memory")
            except Exception as e:
                logger.warning(f"Failed to release jemalloc memory: {e}")
    
    def close(self) -> None:
        """Close the memory manager and release resources"""
        self.stop_memory_monitor()
        self.release_unused_memory()
        
        # Clear allocation tracker
        self.allocation_tracker.clear()


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
    Estimate the memory usage of an Arrow table accurately using a combination of methods.
    
    Args:
        table: Arrow table
        
    Returns:
        Estimated memory usage in bytes
    """
    # First try direct buffer-based estimation (most accurate)
    try:
        # Calculate size based on each column's buffers
        total_size = 0
        
        # Track unique buffer addresses to avoid double-counting shared buffers
        seen_buffers = set()
        
        for column in table.columns:
            for chunk in column.chunks:
                for buffer in chunk.buffers():
                    if buffer is not None:
                        # Use buffer address to avoid double counting
                        buffer_id = id(buffer)
                        if buffer_id not in seen_buffers:
                            seen_buffers.add(buffer_id)
                            total_size += buffer.size
        
        # Add schema overhead
        schema_size = len(table.schema.to_string()) * 2
        
        # Add metadata size if present
        metadata_size = 0
        if table.schema.metadata:
            for k, v in table.schema.metadata.items():
                metadata_size += len(k) + len(v)
        
        # Sanity check - if total_size is unreasonably small for non-empty table
        if total_size < 1000 and table.num_rows > 0 and table.num_columns > 0:
            # Fallback to memory pool based estimation
            return _estimate_with_memory_pool(table)
        
        return total_size + schema_size + metadata_size
        
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Error in direct buffer estimation: {e}")
        # Fallback to memory pool based method
        return _estimate_with_memory_pool(table)

def _estimate_with_memory_pool(table: pa.Table) -> int:
    """
    Fallback method to estimate table size using memory pool.
    
    Args:
        table: Arrow table
        
    Returns:
        Estimated memory usage in bytes
    """
    # Get the default memory pool
    memory_pool = pa.default_memory_pool()
    
    # Use direct measurement with Arrow's memory pool
    import gc
    
    # Enable GC to ensure consistent memory state
    gc_enabled = gc.isenabled()
    if not gc_enabled:
        gc.enable()
    
    # Force a GC cycle to clean up any pending objects
    gc.collect()
    
    # Capture memory before accessing the table
    memory_before = memory_pool.bytes_allocated()
    
    # Access the data to ensure it's materialized
    for col in table.columns:
        for chunk in col.chunks:
            if len(chunk) > 0:
                # Access first element to materialize
                chunk[0]
    
    # Force another GC cycle to clean up temporary objects
    gc.collect()
    
    # Measure memory after access
    memory_after = memory_pool.bytes_allocated()
    
    # Calculate the difference
    size = memory_after - memory_before
    
    # Add schema overhead
    schema_size = len(table.schema.to_string()) * 2
    
    # Add metadata size if present
    metadata_size = 0
    if table.schema.metadata:
        for k, v in table.schema.metadata.items():
            metadata_size += len(k) + len(v)
    
    return max(size + schema_size + metadata_size, 1024)  # Ensure at least 1KB to avoid returning 0


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
    compression_type: str = "zstd", 
    compression_level: int = 3,
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
    import pyarrow.compute as pc
    
    # Apply dictionary encoding to string columns if enabled
    if use_dictionary:
        arrays = []
        fields = []
        
        for i, column in enumerate(table.columns):
            field = table.schema.field(i)
            
            # Apply dictionary encoding to string columns
            if (pa.types.is_string(field.type) or pa.types.is_binary(field.type)) and not pa.types.is_dictionary(field.type):
                try:
                    # Apply dictionary encoding using Arrow compute
                    dict_array = pc.dictionary_encode(column)
                    new_field = pa.field(field.name, dict_array.type, field.nullable)
                    
                    arrays.append(dict_array)
                    fields.append(new_field)
                except Exception as e:
                    # Keep original if encoding fails, but log the error
                    logger.warning(f"Dictionary encoding failed for column '{field.name}': {str(e)}")
                    arrays.append(column)
                    fields.append(field)
            else:
                # Keep non-string columns or already dictionary-encoded columns as is
                arrays.append(column)
                fields.append(field)
        
        # Create new table with compressed columns
        try:
            return pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        except Exception as e:
            # Return original if table creation fails
            logger.error(f"Failed to create compressed table: {str(e)}")
            return table
    
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


def zero_copy_filter(table: pa.Table, mask: pa.Array) -> pa.Table:
    """
    Create a zero-copy filtered view of an Arrow table using a boolean mask
    
    Args:
        table: Arrow table to filter
        mask: Boolean mask array (must be same length as table)
        
    Returns:
        Filtered Arrow table (zero-copy when possible)
    """
    import pyarrow.compute as pc
    
    # Verify mask is a boolean array
    if not pa.types.is_boolean(mask.type):
        raise TypeError("Mask must be a boolean array")
    
    # Verify mask length matches table
    if len(mask) != table.num_rows:
        raise ValueError(f"Mask length ({len(mask)}) must match table row count ({table.num_rows})")
    
    # Use Arrow's filter function which is optimized for zero-copy when possible
    return table.filter(mask)


def efficient_table_chunking(table: pa.Table, max_chunk_size: int = 100000) -> List[pa.Table]:
    """
    Split a table into optimally sized chunks for efficient processing
    
    Args:
        table: Arrow table to chunk
        max_chunk_size: Maximum number of rows per chunk
        
    Returns:
        List of Arrow table chunks (zero-copy when possible)
    """
    # Return empty list for empty table
    if table.num_rows == 0:
        return []
    
    # Calculate optimal chunk size based on table size and available memory
    rows_per_chunk = min(max_chunk_size, table.num_rows)
    num_chunks = (table.num_rows + rows_per_chunk - 1) // rows_per_chunk
    
    # Create chunks using zero-copy slicing
    chunks = []
    for i in range(num_chunks):
        start = i * rows_per_chunk
        size = min(rows_per_chunk, table.num_rows - start)
        chunks.append(table.slice(start, size))
    
    return chunks


def optimize_string_columns(table: pa.Table, min_unique_ratio: float = 0.5) -> pa.Table:
    """
    Optimize string columns in an Arrow table using dictionary encoding
    when beneficial
    
    Args:
        table: Arrow table to optimize
        min_unique_ratio: Minimum ratio of unique values to total values for dictionary encoding
        
    Returns:
        Optimized Arrow table
    """
    import pyarrow.compute as pc
    
    # Process each column
    columns = []
    fields = []
    
    for i, col in enumerate(table.columns):
        field = table.schema.field(i)
        
        # Only process string columns that aren't already dictionary encoded
        if pa.types.is_string(field.type) and not pa.types.is_dictionary(field.type):
            try:
                # Get unique count using Arrow compute function
                unique_count = len(pc.unique(col))
                total_count = len(col)
                
                # Apply dictionary encoding if beneficial
                if unique_count / total_count <= min_unique_ratio:
                    # Apply dictionary encoding directly
                    dict_array = pc.dictionary_encode(col)
                    new_field = pa.field(field.name, dict_array.type, field.nullable)
                    
                    columns.append(dict_array)
                    fields.append(new_field)
                else:
                    # Keep original
                    columns.append(col)
                    fields.append(field)
            except Exception as e:
                # Keep original if encoding fails but log the error
                logger.warning(f"Failed to optimize column '{field.name}': {str(e)}")
                columns.append(col)
                fields.append(field)
        else:
            # Keep non-string columns as is
            columns.append(col)
            fields.append(field)
    
    # Create new table with optimized columns
    try:
        return pa.Table.from_arrays(columns, schema=pa.schema(fields))
    except Exception as e:
        logger.error(f"Failed to create optimized table: {str(e)}")
        return table


def get_table_memory_footprint(table: pa.Table, detailed: bool = False) -> Dict[str, Any]:
    """
    Get detailed memory usage information for an Arrow table
    
    Args:
        table: Arrow table to analyze
        detailed: Whether to include per-column details
        
    Returns:
        Dictionary with memory usage information
    """
    result = {
        "num_rows": table.num_rows,
        "num_columns": table.num_columns,
    }
    
    # Use estimate_table_memory_usage to get the total size
    total_bytes = estimate_table_memory_usage(table)
    
    if detailed:
        # Process each column to get detailed stats
        column_data = []
        for i, column in enumerate(table.columns):
            field = table.schema.field(i)
            
            # Measure column memory using the same approach
            col_bytes = 0
            
            # Add column details
            column_data.append({
                "name": field.name,
                "type": str(field.type),
                "bytes": col_bytes,
                "bytes_per_row": col_bytes / table.num_rows if table.num_rows > 0 else 0,
                "is_nullable": field.nullable,
                "num_chunks": len(column.chunks),
                "is_dictionary": pa.types.is_dictionary(field.type)
            })
        
        result["columns"] = column_data
    
    # Add schema size
    schema_size = len(table.schema.to_string()) * 2
    
    # Add metadata size if present
    metadata_size = 0
    if table.schema.metadata:
        metadata_size = sum(len(k) + len(v) for k, v in table.schema.metadata.items())
    
    # Set results
    result["total_bytes"] = total_bytes
    result["schema_bytes"] = schema_size
    result["metadata_bytes"] = metadata_size
    
    return result 