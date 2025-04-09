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
                        
                        # Collect Python garbage to try to free memory
                        import gc
                        gc.collect()
                        
                        # If we still can't allocate, return None
                        if not self.memory_tracker.can_allocate(nbytes):
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
        
        while not self._stop_monitor_event.is_set():
            try:
                current_time = time.time()
                
                # Check memory pressure
                usage_ratio = self.memory_tracker.get_usage_ratio()
                
                # Check system memory as well
                system_memory_usage = psutil.virtual_memory().percent / 100.0
                
                # Trigger spilling if either our tracked memory or system memory is high
                if (usage_ratio > self.config["memory_spill_threshold"] or 
                   system_memory_usage > self.config["memory_spill_threshold"]) and self.spill_callback:
                    # Calculate how much to free
                    target_ratio = self.config["memory_spill_threshold"] * 0.8  # Target 80% of threshold
                    current_usage = self.memory_tracker.get_allocated_bytes()
                    target_usage = target_ratio * self.memory_tracker.get_limit_bytes()
                    bytes_to_free = int(current_usage - target_usage)
                    
                    # If system memory is the trigger, calculate based on that too
                    if system_memory_usage > self.config["memory_spill_threshold"]:
                        system_bytes_to_free = int((system_memory_usage - self.config["memory_spill_threshold"] * 0.8) * 
                                                   psutil.virtual_memory().total)
                        bytes_to_free = max(bytes_to_free, system_bytes_to_free)
                    
                    if bytes_to_free > 0:
                        logger.info(f"Memory usage at {usage_ratio:.2f}, system at {system_memory_usage:.2f}, triggering spill of {bytes_to_free} bytes")
                        freed = self.spill_callback(bytes_to_free)
                        logger.info(f"Freed {freed} bytes through spilling")
                        
                        # If we couldn't free enough, force a garbage collection
                        if freed < bytes_to_free * 0.5:  # If we freed less than half of what we wanted
                            import gc
                            logger.info("Running garbage collection to free memory")
                            gc.collect()
                
                # Periodically check for memory leaks
                if self.enable_leak_detection and current_time - last_check_time > check_for_leaks_interval:
                    last_check_time = current_time
                    self._check_for_leaks()
                    
            except Exception as e:
                logger.error(f"Error in memory monitor: {e}")
            
            # Sleep for a bit
            self._stop_monitor_event.wait(5.0)  # Check every 5 seconds
    
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
    Estimate the memory usage of an Arrow table accurately using Arrow's native capabilities.
    
    Args:
        table: Arrow table
        
    Returns:
        Estimated memory usage in bytes
    """
    # Get the default memory pool
    memory_pool = pa.default_memory_pool()
    
    # First try direct measurement with Arrow's memory pool
    try:
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
        # The most accurate way is to force materialization of every column
        for col in table.columns:
            for chunk in col.chunks:
                if len(chunk) > 0:
                    # Force materialization of the entire chunk
                    # Just accessing one element might not materialize everything
                    chunk.to_numpy()
        
        # Force another GC cycle to clean up temporary objects
        gc.collect()
        
        # Measure memory after access
        memory_after = memory_pool.bytes_allocated()
        
        # If we got a meaningful difference, use it
        if memory_after > memory_before:
            size = memory_after - memory_before
            
            # Apply a sanity check to prevent unrealistic values
            import psutil
            system_memory = psutil.virtual_memory().total
            if size > system_memory * 0.8:
                logger.warning(f"Memory estimation unrealistically large: {size/(1024*1024*1024):.2f} GB")
                # Use a fallback method
                size = 0  # Will trigger fallback below
            else:
                # Return the measured size with a small overhead for safety
                return int(size * 1.05)  # Add 5% for overhead
    except Exception as e:
        logger.debug(f"Error using Arrow memory pool for size estimation: {e}")
        # Continue to fallback methods
    
    # Fallback: calculate size based on buffer sizes with unique buffer tracking
    try:
        # Use a set to track unique buffer addresses to avoid double-counting
        unique_buffers = set()
        size = 0
        
        for column in table.columns:
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
        
        # Add buffer management overhead
        overhead = table.num_columns * 16  # 16 bytes per column pointer
        
        total_size = size + schema_size + metadata_size + overhead
        
        # Sanity check against system memory
        import psutil
        system_memory = psutil.virtual_memory().total
        if total_size > system_memory * 0.8:
            logger.warning(f"Calculated size ({total_size/(1024*1024*1024):.2f} GB) exceeds 80% of system memory")
            # Cap at a reasonable percentage of system memory
            return int(system_memory * 0.5)  # 50% of system memory as a safe estimate
        
        return total_size
    except Exception as e:
        logger.warning(f"Error in detailed size calculation: {e}")
        
        # Last resort: use a simple estimation based on rows and columns
        try:
            # Get numpy size of a sample if possible
            if table.num_rows > 0:
                # Sample the first column for estimation
                if table.num_columns > 0:
                    sample_col = table.column(0)
                    if len(sample_col) > 0:
                        # Get numpy array for better size estimation
                        import numpy as np
                        try:
                            # Try to convert a small sample to numpy
                            sample_size = min(1000, len(sample_col))
                            sample_array = sample_col.slice(0, sample_size).to_numpy()
                            bytes_per_value = sample_array.nbytes / sample_size
                            
                            # Estimate based on sample size
                            estimated_size = int(table.num_rows * table.num_columns * bytes_per_value)
                            
                            # Add 20% overhead
                            return int(estimated_size * 1.2)
                        except:
                            # If conversion to numpy fails, use default estimate
                            pass
            
            # Very conservative fallback estimate
            avg_bytes_per_value = 8  # Assume 8 bytes per value average
            estimated_size = table.num_rows * table.num_columns * avg_bytes_per_value
            
            # Add 20% overhead
            return int(estimated_size * 1.2)
        except:
            # Absolute last resort if everything else fails
            # Return a size estimate based on row count only
            return max(1024 * 1024, table.num_rows * 100)  # At least 1MB


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
    import gc
    
    # For very large tables, use higher compression
    table_size_bytes = estimate_table_memory_usage(table)
    if table_size_bytes > 1024*1024*1024:  # 1GB+
        # For large tables, use more aggressive compression
        if compression_type == "zstd":
            compression_level = max(compression_level, 5)
        elif compression_type == "lz4":
            # Switch to zstd for better compression ratio
            compression_type = "zstd"
            compression_level = 5
    
    # Apply dictionary encoding to string columns if enabled
    if use_dictionary:
        arrays = []
        fields = []
        
        for i, column in enumerate(table.columns):
            field = table.schema.field(i)
            
            # Apply dictionary encoding to string columns with sufficient size
            if (pa.types.is_string(field.type) or pa.types.is_binary(field.type)) and column.nbytes > 1024:
                try:
                    # Get column cardinality to decide if dictionary encoding is worthwhile
                    unique_count = 0
                    try:
                        # Try to count unique values efficiently
                        unique_count = len(pc.unique(column))
                    except:
                        # Fallback method - handle mixed types more safely
                        try:
                            # Convert to list first to handle mixed types more safely
                            column_list = column.to_pylist()
                            # Filter out None values and ensure all items are string-like
                            column_list = [str(x) if x is not None else None for x in column_list]
                            unique_count = len(set(x for x in column_list if x is not None))
                        except Exception as type_error:
                            logger.warning(f"Failed to count unique values: {type_error}")
                            # Assume encoding won't be beneficial
                            unique_count = max(1, column.length // 2)
                    
                    # Only use dictionary encoding if it's efficient (low cardinality compared to length)
                    if unique_count < min(column.length * 0.5, 1000000):
                        # Try to safely apply dictionary encoding
                        try:
                            # Handle mixed types by converting to string first if needed
                            if not pa.types.is_string(column.type):
                                # Cast to string safely
                                string_array = pc.cast(column, pa.string())
                                dict_array = pc.dictionary_encode(string_array)
                            else:
                                dict_array = pc.dictionary_encode(column)
                            
                            arrays.append(dict_array)
                            
                            # Create a new field with the dictionary-encoded type
                            new_field = pa.field(field.name, dict_array.type, field.nullable)
                            fields.append(new_field)
                        except Exception as encoding_error:
                            logger.warning(f"Failed to encode column {field.name} as dictionary, falling back to original: {encoding_error}")
                            arrays.append(column)
                            fields.append(field)
                    else:
                        # High cardinality - dictionary would be inefficient
                        arrays.append(column)
                        fields.append(field)
                except Exception as e:
                    logger.warning(f"Failed to encode column {field.name} as dictionary: {e}")
                    arrays.append(column)
                    fields.append(field)
            else:
                arrays.append(column)
                fields.append(field)
        
        # Create new table with compressed columns
        compressed_table = pa.Table.from_arrays(arrays, schema=pa.schema(fields))
        
        # Clean up to reduce memory pressure
        del arrays
        gc.collect()
        
        return compressed_table
    
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
        
        # Only process string columns
        if pa.types.is_string(field.type):
            # Check if already dictionary encoded
            if pa.types.is_dictionary(field.type):
                columns.append(col)
                fields.append(field)
                continue
            
            try:
                # Get unique count using Arrow compute function
                unique_count = len(pc.unique(col))
                total_count = len(col)
                
                # Apply dictionary encoding if beneficial
                if unique_count / total_count <= min_unique_ratio:
                    # Apply dictionary encoding
                    dict_array = pc.dictionary_encode(col)
                    new_field = pa.field(field.name, dict_array.type, field.nullable)
                    
                    columns.append(dict_array)
                    fields.append(new_field)
                else:
                    # Keep original
                    columns.append(col)
                    fields.append(field)
            except Exception as e:
                # Keep original if encoding fails
                logger.warning(f"Failed to optimize column {field.name}: {e}")
                columns.append(col)
                fields.append(field)
        else:
            # Keep non-string columns as is
            columns.append(col)
            fields.append(field)
    
    # Create new table with optimized columns
    return pa.Table.from_arrays(columns, schema=pa.schema(fields))


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
        "total_bytes": 0,
        "num_rows": table.num_rows,
        "num_columns": table.num_columns,
    }
    
    column_data = []
    total_bytes = 0
    
    # Process each column
    for i, column in enumerate(table.columns):
        field = table.schema.field(i)
        col_bytes = 0
        
        # Sum buffer sizes
        for chunk in column.chunks:
            for buf in chunk.buffers():
                if buf is not None:
                    col_bytes += buf.size
        
        # Add to total
        total_bytes += col_bytes
        
        # Add column details if requested
        if detailed:
            column_data.append({
                "name": field.name,
                "type": str(field.type),
                "bytes": col_bytes,
                "bytes_per_row": col_bytes / table.num_rows if table.num_rows > 0 else 0,
                "is_nullable": field.nullable,
                "num_chunks": len(column.chunks),
                "is_dictionary": pa.types.is_dictionary(field.type)
            })
    
    # Add schema size
    schema_size = len(table.schema.to_string()) * 2
    total_bytes += schema_size
    
    # Add metadata size if present
    metadata_size = 0
    if table.schema.metadata:
        metadata_size = sum(len(k) + len(v) for k, v in table.schema.metadata.items())
        total_bytes += metadata_size
    
    # Set results
    result["total_bytes"] = total_bytes
    result["schema_bytes"] = schema_size
    result["metadata_bytes"] = metadata_size
    
    if detailed:
        result["columns"] = column_data
    
    return result 