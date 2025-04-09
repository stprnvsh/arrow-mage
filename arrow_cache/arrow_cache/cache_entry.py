import time
from typing import Optional, Any, Dict
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
        size_bytes: int,
        ttl: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Initialize a new cache entry.
        
        Args:
            key: Unique identifier for this cache entry
            table: Arrow table containing the data
            size_bytes: Size of the table in bytes
            ttl: Time-to-live in seconds (None means no expiration)
            metadata: Additional metadata about this entry
        """
        self.key = key
        self.table = table
        self.size_bytes = size_bytes
        self.metadata = metadata or {}
        
        # Track access patterns for eviction policies
        self.created_at = time.time()
        self.last_accessed_at = self.created_at
        self.access_count = 0
        
        # Set expiration time if TTL is provided
        self.expires_at = (self.created_at + ttl) if ttl is not None else None
    
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
    
    def update_ttl(self, ttl: Optional[float]) -> None:
        """Update the TTL for this entry."""
        if ttl is None:
            self.expires_at = None
        else:
            self.expires_at = time.time() + ttl
    
    def estimate_memory_usage(self) -> int:
        """
        Estimate the total memory usage of this entry including the Arrow table.
        This may be different from the reported size_bytes depending on how
        the table is stored and shared.
        """
        # This is a simplistic estimation - in reality memory usage can be complex
        # especially with shared memory and zero-copy operations
        # For a more accurate estimate, you would need to consider:
        # - Table buffer sharing
        # - Memory pool allocation patterns
        # - Overhead of Python objects
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
            "metadata": self.metadata
        }
