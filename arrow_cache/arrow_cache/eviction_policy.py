from abc import ABC, abstractmethod
from typing import Dict, List, Tuple, Any, Optional
import time
import heapq

class EvictionPolicy(ABC):
    """Abstract base class for cache eviction policies."""
    
    @abstractmethod
    def get_eviction_candidates(self, entries: Dict[str, Any], target_bytes: int) -> List[str]:
        """
        Return a list of entry keys to evict to free up at least target_bytes.
        
        Args:
            entries: Dictionary mapping keys to cache entry objects
            target_bytes: Target number of bytes to free
            
        Returns:
            List of keys to evict, in order of eviction priority
        """
        pass
    
    @abstractmethod
    def update_entry_accessed(self, key: str) -> None:
        """
        Update tracking data when an entry is accessed.
        
        Args:
            key: The key of the accessed entry
        """
        pass
        
    @abstractmethod
    def add_entry(self, key: str, size_bytes: int) -> None:
        """
        Track a new entry added to the cache.
        
        Args:
            key: The key of the added entry
            size_bytes: Size of the entry in bytes
        """
        pass
        
    @abstractmethod
    def remove_entry(self, key: str) -> None:
        """
        Remove tracking data for an entry.
        
        Args:
            key: The key of the removed entry
        """
        pass
        
    @abstractmethod
    def clear(self) -> None:
        """
        Clear all tracking data.
        """
        pass


class LRUEvictionPolicy(EvictionPolicy):
    """Least Recently Used eviction policy."""
    
    def __init__(self):
        self.access_times = {}  # key -> last access time
    
    def get_eviction_candidates(self, entries: Dict[str, Any], target_bytes: int) -> List[str]:
        # Sort entries by last accessed time (oldest first)
        sorted_entries = sorted(
            entries.items(),
            key=lambda x: getattr(x[1], 'last_accessed_at', 0)
        )
        
        candidates = []
        freed_bytes = 0
        
        for key, entry in sorted_entries:
            candidates.append(key)
            freed_bytes += entry.size_bytes
            if freed_bytes >= target_bytes:
                break
                
        return candidates
    
    def update_entry_accessed(self, key: str) -> None:
        self.access_times[key] = time.time()
        
    def add_entry(self, key: str, size_bytes: int) -> None:
        self.access_times[key] = time.time()
        
    def remove_entry(self, key: str) -> None:
        if key in self.access_times:
            del self.access_times[key]
            
    def clear(self) -> None:
        self.access_times.clear()


class LFUEvictionPolicy(EvictionPolicy):
    """Least Frequently Used eviction policy."""
    
    def __init__(self):
        self.access_counts = {}  # key -> access count
    
    def get_eviction_candidates(self, entries: Dict[str, Any], target_bytes: int) -> List[str]:
        # Sort entries by access count (least accessed first)
        sorted_entries = sorted(
            entries.items(),
            key=lambda x: getattr(x[1], 'access_count', 0)
        )
        
        candidates = []
        freed_bytes = 0
        
        for key, entry in sorted_entries:
            candidates.append(key)
            freed_bytes += entry.size_bytes
            if freed_bytes >= target_bytes:
                break
                
        return candidates
    
    def update_entry_accessed(self, key: str) -> None:
        self.access_counts[key] = self.access_counts.get(key, 0) + 1
        
    def add_entry(self, key: str, size_bytes: int) -> None:
        self.access_counts[key] = 0
        
    def remove_entry(self, key: str) -> None:
        if key in self.access_counts:
            del self.access_counts[key]
            
    def clear(self) -> None:
        self.access_counts.clear()


class FIFOEvictionPolicy(EvictionPolicy):
    """First In First Out eviction policy."""
    
    def __init__(self):
        self.insertion_times = {}  # key -> insertion time
    
    def get_eviction_candidates(self, entries: Dict[str, Any], target_bytes: int) -> List[str]:
        # Sort entries by creation time (oldest first)
        sorted_entries = sorted(
            entries.items(),
            key=lambda x: getattr(x[1], 'created_at', 0)
        )
        
        candidates = []
        freed_bytes = 0
        
        for key, entry in sorted_entries:
            candidates.append(key)
            freed_bytes += entry.size_bytes
            if freed_bytes >= target_bytes:
                break
                
        return candidates
    
    def update_entry_accessed(self, key: str) -> None:
        # FIFO doesn't update based on access
        pass
        
    def add_entry(self, key: str, size_bytes: int) -> None:
        self.insertion_times[key] = time.time()
        
    def remove_entry(self, key: str) -> None:
        if key in self.insertion_times:
            del self.insertion_times[key]
            
    def clear(self) -> None:
        self.insertion_times.clear()


class SizeBasedEvictionPolicy(EvictionPolicy):
    """Evicts largest entries first."""
    
    def __init__(self):
        self.sizes = {}  # key -> size in bytes
    
    def get_eviction_candidates(self, entries: Dict[str, Any], target_bytes: int) -> List[str]:
        # Sort entries by size (largest first)
        sorted_entries = sorted(
            entries.items(),
            key=lambda x: getattr(x[1], 'size_bytes', 0),
            reverse=True
        )
        
        candidates = []
        freed_bytes = 0
        
        for key, entry in sorted_entries:
            candidates.append(key)
            freed_bytes += entry.size_bytes
            if freed_bytes >= target_bytes:
                break
                
        return candidates
    
    def update_entry_accessed(self, key: str) -> None:
        # Size-based doesn't update based on access
        pass
        
    def add_entry(self, key: str, size_bytes: int) -> None:
        self.sizes[key] = size_bytes
        
    def remove_entry(self, key: str) -> None:
        if key in self.sizes:
            del self.sizes[key]
            
    def clear(self) -> None:
        self.sizes.clear()


class TTLEvictionPolicy(EvictionPolicy):
    """Time To Live eviction policy - evicts expired entries first, then oldest."""
    
    def __init__(self):
        self.expirations = {}  # key -> expiration time
        self.creation_times = {}  # key -> creation time
    
    def get_eviction_candidates(self, entries: Dict[str, Any], target_bytes: int) -> List[str]:
        now = time.time()
        
        # First, collect expired entries
        expired_entries = []
        non_expired_entries = []
        
        for key, entry in entries.items():
            if entry.expires_at is not None and entry.expires_at <= now:
                expired_entries.append((key, entry))
            else:
                non_expired_entries.append((key, entry))
        
        # Sort expired entries by expiration time (most expired first)
        expired_entries.sort(key=lambda x: x[1].expires_at or 0)
        
        # Sort non-expired entries by creation time (oldest first)
        non_expired_entries.sort(key=lambda x: x[1].created_at)
        
        # Combine the lists (expired entries first)
        sorted_entries = expired_entries + non_expired_entries
        
        candidates = []
        freed_bytes = 0
        
        for key, entry in sorted_entries:
            candidates.append(key)
            freed_bytes += entry.size_bytes
            if freed_bytes >= target_bytes:
                break
                
        return candidates
    
    def update_entry_accessed(self, key: str) -> None:
        # TTL doesn't update based on access
        pass
        
    def add_entry(self, key: str, size_bytes: int) -> None:
        self.creation_times[key] = time.time()
        
    def remove_entry(self, key: str) -> None:
        if key in self.creation_times:
            del self.creation_times[key]
        if key in self.expirations:
            del self.expirations[key]
            
    def clear(self) -> None:
        self.creation_times.clear()
        self.expirations.clear()


class WeightedEvictionPolicy(EvictionPolicy):
    """
    Weighted eviction policy that combines multiple factors:
    - Time since last access (LRU factor)
    - Access frequency (LFU factor)
    - Size (Size factor)
    - Age (Age factor)
    """
    
    def __init__(
        self,
        lru_weight: float = 1.0,
        lfu_weight: float = 1.0,
        size_weight: float = 1.0,
        age_weight: float = 0.5
    ):
        """
        Initialize the weighted eviction policy.
        
        Args:
            lru_weight: Weight for the LRU factor (higher means more important)
            lfu_weight: Weight for the LFU factor (higher means more important)
            size_weight: Weight for the size factor (higher means more important)
            age_weight: Weight for the age factor (higher means more important)
        """
        self.lru_weight = lru_weight
        self.lfu_weight = lfu_weight
        self.size_weight = size_weight
        self.age_weight = age_weight
        
        self.access_times = {}  # key -> last access time
        self.access_counts = {}  # key -> access count
        self.sizes = {}  # key -> size in bytes
        self.creation_times = {}  # key -> creation time
    
    def get_eviction_candidates(self, entries: Dict[str, Any], target_bytes: int) -> List[str]:
        now = time.time()
        entry_scores = []
        
        # Calculate normalized scores for each factor across all entries
        max_lru = max((now - getattr(entry, 'last_accessed_at', 0)) for entry in entries.values()) or 1
        max_lfu = max(getattr(entry, 'access_count', 0) for entry in entries.values()) or 1
        max_size = max(getattr(entry, 'size_bytes', 0) for entry in entries.values()) or 1
        max_age = max((now - getattr(entry, 'created_at', 0)) for entry in entries.values()) or 1
        
        for key, entry in entries.items():
            # Higher score means higher priority for eviction
            lru_score = (now - getattr(entry, 'last_accessed_at', 0)) / max_lru
            lfu_score = 1.0 - (getattr(entry, 'access_count', 0) / max_lfu)  # Invert so less used = higher score
            size_score = getattr(entry, 'size_bytes', 0) / max_size
            age_score = (now - getattr(entry, 'created_at', 0)) / max_age
            
            # Weighted combined score
            combined_score = (
                self.lru_weight * lru_score +
                self.lfu_weight * lfu_score +
                self.size_weight * size_score +
                self.age_weight * age_score
            ) / (self.lru_weight + self.lfu_weight + self.size_weight + self.age_weight)
            
            entry_scores.append((combined_score, key, entry))
        
        # Sort by score (highest first)
        entry_scores.sort(reverse=True)
        
        candidates = []
        freed_bytes = 0
        
        for _, key, entry in entry_scores:
            candidates.append(key)
            freed_bytes += entry.size_bytes
            if freed_bytes >= target_bytes:
                break
                
        return candidates
    
    def update_entry_accessed(self, key: str) -> None:
        self.access_times[key] = time.time()
        self.access_counts[key] = self.access_counts.get(key, 0) + 1
        
    def add_entry(self, key: str, size_bytes: int) -> None:
        self.access_times[key] = time.time()
        self.access_counts[key] = 0
        self.sizes[key] = size_bytes
        self.creation_times[key] = time.time()
        
    def remove_entry(self, key: str) -> None:
        if key in self.access_times:
            del self.access_times[key]
        if key in self.access_counts:
            del self.access_counts[key]
        if key in self.sizes:
            del self.sizes[key]
        if key in self.creation_times:
            del self.creation_times[key]
            
    def clear(self) -> None:
        self.access_times.clear()
        self.access_counts.clear()
        self.sizes.clear()
        self.creation_times.clear()


# Factory function to create eviction policies
def create_eviction_policy(policy_name: str, **kwargs) -> EvictionPolicy:
    """
    Create an eviction policy by name.
    
    Args:
        policy_name: Name of the policy ('lru', 'lfu', 'fifo', 'size', 'ttl', 'weighted')
        **kwargs: Additional arguments for the policy
        
    Returns:
        An EvictionPolicy instance
    """
    policy_name = policy_name.lower()
    
    if policy_name == 'lru':
        return LRUEvictionPolicy()
    elif policy_name == 'lfu':
        return LFUEvictionPolicy()
    elif policy_name == 'fifo':
        return FIFOEvictionPolicy()
    elif policy_name == 'size':
        return SizeBasedEvictionPolicy()
    elif policy_name == 'ttl':
        return TTLEvictionPolicy()
    elif policy_name == 'weighted':
        return WeightedEvictionPolicy(
            lru_weight=kwargs.get('lru_weight', 1.0),
            lfu_weight=kwargs.get('lfu_weight', 1.0),
            size_weight=kwargs.get('size_weight', 1.0),
            age_weight=kwargs.get('age_weight', 0.5)
        )
    else:
        raise ValueError(f"Unknown eviction policy: {policy_name}")
