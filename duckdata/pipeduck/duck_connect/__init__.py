"""
DuckConnect: Optimized cross-language data sharing with DuckDB

This module provides an optimized way to share data between Python, R, and Julia
by leveraging DuckDB as a shared memory database with metadata to avoid
expensive push/pull operations between languages.
"""

from .core.connection import ConnectionPool
from .core.facade import DuckConnect

__version__ = '0.1.0'
__all__ = ['DuckConnect', 'ConnectionPool'] 