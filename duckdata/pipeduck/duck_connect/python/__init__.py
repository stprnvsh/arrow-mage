"""
DuckConnect - Python Client

This module provides the Python client for DuckConnect, a cross-language
data sharing system using DuckDB as a shared memory database.
"""

# Import core classes
from pipeduck.duck_connect.core.facade import DuckConnect
from pipeduck.duck_connect.core.connection import ConnectionPool
from pipeduck.duck_connect.core.transactions import TransactionManager

__version__ = '0.1.0'
__all__ = ['DuckConnect', 'ConnectionPool', 'TransactionManager'] 