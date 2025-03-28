"""
PipeDuck: Enhanced Cross-Language Pipeline Orchestration (Python Module)

This module provides the Python implementation for PipeDuck.
"""

from .duck_context import DuckContext
from .pipeduck import run_pipeline, main
from .duck_connector import DuckConnector

__all__ = [
    'DuckContext',
    'run_pipeline',
    'main',
    'DuckConnector'
] 