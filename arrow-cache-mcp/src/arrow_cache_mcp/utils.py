"""
Utility functions for the Arrow Cache MCP.

This module provides helper utilities for working with datasets and file names.
"""

import re
from typing import Any, Dict, List, Tuple, Optional

def clean_dataset_name(name: str) -> str:
    """
    Clean a dataset name to be a valid identifier.
    
    Args:
        name: Original dataset name
        
    Returns:
        Cleaned dataset name
    """
    # Replace parentheses and common separators with underscores
    name = re.sub(r'[\s().,-]+', '_', name)
    # Remove any characters that are not alphanumeric or underscore
    name = re.sub(r'[^\w_]+', '', name)
    # Replace multiple consecutive underscores with a single underscore
    name = re.sub(r'_+', '_', name)
    # Remove leading/trailing underscores
    name = name.strip('_')
    return name

def get_size_display(size_bytes: int) -> str:
    """
    Convert size in bytes to human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human-readable size string
    """
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes/1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes/(1024*1024):.2f} MB"
    else:
        return f"{size_bytes/(1024*1024*1024):.2f} GB"

def extract_table_references(sql_query: str) -> List[str]:
    """
    Extract table references from a SQL query.
    
    Args:
        sql_query: SQL query string
        
    Returns:
        List of table names
    """
    # Simple regex to extract table names from FROM and JOIN clauses
    tables = []
    
    # Extract tables from FROM clauses
    from_pattern = re.compile(r'FROM\s+_cache_(\w+)', re.IGNORECASE)
    for match in from_pattern.finditer(sql_query):
        tables.append(match.group(1))
    
    # Extract tables from JOIN clauses
    join_pattern = re.compile(r'JOIN\s+_cache_(\w+)', re.IGNORECASE)
    for match in join_pattern.finditer(sql_query):
        tables.append(match.group(1))
    
    return list(set(tables))  # Return unique table names 