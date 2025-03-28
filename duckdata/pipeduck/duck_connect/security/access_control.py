"""
Access control module for DuckConnect

This module provides functionality for managing access control for datasets.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Union, Set

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.security.access_control')

def set_access_control(connection_pool, metadata_manager, lock_manager, identifier, 
                     access_rules, language="python"):
    """
    Set access control rules for a dataset
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        lock_manager: Lock manager
        identifier: Dataset ID or name
        access_rules: Dictionary of access control rules
        language: Language performing the operation
        
    Returns:
        True if successful
    """
    # Get dataset metadata
    metadata = metadata_manager.get_dataset_metadata(identifier)
    
    if not metadata:
        raise ValueError(f"Dataset '{identifier}' not found")
        
    dataset_id = metadata['id']
    name = metadata['name']
    
    # Validate access rules
    if not isinstance(access_rules, dict):
        raise ValueError("Access rules must be a dictionary")
    
    # Check required keys
    required_keys = ['users', 'roles']
    for key in required_keys:
        if key not in access_rules:
            access_rules[key] = {}
    
    # Acquire exclusive lock
    if not lock_manager.acquire_lock(dataset_id, lock_type='exclusive'):
        raise TimeoutError(f"Failed to acquire exclusive lock on dataset {name}")
    
    try:
        # Update metadata with access rules
        metadata_manager.update_dataset_metadata(
            dataset_id,
            access_control=access_rules
        )
        
        # Record transaction
        metadata_manager.record_transaction(
            dataset_id,
            'set_access_control',
            language,
            {'access_rules': access_rules}
        )
        
        return True
    finally:
        # Release lock
        lock_manager.release_lock(dataset_id)

def can_access_dataset(connection_pool, identifier, username, operation='read'):
    """
    Check if a user can access a dataset
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        username: Username
        operation: Operation (read, write, delete)
        
    Returns:
        True if access is allowed
    """
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        if identifier.startswith('duck_'):
            # It's an ID
            metadata = conn.execute(
                "SELECT id, name, access_control, owner FROM duck_connect_metadata WHERE id = ?", 
                [identifier]
            ).fetchone()
        else:
            # It's a name
            metadata = conn.execute(
                "SELECT id, name, access_control, owner FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
        if not metadata:
            return False
            
        dataset_id, name, access_control_json, owner = metadata
        
        # Get user info
        user = conn.execute(
            "SELECT role, permissions FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            return False
            
        role, permissions_json = user
        
        # Admins have access to everything
        if role == 'admin':
            return True
            
        # Owner has full access
        if owner == username:
            return True
            
        # Check access control rules
        if access_control_json:
            access_control = json.loads(access_control_json)
            
            # Check user-specific permissions
            if 'users' in access_control and username in access_control['users']:
                user_perms = access_control['users'][username]
                if operation in user_perms and user_perms[operation]:
                    return True
                    
            # Check role-based permissions
            if 'roles' in access_control and role in access_control['roles']:
                role_perms = access_control['roles'][role]
                if operation in role_perms and role_perms[operation]:
                    return True
        
        # Default: no access
        return False

def apply_row_level_security(connection_pool, identifier, username, query, 
                           filters=None):
    """
    Apply row-level security to a query
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        username: Username
        query: Original query
        filters: Additional filters to apply
        
    Returns:
        Modified query with row-level security applied
    """
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        if identifier.startswith('duck_'):
            # It's an ID
            metadata = conn.execute(
                "SELECT id, name, access_control, table_name FROM duck_connect_metadata WHERE id = ?", 
                [identifier]
            ).fetchone()
        else:
            # It's a name
            metadata = conn.execute(
                "SELECT id, name, access_control, table_name FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id, name, access_control_json, table_name = metadata
        
        # Get user info
        user = conn.execute(
            "SELECT role, permissions FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            raise ValueError(f"User '{username}' not found")
            
        role, permissions_json = user
        
        # Check for row-level security rules
        if not access_control_json:
            # No access control, just return the original query
            return query
            
        access_control = json.loads(access_control_json)
        
        # Check if row-level security is configured
        if 'row_filters' not in access_control:
            # No row-level security, just return the original query
            return query
            
        row_filters = access_control['row_filters']
        
        # Check for user-specific filters
        user_filter = None
        if username in row_filters.get('users', {}):
            user_filter = row_filters['users'][username]
            
        # Check for role-based filters
        role_filter = None
        if role in row_filters.get('roles', {}):
            role_filter = row_filters['roles'][role]
            
        # Apply filters
        where_clauses = []
        
        if user_filter:
            where_clauses.append(user_filter)
            
        if role_filter:
            where_clauses.append(role_filter)
            
        if filters:
            for col, value in filters.items():
                if isinstance(value, (list, tuple)):
                    values_str = ", ".join([f"'{v}'" for v in value])
                    where_clauses.append(f"{col} IN ({values_str})")
                else:
                    where_clauses.append(f"{col} = '{value}'")
        
        if not where_clauses:
            # No filters to apply
            return query
            
        # Modify the query to include the security filters
        security_filter = " AND ".join(where_clauses)
        
        if " WHERE " in query:
            # Add to existing WHERE clause
            return query.replace(" WHERE ", f" WHERE ({security_filter}) AND ")
        else:
            # Add new WHERE clause
            # Check if the query has common clauses after FROM
            for clause in [" GROUP BY ", " ORDER BY ", " LIMIT ", " OFFSET "]:
                if clause in query:
                    parts = query.split(clause, 1)
                    return f"{parts[0]} WHERE {security_filter}{clause}{parts[1]}"
            
            # No clauses, just add WHERE at the end
            return f"{query} WHERE {security_filter}"

def apply_column_level_security(connection_pool, identifier, username, columns=None):
    """
    Apply column-level security for a dataset
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        username: Username
        columns: Requested columns (None for all)
        
    Returns:
        List of allowed columns
    """
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        if identifier.startswith('duck_'):
            # It's an ID
            metadata = conn.execute(
                "SELECT id, name, access_control, schema FROM duck_connect_metadata WHERE id = ?", 
                [identifier]
            ).fetchone()
        else:
            # It's a name
            metadata = conn.execute(
                "SELECT id, name, access_control, schema FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id, name, access_control_json, schema_json = metadata
        
        # Get all columns from schema
        schema = json.loads(schema_json) if schema_json else {}
        all_columns = schema.get('columns', [])
        
        # Get user info
        user = conn.execute(
            "SELECT role, permissions FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            raise ValueError(f"User '{username}' not found")
            
        role, permissions_json = user
        
        # Check for column-level security rules
        if not access_control_json:
            # No access control, allow all columns
            return columns if columns else all_columns
            
        access_control = json.loads(access_control_json)
        
        # Check if column-level security is configured
        if 'column_access' not in access_control:
            # No column-level security, allow all columns
            return columns if columns else all_columns
            
        column_access = access_control['column_access']
        
        # Determine which columns the user is allowed to access
        allowed_columns = set()
        
        # Check role-based access first (broader)
        if role in column_access.get('roles', {}):
            role_cols = column_access['roles'][role]
            if role_cols == '*':
                # Allow all columns
                allowed_columns.update(all_columns)
            else:
                # Allow specific columns
                allowed_columns.update(role_cols)
        
        # Check user-specific access (more specific)
        if username in column_access.get('users', {}):
            user_cols = column_access['users'][username]
            if user_cols == '*':
                # Allow all columns
                allowed_columns = set(all_columns)
            else:
                # Allow specific columns
                allowed_columns.update(user_cols)
        
        # If no column access rules matched, default to no access
        if not allowed_columns:
            # No explicit permission, default to no columns
            logger.warning(f"User '{username}' has no column access to dataset '{name}'")
            return []
        
        # If specific columns were requested, intersect with allowed columns
        if columns:
            filtered_columns = [col for col in columns if col in allowed_columns]
            if len(filtered_columns) < len(columns):
                logger.warning(f"User '{username}' requested some columns they don't have access to")
            return filtered_columns
        
        # Return all allowed columns
        return list(allowed_columns) 