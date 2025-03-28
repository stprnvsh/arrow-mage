"""
Schema versioning module for DuckConnect

This module provides functionality for tracking schema changes and evolution.
"""

import json
import uuid
import logging
import deepdiff
from datetime import datetime
from typing import Dict, List, Any, Optional, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.lineage.versioning')

def record_schema_version(connection_pool, metadata_manager, identifier, schema, language="python"):
    """
    Record a new schema version for a dataset
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        identifier: Dataset ID or name
        schema: New schema
        language: Language recording the version
        
    Returns:
        Version ID
    """
    # Get dataset metadata
    metadata = metadata_manager.get_dataset_metadata(identifier)
    
    if not metadata:
        raise ValueError(f"Dataset '{identifier}' not found")
        
    dataset_id = metadata['id']
    
    # Get current schema
    current_schema = metadata.get('schema', {})
    
    # Generate version ID
    version_id = str(uuid.uuid4())
    
    # Calculate differences
    try:
        schema_diff = deepdiff.DeepDiff(current_schema, schema, ignore_order=True)
        diff_json = schema_diff.to_json()
    except ImportError:
        # If deepdiff not available, just use a simple comparison
        schema_diff = {'different': current_schema != schema}
        diff_json = json.dumps(schema_diff)
    
    # Store the schema version information
    with connection_pool.get_connection() as conn:
        # Create schema versions table if not exists
        conn.execute("""
        CREATE TABLE IF NOT EXISTS duck_connect_schema_versions (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            version_number INTEGER,
            schema TEXT,
            prev_version_id TEXT,
            diff TEXT,
            language TEXT,
            timestamp TIMESTAMP,
            comments TEXT
        )
        """)
        
        # Get latest version number
        latest_version = conn.execute("""
        SELECT MAX(version_number) FROM duck_connect_schema_versions
        WHERE dataset_id = ?
        """, [dataset_id]).fetchone()[0]
        
        version_number = (latest_version or 0) + 1
        
        # Get previous version ID
        prev_version_id = conn.execute("""
        SELECT id FROM duck_connect_schema_versions
        WHERE dataset_id = ? AND version_number = ?
        """, [dataset_id, latest_version or 0]).fetchone()
        
        prev_id = prev_version_id[0] if prev_version_id else None
        
        # Record schema version
        conn.execute("""
        INSERT INTO duck_connect_schema_versions (
            id, dataset_id, version_number, schema, prev_version_id, diff,
            language, timestamp, comments
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'), ?)
        """, [
            version_id, dataset_id, version_number, json.dumps(schema),
            prev_id, diff_json, language, f"Schema version {version_number}"
        ])
        
        # Update dataset metadata with new schema
        metadata_manager.update_dataset_metadata(
            dataset_id,
            schema=schema
        )
        
        # Record transaction
        metadata_manager.record_transaction(
            dataset_id,
            'schema_update',
            language,
            {'version_id': version_id, 'version_number': version_number}
        )
        
        return version_id
    
def get_schema_versions(connection_pool, identifier):
    """
    Get schema version history for a dataset
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        
    Returns:
        List of schema versions
    """
    with connection_pool.get_connection() as conn:
        # Get dataset ID if name is provided
        if not identifier.startswith('duck_'):
            # It's a name
            result = conn.execute(
                "SELECT id FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
            if not result:
                raise ValueError(f"Dataset '{identifier}' not found")
                
            dataset_id = result[0]
        else:
            dataset_id = identifier
        
        # Get schema versions
        versions = conn.execute("""
        SELECT id, version_number, schema, prev_version_id, diff, language, timestamp, comments
        FROM duck_connect_schema_versions
        WHERE dataset_id = ?
        ORDER BY version_number DESC
        """, [dataset_id]).fetchdf()
        
        return versions if not versions.empty else []
    
def compare_schemas(connection_pool, identifier, version1, version2=None):
    """
    Compare two schema versions
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        version1: First version number or ID
        version2: Second version number or ID (None for latest)
        
    Returns:
        Difference between schemas
    """
    with connection_pool.get_connection() as conn:
        # Get dataset ID if name is provided
        if not identifier.startswith('duck_'):
            # It's a name
            result = conn.execute(
                "SELECT id FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
            if not result:
                raise ValueError(f"Dataset '{identifier}' not found")
                
            dataset_id = result[0]
        else:
            dataset_id = identifier
            
        # Get first schema version
        if isinstance(version1, int):
            # It's a version number
            schema1_row = conn.execute("""
            SELECT schema FROM duck_connect_schema_versions
            WHERE dataset_id = ? AND version_number = ?
            """, [dataset_id, version1]).fetchone()
        else:
            # It's a version ID
            schema1_row = conn.execute("""
            SELECT schema FROM duck_connect_schema_versions
            WHERE id = ?
            """, [version1]).fetchone()
            
        if not schema1_row:
            raise ValueError(f"Schema version {version1} not found")
            
        schema1 = json.loads(schema1_row[0])
        
        # Get second schema version
        if version2 is None:
            # Use latest version
            schema2_row = conn.execute("""
            SELECT schema FROM duck_connect_schema_versions
            WHERE dataset_id = ?
            ORDER BY version_number DESC
            LIMIT 1
            """, [dataset_id]).fetchone()
        elif isinstance(version2, int):
            # It's a version number
            schema2_row = conn.execute("""
            SELECT schema FROM duck_connect_schema_versions
            WHERE dataset_id = ? AND version_number = ?
            """, [dataset_id, version2]).fetchone()
        else:
            # It's a version ID
            schema2_row = conn.execute("""
            SELECT schema FROM duck_connect_schema_versions
            WHERE id = ?
            """, [version2]).fetchone()
            
        if not schema2_row:
            raise ValueError(f"Schema version {version2} not found")
            
        schema2 = json.loads(schema2_row[0])
        
        # Compare schemas
        try:
            schema_diff = deepdiff.DeepDiff(schema1, schema2, ignore_order=True)
            return schema_diff
        except ImportError:
            # If deepdiff not available, try a simple comparison
            return {
                'columns_added': list(set(schema2.get('columns', [])) - set(schema1.get('columns', []))),
                'columns_removed': list(set(schema1.get('columns', [])) - set(schema2.get('columns', []))),
                'schemas_identical': schema1 == schema2
            }
    
def revert_to_schema_version(connection_pool, metadata_manager, identifier, version):
    """
    Revert a dataset's schema to a previous version
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        identifier: Dataset ID or name
        version: Version number or ID to revert to
        
    Returns:
        True if successful
    """
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        metadata = metadata_manager.get_dataset_metadata(identifier)
        
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id = metadata['id']
        
        # Get target schema version
        if isinstance(version, int):
            # It's a version number
            schema_row = conn.execute("""
            SELECT schema FROM duck_connect_schema_versions
            WHERE dataset_id = ? AND version_number = ?
            """, [dataset_id, version]).fetchone()
        else:
            # It's a version ID
            schema_row = conn.execute("""
            SELECT schema FROM duck_connect_schema_versions
            WHERE id = ?
            """, [version]).fetchone()
            
        if not schema_row:
            raise ValueError(f"Schema version {version} not found")
            
        target_schema = json.loads(schema_row[0])
        
        # Update dataset metadata with reverted schema
        metadata_manager.update_dataset_metadata(
            dataset_id,
            schema=target_schema
        )
        
        # Record transaction
        metadata_manager.record_transaction(
            dataset_id,
            'schema_revert',
            'python',
            {'target_version': version}
        )
        
        return True
        
def generate_schema_migration(connection_pool, identifier, version1, version2=None):
    """
    Generate schema migration steps between two versions
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        version1: From version
        version2: To version (None for latest)
        
    Returns:
        Dictionary with migration steps
    """
    # Get differences between schemas
    diff = compare_schemas(connection_pool, identifier, version1, version2)
    
    # Generate migration steps
    migration = {
        'from_version': version1,
        'to_version': version2,
        'steps': []
    }
    
    # If using deepdiff
    if hasattr(diff, 'to_dict'):
        diff_dict = diff.to_dict()
        
        # Handle added columns
        if 'dictionary_item_added' in diff_dict:
            for item in diff_dict['dictionary_item_added']:
                if item.startswith("root['columns']"):
                    # Extract column name from path
                    col_match = re.search(r"\['([^']+)'\]$", item)
                    if col_match:
                        col_name = col_match.group(1)
                        migration['steps'].append({
                            'action': 'add_column',
                            'column': col_name
                        })
        
        # Handle removed columns
        if 'dictionary_item_removed' in diff_dict:
            for item in diff_dict['dictionary_item_removed']:
                if item.startswith("root['columns']"):
                    # Extract column name from path
                    col_match = re.search(r"\['([^']+)'\]$", item)
                    if col_match:
                        col_name = col_match.group(1)
                        migration['steps'].append({
                            'action': 'drop_column',
                            'column': col_name
                        })
        
        # Handle changed column types
        if 'type_changes' in diff_dict:
            for path, change in diff_dict['type_changes'].items():
                if path.startswith("root['dtypes']"):
                    col_match = re.search(r"\['([^']+)'\]$", path)
                    if col_match:
                        col_name = col_match.group(1)
                        migration['steps'].append({
                            'action': 'alter_column_type',
                            'column': col_name,
                            'from_type': change['old_type'],
                            'to_type': change['new_type']
                        })
    else:
        # Using simple comparison
        for col in diff.get('columns_added', []):
            migration['steps'].append({
                'action': 'add_column',
                'column': col
            })
            
        for col in diff.get('columns_removed', []):
            migration['steps'].append({
                'action': 'drop_column',
                'column': col
            })
    
    return migration 