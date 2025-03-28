"""
Metadata management for DuckConnect

This module provides functionality for managing metadata about datasets,
including schemas, tables, and versioning.
"""

import json
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.metadata')

class MetadataManager:
    """
    Manages metadata for datasets, schemas, and tables
    """
    
    def __init__(self, connection_pool):
        """
        Initialize the metadata manager
        
        Args:
            connection_pool: Connection pool to use for metadata operations
        """
        self.connection_pool = connection_pool
        
        # Create metadata tables if they don't exist
        with self.connection_pool.get_connection() as conn:
            self._setup_metadata_tables(conn)
    
    def _setup_metadata_tables(self, conn):
        """
        Set up metadata tables
        
        Args:
            conn: DuckDB connection
        """
        # Create metadata tables for tracking data
        conn.execute("""
        CREATE TABLE IF NOT EXISTS duck_connect_metadata (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE,
            source_language TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            description TEXT,
            schema TEXT,  -- JSON schema information
            table_name TEXT,  -- Actual DuckDB table name
            available_to_languages TEXT,  -- JSON array of languages
            owner TEXT, -- Owner of the dataset
            is_encrypted BOOLEAN DEFAULT FALSE,
            access_control TEXT, -- JSON for row/column access control
            data_quality_metrics TEXT, -- JSON for data quality stats
            partition_info TEXT -- JSON for partition information
        )
        """)
        
        # Create transactions table to track language operations
        conn.execute("""
        CREATE TABLE IF NOT EXISTS duck_connect_transactions (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            operation TEXT,  -- 'create', 'read', 'update', 'delete'
            language TEXT,
            timestamp TIMESTAMP,
            details TEXT  -- JSON additional details
        )
        """)
        
        # Create lineage table for tracking data transformations
        conn.execute("""
        CREATE TABLE IF NOT EXISTS duck_connect_lineage (
            id TEXT PRIMARY KEY,
            target_dataset_id TEXT,
            source_dataset_id TEXT,
            transformation TEXT,
            language TEXT,
            timestamp TIMESTAMP,
            version TEXT -- For version history tracking
        )
        """)
        
        # Create statistics table for performance tracking
        conn.execute("""
        CREATE TABLE IF NOT EXISTS duck_connect_stats (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            operation TEXT,
            language TEXT,
            timestamp TIMESTAMP,
            duration_ms FLOAT,
            memory_usage_mb FLOAT,
            row_count INTEGER,
            column_count INTEGER
        )
        """)
        
    def create_dataset_metadata(self, dataset_id, name, table_name, source_language, schema,
                              description=None, available_to_languages=None, owner=None,
                              is_encrypted=False, access_control=None, partition_info=None):
        """
        Create metadata for a new dataset
        
        Args:
            dataset_id: Dataset ID
            name: Dataset name
            table_name: Name of the DuckDB table
            source_language: Source language
            schema: Schema information (as dict)
            description: Optional description
            available_to_languages: Languages that can access this dataset
            owner: Owner of the dataset
            is_encrypted: Whether the dataset is encrypted
            access_control: Access control rules
            partition_info: Partitioning information
            
        Returns:
            True if successful
        """
        # Set default available languages if not specified
        if available_to_languages is None:
            available_to_languages = ["python", "r", "julia"]
        
        # Create metadata record
        metadata = {
            'id': dataset_id,
            'name': name,
            'source_language': source_language,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat(),
            'description': description or '',
            'schema': json.dumps(schema),
            'table_name': table_name,
            'available_to_languages': json.dumps(available_to_languages),
            'owner': owner,
            'is_encrypted': is_encrypted,
            'access_control': json.dumps(access_control) if access_control else None,
            'data_quality_metrics': None,
            'partition_info': json.dumps(partition_info) if partition_info else None
        }
        
        # Insert metadata
        with self.connection_pool.get_connection() as conn:
            placeholders = ", ".join(["?"] * len(metadata))
            columns = ", ".join(metadata.keys())
            
            conn.execute(f"""
            INSERT INTO duck_connect_metadata ({columns})
            VALUES ({placeholders})
            """, list(metadata.values()))
            
        return True
    
    def update_dataset_metadata(self, dataset_id, **updates):
        """
        Update metadata for an existing dataset
        
        Args:
            dataset_id: Dataset ID
            **updates: Key-value pairs to update
            
        Returns:
            True if successful
        """
        # Convert dict values to JSON
        for key, value in updates.items():
            if key in ['schema', 'available_to_languages', 'access_control', 'partition_info'] and value is not None:
                updates[key] = json.dumps(value)
        
        # Always update the updated_at timestamp
        updates['updated_at'] = datetime.now().isoformat()
        
        # Build SQL
        set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
        
        # Update metadata
        with self.connection_pool.get_connection() as conn:
            conn.execute(f"""
            UPDATE duck_connect_metadata 
            SET {set_clause}
            WHERE id = ?
            """, list(updates.values()) + [dataset_id])
            
        return True
    
    def get_dataset_metadata(self, identifier):
        """
        Get metadata for a dataset
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            Dictionary of metadata or None if not found
        """
        with self.connection_pool.get_connection() as conn:
            # Check if identifier is an ID or name
            if identifier.startswith('duck_'):
                # It's an ID
                result = conn.execute(
                    "SELECT * FROM duck_connect_metadata WHERE id = ?", 
                    [identifier]
                ).fetchone()
            else:
                # It's a name
                result = conn.execute(
                    "SELECT * FROM duck_connect_metadata WHERE name = ?", 
                    [identifier]
                ).fetchone()
                
            if not result:
                return None
                
            # Convert result to dict using column names
            columns = [col[0] for col in conn.description]
            metadata = {columns[i]: result[i] for i in range(len(columns))}
            
            # Parse JSON fields
            for key in ['schema', 'available_to_languages', 'access_control', 'partition_info', 'data_quality_metrics']:
                if metadata.get(key):
                    try:
                        metadata[key] = json.loads(metadata[key])
                    except:
                        pass
                        
            return metadata
    
    def delete_dataset_metadata(self, dataset_id):
        """
        Delete metadata for a dataset
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            True if successful
        """
        with self.connection_pool.get_connection() as conn:
            conn.execute(
                "DELETE FROM duck_connect_metadata WHERE id = ?",
                [dataset_id]
            )
            
        return True
    
    def record_transaction(self, dataset_id, operation, language, details=None):
        """
        Record a transaction for a dataset
        
        Args:
            dataset_id: Dataset ID
            operation: Operation performed ('create', 'read', 'update', 'delete')
            language: Language performing the operation
            details: Optional details
            
        Returns:
            Transaction ID
        """
        import uuid
        
        # Generate transaction ID
        transaction_id = str(uuid.uuid4())
        
        # Record transaction
        with self.connection_pool.get_connection() as conn:
            conn.execute("""
            INSERT INTO duck_connect_transactions (
                id, dataset_id, operation, language, timestamp, details
            ) VALUES (?, ?, ?, ?, datetime('now'), ?)
            """, [
                transaction_id, dataset_id, operation, language, 
                json.dumps(details) if details else None
            ])
            
        return transaction_id
    
    def record_lineage(self, target_dataset_id, source_dataset_id, transformation, language, version="1.0"):
        """
        Record lineage information for a dataset
        
        Args:
            target_dataset_id: Target dataset ID
            source_dataset_id: Source dataset ID
            transformation: Transformation description
            language: Language performing the transformation
            version: Version of the transformation
            
        Returns:
            Lineage ID
        """
        import uuid
        
        # Generate lineage ID
        lineage_id = str(uuid.uuid4())
        
        # Record lineage
        with self.connection_pool.get_connection() as conn:
            conn.execute("""
            INSERT INTO duck_connect_lineage (
                id, target_dataset_id, source_dataset_id, transformation, language, timestamp, version
            ) VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
            """, [
                lineage_id, target_dataset_id, source_dataset_id, transformation, language, version
            ])
            
        return lineage_id
    
    def get_dataset_lineage(self, dataset_id):
        """
        Get lineage information for a dataset
        
        Args:
            dataset_id: Dataset ID
            
        Returns:
            Dictionary with upstream and downstream lineage
        """
        with self.connection_pool.get_connection() as conn:
            # Get upstream lineage (datasets this one depends on)
            upstream = conn.execute("""
            SELECT l.*, m.name as source_name
            FROM duck_connect_lineage l
            JOIN duck_connect_metadata m ON l.source_dataset_id = m.id
            WHERE l.target_dataset_id = ?
            """, [dataset_id]).fetchdf()
            
            # Get downstream lineage (datasets that depend on this one)
            downstream = conn.execute("""
            SELECT l.*, m.name as target_name
            FROM duck_connect_lineage l
            JOIN duck_connect_metadata m ON l.target_dataset_id = m.id
            WHERE l.source_dataset_id = ?
            """, [dataset_id]).fetchdf()
            
            return {
                'upstream': upstream.to_dict('records') if not upstream.empty else [],
                'downstream': downstream.to_dict('records') if not downstream.empty else []
            }
    
    def list_datasets(self, language=None):
        """
        List all datasets
        
        Args:
            language: Optional filter by language that can access the datasets
            
        Returns:
            DataFrame with datasets
        """
        import pandas as pd
        
        with self.connection_pool.get_connection() as conn:
            if language:
                # Get all datasets
                datasets = conn.execute("""
                SELECT id, name, source_language, created_at, updated_at, description, 
                       table_name, available_to_languages, owner, is_encrypted
                FROM duck_connect_metadata
                ORDER BY updated_at DESC
                """).fetchdf()
                
                # Filter by language
                filtered_datasets = []
                for _, row in datasets.iterrows():
                    available_languages = json.loads(row['available_to_languages']) if row['available_to_languages'] else []
                    if language in available_languages:
                        filtered_datasets.append(row)
                
                if filtered_datasets:
                    return pd.DataFrame(filtered_datasets)
                
                return pd.DataFrame(columns=datasets.columns)
            else:
                # Get all datasets without filtering
                return conn.execute("""
                SELECT id, name, source_language, created_at, updated_at, description, 
                       table_name, owner, is_encrypted
                FROM duck_connect_metadata
                ORDER BY updated_at DESC
                """).fetchdf() 