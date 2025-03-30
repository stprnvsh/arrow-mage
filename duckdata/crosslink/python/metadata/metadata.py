"""
Metadata management module for CrossLink
"""
import json
import uuid
import hashlib
from datetime import datetime

class CrossLinkMetadataManager:
    """Metadata management for cross-language data sharing"""
    
    def __init__(self, conn):
        """Initialize the metadata manager
        
        Args:
            conn: DuckDB connection
        """
        self.conn = conn
        self._setup_metadata_tables()
    
    def _setup_metadata_tables(self):
        """Set up metadata tables for efficient cross-language data sharing"""
        # Main metadata table for datasets
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_metadata (
            id TEXT PRIMARY KEY,
            name TEXT,
            source_language TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            description TEXT,
            schema TEXT,
            table_name TEXT,
            arrow_data BOOLEAN DEFAULT FALSE,
            version INTEGER DEFAULT 1,
            current_version BOOLEAN DEFAULT TRUE,
            lineage TEXT,
            schema_hash TEXT,
            memory_map_path TEXT,         -- Path to memory-mapped file for zero-copy access
            shared_memory_key TEXT,       -- Key for shared memory segment
            arrow_schema TEXT,            -- Serialized Arrow schema for zero-copy
            access_languages TEXT,        -- JSON array of languages that can access this dataset
            memory_layout TEXT            -- Memory layout information for zero-copy access
        )
        """)
        
        # Schema history table for tracking schema evolution
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_schema_history (
            id TEXT,
            version INTEGER,
            schema TEXT,
            schema_hash TEXT,
            changed_at TIMESTAMP,
            change_type TEXT,
            changes TEXT,
            PRIMARY KEY (id, version)
        )
        """)
        
        # Lineage table for tracking data provenance
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_lineage (
            dataset_id TEXT,
            source_dataset_id TEXT,
            source_dataset_version INTEGER,
            transformation TEXT,
            created_at TIMESTAMP,
            PRIMARY KEY (dataset_id, source_dataset_id)
        )
        """)
        
        # Cross-language access tracking
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS crosslink_access_log (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            language TEXT,
            operation TEXT,
            timestamp TIMESTAMP,
            access_method TEXT,       -- 'copy', 'zero-copy', 'view'
            success BOOLEAN,
            error_message TEXT
        )
        """)
        
        # Add missing columns to existing databases for backward compatibility
        self._ensure_columns_exist()
    
    def _ensure_columns_exist(self):
        """Ensure all required columns exist in metadata table"""
        columns_to_check = [
            ("memory_map_path", "TEXT"),
            ("shared_memory_key", "TEXT"),
            ("arrow_schema", "TEXT"),
            ("access_languages", "TEXT"),
            ("memory_layout", "TEXT")
        ]
        
        for col_name, col_type in columns_to_check:
            result = self.conn.execute(f"""
            SELECT count(*) as col_exists FROM pragma_table_info('crosslink_metadata') 
            WHERE name = '{col_name}'
            """).fetchone()
            
            if result[0] == 0:
                self.conn.execute(f"""
                ALTER TABLE crosslink_metadata ADD COLUMN {col_name} {col_type}
                """)
    
    def create_dataset_metadata(self, dataset_id, name, table_name, source_language, schema,
                              description=None, arrow_data=False, memory_map_path=None,
                              shared_memory_key=None, arrow_schema=None, access_languages=None,
                              memory_layout=None):
        """Create metadata for a new dataset
        
        Args:
            dataset_id: Unique ID for the dataset
            name: Human-readable name
            table_name: Table name in DuckDB
            source_language: Language that created the dataset
            schema: Schema as a dictionary
            description: Optional dataset description
            arrow_data: Whether data is in Arrow format
            memory_map_path: Path to memory-mapped file for zero-copy
            shared_memory_key: Key for shared memory segment
            arrow_schema: Serialized Arrow schema
            access_languages: Languages that can access this dataset
            memory_layout: Memory layout information for zero-copy access
            
        Returns:
            True if successful
        """
        # Set defaults
        if access_languages is None:
            access_languages = ["python", "r", "julia", "cpp"]
        
        schema_hash = hashlib.md5(json.dumps(schema, sort_keys=True).encode()).hexdigest()
        
        # Insert metadata
        self.conn.execute("""
        INSERT INTO crosslink_metadata (
            id, name, source_language, created_at, updated_at, description,
            schema, table_name, arrow_data, version, current_version,
            schema_hash, memory_map_path, shared_memory_key, arrow_schema,
            access_languages, memory_layout
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dataset_id, name, source_language, datetime.now(), datetime.now(),
            description or "", json.dumps(schema), table_name, arrow_data, 1, True,
            schema_hash, memory_map_path, shared_memory_key,
            json.dumps(arrow_schema) if arrow_schema else None,
            json.dumps(access_languages), json.dumps(memory_layout) if memory_layout else None
        ])
        
        return True
    
    def get_dataset_metadata(self, identifier):
        """Get metadata for a dataset
        
        Args:
            identifier: Dataset ID or name
            
        Returns:
            Dictionary of metadata or None if not found
        """
        # Check if identifier is an ID or name
        result = self.conn.execute("""
        SELECT * FROM crosslink_metadata 
        WHERE id = ? OR name = ?
        """, [identifier, identifier]).fetchone()
        
        if not result:
            return None
            
        # Convert result to dict
        columns = [col[0] for col in self.conn.description]
        metadata = {columns[i]: result[i] for i in range(len(columns))}
        
        # Parse JSON fields
        for key in ['schema', 'lineage', 'arrow_schema', 'access_languages', 'memory_layout']:
            if metadata.get(key):
                try:
                    metadata[key] = json.loads(metadata[key])
                except:
                    pass
                    
        return metadata
    
    def update_dataset_metadata(self, dataset_id, **updates):
        """Update metadata for an existing dataset
        
        Args:
            dataset_id: Dataset ID
            **updates: Fields to update
            
        Returns:
            True if successful
        """
        # Convert dict values to JSON
        for key, value in updates.items():
            if key in ['schema', 'arrow_schema', 'access_languages', 'memory_layout'] and value is not None:
                updates[key] = json.dumps(value)
        
        # Always update the updated_at timestamp
        updates['updated_at'] = datetime.now()
        
        # Build SQL update statement
        set_clause = ", ".join([f"{key} = ?" for key in updates.keys()])
        values = list(updates.values()) + [dataset_id]
        
        # Update metadata
        self.conn.execute(f"""
        UPDATE crosslink_metadata 
        SET {set_clause}
        WHERE id = ?
        """, values)
        
        return True
    
    def log_access(self, dataset_id, language, operation, access_method, success=True, error_message=None):
        """Log cross-language access to a dataset
        
        Args:
            dataset_id: Dataset ID
            language: Language accessing the dataset
            operation: Operation (read, write, etc.)
            access_method: Method of access (copy, zero-copy, view)
            success: Whether access was successful
            error_message: Error message if access failed
            
        Returns:
            True if log was created
        """
        log_id = str(uuid.uuid4())
        
        self.conn.execute("""
        INSERT INTO crosslink_access_log (
            id, dataset_id, language, operation, timestamp, 
            access_method, success, error_message
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            log_id, dataset_id, language, operation, datetime.now(),
            access_method, success, error_message
        ])
        
        return True
    
    def list_datasets(self):
        """List all datasets in the database
        
        Returns:
            List of dataset metadata dictionaries
        """
        # Get all datasets
        results = self.conn.execute("""
        SELECT * FROM crosslink_metadata
        WHERE current_version = True
        ORDER BY name
        """).fetchall()
        
        if not results:
            return []
            
        # Convert results to list of dicts
        columns = [col[0] for col in self.conn.description]
        datasets = []
        
        for result in results:
            metadata = {columns[i]: result[i] for i in range(len(columns))}
            
            # Parse JSON fields
            for key in ['schema', 'lineage', 'arrow_schema', 'access_languages', 'memory_layout']:
                if metadata.get(key):
                    try:
                        metadata[key] = json.loads(metadata[key])
                    except:
                        pass
                        
            datasets.append(metadata)
                    
        return datasets 