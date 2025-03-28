"""
Encryption module for DuckConnect

This module provides data encryption functionality for protecting sensitive data.
"""

import base64
import json
import logging
import pandas as pd
from typing import Union, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.security.encryption')

def encrypt_dataset(connection_pool, metadata_manager, lock_manager, 
                  identifier, encryption_key, language="python"):
    """
    Encrypt a dataset
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        lock_manager: Lock manager
        identifier: Dataset ID or name
        encryption_key: Encryption key
        language: Language performing the operation
        
    Returns:
        ID of the encrypted dataset
    """
    try:
        from cryptography.fernet import Fernet
        import hashlib
    except ImportError:
        raise ImportError("cryptography package is required for encryption")
    
    # Derive a valid Fernet key from the provided key
    key_bytes = hashlib.sha256(encryption_key.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    cipher = Fernet(fernet_key)
    
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        metadata = metadata_manager.get_dataset_metadata(identifier)
        
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id = metadata['id']
        name = metadata['name']
        table_name = metadata['table_name']
        
        # Acquire exclusive lock
        if not lock_manager.acquire_lock(dataset_id, lock_type='exclusive'):
            raise TimeoutError(f"Failed to acquire exclusive lock on dataset {name}")
        
        try:
            # Get data
            data_df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Create encrypted table name
            encrypted_table = f"{table_name}_encrypted"
            
            # For string columns, encrypt the values
            # For numeric columns, leave as-is or apply transformation
            for col in data_df.columns:
                if data_df[col].dtype == 'object':
                    # Encrypt string columns
                    data_df[col] = data_df[col].apply(
                        lambda x: cipher.encrypt(str(x).encode()).decode() if x is not None else None
                    )
            
            # Store encrypted data
            conn.execute(f"DROP TABLE IF EXISTS {encrypted_table}")
            conn.execute(f"CREATE TABLE {encrypted_table} AS SELECT * FROM data_df", {
                "data_df": data_df
            })
            
            # Clone metadata and mark as encrypted
            schema = metadata.get('schema', {})
            schema['encrypted'] = True
            
            encrypted_id = f"duck_{dataset_id.split('_')[1]}_encrypted"
            encrypted_name = f"{name}_encrypted"
            
            # Store encrypted metadata
            metadata_manager.create_dataset_metadata(
                encrypted_id,
                encrypted_name,
                encrypted_table,
                language,
                schema,
                description=f"{metadata.get('description', '')} (encrypted)",
                available_to_languages=metadata.get('available_to_languages'),
                owner=metadata.get('owner'),
                is_encrypted=True,
                access_control=metadata.get('access_control'),
                partition_info=None  # Encrypted datasets don't preserve partitioning
            )
            
            # Record transformation
            metadata_manager.record_lineage(
                encrypted_id,
                dataset_id,
                'encryption',
                language,
                '1.0'
            )
            
            # Record transaction
            metadata_manager.record_transaction(
                dataset_id,
                'encrypt',
                language,
                {'encrypted_id': encrypted_id}
            )
            
            return encrypted_id
            
        finally:
            # Release lock
            lock_manager.release_lock(dataset_id)

def decrypt_dataset(connection_pool, metadata_manager, lock_manager,
                  identifier, encryption_key, language="python") -> Union[pd.DataFrame, None]:
    """
    Decrypt a dataset
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        lock_manager: Lock manager
        identifier: Dataset ID or name
        encryption_key: Encryption key
        language: Language performing the operation
        
    Returns:
        Decrypted DataFrame or None if not encrypted
    """
    try:
        from cryptography.fernet import Fernet
        import hashlib
    except ImportError:
        raise ImportError("cryptography package is required for encryption")
    
    # Derive a valid Fernet key from the provided key
    key_bytes = hashlib.sha256(encryption_key.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    cipher = Fernet(fernet_key)
    
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        metadata = metadata_manager.get_dataset_metadata(identifier)
        
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id = metadata['id']
        name = metadata['name']
        table_name = metadata['table_name']
        is_encrypted = metadata.get('is_encrypted', False)
        
        if not is_encrypted:
            logger.warning(f"Dataset '{name}' is not encrypted")
            return None
        
        # Acquire read lock
        lock_manager.acquire_lock(dataset_id, lock_type='read')
        
        try:
            # Get encrypted data
            data_df = conn.execute(f"SELECT * FROM {table_name}").fetchdf()
            
            # Parse schema to know which columns are encrypted
            schema = metadata.get('schema', {})
            
            # For string columns, decrypt the values
            for col in data_df.columns:
                if data_df[col].dtype == 'object':
                    # Decrypt string columns
                    try:
                        data_df[col] = data_df[col].apply(
                            lambda x: cipher.decrypt(x.encode()).decode() if x is not None else None
                        )
                    except Exception as e:
                        logger.error(f"Failed to decrypt column {col}: {e}")
                        # If decryption fails, the key might be wrong
                        raise ValueError("Decryption failed. Invalid encryption key.")
            
            # Record transaction
            metadata_manager.record_transaction(
                dataset_id,
                'decrypt',
                language,
                {'temp_decryption': True}
            )
            
            return data_df
            
        finally:
            # Release lock
            lock_manager.release_lock(dataset_id)

def register_encrypted_dataset(connection_pool, metadata_manager, transaction_manager,
                             data, name, encryption_key, **kwargs):
    """
    Register an encrypted dataset directly (encrypt during registration)
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        transaction_manager: Transaction manager
        data: Data to encrypt and register
        name: Dataset name
        encryption_key: Encryption key
        **kwargs: Additional arguments for register_dataset
        
    Returns:
        Dataset ID
    """
    from duck_connect.datasets.registry import register_dataset
    
    try:
        from cryptography.fernet import Fernet
        import hashlib
    except ImportError:
        raise ImportError("cryptography package is required for encryption")
    
    # Derive a valid Fernet key from the provided key
    key_bytes = hashlib.sha256(encryption_key.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    cipher = Fernet(fernet_key)
    
    # Deep copy dataframe to avoid modifying the original
    import copy
    if isinstance(data, pd.DataFrame):
        encrypted_data = data.copy()
        
        # Encrypt string columns
        for col in encrypted_data.columns:
            if encrypted_data[col].dtype == 'object':
                encrypted_data[col] = encrypted_data[col].apply(
                    lambda x: cipher.encrypt(str(x).encode()).decode() if x is not None else None
                )
        
        # Register encrypted dataset
        return register_dataset(
            connection_pool,
            metadata_manager,
            transaction_manager,
            encrypted_data,
            name,
            is_encrypted=True,
            **kwargs
        )
    else:
        # For Arrow tables or other types, first register normally then encrypt
        dataset_id = register_dataset(
            connection_pool,
            metadata_manager,
            transaction_manager,
            data,
            name,
            **kwargs
        )
        
        # Get the language from kwargs or default to python
        language = kwargs.get('source_language', 'python')
        
        # Now encrypt the dataset
        from duck_connect.core.transactions import LockManager
        lock_manager = LockManager(connection_pool)
        
        encrypted_id = encrypt_dataset(
            connection_pool,
            metadata_manager,
            lock_manager,
            dataset_id,
            encryption_key,
            language
        )
        
        return encrypted_id 