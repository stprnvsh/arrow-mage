"""
Authentication module for DuckConnect

This module provides user management and authentication functionality.
"""

import os
import json
import logging
import hashlib

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.security.auth')

def add_user(connection_pool, username, password, role="user", permissions=None):
    """
    Add a user for authentication and authorization
    
    Args:
        connection_pool: Connection pool
        username: Username
        password: Password (will be hashed)
        role: Role (admin, user, etc.)
        permissions: Dictionary of permissions
        
    Returns:
        True if successful
    """
    # Hash the password with salt
    salt = os.urandom(32)
    password_hash = hashlib.pbkdf2_hmac(
        'sha256', 
        password.encode('utf-8'), 
        salt, 
        100000
    )
    
    # Store salt:hash
    hash_str = salt.hex() + ':' + password_hash.hex()
    
    with connection_pool.get_connection() as conn:
        # Check if user exists
        existing = conn.execute(
            "SELECT username FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if existing:
            raise ValueError(f"User '{username}' already exists")
            
        # Insert user
        conn.execute("""
        INSERT INTO duck_connect_users (
            username, password_hash, role, permissions, created_at, last_login
        ) VALUES (?, ?, ?, ?, datetime('now'), NULL)
        """, [
            username, 
            hash_str, 
            role, 
            json.dumps(permissions) if permissions else '{}'
        ])
        
        logger.info(f"Added user '{username}' with role '{role}'")
        return True

def authenticate(connection_pool, username, password):
    """
    Authenticate a user
    
    Args:
        connection_pool: Connection pool
        username: Username
        password: Password
        
    Returns:
        True if authentication successful, False otherwise
    """
    with connection_pool.get_connection() as conn:
        # Get user
        user = conn.execute(
            "SELECT password_hash FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            logger.warning(f"Authentication failed: User '{username}' not found")
            return False
            
        # Verify password
        stored_hash = user[0]
        salt_hex, hash_hex = stored_hash.split(':')
        salt = bytes.fromhex(salt_hex)
        stored_password_hash = bytes.fromhex(hash_hex)
        
        # Hash the provided password with the same salt
        password_hash = hashlib.pbkdf2_hmac(
            'sha256', 
            password.encode('utf-8'), 
            salt, 
            100000
        )
        
        # Compare hashes
        if password_hash == stored_password_hash:
            # Update last login
            conn.execute(
                "UPDATE duck_connect_users SET last_login = datetime('now') WHERE username = ?",
                [username]
            )
            logger.info(f"User '{username}' authenticated successfully")
            return True
        else:
            logger.warning(f"Authentication failed: Invalid password for user '{username}'")
            return False

def get_user_permissions(connection_pool, username):
    """
    Get permissions for a user
    
    Args:
        connection_pool: Connection pool
        username: Username
        
    Returns:
        Dictionary of permissions or None if user not found
    """
    with connection_pool.get_connection() as conn:
        # Get user
        user = conn.execute(
            "SELECT role, permissions FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            return None
            
        role, permissions_json = user
        
        # Parse permissions
        permissions = json.loads(permissions_json) if permissions_json else {}
        
        # Add role information
        permissions['role'] = role
        
        return permissions

def update_user_permissions(connection_pool, username, permissions):
    """
    Update permissions for a user
    
    Args:
        connection_pool: Connection pool
        username: Username
        permissions: Dictionary of permissions
        
    Returns:
        True if successful, False if user not found
    """
    with connection_pool.get_connection() as conn:
        # Check if user exists
        user = conn.execute(
            "SELECT username FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            return False
            
        # Update permissions
        conn.execute(
            "UPDATE duck_connect_users SET permissions = ? WHERE username = ?",
            [json.dumps(permissions), username]
        )
        
        logger.info(f"Updated permissions for user '{username}'")
        return True

def update_user_role(connection_pool, username, role):
    """
    Update role for a user
    
    Args:
        connection_pool: Connection pool
        username: Username
        role: New role
        
    Returns:
        True if successful, False if user not found
    """
    with connection_pool.get_connection() as conn:
        # Check if user exists
        user = conn.execute(
            "SELECT username FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            return False
            
        # Update role
        conn.execute(
            "UPDATE duck_connect_users SET role = ? WHERE username = ?",
            [role, username]
        )
        
        logger.info(f"Updated role for user '{username}' to '{role}'")
        return True

def delete_user(connection_pool, username):
    """
    Delete a user
    
    Args:
        connection_pool: Connection pool
        username: Username
        
    Returns:
        True if successful, False if user not found
    """
    with connection_pool.get_connection() as conn:
        # Check if user exists
        user = conn.execute(
            "SELECT username FROM duck_connect_users WHERE username = ?",
            [username]
        ).fetchone()
        
        if not user:
            return False
            
        # Delete user
        conn.execute(
            "DELETE FROM duck_connect_users WHERE username = ?",
            [username]
        )
        
        logger.info(f"Deleted user '{username}'")
        return True 