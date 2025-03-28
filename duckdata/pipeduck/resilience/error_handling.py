"""
Error handling for PipeDuck

This module provides functionality for handling errors in pipeline execution.
"""

import os
import time
import json
import logging
import tempfile
import subprocess
from typing import Dict, Any, Tuple, Optional, Callable

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck.resilience.error_handling')

class ErrorHandler:
    """
    Handles errors in pipeline execution
    """
    
    def __init__(self, config: Dict[str, Any], global_config: Optional[Dict[str, Any]] = None):
        """
        Initialize error handler
        
        Args:
            config: Error handler configuration
            global_config: Global pipeline configuration
        """
        self.config = config
        self.global_config = global_config or {}
        
        # Set default values
        self.max_retries = config.get('max_retries', 3)
        self.retry_delay = config.get('retry_delay', 60)  # seconds
        self.retry_backoff = config.get('retry_backoff', 2.0)
        self.action = config.get('action', 'retry')  # retry, fail, skip, callback
        self.callback = config.get('callback')
    
    def handle_error(self, node_id: str, error: Exception, retry_count: int) -> Tuple[bool, int]:
        """
        Handle a node error based on error handler configuration
        
        Args:
            node_id: Node ID that failed
            error: Exception that occurred
            retry_count: Current retry count
            
        Returns:
            Tuple of (should_retry, new_retry_count)
        """
        # Use different strategies based on the action
        if self.action == 'fail':
            # Immediately fail the pipeline
            logger.error(f"Node '{node_id}' failed, configured to fail pipeline: {error}")
            return False, retry_count
            
        elif self.action == 'skip':
            # Skip the node and continue
            logger.warning(f"Node '{node_id}' failed but configured to skip: {error}")
            return False, retry_count
            
        elif self.action == 'retry':
            # Retry with configured parameters
            if retry_count < self.max_retries:
                retry_delay = self.retry_delay * (self.retry_backoff ** retry_count)
                logger.info(f"Node '{node_id}' failed, retrying in {retry_delay:.1f}s (attempt {retry_count + 1}/{self.max_retries})")
                time.sleep(retry_delay)
                return True, retry_count + 1
            else:
                logger.error(f"Node '{node_id}' failed after {retry_count} retries")
                return False, retry_count
                
        elif self.action == 'callback':
            # Execute a callback function or script
            if self.callback:
                return self._execute_callback(node_id, error, retry_count)
            else:
                logger.warning(f"Callback action specified but no callback provided for node '{node_id}'")
                return self._default_retry(node_id, error, retry_count)
        else:
            # Unknown action, use default behavior
            logger.warning(f"Unknown error handler action '{self.action}' for node '{node_id}', using default behavior")
            return self._default_retry(node_id, error, retry_count)
    
    def _execute_callback(self, node_id: str, error: Exception, retry_count: int) -> Tuple[bool, int]:
        """
        Execute a callback script for error handling
        
        Args:
            node_id: Node ID that failed
            error: Exception that occurred
            retry_count: Current retry count
            
        Returns:
            Tuple of (should_retry, new_retry_count)
        """
        logger.info(f"Executing error callback for node '{node_id}'")
        
        # Create error details
        error_details = {
            'node_id': node_id,
            'error': str(error),
            'retry_count': retry_count,
            'timestamp': time.time(),
            'max_retries': self.max_retries
        }
        
        # Write to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(error_details, f, indent=2)
            error_file = f.name
        
        # Execute callback
        try:
            result = subprocess.run(
                [self.callback, error_file],
                capture_output=True,
                text=True,
                check=False
            )
            
            if result.returncode == 0:
                # Callback successful, check result
                try:
                    callback_result = json.loads(result.stdout)
                    if callback_result.get('retry', False):
                        # Callback says to retry
                        retry_delay = callback_result.get('delay', self.retry_delay)
                        logger.info(f"Error callback for node '{node_id}' requests retry in {retry_delay}s")
                        time.sleep(retry_delay)
                        return True, retry_count + 1
                except:
                    pass
        except Exception as e:
            logger.error(f"Error executing callback for node '{node_id}': {e}")
        
        # Clean up temp file
        try:
            os.remove(error_file)
        except:
            pass
        
        # Default to not retry if callback doesn't specify or fails
        return False, retry_count
    
    def _default_retry(self, node_id: str, error: Exception, retry_count: int) -> Tuple[bool, int]:
        """
        Default retry behavior
        
        Args:
            node_id: Node ID that failed
            error: Exception that occurred
            retry_count: Current retry count
            
        Returns:
            Tuple of (should_retry, new_retry_count)
        """
        if retry_count < self.max_retries:
            delay = self.retry_delay * (self.retry_backoff ** retry_count)
            logger.info(f"Node '{node_id}' failed, retrying in {delay:.1f}s (attempt {retry_count + 1}/{self.max_retries})")
            time.sleep(delay)
            return True, retry_count + 1
        else:
            logger.error(f"Node '{node_id}' failed after {retry_count} retries")
            return False, retry_count

class ErrorHandlerRegistry:
    """
    Registry for error handlers
    """
    
    def __init__(self, pipeline_config: Dict[str, Any]):
        """
        Initialize error handler registry
        
        Args:
            pipeline_config: Pipeline configuration
        """
        self.pipeline_config = pipeline_config
        self.error_handlers = {}
        self._setup_error_handlers()
    
    def _setup_error_handlers(self):
        """Setup error handlers for nodes"""
        # Global error handler
        if 'error_handler' in self.pipeline_config:
            handler = self.pipeline_config['error_handler']
            if isinstance(handler, dict):
                self.error_handlers['_global'] = ErrorHandler(handler, self.pipeline_config)
                
        # Node-specific error handlers
        for node in self.pipeline_config.get('nodes', []):
            if 'error_handler' in node:
                handler = node['error_handler']
                if isinstance(handler, dict):
                    self.error_handlers[node['id']] = ErrorHandler(handler, self.pipeline_config)
    
    def get_error_handler(self, node_id: str) -> ErrorHandler:
        """
        Get error handler for a node
        
        Args:
            node_id: Node ID
            
        Returns:
            Error handler for the node, or global handler if none exists
        """
        if node_id in self.error_handlers:
            return self.error_handlers[node_id]
        elif '_global' in self.error_handlers:
            return self.error_handlers['_global']
        else:
            # Create a default error handler
            return ErrorHandler({
                'max_retries': self.pipeline_config.get('max_retries', 3),
                'retry_delay': self.pipeline_config.get('retry_delay', 60),
                'retry_backoff': self.pipeline_config.get('retry_backoff', 2.0),
                'action': 'retry'
            }, self.pipeline_config)
            
    def handle_error(self, node_id: str, error: Exception, retry_count: int) -> Tuple[bool, int]:
        """
        Handle a node error
        
        Args:
            node_id: Node ID that failed
            error: Exception that occurred
            retry_count: Current retry count
            
        Returns:
            Tuple of (should_retry, new_retry_count)
        """
        handler = self.get_error_handler(node_id)
        return handler.handle_error(node_id, error, retry_count) 