"""
Checkpointing for PipeDuck

This module provides functionality for checkpointing pipeline state to allow for
resuming execution after failures.
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Any, Set, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck.resilience.checkpointing')

class CheckpointManager:
    """
    Manages checkpoints for pipeline execution
    """
    
    def __init__(self, checkpoint_dir: str, pipeline_id: str):
        """
        Initialize checkpoint manager
        
        Args:
            checkpoint_dir: Directory to store checkpoints
            pipeline_id: Pipeline ID
        """
        self.checkpoint_dir = checkpoint_dir
        self.pipeline_id = pipeline_id
        
        # Create checkpoint directory if it doesn't exist
        os.makedirs(checkpoint_dir, exist_ok=True)
    
    def save_checkpoint(self, completed_nodes: Set[str], node_metrics: Dict[str, Any]):
        """
        Save a checkpoint of pipeline state
        
        Args:
            completed_nodes: Set of completed node IDs
            node_metrics: Node execution metrics
            
        Returns:
            Path to checkpoint file
        """
        checkpoint = {
            'pipeline_id': self.pipeline_id,
            'timestamp': datetime.now().isoformat(),
            'completed_nodes': list(completed_nodes),
            'metrics': node_metrics
        }
        
        checkpoint_file = os.path.join(
            self.checkpoint_dir, 
            f"checkpoint_{self.pipeline_id}_{int(time.time())}.json"
        )
        
        with open(checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
            
        logger.info(f"Checkpoint saved to {checkpoint_file}")
        
        # Update the latest checkpoint link
        latest_file = os.path.join(self.checkpoint_dir, f"checkpoint_{self.pipeline_id}_latest.json")
        try:
            if os.path.exists(latest_file):
                os.remove(latest_file)
            os.symlink(checkpoint_file, latest_file)
        except Exception as e:
            logger.warning(f"Failed to update latest checkpoint link: {e}")
            
        return checkpoint_file
    
    def load_checkpoint(self) -> Dict[str, Any]:
        """
        Load the latest checkpoint
        
        Returns:
            Dictionary with checkpoint data or empty dict if no checkpoint exists
        """
        checkpoint_file = os.path.join(self.checkpoint_dir, f"checkpoint_{self.pipeline_id}_latest.json")
        
        if not os.path.exists(checkpoint_file):
            logger.info(f"No checkpoint found at {checkpoint_file}")
            return {}
            
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                
            logger.info(f"Loaded checkpoint with {len(checkpoint.get('completed_nodes', []))} completed nodes")
            return checkpoint
        except Exception as e:
            logger.error(f"Failed to load checkpoint: {e}")
            return {}
    
    def get_completed_nodes(self) -> Set[str]:
        """
        Get the set of completed nodes from the latest checkpoint
        
        Returns:
            Set of completed node IDs
        """
        checkpoint = self.load_checkpoint()
        return set(checkpoint.get('completed_nodes', []))
    
    def get_node_metrics(self) -> Dict[str, Any]:
        """
        Get the node metrics from the latest checkpoint
        
        Returns:
            Dictionary of node metrics
        """
        checkpoint = self.load_checkpoint()
        return checkpoint.get('metrics', {})
    
    def clear_checkpoints(self) -> int:
        """
        Clear all checkpoints for the pipeline
        
        Returns:
            Number of checkpoints cleared
        """
        count = 0
        for file in os.listdir(self.checkpoint_dir):
            if file.startswith(f"checkpoint_{self.pipeline_id}"):
                os.remove(os.path.join(self.checkpoint_dir, file))
                count += 1
                
        logger.info(f"Cleared {count} checkpoints for pipeline {self.pipeline_id}")
        return count
    
    def list_checkpoints(self) -> List[Dict[str, Any]]:
        """
        List all checkpoints for the pipeline
        
        Returns:
            List of checkpoint information dictionaries
        """
        checkpoints = []
        
        for file in os.listdir(self.checkpoint_dir):
            if file.startswith(f"checkpoint_{self.pipeline_id}") and file.endswith(".json"):
                if not file.endswith("_latest.json"):
                    checkpoint_path = os.path.join(self.checkpoint_dir, file)
                    try:
                        with open(checkpoint_path, 'r') as f:
                            data = json.load(f)
                            checkpoints.append({
                                'file': file,
                                'path': checkpoint_path,
                                'timestamp': data.get('timestamp'),
                                'completed_nodes': len(data.get('completed_nodes', []))
                            })
                    except Exception as e:
                        logger.warning(f"Failed to read checkpoint {file}: {e}")
        
        # Sort by timestamp
        checkpoints.sort(key=lambda x: x['timestamp'], reverse=True)
        
        return checkpoints
    
    def load_specific_checkpoint(self, checkpoint_file: str) -> Dict[str, Any]:
        """
        Load a specific checkpoint file
        
        Args:
            checkpoint_file: Path to checkpoint file
            
        Returns:
            Dictionary with checkpoint data
        """
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint = json.load(f)
                
            logger.info(f"Loaded checkpoint {checkpoint_file} with {len(checkpoint.get('completed_nodes', []))} completed nodes")
            return checkpoint
        except Exception as e:
            logger.error(f"Failed to load checkpoint {checkpoint_file}: {e}")
            return {} 