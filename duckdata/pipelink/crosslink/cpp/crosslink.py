"""
CrossLink C++ interface

This module provides utility functions for working with C++ nodes in PipeLink.
"""
import os
import subprocess
import yaml
import tempfile
import uuid
from pathlib import Path

def get_cpp_binary_path():
    """
    Get the path to the cpp-pipelink binary.
    
    Returns:
        str: Path to the compiled binary
    """
    # Check if we're in a development environment
    script_dir = Path(__file__).parent.parent.parent.parent
    cpp_binary = script_dir / "cpp-pipelink" / "build" / "pipelink"
    
    if cpp_binary.exists():
        return str(cpp_binary)
    
    # Check for installed binary (this would be set in a proper installation)
    return "pipelink"  # Assuming it's in PATH after installation

def prepare_node_execution(node_id, pipeline_name, inputs, outputs, parameters, db_path="crosslink.duckdb"):
    """
    Prepare metadata for a C++ node execution.
    
    Args:
        node_id: Unique identifier for the node
        pipeline_name: Name of the pipeline
        inputs: Dictionary of input datasets
        outputs: List of expected output names
        parameters: Dictionary of parameters to pass to the node
        db_path: Path to the DuckDB database
        
    Returns:
        str: Path to the metadata YAML file
    """
    # Create metadata
    metadata = {
        "node_id": node_id,
        "pipeline_name": pipeline_name,
        "inputs": inputs,
        "outputs": outputs,
        "parameters": parameters,
        "db_path": db_path
    }
    
    # Create a temporary file to store the metadata
    temp_dir = tempfile.gettempdir()
    meta_file = os.path.join(temp_dir, f"pipelink_meta_{uuid.uuid4()}.yaml")
    
    # Write metadata to file
    with open(meta_file, 'w') as f:
        yaml.dump(metadata, f)
    
    return meta_file

def run_cpp_node(script_path, meta_file):
    """
    Run a C++ node with the given metadata.
    
    Args:
        script_path: Path to the compiled C++ node executable
        meta_file: Path to the metadata file
        
    Returns:
        int: Return code from the process
    """
    try:
        # Make sure the executable has execute permissions
        os.chmod(script_path, 0o755)
        
        # Run the C++ node with the metadata file as an argument
        process = subprocess.run([script_path, meta_file], check=True)
        return process.returncode
    except subprocess.CalledProcessError as e:
        print(f"Error running C++ node: {e}")
        return e.returncode
    finally:
        # Clean up the temporary metadata file
        if os.path.exists(meta_file):
            os.unlink(meta_file) 