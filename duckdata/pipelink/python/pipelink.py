"""
PipeLink: Cross-Language Pipeline Orchestration

Main module for defining and running pipelines.
"""
import os
import sys
import yaml
import tempfile
import subprocess
import logging
import argparse
from typing import Dict, List, Any, Optional, Set
import networkx as nx
from pathlib import Path
import warnings
import uuid
import time

# Import monitoring module
try:
    from pipelink.python.monitoring import PipelineMonitor, ResourceMonitor
except ImportError:
    # Fall back to relative import for development mode
    try:
        # Add the parent directory to the path if being run directly
        sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        from pipelink.python.monitoring import PipelineMonitor, ResourceMonitor
    except ImportError:
        try:
            from .monitoring import PipelineMonitor, ResourceMonitor
        except ImportError:
            raise ImportError("Cannot import monitoring module. Make sure the pipelink package is installed.")

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipelink')

def _resolve_path(base_dir: str, path: str) -> str:
    """
    Resolve a path relative to a base directory
    
    Args:
        base_dir: Base directory
        path: Path to resolve
        
    Returns:
        Absolute path
    """
    if os.path.isabs(path):
        return path
    return os.path.normpath(os.path.join(base_dir, path))

def _validate_pipeline(pipeline: Dict[str, Any]) -> None:
    """
    Validate pipeline configuration
    
    Args:
        pipeline: Pipeline configuration dict
        
    Raises:
        ValueError: If pipeline configuration is invalid
    """
    # Check required fields
    required_fields = ['name', 'nodes']
    for field in required_fields:
        if field not in pipeline:
            raise ValueError(f"Pipeline configuration missing required field: {field}")
    
    # Check nodes
    if not pipeline['nodes']:
        raise ValueError("Pipeline must have at least one node")
    
    # Check node fields
    node_names = set()
    for i, node in enumerate(pipeline['nodes']):
        # Check required node fields
        if 'id' not in node:
            raise ValueError(f"Node at index {i} missing required field: id")
        if 'language' not in node:
            raise ValueError(f"Node '{node['id']}' missing required field: language")
        if 'script' not in node and node['language'] != 'data':
            raise ValueError(f"Node '{node['id']}' missing required field: script")
        
        # Check for duplicate node IDs
        if node['id'] in node_names:
            raise ValueError(f"Duplicate node ID: {node['id']}")
        node_names.add(node['id'])
        
        # Check language
        if node['language'] not in ['python', 'r', 'julia', 'data']:
            raise ValueError(f"Node '{node['id']}' has unsupported language: {node['language']}")
            
        # For data nodes, check connection configuration
        if node['language'] == 'data':
            if 'connection' not in node:
                raise ValueError(f"Data node '{node['id']}' missing required field: connection")
            if 'type' not in node['connection']:
                raise ValueError(f"Data node '{node['id']}' connection missing required field: type")
        
        # Validate depends_on if present
        if 'depends_on' in node:
            depends_on = node['depends_on']
            if isinstance(depends_on, str):
                # Single dependency as string
                pass  # Will check for existence later in build_dependency_graph
            elif isinstance(depends_on, list):
                # List of dependencies
                if not all(isinstance(dep, str) for dep in depends_on):
                    raise ValueError(f"Node '{node['id']}' has invalid depends_on format. Must be string or list of strings.")
            else:
                raise ValueError(f"Node '{node['id']}' has invalid depends_on format. Must be string or list of strings.")

def _build_dependency_graph(pipeline: Dict[str, Any]) -> nx.DiGraph:
    """
    Build dependency graph from pipeline configuration
    
    Args:
        pipeline: Pipeline configuration dict
        
    Returns:
        NetworkX DiGraph representing dependencies
    """
    G = nx.DiGraph()
    
    # Add all nodes
    for node in pipeline['nodes']:
        G.add_node(node['id'], **node)
    
    # Add edges for dependencies
    for node in pipeline['nodes']:
        # Check for explicit dependencies first
        if 'depends_on' in node:
            # Handle both single string and list formats
            depends_on = node['depends_on']
            if isinstance(depends_on, str):
                depends_on = [depends_on]
                
            for dependency in depends_on:
                if dependency not in G:
                    raise ValueError(f"Node '{node['id']}' depends on non-existent node '{dependency}'")
                G.add_edge(dependency, node['id'])
        
        # Get inputs (for backward compatibility and implicit dependencies)
        inputs = node.get('inputs', [])
        
        # Find nodes that produce these inputs
        for input_name in inputs:
            # Find the node that produces this output
            producers = []
            for producer in pipeline['nodes']:
                outputs = producer.get('outputs', [])
                if input_name in outputs:
                    producers.append(producer['id'])
                    # Add edge from producer to consumer
                    G.add_edge(producer['id'], node['id'])
            
            if not producers and inputs:
                warnings.warn(f"Input '{input_name}' for node '{node['id']}' has no producer.")
    
    # Check for cycles
    if not nx.is_directed_acyclic_graph(G):
        cycles = list(nx.simple_cycles(G))
        raise ValueError(f"Pipeline has circular dependencies. Cycles: {cycles}")
    
    return G

def _get_execution_order(G: nx.DiGraph, only_nodes: Optional[List[str]] = None, 
                         start_from: Optional[str] = None) -> List[str]:
    """
    Get execution order for nodes
    
    Args:
        G: NetworkX DiGraph representing dependencies
        only_nodes: Only execute these nodes (and their dependencies)
        start_from: Start execution from this node
        
    Returns:
        List of node IDs in execution order
    """
    # Get topological sort (execution order)
    execution_order = list(nx.topological_sort(G))
    
    # Filter nodes if specified
    if only_nodes:
        # Find all dependencies for the specified nodes
        required_nodes = set(only_nodes)
        for node in only_nodes:
            # Get all ancestors (dependencies)
            required_nodes.update(nx.ancestors(G, node))
        
        # Filter execution order
        execution_order = [node for node in execution_order if node in required_nodes]
    
    # Start from a specific node if specified
    if start_from:
        if start_from not in G.nodes:
            raise ValueError(f"Start node not found: {start_from}")
        
        # Get index of start node
        start_idx = execution_order.index(start_from)
        
        # Skip nodes before start node
        execution_order = execution_order[start_idx:]
    
    return execution_order

def _run_node(node_config: Dict[str, Any], pipeline_config: Dict[str, Any], 
             working_dir: str, pipeline_id: str, verbose: bool = False) -> bool:
    """
    Run a single pipeline node
    
    Args:
        node_config: Node configuration
        pipeline_config: Full pipeline configuration
        working_dir: Working directory
        pipeline_id: Pipeline monitoring ID
        verbose: Enable verbose output
        
    Returns:
        bool: True if successful, False if failed
    """
    node_id = node_config['id']
    language = node_config['language']
    
    logger.info(f"Running node '{node_id}' ({language})")
    
    # Get pipeline metrics for monitoring
    pipeline_metrics = PipelineMonitor.get_pipeline_metrics(pipeline_id)
    if pipeline_metrics:
        pipeline_metrics.start_node(node_id)
    
    # Create resource monitor
    resource_monitor = ResourceMonitor()
    resource_monitor.start()
    
    # Create metadata file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        meta_path = f.name
        
        # Prepare metadata
        metadata = {
            'node_id': node_id,
            'pipeline_name': pipeline_config['name'],
            'inputs': node_config.get('inputs', []),
            'outputs': node_config.get('outputs', []),
            'params': node_config.get('params', {}),
            'db_path': pipeline_config.get('db_path', 'crosslink.duckdb'),
            'language': language
        }
        
        # Add connection details for data nodes
        if language == 'data':
            metadata['connection'] = node_config.get('connection', {})
        
        # Write metadata
        yaml.dump(metadata, f)
    
    success = True
    try:
        # Run script based on language
        if language == 'python':
            script_path = _resolve_path(working_dir, node_config['script'])
            cmd = [sys.executable, script_path, meta_path]
        elif language == 'r':
            script_path = _resolve_path(working_dir, node_config['script'])
            cmd = ['Rscript', script_path, meta_path]
        elif language == 'julia':
            script_path = _resolve_path(working_dir, node_config['script'])
            cmd = ['julia', script_path, meta_path]
        elif language == 'data':
            # For data nodes, use a standard data connector script
            script_path = os.path.join(os.path.dirname(__file__), 'data_node_runner.py')
            cmd = [sys.executable, script_path, meta_path]
        else:
            raise ValueError(f"Unsupported language: {language}")
        
        # Run command
        env = os.environ.copy()
        proc = subprocess.run(
            cmd,
            env=env,
            cwd=working_dir,
            stdout=subprocess.PIPE if not verbose else None,
            stderr=subprocess.PIPE if not verbose else None,
            text=True
        )
        
        # Check result
        if proc.returncode != 0:
            success = False
            error_message = f"Node '{node_id}' failed with exit code {proc.returncode}"
            
            if not verbose:
                logger.error(error_message)
                logger.error(f"STDOUT: {proc.stdout}")
                logger.error(f"STDERR: {proc.stderr}")
                
            if pipeline_metrics:
                pipeline_metrics.fail_node(node_id, f"{error_message}\nSTDOUT: {proc.stdout}\nSTDERR: {proc.stderr}")
                
            raise RuntimeError(error_message)
        
        if verbose and proc.stdout:
            logger.info(f"Output from '{node_id}':\n{proc.stdout}")
        
        logger.info(f"Node '{node_id}' completed successfully")
    
    except Exception as e:
        success = False
        error_message = str(e)
        
        # Record failure in metrics
        if pipeline_metrics:
            pipeline_metrics.fail_node(node_id, error_message)
            
        logger.error(f"Error executing node '{node_id}': {error_message}")
    
    finally:
        # Clean up metadata file
        if os.path.exists(meta_path):
            os.unlink(meta_path)
        
        # Collect and record resource metrics
        resource_metrics = resource_monitor.stop()
        if pipeline_metrics and success:
            pipeline_metrics.complete_node(
                node_id, 
                memory_mb=resource_metrics["memory_mb_max"],
                cpu_percent=resource_metrics["cpu_percent_mean"]
            )
    
    return success

def run_pipeline(pipeline_file: str, only_nodes: Optional[List[str]] = None, 
               start_from: Optional[str] = None, verbose: bool = False) -> None:
    """
    Run a pipeline from a YAML file
    
    Args:
        pipeline_file: Path to pipeline YAML file
        only_nodes: Only execute these nodes (and their dependencies)
        start_from: Start execution from this node
        verbose: Enable verbose output
    """
    logger.info(f"Loading pipeline from {pipeline_file}")
    
    # Load pipeline configuration
    with open(pipeline_file, 'r') as f:
        pipeline = yaml.safe_load(f)
    
    # Set up working directory
    working_dir = os.path.dirname(os.path.abspath(pipeline_file))
    if 'working_dir' in pipeline:
        if os.path.isabs(pipeline['working_dir']):
            working_dir = pipeline['working_dir']
        else:
            working_dir = os.path.normpath(os.path.join(working_dir, pipeline['working_dir']))
    
    # Validate pipeline configuration
    _validate_pipeline(pipeline)
    
    # Build dependency graph
    G = _build_dependency_graph(pipeline)
    
    # Get execution order
    execution_order = _get_execution_order(G, only_nodes, start_from)
    
    if verbose:
        logger.info(f"Execution order: {', '.join(execution_order)}")
    
    # Determine pipeline database path
    db_path = pipeline.get('db_path', 'pipeline.duckdb')
    db_path = _resolve_path(working_dir, db_path)
    
    logger.info(f"Using database at {db_path}")
    
    # Generate a unique pipeline ID
    pipeline_id = str(uuid.uuid4())
    
    # Initialize pipeline monitoring
    start_time = time.time()
    PipelineMonitor.start_pipeline(
        pipeline_id=pipeline_id,
        pipeline_name=pipeline.get('name', 'Unnamed Pipeline'),
        pipeline_file=pipeline_file,
        nodes=execution_order
    )
    
    # Run each node in the execution order
    success = True
    for node_id in execution_order:
        # Get node configuration
        node_config = G.nodes[node_id]
        
        # Run node
        node_success = _run_node(node_config, pipeline, working_dir, pipeline_id, verbose)
        
        if not node_success:
            success = False
            logger.error(f"Node '{node_id}' failed. Stopping pipeline.")
            break
    
    # Finalize pipeline monitoring
    end_time = time.time()
    execution_time = round(end_time - start_time, 2)
    
    PipelineMonitor.finish_pipeline(
        pipeline_id=pipeline_id,
        success=success,
        execution_time=execution_time
    )
    
    if success:
        logger.info(f"Pipeline completed successfully in {execution_time} seconds")
    else:
        logger.error(f"Pipeline failed after {execution_time} seconds")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='PipeLink: Cross-Language Pipeline Orchestration')
    parser.add_argument('pipeline_file', help='Path to pipeline YAML file')
    parser.add_argument('--only-nodes', help='Only execute these nodes (comma-separated)', default=None)
    parser.add_argument('--start-from', help='Start execution from this node', default=None)
    parser.add_argument('--verbose', help='Enable verbose output', action='store_true')
    
    args = parser.parse_args()
    
    only_nodes = args.only_nodes.split(',') if args.only_nodes else None
    
    run_pipeline(
        args.pipeline_file, 
        only_nodes=only_nodes, 
        start_from=args.start_from, 
        verbose=args.verbose
    )

if __name__ == '__main__':
    main() 