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
        # Get inputs
        inputs = node.get('inputs', [])
        
        # Find nodes that produce these inputs
        for input_name in inputs:
            # Find the node that produces this output
            for producer in pipeline['nodes']:
                outputs = producer.get('outputs', [])
                if input_name in outputs:
                    # Add edge from producer to consumer
                    G.add_edge(producer['id'], node['id'])
    
    # Check for cycles
    if not nx.is_directed_acyclic_graph(G):
        raise ValueError("Pipeline has circular dependencies")
    
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
               start_from: Optional[str] = None, verbose: bool = False,
               metrics_dir: Optional[str] = None, generate_report: bool = False) -> bool:
    """
    Run a pipeline
    
    Args:
        pipeline_file: Path to pipeline YAML file
        only_nodes: Only execute these nodes (and their dependencies)
        start_from: Start execution from this node
        verbose: Enable verbose output
        metrics_dir: Directory to store metrics
        generate_report: Whether to generate a performance report
        
    Returns:
        bool: True if pipeline completed successfully, False otherwise
    """
    # Get absolute path to pipeline file
    pipeline_file = os.path.abspath(pipeline_file)
    
    # Set working directory to pipeline file directory by default
    working_dir = os.path.dirname(pipeline_file)
    
    # Load pipeline configuration
    with open(pipeline_file, 'r') as f:
        pipeline = yaml.safe_load(f)
    
    # Validate pipeline configuration
    _validate_pipeline(pipeline)
    
    # Build dependency graph
    G = _build_dependency_graph(pipeline)
    
    # Get execution order
    execution_order = _get_execution_order(G, only_nodes, start_from)
    
    # Get all nodes to execute
    nodes_to_execute = [node for node in pipeline['nodes'] if node['id'] in execution_order]
    
    # Initialize pipeline monitoring
    PipelineMonitor.initialize(metrics_dir)
    pipeline_id = PipelineMonitor.start_pipeline(pipeline['name'], [node['id'] for node in nodes_to_execute])
    
    logger.info(f"Running pipeline '{pipeline['name']}'")
    logger.info(f"Execution order: {', '.join(execution_order)}")
    
    # Override db_path if it's a relative path
    if 'db_path' in pipeline and not os.path.isabs(pipeline['db_path']):
        pipeline['db_path'] = os.path.join(working_dir, pipeline['db_path'])
    
    # Set working_dir if specified in pipeline
    if 'working_dir' in pipeline:
        working_dir = _resolve_path(working_dir, pipeline['working_dir'])
    
    # Run nodes in order
    success = True
    for node in nodes_to_execute:
        try:
            node_success = _run_node(node, pipeline, working_dir, pipeline_id, verbose)
            if not node_success:
                success = False
                break
        except Exception as e:
            logger.error(f"Error running node '{node['id']}': {e}")
            success = False
            break
    
    # Complete pipeline monitoring
    pipeline_metrics = PipelineMonitor.get_pipeline_metrics(pipeline_id)
    if pipeline_metrics:
        pipeline_metrics.complete_pipeline(success=success)
        
        # Generate report if requested
        if generate_report:
            report_dir = pipeline_metrics.generate_report()
            logger.info(f"Pipeline performance report generated in {report_dir}")
    
    if success:
        logger.info(f"Pipeline '{pipeline['name']}' completed successfully")
    else:
        logger.error(f"Pipeline '{pipeline['name']}' failed")
    
    return success

def main():
    parser = argparse.ArgumentParser(description='PipeLink: Cross-Language Pipeline Orchestration')
    parser.add_argument('pipeline_file', help='Path to pipeline YAML file')
    parser.add_argument('--only-nodes', help='Only execute these nodes (comma-separated)', default=None)
    parser.add_argument('--start-from', help='Start execution from this node', default=None)
    parser.add_argument('--verbose', help='Enable verbose output', action='store_true')
    parser.add_argument('--metrics-dir', help='Directory to store metrics', default=None)
    parser.add_argument('--generate-report', help='Generate performance report', action='store_true')
    
    args = parser.parse_args()
    
    only_nodes = args.only_nodes.split(',') if args.only_nodes else None
    
    run_pipeline(
        args.pipeline_file, 
        only_nodes=only_nodes, 
        start_from=args.start_from, 
        verbose=args.verbose,
        metrics_dir=args.metrics_dir,
        generate_report=args.generate_report
    )

if __name__ == '__main__':
    main() 