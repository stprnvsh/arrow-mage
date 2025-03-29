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
import threading
from typing import Dict, List, Any, Optional, Set
import networkx as nx
from pathlib import Path
import warnings
import uuid
import time
import json
import datetime
import traceback

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

# Import the language daemon manager
from .language_daemon import LanguageDaemonManager

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipelink')

# Global daemon manager instance
_daemon_manager = None

def get_daemon_manager():
    """Get global daemon manager instance"""
    global _daemon_manager
    if _daemon_manager is None:
        _daemon_manager = LanguageDaemonManager()
    return _daemon_manager

# Helper functions for multiprocessing that need to be at module level for pickling
def worker_process(task_queue, completed_nodes, failed_nodes, pipeline_config, 
                  working_dir, pipeline_id, verbose, use_daemon, execution_order,
                  node_processing_lock, nodes_in_process):
    """Worker process that executes nodes from the task queue"""
    while True:
        try:
            # Get next task with timeout
            try:
                node_id = task_queue.get(timeout=0.5)
            except Exception:  # Using general Exception to handle Empty from queue
                # Queue is empty, check if we should terminate
                if len(completed_nodes) + len(failed_nodes) >= len(execution_order):
                    break
                time.sleep(0.1)  # Short sleep to avoid busy waiting
                continue
            
            # Use lock to check if another worker is already processing this node
            with node_processing_lock:
                if node_id in nodes_in_process:
                    # Another worker is handling this node, put it back in the queue
                    task_queue.put(node_id)
                    continue
                
                if node_id in completed_nodes or node_id in failed_nodes:
                    # Node has already been processed by another worker
                    continue
                    
                # Mark node as being processed
                nodes_in_process[node_id] = True
            
            # Execute node
            logger.info(f"Worker executing node '{node_id}'")
            try:
                # Get node configuration from pipeline config
                node_config = None
                for node in pipeline_config['nodes']:
                    if node['id'] == node_id:
                        node_config = node
                        break
                
                if node_config is None:
                    failed_nodes[node_id] = f"Node '{node_id}' not found in pipeline configuration"
                    logger.error(f"Node '{node_id}' not found in pipeline configuration")
                    with node_processing_lock:
                        if node_id in nodes_in_process:
                            del nodes_in_process[node_id]
                    continue
                
                success = _run_node(
                    node_config, pipeline_config, working_dir, 
                    pipeline_id, verbose, use_daemon
                )
                
                if success:
                    completed_nodes[node_id] = True
                    logger.info(f"Node '{node_id}' completed successfully")
                else:
                    failed_nodes[node_id] = "Execution failed"
                    logger.error(f"Node '{node_id}' failed execution")
            except Exception as e:
                failed_nodes[node_id] = str(e)
                logger.error(f"Error executing node '{node_id}': {str(e)}")
            finally:
                # Remove node from in-process set
                with node_processing_lock:
                    if node_id in nodes_in_process:
                        del nodes_in_process[node_id]
        except Exception as e:
            logger.error(f"Worker process error: {e}")
            break

def dependency_checker(node_dependencies, completed_nodes, failed_nodes, task_queue, execution_order, nodes_in_process):
    """Background thread that continuously checks for nodes ready to run"""
    already_queued = set()
    
    while len(completed_nodes) + len(failed_nodes) < len(execution_order):
        # Check for failed dependency chains
        if failed_nodes:
            for node_id in execution_order:
                if node_id in already_queued or node_id in completed_nodes or node_id in failed_nodes:
                    continue
                    
                # Check if any dependency has failed
                deps = node_dependencies[node_id]
                if any(dep in failed_nodes for dep in deps):
                    failed_nodes[node_id] = "Dependency failed"
                    logger.info(f"Node '{node_id}' skipped due to failed dependency")
        
        # Find nodes that are ready to run
        for node_id in execution_order:
            if (node_id in already_queued or node_id in completed_nodes or 
                node_id in failed_nodes or node_id in nodes_in_process):
                continue
                
            # Check if all dependencies are completed
            deps = node_dependencies[node_id]
            if all(dep in completed_nodes for dep in deps):
                logger.info(f"Node '{node_id}' is ready to run, adding to queue")
                task_queue.put(node_id)
                already_queued.add(node_id)
        
        time.sleep(0.1)  # Short sleep to avoid busy waiting

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
             working_dir: str, pipeline_id: str, verbose: bool = False, 
             use_daemon: bool = True) -> bool:
    """
    Run a single pipeline node
    
    Args:
        node_config: Node configuration dictionary
        pipeline_config: Complete pipeline configuration dictionary
        working_dir: Working directory for the pipeline
        pipeline_id: Unique pipeline ID
        verbose: Enable verbose output
        use_daemon: Use daemon processes for language execution
        
    Returns:
        True if successful, False otherwise
    """
    node_id = node_config['id']
    language = node_config['language']
    
    logger.info(f"Running node '{node_id}' ({language})")
    
    # Start resource monitoring
    resource_monitor = ResourceMonitor()
    resource_monitor.start()
    
    # Get pipeline metrics instance
    pipeline_metrics = PipelineMonitor.get_pipeline_metrics(pipeline_id)
    if pipeline_metrics:
        pipeline_metrics.start_node(node_id)
    
    # Create metadata file
    meta_fd, meta_path = tempfile.mkstemp(suffix='.yml')
    os.close(meta_fd)
    
    with open(meta_path, 'w') as f:
        # Create metadata object
        metadata = {
            'pipeline_id': pipeline_id,
            'node_id': node_id,
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
        # Check if we should use daemon for this language
        if use_daemon and language in ['python', 'r', 'julia']:
            # Use daemon manager to execute
            try:
                daemon_manager = get_daemon_manager()
                success, stdout, stderr = daemon_manager.execute_node(
                    node_config, pipeline_config, working_dir, meta_path)
                
                # Process output
                if not success:
                    error_message = f"Node '{node_id}' failed with daemon execution"
                    if not verbose:
                        logger.error(error_message)
                        logger.error(f"STDOUT: {stdout}")
                        logger.error(f"STDERR: {stderr}")
                    
                    if pipeline_metrics:
                        pipeline_metrics.fail_node(
                            node_id, 
                            f"{error_message}\nSTDOUT: {stdout}\nSTDERR: {stderr}"
                        )
                    
                    raise RuntimeError(error_message)
                
                if verbose and stdout:
                    logger.info(f"Output from '{node_id}':\n{stdout}")
            except Exception as e:
                logger.warning(f"Daemon execution failed, falling back to subprocess: {e}")
                # Fall back to subprocess approach
                use_daemon = False
        
        # Use subprocess approach if daemon is not used
        if not use_daemon:
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
               use_daemon: bool = True, max_concurrency: Optional[int] = None) -> None:
    """
    Run a pipeline from a YAML file
    
    Args:
        pipeline_file: Path to pipeline YAML file
        only_nodes: Only execute these nodes (and their dependencies)
        start_from: Start execution from this node
        verbose: Enable verbose output
        use_daemon: Use persistent daemon processes for language execution
        max_concurrency: Maximum number of nodes to run concurrently (defaults to CPU count)
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

    # Implement concurrent execution using multiprocessing
    import multiprocessing as mp
    from multiprocessing import Manager
    
    # Set maximum concurrency (default to CPU count - 1)
    if max_concurrency is None:
        max_concurrency = max(1, mp.cpu_count() - 1)
    
    # Create shared manager for cross-process data structures
    manager = Manager()
    task_queue = manager.Queue()  # Queue for ready-to-run tasks
    completed_nodes = manager.dict()  # Track completed nodes
    failed_nodes = manager.dict()  # Track failed nodes with error messages
    node_dependencies = manager.dict()  # Store dependencies for each node
    node_processing_lock = manager.Lock()  # Lock for coordinating node processing
    nodes_in_process = manager.dict()  # Track which nodes are currently being processed
    
    # Initialize dependency tracking
    for node_id in G.nodes():
        # Get predecessors (dependencies) for this node
        predecessors = list(G.predecessors(node_id))
        node_dependencies[node_id] = set(predecessors)
    
    # Start dependency checker thread
    checker_thread = threading.Thread(
        target=dependency_checker,
        args=(node_dependencies, completed_nodes, failed_nodes, task_queue, execution_order, nodes_in_process)
    )
    checker_thread.daemon = True
    checker_thread.start()
    
    # Start worker processes
    logger.info(f"Starting {max_concurrency} worker processes for concurrent execution")
    workers = []
    for _ in range(max_concurrency):
        p = mp.Process(
            target=worker_process,
            args=(task_queue, completed_nodes, failed_nodes, pipeline, 
                 working_dir, pipeline_id, verbose, use_daemon, execution_order, node_processing_lock, nodes_in_process)
        )
        p.daemon = True
        p.start()
        workers.append(p)
    
    # Add nodes with no dependencies to the queue to start
    for node_id in execution_order:
        if not node_dependencies[node_id]:
            logger.info(f"Initial node '{node_id}' added to queue")
            task_queue.put(node_id)
    
    # Wait for all workers to finish
    for worker in workers:
        worker.join()
    
    # Check results
    success = len(failed_nodes) == 0
    
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
        
        # Print detailed error information
        for node_id, error in failed_nodes.items():
            logger.error(f"Node '{node_id}' failure: {error}")
            
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description='PipeLink: Cross-Language Pipeline Orchestration')
    parser.add_argument('pipeline_file', help='Path to pipeline YAML file')
    parser.add_argument('--only-nodes', help='Only execute these nodes (comma-separated)', default=None)
    parser.add_argument('--start-from', help='Start execution from this node', default=None)
    parser.add_argument('--verbose', help='Enable verbose output', action='store_true')
    parser.add_argument('--no-daemon', help='Disable daemon processes', action='store_true')
    
    args = parser.parse_args()
    
    only_nodes = args.only_nodes.split(',') if args.only_nodes else None
    
    run_pipeline(
        args.pipeline_file, 
        only_nodes=only_nodes, 
        start_from=args.start_from, 
        verbose=args.verbose,
        use_daemon=not args.no_daemon
    )

if __name__ == '__main__':
    main() 