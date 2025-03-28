"""
DAG builder for PipeDuck

This module provides functionality for building pipeline DAGs from configurations.
"""

import logging
import networkx as nx
from typing import Dict, List, Any, Set

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck.dag.builder')

class PipelineDAG:
    """
    Directed Acyclic Graph representation of a pipeline with advanced execution tracking
    """
    
    def __init__(self, nodes: List[Dict]):
        """
        Initialize DAG with nodes
        
        Args:
            nodes: List of node dictionaries with pipeline configuration
        """
        self.graph = nx.DiGraph()
        self.node_configs = {node['id']: node for node in nodes}
        self._build_graph(nodes)
        
    def _build_graph(self, nodes: List[Dict]):
        """
        Build the directed graph from nodes
        
        Args:
            nodes: List of node dictionaries
        """
        # Add nodes
        for node in nodes:
            if 'id' not in node:
                raise ValueError(f"Node is missing required 'id' field: {node}")
                
            node_id = node['id']
            self.graph.add_node(node_id, **node)
        
        # Add edges for explicit dependencies
        for node in nodes:
            node_id = node['id']
            
            if 'depends_on' in node:
                depends_on = node['depends_on']
                
                # Handle both string and list dependencies
                if isinstance(depends_on, str):
                    depends_on = [depends_on]
                
                for dependency in depends_on:
                    if dependency not in self.graph.nodes:
                        raise ValueError(f"Node '{node_id}' depends on non-existent node '{dependency}'")
                    self.graph.add_edge(dependency, node_id)
        
        # Add edges for implicit dependencies via inputs/outputs
        for node in nodes:
            node_id = node['id']
            
            # Skip nodes without inputs
            if 'inputs' not in node:
                continue
                
            inputs = node['inputs']
            if not isinstance(inputs, list):
                inputs = [inputs]
            
            # For each input, find which node produces it as output
            for input_name in inputs:
                for producer in nodes:
                    # Skip if this is the same node
                    if producer['id'] == node_id:
                        continue
                        
                    # Skip nodes without outputs
                    if 'outputs' not in producer:
                        continue
                        
                    outputs = producer['outputs']
                    if not isinstance(outputs, list):
                        outputs = [outputs]
                    
                    # If this node produces our input, add a dependency
                    if input_name in outputs:
                        self.graph.add_edge(producer['id'], node_id)
        
        # Check for cycles
        if not nx.is_directed_acyclic_graph(self.graph):
            cycles = list(nx.simple_cycles(self.graph))
            raise ValueError(f"Pipeline contains cycles: {cycles}")
            
    def get_execution_order(self) -> List[str]:
        """
        Get the execution order of nodes
        
        Returns:
            List of node IDs in execution order
        """
        return list(nx.topological_sort(self.graph))
        
    def get_subgraph(self, start_node: str) -> 'PipelineDAG':
        """
        Get a subgraph starting from a specific node
        
        Args:
            start_node: Starting node ID
            
        Returns:
            PipelineDAG with only the subgraph
        """
        # Get all nodes reachable from start_node
        reachable = nx.descendants(self.graph, start_node)
        reachable.add(start_node)
        
        # Create subgraph
        subgraph = self.graph.subgraph(reachable).copy()
        
        # Convert back to PipelineDAG
        nodes = [self.node_configs[n] for n in subgraph.nodes]
        return PipelineDAG(nodes)
        
    def get_dependencies(self, node_id: str) -> List[str]:
        """
        Get immediate dependencies of a node
        
        Args:
            node_id: Node ID
            
        Returns:
            List of node IDs that are immediate dependencies
        """
        return list(self.graph.predecessors(node_id))
        
    def get_dependents(self, node_id: str) -> List[str]:
        """
        Get immediate dependents of a node
        
        Args:
            node_id: Node ID
            
        Returns:
            List of node IDs that immediately depend on this node
        """
        return list(self.graph.successors(node_id))
        
    def get_all_dependencies(self, node_id: str) -> Set[str]:
        """
        Get all dependencies of a node (recursive)
        
        Args:
            node_id: Node ID
            
        Returns:
            Set of all node IDs that are dependencies
        """
        dependencies = set()
        queue = list(self.graph.predecessors(node_id))
        
        while queue:
            dep = queue.pop(0)
            dependencies.add(dep)
            queue.extend([n for n in self.graph.predecessors(dep) if n not in dependencies])
            
        return dependencies
        
    def get_all_dependents(self, node_id: str) -> Set[str]:
        """
        Get all dependents of a node (recursive)
        
        Args:
            node_id: Node ID
            
        Returns:
            Set of all node IDs that depend on this node
        """
        return nx.descendants(self.graph, node_id)
        
    def get_node_config(self, node_id: str) -> Dict:
        """
        Get the configuration for a node
        
        Args:
            node_id: Node ID
            
        Returns:
            Node configuration dictionary
        """
        if node_id not in self.node_configs:
            raise ValueError(f"Node '{node_id}' not found in pipeline")
            
        return self.node_configs[node_id]

def build_dag_from_config(config: Dict) -> PipelineDAG:
    """
    Build a DAG from a pipeline configuration
    
    Args:
        config: Pipeline configuration dictionary
        
    Returns:
        Pipeline DAG
    """
    if 'nodes' not in config:
        raise ValueError("Pipeline configuration must contain 'nodes' section")
        
    return PipelineDAG(config['nodes']) 