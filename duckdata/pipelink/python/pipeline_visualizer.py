"""
Pipeline visualizer for PipeLink DAGs.

This module provides functionality to visualize pipeline DAGs.
"""
import os
import yaml
import networkx as nx
import matplotlib.pyplot as plt
from typing import Dict, Any, Optional, Union, List

def load_pipeline(pipeline_file: str) -> Dict[str, Any]:
    """
    Load pipeline configuration from YAML file.
    
    Args:
        pipeline_file: Path to pipeline YAML file
        
    Returns:
        Pipeline configuration dict
    """
    with open(pipeline_file, 'r') as f:
        return yaml.safe_load(f)

def build_graph(pipeline: Dict[str, Any]) -> nx.DiGraph:
    """
    Build a NetworkX graph from pipeline configuration.
    
    Args:
        pipeline: Pipeline configuration dict
        
    Returns:
        NetworkX DiGraph
    """
    G = nx.DiGraph()
    
    # Add nodes
    for node in pipeline['nodes']:
        G.add_node(node['id'], **node)
    
    # Add edges for explicit dependencies
    for node in pipeline['nodes']:
        if 'depends_on' in node:
            depends_on = node['depends_on']
            if isinstance(depends_on, str):
                depends_on = [depends_on]
                
            for dependency in depends_on:
                G.add_edge(dependency, node['id'])
    
    # Add edges for implicit dependencies via inputs/outputs
    for node in pipeline['nodes']:
        inputs = node.get('inputs', [])
        
        for input_name in inputs:
            for producer in pipeline['nodes']:
                outputs = producer.get('outputs', [])
                if input_name in outputs:
                    G.add_edge(producer['id'], node['id'])
    
    return G

def visualize_pipeline(pipeline_file: str, output_file: Optional[str] = None, 
                       show: bool = True) -> None:
    """
    Visualize a pipeline as a directed graph.
    
    Args:
        pipeline_file: Path to pipeline YAML file
        output_file: Path to save visualization image (optional)
        show: Whether to display the visualization
    """
    # Load pipeline and build graph
    pipeline = load_pipeline(pipeline_file)
    G = build_graph(pipeline)
    
    # Create figure
    plt.figure(figsize=(12, 8))
    
    # Use a hierarchical layout for DAG visualization
    pos = nx.nx_agraph.graphviz_layout(G, prog='dot') if nx.nx_agraph_available else nx.spring_layout(G)
    
    # Draw nodes with different colors based on language
    language_colors = {
        'python': 'skyblue',
        'r': 'lightgreen',
        'julia': 'orange',
        'data': 'lightgray'
    }
    
    # Draw nodes grouped by language
    for language, color in language_colors.items():
        nodes = [n for n, d in G.nodes(data=True) if d.get('language') == language]
        nx.draw_networkx_nodes(G, pos, nodelist=nodes, node_color=color, 
                               node_size=2000, alpha=0.8, label=language)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, edge_color='gray', width=1.5, alpha=0.7, 
                          arrowsize=20, arrowstyle='->')
    
    # Draw labels
    nx.draw_networkx_labels(G, pos, font_size=10, font_family='sans-serif')
    
    # Add legend and title
    plt.legend()
    plt.title(f"PipeLink DAG: {pipeline.get('name', 'Unnamed Pipeline')}")
    
    # Remove axis
    plt.axis('off')
    
    # Save if output file specified
    if output_file:
        plt.savefig(output_file, bbox_inches='tight', dpi=300)
    
    # Show if requested
    if show:
        plt.show()
    else:
        plt.close()

def visualize_from_dict(pipeline: Dict[str, Any], output_file: Optional[str] = None,
                         show: bool = True) -> None:
    """
    Visualize a pipeline directly from configuration dict.
    
    Args:
        pipeline: Pipeline configuration dict
        output_file: Path to save visualization image (optional)
        show: Whether to display the visualization
    """
    # Build graph
    G = build_graph(pipeline)
    
    # Create figure
    plt.figure(figsize=(12, 8))
    
    # Use a hierarchical layout for DAG visualization
    pos = nx.nx_agraph.graphviz_layout(G, prog='dot') if nx.nx_agraph_available else nx.spring_layout(G)
    
    # Draw nodes with different colors based on language
    language_colors = {
        'python': 'skyblue',
        'r': 'lightgreen',
        'julia': 'orange',
        'data': 'lightgray'
    }
    
    # Draw nodes grouped by language
    for language, color in language_colors.items():
        nodes = [n for n, d in G.nodes(data=True) if d.get('language') == language]
        nx.draw_networkx_nodes(G, pos, nodelist=nodes, node_color=color, 
                               node_size=2000, alpha=0.8, label=language)
    
    # Draw edges
    nx.draw_networkx_edges(G, pos, edge_color='gray', width=1.5, alpha=0.7, 
                          arrowsize=20, arrowstyle='->')
    
    # Draw labels
    nx.draw_networkx_labels(G, pos, font_size=10, font_family='sans-serif')
    
    # Add legend and title
    plt.legend()
    plt.title(f"PipeLink DAG: {pipeline.get('name', 'Unnamed Pipeline')}")
    
    # Remove axis
    plt.axis('off')
    
    # Save if output file specified
    if output_file:
        plt.savefig(output_file, bbox_inches='tight', dpi=300)
    
    # Show if requested
    if show:
        plt.show()
    else:
        plt.close()

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Visualize PipeLink pipeline DAGs')
    parser.add_argument('pipeline', help='Path to pipeline YAML file')
    parser.add_argument('--output', '-o', help='Path to save visualization image')
    parser.add_argument('--no-show', action='store_true', help='Do not display visualization')
    
    args = parser.parse_args()
    
    visualize_pipeline(args.pipeline, args.output, not args.no_show) 