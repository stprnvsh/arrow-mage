"""
DAG visualization for PipeDuck

This module provides functionality for visualizing pipeline DAGs.
"""

import os
import logging
from typing import Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck.dag.visualization')

def visualize_dag(dag, output_file: Optional[str] = None, show: bool = False):
    """
    Visualize a pipeline DAG
    
    Args:
        dag: Pipeline DAG object
        output_file: Output file path for the visualization
        show: Whether to display the visualization
        
    Returns:
        Path to the output file if saved
    """
    try:
        import matplotlib.pyplot as plt
        import networkx as nx
    except ImportError:
        logger.error("Matplotlib and NetworkX are required for visualization")
        return None
        
    plt.figure(figsize=(12, 8))
    
    # Get node attributes for coloring
    node_colors = []
    for node in dag.graph.nodes:
        node_data = dag.graph.nodes[node]
        language = node_data.get('language', 'python')
        
        if language == 'python':
            node_colors.append('lightblue')
        elif language == 'r':
            node_colors.append('lightgreen')
        elif language == 'julia':
            node_colors.append('lightcoral')
        else:
            node_colors.append('lightgray')
    
    # Create position layout
    try:
        pos = nx.nx_agraph.graphviz_layout(dag.graph, prog='dot')
    except:
        # Fall back to spring layout if graphviz not available
        pos = nx.spring_layout(dag.graph)
    
    # Draw nodes
    nx.draw_networkx_nodes(
        dag.graph, pos,
        node_color=node_colors,
        node_size=1500,
        alpha=0.8
    )
    
    # Draw edges
    nx.draw_networkx_edges(
        dag.graph, pos,
        arrows=True,
        arrowstyle='-|>',
        arrowsize=15,
        width=1.5,
        alpha=0.7
    )
    
    # Draw labels
    nx.draw_networkx_labels(
        dag.graph, pos,
        font_size=10,
        font_family='sans-serif'
    )
    
    # Add title
    plt.title('Pipeline DAG', fontsize=16)
    plt.axis('off')
    
    # Save to file if specified
    if output_file:
        plt.savefig(output_file, bbox_inches='tight', dpi=300)
        logger.info(f"DAG visualization saved to {output_file}")
    
    # Show plot if specified
    if show:
        plt.show()
    else:
        plt.close()
    
    return output_file if output_file else None

def create_dot_representation(dag) -> str:
    """
    Create a DOT language representation of the DAG
    
    Args:
        dag: Pipeline DAG object
        
    Returns:
        DOT language representation as string
    """
    try:
        import networkx as nx
    except ImportError:
        logger.error("NetworkX is required for DOT representation")
        return ""
        
    # Create DOT representation
    dot_data = ["digraph PipelineDag {"]
    dot_data.append("  node [shape=box, style=filled];")
    
    # Add nodes
    for node in dag.graph.nodes:
        node_data = dag.graph.nodes[node]
        language = node_data.get('language', 'python')
        name = node_data.get('name', node)
        
        if language == 'python':
            color = 'lightblue'
        elif language == 'r':
            color = 'lightgreen'
        elif language == 'julia':
            color = 'lightcoral'
        else:
            color = 'lightgray'
            
        dot_data.append(f'  "{node}" [label="{name}", fillcolor="{color}"];')
    
    # Add edges
    for edge in dag.graph.edges:
        source, target = edge
        dot_data.append(f'  "{source}" -> "{target}";')
    
    dot_data.append("}")
    
    return "\n".join(dot_data)

def export_dag_visualization(dag, output_format='png', output_file=None):
    """
    Export a visualization of the DAG in various formats
    
    Args:
        dag: Pipeline DAG object
        output_format: Format to export (png, svg, pdf, html)
        output_file: Output file path
        
    Returns:
        Path to the output file
    """
    if output_format in ['png', 'svg', 'pdf']:
        return visualize_dag(dag, output_file=output_file or f"pipeline_dag.{output_format}")
    elif output_format == 'html':
        try:
            from pyvis.network import Network
            import networkx as nx
        except ImportError:
            logger.error("Pyvis is required for HTML visualization")
            return None
            
        # Create interactive HTML visualization
        net = Network(notebook=False, height="750px", width="100%", directed=True)
        
        # Add nodes
        for node in dag.graph.nodes:
            node_data = dag.graph.nodes[node]
            language = node_data.get('language', 'python')
            name = node_data.get('name', node)
            
            if language == 'python':
                color = '#ADD8E6'  # lightblue
            elif language == 'r':
                color = '#90EE90'  # lightgreen
            elif language == 'julia':
                color = '#F08080'  # lightcoral
            else:
                color = '#D3D3D3'  # lightgray
                
            net.add_node(node, label=name, title=name, color=color)
        
        # Add edges
        for edge in dag.graph.edges:
            source, target = edge
            net.add_edge(source, target)
        
        # Save to file
        output_path = output_file or "pipeline_dag.html"
        net.save_graph(output_path)
        logger.info(f"Interactive DAG visualization saved to {output_path}")
        
        return output_path
    else:
        logger.error(f"Unsupported output format: {output_format}")
        return None 