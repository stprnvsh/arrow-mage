"""
Lineage tracking module for DuckConnect

This module provides functionality for tracking lineage between datasets.
"""

import uuid
import json
import logging
import pandas as pd
from typing import Dict, List, Optional, Union

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.lineage.tracking')

def record_lineage(connection_pool, target_dataset_id, source_dataset_id, 
                  transformation, language, version="1.0"):
    """
    Record lineage between datasets
    
    Args:
        connection_pool: Connection pool
        target_dataset_id: Target dataset ID (the result)
        source_dataset_id: Source dataset ID (the input)
        transformation: Description of the transformation
        language: Language performing the transformation
        version: Version of the transformation
        
    Returns:
        Lineage ID
    """
    # Generate lineage ID
    lineage_id = str(uuid.uuid4())
    
    # Record lineage
    with connection_pool.get_connection() as conn:
        conn.execute("""
        INSERT INTO duck_connect_lineage (
            id, target_dataset_id, source_dataset_id, transformation, language, timestamp, version
        ) VALUES (?, ?, ?, ?, ?, datetime('now'), ?)
        """, [
            lineage_id, target_dataset_id, source_dataset_id, transformation, language, version
        ])
        
    return lineage_id

def get_dataset_lineage(connection_pool, identifier):
    """
    Get lineage information for a dataset
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        
    Returns:
        Dictionary with upstream and downstream lineage
    """
    with connection_pool.get_connection() as conn:
        # Get dataset ID if name is provided
        if not identifier.startswith('duck_'):
            # It's a name
            result = conn.execute(
                "SELECT id FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
            if not result:
                raise ValueError(f"Dataset '{identifier}' not found")
                
            dataset_id = result[0]
        else:
            dataset_id = identifier
        
        # Get upstream lineage (datasets this one depends on)
        upstream = conn.execute("""
        SELECT l.*, m.name as source_name
        FROM duck_connect_lineage l
        JOIN duck_connect_metadata m ON l.source_dataset_id = m.id
        WHERE l.target_dataset_id = ?
        """, [dataset_id]).fetchdf()
        
        # Get downstream lineage (datasets that depend on this one)
        downstream = conn.execute("""
        SELECT l.*, m.name as target_name
        FROM duck_connect_lineage l
        JOIN duck_connect_metadata m ON l.target_dataset_id = m.id
        WHERE l.source_dataset_id = ?
        """, [dataset_id]).fetchdf()
        
        return {
            'upstream': upstream.to_dict('records') if not upstream.empty else [],
            'downstream': downstream.to_dict('records') if not downstream.empty else []
        }
        
def visualize_lineage(connection_pool, identifier, depth=2, output_file=None):
    """
    Visualize lineage graph for a dataset
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        depth: Depth of lineage graph (how many levels up/down to traverse)
        output_file: Output file path for visualization
        
    Returns:
        Path to the visualization file
    """
    try:
        import networkx as nx
        import matplotlib.pyplot as plt
    except ImportError:
        logger.error("Matplotlib and NetworkX are required for visualization")
        return None
    
    with connection_pool.get_connection() as conn:
        # Get dataset ID if name is provided
        if not identifier.startswith('duck_'):
            # It's a name
            result = conn.execute(
                "SELECT id, name FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
            if not result:
                raise ValueError(f"Dataset '{identifier}' not found")
                
            dataset_id, dataset_name = result
        else:
            # It's an ID
            result = conn.execute(
                "SELECT id, name FROM duck_connect_metadata WHERE id = ?", 
                [identifier]
            ).fetchone()
            
            if not result:
                raise ValueError(f"Dataset with ID '{identifier}' not found")
                
            dataset_id, dataset_name = result
    
        # Create directed graph
        G = nx.DiGraph()
        
        # Add the main dataset
        G.add_node(dataset_id, name=dataset_name, main=True)
        
        # Helper function to add upstream nodes recursively
        def add_upstream_nodes(node_id, current_depth):
            if current_depth > depth:
                return
                
            upstream = conn.execute("""
            SELECT l.source_dataset_id, m.name, l.transformation
            FROM duck_connect_lineage l
            JOIN duck_connect_metadata m ON l.source_dataset_id = m.id
            WHERE l.target_dataset_id = ?
            """, [node_id]).fetchall()
            
            for source_id, source_name, transformation in upstream:
                G.add_node(source_id, name=source_name, main=False)
                G.add_edge(source_id, node_id, transformation=transformation)
                
                # Recurse to add more upstream nodes
                add_upstream_nodes(source_id, current_depth + 1)
        
        # Helper function to add downstream nodes recursively
        def add_downstream_nodes(node_id, current_depth):
            if current_depth > depth:
                return
                
            downstream = conn.execute("""
            SELECT l.target_dataset_id, m.name, l.transformation
            FROM duck_connect_lineage l
            JOIN duck_connect_metadata m ON l.target_dataset_id = m.id
            WHERE l.source_dataset_id = ?
            """, [node_id]).fetchall()
            
            for target_id, target_name, transformation in downstream:
                G.add_node(target_id, name=target_name, main=False)
                G.add_edge(node_id, target_id, transformation=transformation)
                
                # Recurse to add more downstream nodes
                add_downstream_nodes(target_id, current_depth + 1)
        
        # Build the graph
        add_upstream_nodes(dataset_id, 1)
        add_downstream_nodes(dataset_id, 1)
        
        # Create visualization
        plt.figure(figsize=(12, 8))
        
        # Use hierarchical layout
        pos = nx.nx_agraph.graphviz_layout(G, prog='dot') if nx.nx_agraph else nx.spring_layout(G)
        
        # Draw nodes with different colors for the main dataset
        node_colors = ['red' if G.nodes[n].get('main', False) else 'lightblue' for n in G.nodes()]
        nx.draw_networkx_nodes(G, pos, node_color=node_colors, node_size=1500, alpha=0.8)
        
        # Draw edges
        nx.draw_networkx_edges(G, pos, arrows=True, arrowstyle='-|>', arrowsize=15, width=1.5, alpha=0.7)
        
        # Add labels with dataset names instead of IDs
        labels = {n: G.nodes[n]['name'] for n in G.nodes()}
        nx.draw_networkx_labels(G, pos, labels=labels, font_size=10, font_family='sans-serif')
        
        # Add edge labels for transformations
        edge_labels = {(u, v): d['transformation'] for u, v, d in G.edges(data=True)}
        nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8)
        
        # Add title
        plt.title(f"Lineage Graph for Dataset: {dataset_name}", fontsize=16)
        plt.axis('off')
        
        # Save to file if specified
        if output_file:
            plt.savefig(output_file, bbox_inches='tight', dpi=300)
            logger.info(f"Lineage visualization saved to {output_file}")
            plt.close()
            return output_file
        else:
            # Show interactive plot
            plt.show()
            plt.close()
            return None

def get_dataset_transformation_history(connection_pool, identifier):
    """
    Get the transformation history for a dataset
    
    Args:
        connection_pool: Connection pool
        identifier: Dataset ID or name
        
    Returns:
        DataFrame with transformation history
    """
    with connection_pool.get_connection() as conn:
        # Get dataset ID if name is provided
        if not identifier.startswith('duck_'):
            # It's a name
            result = conn.execute(
                "SELECT id FROM duck_connect_metadata WHERE name = ?", 
                [identifier]
            ).fetchone()
            
            if not result:
                raise ValueError(f"Dataset '{identifier}' not found")
                
            dataset_id = result[0]
        else:
            dataset_id = identifier
        
        # Get transformation history
        history = conn.execute("""
        WITH RECURSIVE transformation_chain(source_id, target_id, transformation, level) AS (
            -- Base case: direct transformations to our dataset
            SELECT source_dataset_id, target_dataset_id, transformation, 1
            FROM duck_connect_lineage
            WHERE target_dataset_id = ?
            
            UNION ALL
            
            -- Recursive case: add upstream transformations
            SELECT l.source_dataset_id, l.target_dataset_id, l.transformation, tc.level + 1
            FROM duck_connect_lineage l
            JOIN transformation_chain tc ON l.target_dataset_id = tc.source_id
        )
        SELECT tc.*, m1.name as source_name, m2.name as target_name
        FROM transformation_chain tc
        JOIN duck_connect_metadata m1 ON tc.source_id = m1.id
        JOIN duck_connect_metadata m2 ON tc.target_id = m2.id
        ORDER BY level DESC
        """, [dataset_id]).fetchdf()
        
        return history 