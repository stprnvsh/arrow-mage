"""
Data quality metrics module for DuckConnect

This module provides functionality for collecting and analyzing data quality metrics.
"""

import json
import uuid
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any, Optional

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duck_connect.lineage.quality')

def collect_data_quality_metrics(connection_pool, metadata_manager, identifier, language="python"):
    """
    Collect data quality metrics for a dataset
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        identifier: Dataset ID or name
        language: Language performing the operation
        
    Returns:
        Dictionary of data quality metrics
    """
    with connection_pool.get_connection() as conn:
        # Get dataset metadata
        metadata = metadata_manager.get_dataset_metadata(identifier)
        
        if not metadata:
            raise ValueError(f"Dataset '{identifier}' not found")
            
        dataset_id = metadata['id']
        name = metadata['name']
        table_name = metadata['table_name']
        schema = metadata.get('schema', {})
        
        # Calculate basic metrics
        metrics = {
            'row_count': conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0],
            'column_count': len(schema.get('columns', [])),
            'columns': {},
            'null_counts': {},
            'unique_counts': {},
            'min_values': {},
            'max_values': {},
            'mean_values': {},
            'std_values': {},
            'histograms': {},
            'collection_time': datetime.now().isoformat()
        }
        
        # Collect metrics for each column
        for col in schema.get('columns', []):
            # Get column type from schema
            col_type = schema.get('dtypes', {}).get(col, 'unknown')
            metrics['columns'][col] = col_type
            
            # Count nulls
            null_count = conn.execute(f"SELECT COUNT(*) FROM {table_name} WHERE {col} IS NULL").fetchone()[0]
            metrics['null_counts'][col] = null_count
            
            # Count unique values
            unique_count = conn.execute(f"SELECT COUNT(DISTINCT {col}) FROM {table_name}").fetchone()[0]
            metrics['unique_counts'][col] = unique_count
            
            # Get statistics for numeric columns
            if 'int' in col_type.lower() or 'float' in col_type.lower() or 'double' in col_type.lower():
                min_val = conn.execute(f"SELECT MIN({col}) FROM {table_name}").fetchone()[0]
                max_val = conn.execute(f"SELECT MAX({col}) FROM {table_name}").fetchone()[0]
                avg_val = conn.execute(f"SELECT AVG({col}) FROM {table_name}").fetchone()[0]
                
                # Check if STDDEV is supported
                try:
                    std_val = conn.execute(f"SELECT STDDEV({col}) FROM {table_name}").fetchone()[0]
                except:
                    # Fall back to using VAR_SAMP
                    try:
                        var_val = conn.execute(f"SELECT VAR_SAMP({col}) FROM {table_name}").fetchone()[0]
                        std_val = np.sqrt(var_val) if var_val is not None else None
                    except:
                        std_val = None
                
                metrics['min_values'][col] = min_val
                metrics['max_values'][col] = max_val
                metrics['mean_values'][col] = avg_val
                metrics['std_values'][col] = std_val
                
                # Calculate histogram (10 bins) if min and max are available
                if min_val is not None and max_val is not None and min_val != max_val:
                    bin_width = (max_val - min_val) / 10
                    histograms = []
                    
                    for i in range(10):
                        bin_start = min_val + i * bin_width
                        bin_end = min_val + (i + 1) * bin_width
                        
                        if i == 9:  # Include the max value in the last bin
                            count = conn.execute(
                                f"SELECT COUNT(*) FROM {table_name} WHERE {col} >= ? AND {col} <= ?",
                                [bin_start, bin_end]
                            ).fetchone()[0]
                        else:
                            count = conn.execute(
                                f"SELECT COUNT(*) FROM {table_name} WHERE {col} >= ? AND {col} < ?",
                                [bin_start, bin_end]
                            ).fetchone()[0]
                            
                        histograms.append({
                            'bin_start': bin_start,
                            'bin_end': bin_end,
                            'count': count
                        })
                        
                    metrics['histograms'][col] = histograms
        
        # Update metadata with quality metrics
        metadata_manager.update_dataset_metadata(
            dataset_id,
            data_quality_metrics=metrics
        )
        
        # Record transaction
        metadata_manager.record_transaction(
            dataset_id,
            'collect_metrics',
            language,
            {'metrics_collected': list(metrics.keys())}
        )
        
        return metrics

def get_data_quality_metrics(metadata_manager, identifier):
    """
    Get previously collected data quality metrics for a dataset
    
    Args:
        metadata_manager: Metadata manager
        identifier: Dataset ID or name
        
    Returns:
        Dictionary of data quality metrics or None if not collected
    """
    # Get dataset metadata
    metadata = metadata_manager.get_dataset_metadata(identifier)
    
    if not metadata:
        raise ValueError(f"Dataset '{identifier}' not found")
        
    # Return quality metrics if available
    return metadata.get('data_quality_metrics')

def generate_data_quality_report(connection_pool, metadata_manager, identifier, format='html'):
    """
    Generate a data quality report for a dataset
    
    Args:
        connection_pool: Connection pool
        metadata_manager: Metadata manager
        identifier: Dataset ID or name
        format: Output format (html, json, text)
        
    Returns:
        Report in the specified format
    """
    # Get quality metrics
    metrics = get_data_quality_metrics(metadata_manager, identifier)
    
    if not metrics:
        # Collect metrics if not available
        metrics = collect_data_quality_metrics(connection_pool, metadata_manager, identifier)
    
    # Get dataset metadata
    metadata = metadata_manager.get_dataset_metadata(identifier)
    dataset_name = metadata.get('name', identifier)
    
    if format == 'json':
        # Return JSON representation
        return json.dumps(metrics, indent=2)
        
    elif format == 'text':
        # Generate text report
        report = [f"Data Quality Report for: {dataset_name}"]
        report.append(f"Collection Time: {metrics.get('collection_time')}")
        report.append(f"Row Count: {metrics.get('row_count')}")
        report.append(f"Column Count: {metrics.get('column_count')}")
        report.append("")
        report.append("Column Statistics:")
        
        # Get columns sorted by null percentage
        columns = list(metrics.get('columns', {}).keys())
        columns.sort(key=lambda c: metrics.get('null_counts', {}).get(c, 0) / metrics.get('row_count', 1) 
                     if metrics.get('row_count', 0) > 0 else 0, 
                     reverse=True)
        
        # Add column stats
        for col in columns:
            col_type = metrics.get('columns', {}).get(col, 'unknown')
            null_count = metrics.get('null_counts', {}).get(col, 0)
            null_pct = (null_count / metrics.get('row_count', 1) * 100) if metrics.get('row_count', 0) > 0 else 0
            unique_count = metrics.get('unique_counts', {}).get(col, 0)
            
            report.append(f"  {col} ({col_type}):")
            report.append(f"    - Null Count: {null_count} ({null_pct:.2f}%)")
            report.append(f"    - Unique Values: {unique_count}")
            
            # Add numeric stats if available
            if col in metrics.get('min_values', {}):
                min_val = metrics.get('min_values', {}).get(col)
                max_val = metrics.get('max_values', {}).get(col)
                mean_val = metrics.get('mean_values', {}).get(col)
                std_val = metrics.get('std_values', {}).get(col)
                
                report.append(f"    - Min: {min_val}")
                report.append(f"    - Max: {max_val}")
                report.append(f"    - Mean: {mean_val}")
                report.append(f"    - Std Dev: {std_val}")
        
        return "\n".join(report)
        
    else:  # html format
        # Generate HTML report
        html = ["<html><head>"]
        html.append("<style>")
        html.append("body { font-family: Arial, sans-serif; margin: 20px; }")
        html.append("h1, h2 { color: #333; }")
        html.append("table { border-collapse: collapse; width: 100%; }")
        html.append("th, td { border: 1px solid #ddd; padding: 8px; text-align: left; }")
        html.append("th { background-color: #f2f2f2; }")
        html.append(".histogram { width: 300px; height: 150px; }")
        html.append("</style>")
        html.append("</head><body>")
        html.append(f"<h1>Data Quality Report: {dataset_name}</h1>")
        html.append(f"<p><b>Collection Time:</b> {metrics.get('collection_time')}</p>")
        html.append(f"<p><b>Row Count:</b> {metrics.get('row_count')}</p>")
        html.append(f"<p><b>Column Count:</b> {metrics.get('column_count')}</p>")
        
        # Add column statistics table
        html.append("<h2>Column Statistics</h2>")
        html.append("<table>")
        html.append("<tr><th>Column</th><th>Type</th><th>Null Count</th><th>Null %</th><th>Unique Values</th><th>Min</th><th>Max</th><th>Mean</th><th>Std Dev</th></tr>")
        
        # Get columns
        columns = list(metrics.get('columns', {}).keys())
        for col in columns:
            col_type = metrics.get('columns', {}).get(col, 'unknown')
            null_count = metrics.get('null_counts', {}).get(col, 0)
            null_pct = (null_count / metrics.get('row_count', 1) * 100) if metrics.get('row_count', 0) > 0 else 0
            unique_count = metrics.get('unique_counts', {}).get(col, 0)
            
            min_val = metrics.get('min_values', {}).get(col, '')
            max_val = metrics.get('max_values', {}).get(col, '')
            mean_val = metrics.get('mean_values', {}).get(col, '')
            std_val = metrics.get('std_values', {}).get(col, '')
            
            html.append(f"<tr><td>{col}</td><td>{col_type}</td><td>{null_count}</td><td>{null_pct:.2f}%</td><td>{unique_count}</td><td>{min_val}</td><td>{max_val}</td><td>{mean_val}</td><td>{std_val}</td></tr>")
        
        html.append("</table>")
        
        # Add histograms for numeric columns
        if any(metrics.get('histograms', {})):
            html.append("<h2>Histograms</h2>")
            
            for col, histogram in metrics.get('histograms', {}).items():
                html.append(f"<h3>Histogram: {col}</h3>")
                html.append("<table>")
                html.append("<tr><th>Bin Range</th><th>Count</th><th>Distribution</th></tr>")
                
                # Get max count for scaling bars
                max_count = max([h.get('count', 0) for h in histogram])
                
                for bin_data in histogram:
                    bin_start = bin_data.get('bin_start')
                    bin_end = bin_data.get('bin_end')
                    count = bin_data.get('count', 0)
                    
                    # Create a bar using div
                    bar_width = int(count / max_count * 100) if max_count > 0 else 0
                    bar = f"<div style='background-color: #4CAF50; width: {bar_width}%; height: 20px;'></div>"
                    
                    html.append(f"<tr><td>{bin_start:.2f} - {bin_end:.2f}</td><td>{count}</td><td>{bar}</td></tr>")
                
                html.append("</table>")
        
        html.append("</body></html>")
        return "\n".join(html) 