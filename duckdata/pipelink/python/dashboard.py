"""
PipeLink: Web dashboard for monitoring pipelines

This module provides a simple Flask-based web dashboard for viewing
pipeline monitoring data, including pipeline status and performance metrics.
"""

import os
import json
import datetime
from typing import Dict, List, Any, Optional

from flask import Flask, render_template, jsonify, send_from_directory, request
from .monitoring import PipelineMonitor

app = Flask(__name__, 
            template_folder=os.path.join(os.path.dirname(__file__), 'dashboard_templates'),
            static_folder=os.path.join(os.path.dirname(__file__), 'dashboard_static'))

# Ensure metrics directory exists
metrics_dir = None

@app.route('/')
def index():
    """Dashboard home page"""
    return render_template('index.html')

@app.route('/api/pipelines')
def list_pipelines():
    """List all pipelines"""
    pipelines = PipelineMonitor.list_all_pipelines()
    
    # Format timestamps
    for pipeline in pipelines:
        if isinstance(pipeline.get('start_time'), datetime.datetime):
            pipeline['start_time'] = pipeline['start_time'].isoformat()
        if isinstance(pipeline.get('end_time'), datetime.datetime):
            pipeline['end_time'] = pipeline['end_time'].isoformat()
    
    return jsonify(pipelines)

@app.route('/api/pipelines/<pipeline_id>')
def get_pipeline(pipeline_id):
    """Get details for a specific pipeline"""
    # First try active pipelines
    metrics = PipelineMonitor.get_pipeline_metrics(pipeline_id)
    
    if metrics:
        # Convert to dictionary for JSON
        metrics_data = {
            'pipeline_id': metrics.pipeline_id,
            'pipeline_name': metrics.pipeline_name,
            'start_time': metrics.start_time.isoformat() if metrics.start_time else None,
            'end_time': metrics.end_time.isoformat() if metrics.end_time else None,
            'status': metrics.status,
            'overall_metrics': metrics.overall_metrics,
            'node_metrics': {}
        }
        
        # Convert node metrics
        for node_id, node_metrics in metrics.node_metrics.items():
            node_data = node_metrics.copy()
            if isinstance(node_data.get('start_time'), datetime.datetime):
                node_data['start_time'] = node_data['start_time'].isoformat()
            if isinstance(node_data.get('end_time'), datetime.datetime):
                node_data['end_time'] = node_data['end_time'].isoformat()
            
            metrics_data['node_metrics'][node_id] = node_data
        
        return jsonify(metrics_data)
    
    # If not active, load from disk
    metrics_data = PipelineMonitor.load_pipeline_metrics(pipeline_id)
    if metrics_data:
        return jsonify(metrics_data)
    
    # Not found
    return jsonify({'error': f'Pipeline {pipeline_id} not found'}), 404

@app.route('/api/reports')
def list_reports():
    """List all available reports"""
    if not metrics_dir:
        return jsonify({'error': 'Metrics directory not configured'}), 500
    
    reports = []
    try:
        # Look for report directories
        for item in os.listdir(metrics_dir):
            if item.startswith('report_') and os.path.isdir(os.path.join(metrics_dir, item)):
                pipeline_id = item.replace('report_', '')
                summary_file = os.path.join(metrics_dir, item, 'summary.json')
                
                if os.path.exists(summary_file):
                    with open(summary_file, 'r') as f:
                        summary = json.load(f)
                        reports.append({
                            'pipeline_id': pipeline_id,
                            'pipeline_name': summary.get('pipeline_name', 'Unknown'),
                            'status': summary.get('status', 'unknown'),
                            'start_time': summary.get('start_time'),
                            'end_time': summary.get('end_time'),
                            'duration': summary.get('duration'),
                            'report_path': item
                        })
    except Exception as e:
        return jsonify({'error': f'Error listing reports: {str(e)}'}), 500
    
    return jsonify(reports)

@app.route('/api/reports/<pipeline_id>')
def get_report(pipeline_id):
    """Get a specific report"""
    if not metrics_dir:
        return jsonify({'error': 'Metrics directory not configured'}), 500
    
    report_dir = os.path.join(metrics_dir, f'report_{pipeline_id}')
    if not os.path.exists(report_dir):
        return jsonify({'error': f'Report for pipeline {pipeline_id} not found'}), 404
    
    try:
        # Load summary
        summary_file = os.path.join(report_dir, 'summary.json')
        with open(summary_file, 'r') as f:
            summary = json.load(f)
        
        # Load node metrics
        node_metrics_file = os.path.join(report_dir, 'node_metrics.json')
        with open(node_metrics_file, 'r') as f:
            node_metrics = json.load(f)
        
        # Check if resource metrics exist
        resource_metrics_file = os.path.join(report_dir, 'resource_metrics.json')
        resource_metrics = None
        if os.path.exists(resource_metrics_file):
            with open(resource_metrics_file, 'r') as f:
                resource_metrics = json.load(f)
        
        # Check for chart images
        charts = {}
        for chart_name in ['cpu_usage.png', 'memory_usage.png']:
            chart_path = os.path.join(report_dir, chart_name)
            if os.path.exists(chart_path):
                charts[chart_name] = f'/reports/{pipeline_id}/{chart_name}'
        
        return jsonify({
            'summary': summary,
            'node_metrics': node_metrics,
            'resource_metrics': resource_metrics,
            'charts': charts
        })
    except Exception as e:
        return jsonify({'error': f'Error loading report: {str(e)}'}), 500

@app.route('/reports/<pipeline_id>/<filename>')
def get_report_file(pipeline_id, filename):
    """Get a file from a report directory"""
    if not metrics_dir:
        return jsonify({'error': 'Metrics directory not configured'}), 500
    
    report_dir = os.path.join(metrics_dir, f'report_{pipeline_id}')
    if not os.path.exists(report_dir):
        return jsonify({'error': f'Report for pipeline {pipeline_id} not found'}), 404
    
    return send_from_directory(report_dir, filename)

def create_dashboard_dir():
    """Create dashboard template and static directories if they don't exist"""
    # Create template directory
    template_dir = os.path.join(os.path.dirname(__file__), 'dashboard_templates')
    os.makedirs(template_dir, exist_ok=True)
    
    # Create index.html if it doesn't exist
    index_file = os.path.join(template_dir, 'index.html')
    if not os.path.exists(index_file):
        with open(index_file, 'w') as f:
            f.write("""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>PipeLink Dashboard</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/css/bootstrap.min.css">
    <style>
        .pipeline-card { cursor: pointer; }
        .pipeline-card:hover { background-color: #f8f9fa; }
        .node-pending { color: #6c757d; }
        .node-running { color: #007bff; }
        .node-completed { color: #28a745; }
        .node-failed { color: #dc3545; }
    </style>
</head>
<body>
    <nav class="navbar navbar-dark bg-dark mb-4">
        <div class="container">
            <a class="navbar-brand" href="#">PipeLink Dashboard</a>
        </div>
    </nav>

    <div class="container">
        <div class="row mb-4">
            <div class="col">
                <h2>Active Pipelines</h2>
                <div id="active-pipelines" class="row">
                    <div class="col">
                        <p class="text-muted">Loading pipelines...</p>
                    </div>
                </div>
            </div>
        </div>

        <div class="row mb-4">
            <div class="col">
                <h2>Completed Pipelines</h2>
                <div id="completed-pipelines" class="row">
                    <div class="col">
                        <p class="text-muted">Loading pipelines...</p>
                    </div>
                </div>
            </div>
        </div>

        <div class="row mb-4">
            <div class="col">
                <h2>Pipeline Reports</h2>
                <div id="pipeline-reports" class="row">
                    <div class="col">
                        <p class="text-muted">Loading reports...</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- Pipeline Details Modal -->
        <div class="modal fade" id="pipelineModal" tabindex="-1" aria-labelledby="pipelineModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="pipelineModalLabel">Pipeline Details</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body" id="pipeline-details">
                        <p class="text-center">Loading pipeline details...</p>
                    </div>
                </div>
            </div>
        </div>

        <!-- Report Modal -->
        <div class="modal fade" id="reportModal" tabindex="-1" aria-labelledby="reportModalLabel" aria-hidden="true">
            <div class="modal-dialog modal-xl">
                <div class="modal-content">
                    <div class="modal-header">
                        <h5 class="modal-title" id="reportModalLabel">Pipeline Report</h5>
                        <button type="button" class="btn-close" data-bs-dismiss="modal" aria-label="Close"></button>
                    </div>
                    <div class="modal-body" id="report-details">
                        <p class="text-center">Loading report...</p>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.2.3/dist/js/bootstrap.bundle.min.js"></script>
    <script>
        // Format date
        function formatDate(dateStr) {
            if (!dateStr) return 'N/A';
            const date = new Date(dateStr);
            return date.toLocaleString();
        }

        // Format duration
        function formatDuration(seconds) {
            if (!seconds) return 'N/A';
            
            const hrs = Math.floor(seconds / 3600);
            const mins = Math.floor((seconds % 3600) / 60);
            const secs = Math.floor(seconds % 60);
            
            if (hrs > 0) {
                return `${hrs}h ${mins}m ${secs}s`;
            } else if (mins > 0) {
                return `${mins}m ${secs}s`;
            } else {
                return `${secs}s`;
            }
        }

        // Get status badge
        function getStatusBadge(status) {
            switch(status) {
                case 'running':
                    return '<span class="badge bg-primary">Running</span>';
                case 'completed':
                    return '<span class="badge bg-success">Completed</span>';
                case 'failed':
                    return '<span class="badge bg-danger">Failed</span>';
                default:
                    return `<span class="badge bg-secondary">${status}</span>`;
            }
        }

        // Get node status class
        function getNodeStatusClass(status) {
            switch(status) {
                case 'pending': return 'node-pending';
                case 'running': return 'node-running';
                case 'completed': return 'node-completed';
                case 'failed': return 'node-failed';
                default: return '';
            }
        }

        // Load pipelines
        async function loadPipelines() {
            try {
                const response = await fetch('/api/pipelines');
                const pipelines = await response.json();
                
                const activePipelines = pipelines.filter(p => p.status === 'running' || p.status === 'initializing');
                const completedPipelines = pipelines.filter(p => p.status !== 'running' && p.status !== 'initializing');
                
                renderPipelines('active-pipelines', activePipelines);
                renderPipelines('completed-pipelines', completedPipelines);
                
                // Refresh active pipelines every 5 seconds
                if (activePipelines.length > 0) {
                    setTimeout(loadPipelines, 5000);
                }
            } catch (error) {
                console.error('Error loading pipelines:', error);
                document.getElementById('active-pipelines').innerHTML = '<div class="col"><p class="text-danger">Error loading pipelines</p></div>';
                document.getElementById('completed-pipelines').innerHTML = '<div class="col"><p class="text-danger">Error loading pipelines</p></div>';
            }
        }

        // Render pipelines
        function renderPipelines(containerId, pipelines) {
            const container = document.getElementById(containerId);
            
            if (pipelines.length === 0) {
                container.innerHTML = '<div class="col"><p class="text-muted">No pipelines found</p></div>';
                return;
            }
            
            let html = '';
            pipelines.forEach(pipeline => {
                const completedPercent = pipeline.nodes_total ? 
                    Math.round((pipeline.nodes_completed / pipeline.nodes_total) * 100) : 0;
                
                html += `
                <div class="col-md-4 mb-3">
                    <div class="card pipeline-card" data-pipeline-id="${pipeline.pipeline_id}">
                        <div class="card-body">
                            <h5 class="card-title">${pipeline.pipeline_name} ${getStatusBadge(pipeline.status)}</h5>
                            <p class="card-text">
                                <strong>Started:</strong> ${formatDate(pipeline.start_time)}<br>
                                <strong>Nodes:</strong> ${pipeline.nodes_completed || 0}/${pipeline.nodes_total || 0} completed
                            </p>
                            <div class="progress mb-2">
                                <div class="progress-bar ${pipeline.status === 'failed' ? 'bg-danger' : ''}" 
                                     role="progressbar" style="width: ${completedPercent}%" 
                                     aria-valuenow="${completedPercent}" aria-valuemin="0" aria-valuemax="100">
                                    ${completedPercent}%
                                </div>
                            </div>
                        </div>
                    </div>
                </div>`;
            });
            
            container.innerHTML = html;
            
            // Add click handlers
            container.querySelectorAll('.pipeline-card').forEach(card => {
                card.addEventListener('click', function() {
                    const pipelineId = this.getAttribute('data-pipeline-id');
                    showPipelineDetails(pipelineId);
                });
            });
        }

        // Show pipeline details
        async function showPipelineDetails(pipelineId) {
            const modal = new bootstrap.Modal(document.getElementById('pipelineModal'));
            modal.show();
            
            const detailsContainer = document.getElementById('pipeline-details');
            detailsContainer.innerHTML = '<p class="text-center">Loading pipeline details...</p>';
            
            try {
                const response = await fetch(`/api/pipelines/${pipelineId}`);
                const pipeline = await response.json();
                
                if (pipeline.error) {
                    detailsContainer.innerHTML = `<p class="text-danger">${pipeline.error}</p>`;
                    return;
                }
                
                // Format details
                let nodeTableRows = '';
                for (const [nodeId, metrics] of Object.entries(pipeline.node_metrics)) {
                    const statusClass = getNodeStatusClass(metrics.status);
                    nodeTableRows += `
                    <tr class="${statusClass}">
                        <td>${nodeId}</td>
                        <td>${metrics.status}</td>
                        <td>${formatDate(metrics.start_time)}</td>
                        <td>${formatDate(metrics.end_time)}</td>
                        <td>${formatDuration(metrics.duration)}</td>
                        <td>${metrics.memory_usage ? Math.round(metrics.memory_usage) + ' MB' : 'N/A'}</td>
                        <td>${metrics.cpu_usage ? Math.round(metrics.cpu_usage) + '%' : 'N/A'}</td>
                    </tr>`;
                }
                
                const duration = pipeline.overall_metrics.duration || 
                    (pipeline.end_time && pipeline.start_time ? 
                        (new Date(pipeline.end_time) - new Date(pipeline.start_time)) / 1000 : null);
                
                detailsContainer.innerHTML = `
                <div class="row mb-3">
                    <div class="col">
                        <h4>${pipeline.pipeline_name} ${getStatusBadge(pipeline.status)}</h4>
                        <p>
                            <strong>Pipeline ID:</strong> ${pipeline.pipeline_id}<br>
                            <strong>Started:</strong> ${formatDate(pipeline.start_time)}<br>
                            <strong>Completed:</strong> ${formatDate(pipeline.end_time)}<br>
                            <strong>Duration:</strong> ${formatDuration(duration)}<br>
                        </p>
                    </div>
                </div>
                
                <div class="row mb-3">
                    <div class="col">
                        <h5>Node Metrics</h5>
                        <div class="table-responsive">
                            <table class="table table-striped">
                                <thead>
                                    <tr>
                                        <th>Node ID</th>
                                        <th>Status</th>
                                        <th>Started</th>
                                        <th>Completed</th>
                                        <th>Duration</th>
                                        <th>Memory</th>
                                        <th>CPU</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${nodeTableRows}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>`;
                
                // Auto-refresh for active pipelines
                if (pipeline.status === 'running' || pipeline.status === 'initializing') {
                    setTimeout(() => {
                        if (document.getElementById('pipelineModal').classList.contains('show')) {
                            showPipelineDetails(pipelineId);
                        }
                    }, 5000);
                }
            } catch (error) {
                console.error('Error loading pipeline details:', error);
                detailsContainer.innerHTML = '<p class="text-danger">Error loading pipeline details</p>';
            }
        }

        // Load reports
        async function loadReports() {
            try {
                const response = await fetch('/api/reports');
                const reports = await response.json();
                
                if (reports.error) {
                    document.getElementById('pipeline-reports').innerHTML = `<div class="col"><p class="text-muted">${reports.error}</p></div>`;
                    return;
                }
                
                if (reports.length === 0) {
                    document.getElementById('pipeline-reports').innerHTML = '<div class="col"><p class="text-muted">No reports found</p></div>';
                    return;
                }
                
                let html = '';
                reports.forEach(report => {
                    html += `
                    <div class="col-md-4 mb-3">
                        <div class="card pipeline-card" data-report-id="${report.pipeline_id}">
                            <div class="card-body">
                                <h5 class="card-title">${report.pipeline_name} ${getStatusBadge(report.status)}</h5>
                                <p class="card-text">
                                    <strong>Run:</strong> ${formatDate(report.start_time)}<br>
                                    <strong>Duration:</strong> ${formatDuration(report.duration)}
                                </p>
                            </div>
                        </div>
                    </div>`;
                });
                
                document.getElementById('pipeline-reports').innerHTML = html;
                
                // Add click handlers
                document.querySelectorAll('#pipeline-reports .pipeline-card').forEach(card => {
                    card.addEventListener('click', function() {
                        const reportId = this.getAttribute('data-report-id');
                        showReportDetails(reportId);
                    });
                });
            } catch (error) {
                console.error('Error loading reports:', error);
                document.getElementById('pipeline-reports').innerHTML = '<div class="col"><p class="text-danger">Error loading reports</p></div>';
            }
        }

        // Show report details
        async function showReportDetails(reportId) {
            const modal = new bootstrap.Modal(document.getElementById('reportModal'));
            modal.show();
            
            const detailsContainer = document.getElementById('report-details');
            detailsContainer.innerHTML = '<p class="text-center">Loading report...</p>';
            
            try {
                const response = await fetch(`/api/reports/${reportId}`);
                const report = await response.json();
                
                if (report.error) {
                    detailsContainer.innerHTML = `<p class="text-danger">${report.error}</p>`;
                    return;
                }
                
                // Format node metrics table
                let nodeTableRows = '';
                report.node_metrics.forEach(node => {
                    const statusClass = getNodeStatusClass(node.status);
                    nodeTableRows += `
                    <tr class="${statusClass}">
                        <td>${node.node_id}</td>
                        <td>${node.status}</td>
                        <td>${formatDate(node.start_time)}</td>
                        <td>${formatDate(node.end_time)}</td>
                        <td>${formatDuration(node.duration)}</td>
                        <td>${node.memory_usage ? Math.round(node.memory_usage) + ' MB' : 'N/A'}</td>
                        <td>${node.cpu_usage ? Math.round(node.cpu_usage) + '%' : 'N/A'}</td>
                    </tr>`;
                });
                
                // Format charts
                let chartsHtml = '';
                if (Object.keys(report.charts).length > 0) {
                    chartsHtml = '<div class="row mb-3">';
                    for (const [chartName, chartUrl] of Object.entries(report.charts)) {
                        const title = chartName.replace('.png', '').replace('_', ' ').split(' ')
                            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
                            .join(' ');
                            
                        chartsHtml += `
                        <div class="col-md-6 mb-3">
                            <div class="card">
                                <div class="card-header">${title}</div>
                                <div class="card-body text-center">
                                    <img src="${chartUrl}" alt="${title}" class="img-fluid">
                                </div>
                            </div>
                        </div>`;
                    }
                    chartsHtml += '</div>';
                }
                
                detailsContainer.innerHTML = `
                <div class="row mb-3">
                    <div class="col">
                        <h4>${report.summary.pipeline_name} ${getStatusBadge(report.summary.status)}</h4>
                        <p>
                            <strong>Pipeline ID:</strong> ${report.summary.pipeline_id}<br>
                            <strong>Started:</strong> ${formatDate(report.summary.start_time)}<br>
                            <strong>Completed:</strong> ${formatDate(report.summary.end_time)}<br>
                            <strong>Duration:</strong> ${formatDuration(report.summary.duration)}<br>
                            <strong>Nodes:</strong> ${report.summary.nodes_completed}/${report.summary.nodes_total} completed, 
                                ${report.summary.nodes_failed} failed
                        </p>
                    </div>
                </div>
                
                ${chartsHtml}
                
                <div class="row mb-3">
                    <div class="col">
                        <h5>Node Metrics</h5>
                        <div class="table-responsive">
                            <table class="table table-striped">
                                <thead>
                                    <tr>
                                        <th>Node ID</th>
                                        <th>Status</th>
                                        <th>Started</th>
                                        <th>Completed</th>
                                        <th>Duration</th>
                                        <th>Memory</th>
                                        <th>CPU</th>
                                    </tr>
                                </thead>
                                <tbody>
                                    ${nodeTableRows}
                                </tbody>
                            </table>
                        </div>
                    </div>
                </div>`;
            } catch (error) {
                console.error('Error loading report:', error);
                detailsContainer.innerHTML = '<p class="text-danger">Error loading report</p>';
            }
        }

        // Load data on page load
        document.addEventListener('DOMContentLoaded', function() {
            loadPipelines();
            loadReports();
        });
    </script>
</body>
</html>""")
    
    # Create static directory
    static_dir = os.path.join(os.path.dirname(__file__), 'dashboard_static')
    os.makedirs(static_dir, exist_ok=True)

def run_dashboard(host='127.0.0.1', port=5000, metrics_dir_path=None):
    """
    Run the monitoring dashboard
    
    Args:
        host: Host to listen on
        port: Port to listen on
        metrics_dir_path: Path to metrics directory
    """
    global metrics_dir
    
    # Initialize PipelineMonitor
    metrics_dir = metrics_dir_path
    PipelineMonitor.initialize(metrics_dir)
    
    # Create dashboard directories and templates
    create_dashboard_dir()
    
    # Run Flask app
    app.run(host=host, port=port)

if __name__ == '__main__':
    import argparse
    
    parser = argparse.ArgumentParser(description='PipeLink Monitoring Dashboard')
    parser.add_argument('--host', default='127.0.0.1', help='Host to listen on')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument('--metrics-dir', default=None, help='Path to metrics directory')
    
    args = parser.parse_args()
    
    run_dashboard(args.host, args.port, args.metrics_dir) 