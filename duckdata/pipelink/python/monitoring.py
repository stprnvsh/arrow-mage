"""
Monitoring: Real-time monitoring for PipeLink pipelines

This module provides functionality for tracking, storing, and reporting metrics
for pipeline execution, enabling real-time monitoring of pipeline status and performance.
"""

import os
import time
import json
import uuid
import logging
import datetime
import threading
import psutil
from typing import Dict, List, Any, Optional, Set
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

# Set up logging
logger = logging.getLogger('pipelink.monitoring')

class PipelineMetrics:
    """Store and manage metrics for pipeline execution"""
    
    def __init__(self, pipeline_id: str, pipeline_name: str, metrics_dir: Optional[str] = None):
        """
        Initialize PipelineMetrics
        
        Args:
            pipeline_id: Unique ID for the pipeline run
            pipeline_name: Name of the pipeline
            metrics_dir: Directory to store metrics (default: ~/.pipelink/metrics)
        """
        self.pipeline_id = pipeline_id
        self.pipeline_name = pipeline_name
        self.start_time = datetime.datetime.now()
        self.end_time = None
        self.status = "initializing"
        self.metrics_dir = metrics_dir or os.path.join(os.path.expanduser("~"), ".pipelink", "metrics")
        self.node_metrics = {}
        self.overall_metrics = {
            "nodes_total": 0,
            "nodes_completed": 0,
            "nodes_failed": 0,
            "nodes_running": 0,
            "nodes_pending": 0
        }
        self.resource_metrics = []
        self._resource_monitoring = False
        self._resource_monitor_thread = None
        
        # Ensure metrics directory exists
        os.makedirs(self.metrics_dir, exist_ok=True)
        
        # Initialize metrics file
        self._save_metrics()
    
    def start_pipeline(self, nodes: List[str]):
        """
        Record pipeline start
        
        Args:
            nodes: List of node IDs in the pipeline
        """
        self.start_time = datetime.datetime.now()
        self.status = "running"
        self.overall_metrics["nodes_total"] = len(nodes)
        self.overall_metrics["nodes_pending"] = len(nodes)
        
        # Initialize node metrics
        for node_id in nodes:
            self.node_metrics[node_id] = {
                "status": "pending",
                "start_time": None,
                "end_time": None,
                "duration": None,
                "memory_usage": None,
                "cpu_usage": None,
                "error": None
            }
        
        # Start resource monitoring
        self.start_resource_monitoring()
        
        # Save initial metrics
        self._save_metrics()
        
        logger.info(f"Started pipeline monitoring for '{self.pipeline_name}' (ID: {self.pipeline_id})")
    
    def start_node(self, node_id: str):
        """
        Record node start
        
        Args:
            node_id: ID of the node
        """
        self.node_metrics[node_id]["status"] = "running"
        self.node_metrics[node_id]["start_time"] = datetime.datetime.now()
        
        # Update overall metrics
        self.overall_metrics["nodes_running"] += 1
        self.overall_metrics["nodes_pending"] -= 1
        
        # Save updated metrics
        self._save_metrics()
        
        logger.info(f"Node '{node_id}' started")
    
    def complete_node(self, node_id: str, memory_mb: Optional[float] = None, cpu_percent: Optional[float] = None):
        """
        Record node completion
        
        Args:
            node_id: ID of the node
            memory_mb: Peak memory usage in MB
            cpu_percent: Average CPU usage percentage
        """
        end_time = datetime.datetime.now()
        
        if node_id in self.node_metrics:
            node = self.node_metrics[node_id]
            node["status"] = "completed"
            node["end_time"] = end_time
            
            # Calculate duration if we have a start time
            if node["start_time"]:
                duration = (end_time - node["start_time"]).total_seconds()
                node["duration"] = duration
            
            # Record resource usage
            if memory_mb is not None:
                node["memory_usage"] = memory_mb
            if cpu_percent is not None:
                node["cpu_usage"] = cpu_percent
            
            # Update overall metrics
            self.overall_metrics["nodes_completed"] += 1
            self.overall_metrics["nodes_running"] -= 1
        
            # Save updated metrics
            self._save_metrics()
            
            duration_str = f"{node['duration']:.2f}s" if node['duration'] else "unknown"
            logger.info(f"Node '{node_id}' completed in {duration_str}")
    
    def fail_node(self, node_id: str, error: str):
        """
        Record node failure
        
        Args:
            node_id: ID of the node
            error: Error message
        """
        end_time = datetime.datetime.now()
        
        if node_id in self.node_metrics:
            node = self.node_metrics[node_id]
            node["status"] = "failed"
            node["end_time"] = end_time
            node["error"] = error
            
            # Calculate duration if we have a start time
            if node["start_time"]:
                duration = (end_time - node["start_time"]).total_seconds()
                node["duration"] = duration
            
            # Update overall metrics
            self.overall_metrics["nodes_failed"] += 1
            self.overall_metrics["nodes_running"] -= 1
        
            # Save updated metrics
            self._save_metrics()
            
            duration_str = f"{node['duration']:.2f}s" if node['duration'] else "unknown"
            logger.error(f"Node '{node_id}' failed after {duration_str}: {error}")
    
    def complete_pipeline(self, success: bool = True):
        """
        Record pipeline completion
        
        Args:
            success: Whether the pipeline completed successfully
        """
        self.end_time = datetime.datetime.now()
        self.status = "completed" if success else "failed"
        
        # Calculate overall duration
        duration = (self.end_time - self.start_time).total_seconds()
        self.overall_metrics["duration"] = duration
        
        # Stop resource monitoring
        self.stop_resource_monitoring()
        
        # Save final metrics
        self._save_metrics()
        
        logger.info(f"Pipeline '{self.pipeline_name}' {self.status} in {duration:.2f}s")
    
    def start_resource_monitoring(self, interval: float = 1.0):
        """
        Start monitoring system resources
        
        Args:
            interval: Sampling interval in seconds
        """
        if self._resource_monitoring:
            return
            
        self._resource_monitoring = True
        self._resource_monitor_thread = threading.Thread(
            target=self._resource_monitor_worker,
            args=(interval,),
            daemon=True
        )
        self._resource_monitor_thread.start()
        
        logger.debug("Resource monitoring started")
    
    def stop_resource_monitoring(self):
        """Stop monitoring system resources"""
        self._resource_monitoring = False
        if self._resource_monitor_thread:
            self._resource_monitor_thread.join(timeout=5.0)
            self._resource_monitor_thread = None
            
        logger.debug("Resource monitoring stopped")
    
    def _resource_monitor_worker(self, interval: float):
        """Background worker to monitor system resources"""
        while self._resource_monitoring:
            try:
                # Get current process
                process = psutil.Process(os.getpid())
                
                # Collect metrics
                cpu_percent = process.cpu_percent(interval=0.1)
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
                
                # Record metrics
                timestamp = datetime.datetime.now()
                self.resource_metrics.append({
                    "timestamp": timestamp,
                    "cpu_percent": cpu_percent,
                    "memory_mb": memory_mb
                })
                
                # Update metrics file periodically
                if len(self.resource_metrics) % 10 == 0:
                    self._save_metrics()
                
                # Sleep for the interval
                time.sleep(interval)
            except Exception as e:
                logger.error(f"Error in resource monitoring: {e}")
                time.sleep(interval)
    
    def generate_report(self, output_dir: Optional[str] = None):
        """
        Generate a performance report for the pipeline
        
        Args:
            output_dir: Directory to save the report (default: metrics_dir)
        """
        if output_dir is None:
            output_dir = self.metrics_dir
            
        os.makedirs(output_dir, exist_ok=True)
        
        # Create report directory
        report_dir = os.path.join(output_dir, f"report_{self.pipeline_id}")
        os.makedirs(report_dir, exist_ok=True)
        
        # Generate summary report
        summary = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "duration": self.overall_metrics.get("duration"),
            "nodes_total": self.overall_metrics["nodes_total"],
            "nodes_completed": self.overall_metrics["nodes_completed"],
            "nodes_failed": self.overall_metrics["nodes_failed"],
        }
        
        with open(os.path.join(report_dir, "summary.json"), "w") as f:
            json.dump(summary, f, indent=2)
        
        # Generate node metrics report
        node_metrics_list = []
        for node_id, metrics in self.node_metrics.items():
            node_data = {"node_id": node_id}
            node_data.update(metrics)
            
            # Convert datetime objects to ISO format
            if node_data["start_time"]:
                node_data["start_time"] = node_data["start_time"].isoformat()
            if node_data["end_time"]:
                node_data["end_time"] = node_data["end_time"].isoformat()
                
            node_metrics_list.append(node_data)
        
        with open(os.path.join(report_dir, "node_metrics.json"), "w") as f:
            json.dump(node_metrics_list, f, indent=2)
        
        # Generate resource metrics report if available
        if self.resource_metrics:
            resource_df = pd.DataFrame(self.resource_metrics)
            
            # Convert timestamps to strings for JSON
            resource_metrics_json = []
            for metric in self.resource_metrics:
                metric_copy = metric.copy()
                metric_copy["timestamp"] = metric_copy["timestamp"].isoformat()
                resource_metrics_json.append(metric_copy)
                
            with open(os.path.join(report_dir, "resource_metrics.json"), "w") as f:
                json.dump(resource_metrics_json, f, indent=2)
            
            # Generate resource usage charts
            if len(resource_df) > 1:
                try:
                    # CPU usage chart
                    plt.figure(figsize=(10, 6))
                    plt.plot(resource_df["timestamp"], resource_df["cpu_percent"])
                    plt.title("CPU Usage")
                    plt.xlabel("Time")
                    plt.ylabel("CPU (%)")
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    plt.savefig(os.path.join(report_dir, "cpu_usage.png"))
                    plt.close()
                    
                    # Memory usage chart
                    plt.figure(figsize=(10, 6))
                    plt.plot(resource_df["timestamp"], resource_df["memory_mb"])
                    plt.title("Memory Usage")
                    plt.xlabel("Time")
                    plt.ylabel("Memory (MB)")
                    plt.xticks(rotation=45)
                    plt.tight_layout()
                    plt.savefig(os.path.join(report_dir, "memory_usage.png"))
                    plt.close()
                except Exception as e:
                    logger.error(f"Error generating resource charts: {e}")
        
        logger.info(f"Pipeline report generated in {report_dir}")
        return report_dir
    
    def _save_metrics(self):
        """Save metrics to disk"""
        metrics_file = os.path.join(self.metrics_dir, f"{self.pipeline_id}.json")
        
        # Prepare metrics data
        metrics_data = {
            "pipeline_id": self.pipeline_id,
            "pipeline_name": self.pipeline_name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "status": self.status,
            "overall_metrics": self.overall_metrics,
            "node_metrics": {}
        }
        
        # Convert node metrics (handling datetime objects)
        for node_id, metrics in self.node_metrics.items():
            node_data = metrics.copy()
            if node_data["start_time"]:
                node_data["start_time"] = node_data["start_time"].isoformat()
            if node_data["end_time"]:
                node_data["end_time"] = node_data["end_time"].isoformat()
            
            metrics_data["node_metrics"][node_id] = node_data
        
        # Save to file
        with open(metrics_file, "w") as f:
            json.dump(metrics_data, f, indent=2)


class PipelineMonitor:
    """Singleton manager for pipeline monitoring"""
    
    _instance = None
    _metrics_dir = None
    _active_pipelines = {}
    
    @classmethod
    def initialize(cls, metrics_dir: Optional[str] = None):
        """
        Initialize the pipeline monitor
        
        Args:
            metrics_dir: Directory to store metrics (default: ~/.pipelink/metrics)
        """
        if cls._instance is None:
            cls._instance = cls()
            cls._metrics_dir = metrics_dir or os.path.join(os.path.expanduser("~"), ".pipelink", "metrics")
            os.makedirs(cls._metrics_dir, exist_ok=True)
            
            logger.info(f"PipelineMonitor initialized with metrics directory: {cls._metrics_dir}")
    
    @classmethod
    def start_pipeline(cls, pipeline_name: str, nodes: List[str]) -> str:
        """
        Start monitoring a pipeline
        
        Args:
            pipeline_name: Name of the pipeline
            nodes: List of node IDs in the pipeline
            
        Returns:
            pipeline_id: Unique ID for the pipeline run
        """
        if cls._instance is None:
            cls.initialize()
        
        pipeline_id = str(uuid.uuid4())
        metrics = PipelineMetrics(pipeline_id, pipeline_name, cls._metrics_dir)
        metrics.start_pipeline(nodes)
        
        cls._active_pipelines[pipeline_id] = metrics
        return pipeline_id
    
    @classmethod
    def get_pipeline_metrics(cls, pipeline_id: str) -> Optional[PipelineMetrics]:
        """
        Get metrics for a pipeline
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            PipelineMetrics object or None if not found
        """
        return cls._active_pipelines.get(pipeline_id)
    
    @classmethod
    def list_active_pipelines(cls) -> List[Dict[str, Any]]:
        """
        List all active pipelines
        
        Returns:
            List of pipeline info dictionaries
        """
        return [
            {
                "pipeline_id": pid,
                "pipeline_name": metrics.pipeline_name,
                "status": metrics.status,
                "start_time": metrics.start_time,
                "nodes_total": metrics.overall_metrics["nodes_total"],
                "nodes_completed": metrics.overall_metrics["nodes_completed"],
                "nodes_failed": metrics.overall_metrics["nodes_failed"],
                "nodes_running": metrics.overall_metrics["nodes_running"],
                "nodes_pending": metrics.overall_metrics["nodes_pending"],
            }
            for pid, metrics in cls._active_pipelines.items()
        ]
    
    @classmethod
    def load_pipeline_metrics(cls, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Load metrics for a completed pipeline from disk
        
        Args:
            pipeline_id: Pipeline ID
            
        Returns:
            Dictionary of pipeline metrics or None if not found
        """
        if cls._instance is None:
            cls.initialize()
            
        metrics_file = os.path.join(cls._metrics_dir, f"{pipeline_id}.json")
        
        if not os.path.exists(metrics_file):
            return None
            
        try:
            with open(metrics_file, "r") as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading pipeline metrics: {e}")
            return None
    
    @classmethod
    def list_all_pipelines(cls) -> List[Dict[str, Any]]:
        """
        List all pipelines (active and completed)
        
        Returns:
            List of pipeline info dictionaries
        """
        if cls._instance is None:
            cls.initialize()
            
        pipelines = []
        
        # Add active pipelines
        pipelines.extend(cls.list_active_pipelines())
        
        # Add completed pipelines from disk
        try:
            metrics_files = [f for f in os.listdir(cls._metrics_dir) if f.endswith(".json")]
            
            for metrics_file in metrics_files:
                pipeline_id = metrics_file.replace(".json", "")
                
                # Skip active pipelines
                if pipeline_id in cls._active_pipelines:
                    continue
                    
                metrics = cls.load_pipeline_metrics(pipeline_id)
                if metrics:
                    pipelines.append({
                        "pipeline_id": metrics["pipeline_id"],
                        "pipeline_name": metrics["pipeline_name"],
                        "status": metrics["status"],
                        "start_time": metrics["start_time"],
                        "end_time": metrics["end_time"],
                        "nodes_total": metrics["overall_metrics"]["nodes_total"],
                        "nodes_completed": metrics["overall_metrics"]["nodes_completed"],
                        "nodes_failed": metrics["overall_metrics"]["nodes_failed"],
                    })
        except Exception as e:
            logger.error(f"Error listing all pipelines: {e}")
        
        return pipelines


class ResourceMonitor:
    """Monitor resource usage for a specific process or node"""
    
    def __init__(self):
        """Initialize ResourceMonitor"""
        self.start_time = None
        self.cpu_samples = []
        self.memory_samples = []
        self.sampling = False
        self._monitor_thread = None
    
    def start(self, interval: float = 1.0):
        """
        Start monitoring resources
        
        Args:
            interval: Sampling interval in seconds
        """
        if self.sampling:
            return
            
        self.start_time = datetime.datetime.now()
        self.sampling = True
        self._monitor_thread = threading.Thread(
            target=self._monitor_worker,
            args=(interval,),
            daemon=True
        )
        self._monitor_thread.start()
    
    def stop(self) -> Dict[str, Any]:
        """
        Stop monitoring and return metrics
        
        Returns:
            Dictionary of resource metrics
        """
        self.sampling = False
        if self._monitor_thread:
            self._monitor_thread.join(timeout=5.0)
            self._monitor_thread = None
        
        end_time = datetime.datetime.now()
        duration = (end_time - self.start_time).total_seconds() if self.start_time else 0
        
        # Calculate metrics
        cpu_mean = sum(self.cpu_samples) / len(self.cpu_samples) if self.cpu_samples else 0
        cpu_max = max(self.cpu_samples) if self.cpu_samples else 0
        
        memory_mean = sum(self.memory_samples) / len(self.memory_samples) if self.memory_samples else 0
        memory_max = max(self.memory_samples) if self.memory_samples else 0
        
        return {
            "duration": duration,
            "cpu_percent_mean": cpu_mean,
            "cpu_percent_max": cpu_max,
            "memory_mb_mean": memory_mean,
            "memory_mb_max": memory_max
        }
    
    def _monitor_worker(self, interval: float):
        """Resource monitoring worker thread"""
        while self.sampling:
            try:
                # Get current process
                process = psutil.Process(os.getpid())
                
                # Sample CPU and memory
                cpu_percent = process.cpu_percent(interval=0.1)
                memory_info = process.memory_info()
                memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
                
                # Store samples
                self.cpu_samples.append(cpu_percent)
                self.memory_samples.append(memory_mb)
                
                # Sleep for the specified interval
                time.sleep(interval)
            except Exception:
                # Just continue on errors
                time.sleep(interval) 