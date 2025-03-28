"""
PipeDuck: Enhanced Cross-Language Pipeline Orchestration

This module provides the main PipeDuck class that integrates all components
for pipeline orchestration.
"""

import os
import json
import uuid
import time
import sys
import logging
import yaml
import tempfile
import subprocess
import signal
from datetime import datetime
from typing import Dict, List, Any, Set, Optional, Union, Callable

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck')

# Import components
try:
    from .dag.builder import PipelineDAG, build_dag_from_config
    from .resilience.checkpointing import CheckpointManager
    from .resilience.error_handling import ErrorHandlerRegistry
except ImportError:
    # Fallback for direct execution
    from dag.builder import PipelineDAG, build_dag_from_config
    from resilience.checkpointing import CheckpointManager
    from resilience.error_handling import ErrorHandlerRegistry

# Import DuckConnect for data sharing
# Try different import paths to handle both package and local imports
duck_connect_imported = False

# Try direct import first
try:
    from duck_connect import DuckConnect
    duck_connect_imported = True
except ImportError:
    # Try relative import within pipeduck
    try:
        from .duck_connect.core.facade import DuckConnect
        duck_connect_imported = True
    except ImportError:
        try:
            # Last resort, try legacy import
            sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
            from pipeduck.duck_connect.core.facade import DuckConnect
            duck_connect_imported = True
        except ImportError:
            # We'll raise a proper error later
            pass

if not duck_connect_imported:
    logger.warning("Could not import DuckConnect. Some features may be limited.")
    # Instead of raising an error immediately, define a placeholder class
    # This allows the module to be imported but will raise an error when DuckConnect is actually used
    class DuckConnect:
        def __init__(self, *args, **kwargs):
            raise ImportError("Could not import DuckConnect. Please ensure it is installed correctly.")
                
class PipeDuck:
    """
    PipeDuck: Enhanced Cross-Language Pipeline Orchestration
    
    This class integrates pipeline orchestration with efficient data sharing
    for cross-language data processing pipelines.
    """
    
    def __init__(self, config_path=None, config=None):
        """
        Initialize PipeDuck with a configuration
        
        Args:
            config_path: Path to pipeline configuration file
            config: Pipeline configuration dictionary
        """
        # Load configuration
        if config_path and os.path.exists(config_path):
            with open(config_path, 'r') as f:
                if config_path.endswith('.yaml') or config_path.endswith('.yml'):
                    self.config = yaml.safe_load(f)
                else:
                    self.config = json.load(f)
            self.config_path = config_path
        elif config:
            self.config = config
            self.config_path = None
        else:
            self.config = {"nodes": []}
            self.config_path = None
            
        # Generate pipeline ID if not present
        if 'id' not in self.config:
            self.config['id'] = f"pipeline_{uuid.uuid4().hex[:8]}"
            
        # Setup pipeline components
        self.pipeline_id = self.config['id']
        self.pipeline_name = self.config.get('name', 'Unnamed Pipeline')
        
        # Initialize DuckConnect for data sharing
        db_path = self.config.get('db_path', 'pipeline.duckdb')
        self.duck_connect = DuckConnect(db_path)
        
        # Create DAG from configuration
        self.dag = build_dag_from_config(self.config)
        
        # Initialize checkpoint manager
        checkpoint_dir = self.config.get('checkpoint_dir', os.path.join(tempfile.gettempdir(), 'pipeduck_checkpoints'))
        self.checkpoint_manager = CheckpointManager(checkpoint_dir, self.pipeline_id)
        
        # Initialize error handler
        self.error_handler = ErrorHandlerRegistry(self.config)
        
        # For scheduling
        self.schedule = self.config.get('schedule', None)
        self.last_run = None
        self.next_run = None
        if self.schedule:
            self._setup_schedule()
            
        # For metrics
        self.metrics_dir = self.config.get('metrics_dir', '.metrics')
        os.makedirs(self.metrics_dir, exist_ok=True)
        self.node_metrics = {}
        
        # Set up execution environment
        self.setup_environments()
        
    def _setup_schedule(self):
        """Setup pipeline scheduling"""
        try:
            import croniter
            now = datetime.now()
            cron = croniter.croniter(self.schedule, now)
            self.next_run = cron.get_next(datetime)
            logger.info(f"Pipeline scheduled to run at: {self.next_run}")
        except ImportError:
            logger.warning("croniter package not available, scheduling disabled")
            self.schedule = None
        
    def setup_environments(self):
        """Setup execution environments for different languages"""
        # Setup environments for different languages
        self.environments = {}
        
        # Get all required languages
        languages = set()
        for node_id in self.dag.graph.nodes:
            node_config = self.dag.get_node_config(node_id)
            languages.add(node_config.get('language', 'python'))
            
        # Initialize environments for each language
        for language in languages:
            try:
                self.environments[language] = self._create_environment(language)
                logger.info(f"Initialized environment for language: {language}")
            except Exception as e:
                logger.error(f"Failed to initialize environment for language {language}: {e}")
            
    def _create_environment(self, language):
        """Create execution environment for a language"""
        env = {
            'language': language,
            'ready': False
        }
        
        # Check if language runtime is available
        if language == 'python':
            env['ready'] = True
            env['binary'] = sys.executable
        elif language == 'r':
            # Check if R is installed
            try:
                result = subprocess.run(['Rscript', '--version'], 
                                        capture_output=True, 
                                        text=True)
                if result.returncode == 0:
                    env['ready'] = True
                    env['binary'] = 'Rscript'
            except FileNotFoundError:
                logger.warning("R runtime not found")
        elif language == 'julia':
            # Check if Julia is installed
            try:
                result = subprocess.run(['julia', '--version'], 
                                        capture_output=True, 
                                        text=True)
                if result.returncode == 0:
                    env['ready'] = True
                    env['binary'] = 'julia'
            except FileNotFoundError:
                logger.warning("Julia runtime not found")
        
        return env
        
    def execute(self, start_node=None, resume=True):
        """
        Execute the pipeline
        
        Args:
            start_node: Optional node ID to start execution from
            resume: Whether to resume from checkpoint
            
        Returns:
            Dictionary with execution results
        """
        # Start execution timing
        start_time = time.time()
        logger.info(f"Starting pipeline execution: {self.pipeline_id} - {self.pipeline_name}")
        
        # Determine execution order
        if start_node:
            # Create a subgraph starting from the specified node
            sub_dag = self.dag.get_subgraph(start_node)
            execution_order = sub_dag.get_execution_order()
        else:
            execution_order = self.dag.get_execution_order()
            
        logger.info(f"Execution order: {execution_order}")
        
        # Load checkpoint if resuming
        completed_nodes = set()
        node_metrics = {}
        if resume:
            checkpoint = self.checkpoint_manager.load_checkpoint()
            if checkpoint:
                completed_nodes = set(checkpoint.get('completed_nodes', []))
                node_metrics = checkpoint.get('metrics', {})
                logger.info(f"Resuming from checkpoint with {len(completed_nodes)} completed nodes")
                
        # Execute nodes in order
        failed_nodes = set()
        results = {}
        
        for node_id in execution_order:
            # Skip if already completed
            if node_id in completed_nodes:
                logger.info(f"Skipping node '{node_id}' (already completed)")
                continue
                
            # Skip if dependencies failed
            dependencies = self.dag.get_dependencies(node_id)
            if any(dep in failed_nodes for dep in dependencies):
                logger.warning(f"Skipping node '{node_id}' (dependencies failed)")
                failed_nodes.add(node_id)
                continue
                
            # Get node configuration
            node_config = self.dag.get_node_config(node_id)
            
            # Execute node
            success, result, metrics = self._execute_node(node_id, node_config)
            
            if success:
                # Store result
                results[node_id] = result
                node_metrics[node_id] = metrics
                completed_nodes.add(node_id)
                
                # Save checkpoint
                self.checkpoint_manager.save_checkpoint(completed_nodes, node_metrics)
            else:
                # Node failed and couldn't be recovered
                failed_nodes.add(node_id)
                
                # Check if we should continue
                isolation_mode = self.config.get('node_failure_isolation', 'fail_pipeline')
                if isolation_mode == 'fail_pipeline':
                    logger.error(f"Node '{node_id}' failed, stopping pipeline")
                    break
                else:
                    logger.warning(f"Node '{node_id}' failed but continuing with next nodes")
                
        # Execution complete
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Format results
        execution_results = {
            'pipeline_id': self.pipeline_id,
            'pipeline_name': self.pipeline_name,
            'start_time': datetime.fromtimestamp(start_time).isoformat(),
            'end_time': datetime.fromtimestamp(end_time).isoformat(),
            'execution_time': execution_time,
            'successful_nodes': list(completed_nodes),
            'failed_nodes': list(failed_nodes),
            'results': results,
            'metrics': node_metrics
        }
        
        # Save execution results to file
        results_file = os.path.join(self.metrics_dir, f"{self.pipeline_id}_results.json")
        with open(results_file, 'w') as f:
            json.dump(execution_results, f, indent=2)
        
        # Log completion
        logger.info(f"Pipeline execution completed in {execution_time:.2f}s: {len(completed_nodes)} succeeded, {len(failed_nodes)} failed")
        
        # Schedule next run if needed
        if self.schedule:
            try:
                import croniter
                now = datetime.now()
                cron = croniter.croniter(self.schedule, now)
                self.last_run = now
                self.next_run = cron.get_next(datetime)
                logger.info(f"Next run scheduled for: {self.next_run}")
            except ImportError:
                pass
        
        return execution_results
        
    def _execute_node(self, node_id, node_config):
        """
        Execute a single pipeline node
        
        Args:
            node_id: Node ID
            node_config: Node configuration
            
        Returns:
            Tuple of (success, result, metrics)
        """
        logger.info(f"Executing node: {node_id}")
        node_start_time = time.time()
        
        # Get language
        language = node_config.get('language', 'python')
        
        # Check if language environment is ready
        if language not in self.environments or not self.environments[language].get('ready', False):
            logger.error(f"Environment for language '{language}' not ready")
            return False, None, {'error': f"Environment for language '{language}' not ready"}
            
        # Create node metadata
        metadata = self._create_node_metadata(node_id, node_config)
        
        # Initialize retry count
        retry_count = 0
        max_retries = node_config.get('max_retries', self.config.get('max_retries', 3))
        
        while retry_count <= max_retries:
            try:
                # Write metadata to temp file
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                    json.dump(metadata, f, indent=2)
                    metadata_file = f.name
                
                # Get script path
                script = node_config.get('script')
                if not script:
                    raise ValueError(f"Node '{node_id}' is missing required 'script' field")
                
                # Check if script exists
                script_path = os.path.abspath(script)
                if not os.path.exists(script_path):
                    raise FileNotFoundError(f"Script not found: {script_path}")
                
                # Determine language and execute accordingly
                if language == 'python':
                    cmd = [sys.executable, script_path, metadata_file]
                elif language == 'r':
                    cmd = ['Rscript', script_path, metadata_file]
                elif language == 'julia':
                    cmd = ['julia', script_path, metadata_file]
                else:
                    raise ValueError(f"Unsupported language: {language}")
                
                # Set up environment variables
                env = os.environ.copy()
                env['PIPEDUCK_NODE_ID'] = node_id
                env['PIPEDUCK_PIPELINE_ID'] = self.pipeline_id
                env['PIPEDUCK_PIPELINE_NAME'] = self.pipeline_name
                
                # Set timeout if specified
                timeout = node_config.get('timeout')
                
                # Add working directory if specified
                cwd = node_config.get('working_dir')
                
                # Execute the script
                logger.info(f"Executing: {' '.join(cmd)}")
                try:
                    result = subprocess.run(
                        cmd,
                        capture_output=True,
                        text=True,
                        timeout=timeout,
                        env=env,
                        cwd=cwd,
                        check=False  # We'll handle errors ourselves
                    )
                    
                    # Clean up metadata file
                    try:
                        os.remove(metadata_file)
                    except:
                        pass
                    
                    # Check result
                    if result.returncode != 0:
                        error_message = result.stderr.strip() or f"Process returned non-zero exit code: {result.returncode}"
                        raise RuntimeError(f"Node execution failed: {error_message}")
                    
                    # Parse output for result
                    result_data = None
                    try:
                        # Check if output contains JSON result
                        import re
                        json_match = re.search(r'PIPEDUCK_RESULT_JSON:(.*?)PIPEDUCK_RESULT_END', 
                                               result.stdout, re.DOTALL)
                        if json_match:
                            result_json = json_match.group(1).strip()
                            result_data = json.loads(result_json)
                    except:
                        # If we can't parse result, use stdout as string result
                        result_data = result.stdout.strip()
                    
                    # If we get here, the node succeeded
                    success = True
                    break
                    
                except subprocess.TimeoutExpired:
                    error_message = f"Node execution timed out after {timeout} seconds"
                    raise TimeoutError(error_message)
                    
            except Exception as e:
                result_data = None
                logger.error(f"Error running node '{node_id}': {e}")
                
                # Handle error with retry logic
                should_retry, retry_count = self.error_handler.handle_error(node_id, e, retry_count)
                
                if not should_retry:
                    success = False
                    break
                    
                # If we should retry, continue the loop
                continue
        
        # Calculate duration and record end time
        node_end_time = time.time()
        duration = (node_end_time - node_start_time)
        
        # Generate metrics
        metrics = {
            'status': 'success' if success else 'failed',
            'start_time': datetime.fromtimestamp(node_start_time).isoformat(),
            'end_time': datetime.fromtimestamp(node_end_time).isoformat(),
            'duration': duration,
            'error': str(e) if 'e' in locals() else None
        }
        
        # Store metrics
        self.node_metrics[node_id] = metrics
        
        # Save running metrics
        metrics_file = os.path.join(self.metrics_dir, f"{self.pipeline_id}_metrics.json")
        with open(metrics_file, 'w') as f:
            json.dump(self.node_metrics, f, indent=2)
        
        # Log result
        if success:
            logger.info(f"Node '{node_id}' completed successfully in {duration:.2f}s")
        else:
            logger.error(f"Node '{node_id}' failed after {retry_count} retries")
            
            # Send alerts if configured
            if self.config.get('alerting', {}).get('enabled', False):
                self._send_alert(node_id, metrics.get('error'))
        
        return success, result_data, metrics
    
    def _create_node_metadata(self, node_id, node_config):
        """
        Create metadata for node execution
        
        Args:
            node_id: Node ID
            node_config: Node configuration
            
        Returns:
            Dictionary with node metadata
        """
        # Create basic metadata
        metadata = {
            'node_id': node_id,
            'pipeline_id': self.pipeline_id,
            'pipeline_name': self.pipeline_name,
            'run_id': str(uuid.uuid4()),
            'timestamp': datetime.now().isoformat(),
            'db_path': self.duck_connect.db_path,
            'language': node_config.get('language', 'python'),
        }
        
        # Add inputs, outputs, and parameters
        metadata['inputs'] = node_config.get('inputs', [])
        metadata['outputs'] = node_config.get('outputs', [])
        metadata['params'] = node_config.get('params', {})
        
        # Add environment variables
        metadata['env'] = {
            key: value for key, value in os.environ.items()
            if key.startswith(('PIPEDUCK_', 'DUCK_CONNECT_'))
        }
        
        # Add workspace information if available
        if 'workspace' in self.config:
            metadata['workspace'] = self.config['workspace']
            
        return metadata
    
    def _send_alert(self, node_id, error):
        """
        Send an alert for a failed node
        
        Args:
            node_id: Node ID that failed
            error: Error message
        """
        alert_config = self.config.get('alerting', {})
        alert_type = alert_config.get('type', 'log')
        
        alert_data = {
            'pipeline_id': self.pipeline_id,
            'pipeline_name': self.pipeline_name,
            'node_id': node_id,
            'error': error,
            'timestamp': datetime.now().isoformat(),
            'environment': alert_config.get('environment', 'development')
        }
        
        if alert_type == 'webhook':
            # Send webhook alert
            try:
                import requests
                webhook_url = alert_config.get('webhook_url')
                
                if webhook_url:
                    try:
                        headers = {'Content-Type': 'application/json'}
                        response = requests.post(
                            webhook_url,
                            json=alert_data,
                            headers=headers,
                            timeout=5
                        )
                        if response.status_code < 300:
                            logger.info(f"Alert sent to webhook for node '{node_id}'")
                        else:
                            logger.warning(f"Failed to send webhook alert: {response.status_code}")
                    except Exception as e:
                        logger.error(f"Error sending webhook alert: {e}")
            except ImportError:
                logger.warning("Requests package not available, webhook alert not sent")
                    
        elif alert_type == 'email':
            # Send email alert
            try:
                import smtplib
                from email.mime.text import MIMEText
                
                smtp_server = alert_config.get('smtp_server')
                smtp_port = alert_config.get('smtp_port', 587)
                smtp_user = alert_config.get('smtp_user')
                smtp_password = alert_config.get('smtp_password')
                from_email = alert_config.get('from_email')
                to_emails = alert_config.get('to_emails', [])
                
                if smtp_server and from_email and to_emails:
                    try:
                        # Format message
                        subject = f"Pipeline Alert: {self.pipeline_name} - Node {node_id} Failed"
                        body = f"""
                        Pipeline: {self.pipeline_name} ({self.pipeline_id})
                        Node: {node_id}
                        Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
                        
                        Error: {error}
                        """
                        
                        msg = MIMEText(body)
                        msg['Subject'] = subject
                        msg['From'] = from_email
                        msg['To'] = ', '.join(to_emails)
                        
                        # Send email
                        server = smtplib.SMTP(smtp_server, smtp_port)
                        server.starttls()
                        if smtp_user and smtp_password:
                            server.login(smtp_user, smtp_password)
                        server.sendmail(from_email, to_emails, msg.as_string())
                        server.quit()
                        
                        logger.info(f"Alert email sent for node '{node_id}'")
                    except Exception as e:
                        logger.error(f"Error sending email alert: {e}")
            except ImportError:
                logger.warning("Email packages not available, email alert not sent")
                    
        else:
            # Default to logging alert
            logger.warning(f"ALERT: Node '{node_id}' failed with error: {error}")
        
    def visualize(self, output_format='png', output_file=None, show=False):
        """
        Visualize the pipeline DAG
        
        Args:
            output_format: Output format (png, svg, pdf, html)
            output_file: Output file path
            show: Whether to display the visualization
            
        Returns:
            Path to the visualization file
        """
        from pipeduck.dag.visualization import export_dag_visualization
        
        if output_file is None:
            output_file = f"{self.pipeline_id}_dag.{output_format}"
        
        return export_dag_visualization(
            self.dag,
            output_format=output_format,
            output_file=output_file
        )
        
    def get_checkpoint_list(self):
        """
        Get list of checkpoints
        
        Returns:
            List of checkpoint information
        """
        return self.checkpoint_manager.list_checkpoints()
        
    def restore_checkpoint(self, checkpoint_file):
        """
        Restore a specific checkpoint
        
        Args:
            checkpoint_file: Path to checkpoint file
            
        Returns:
            True if successful
        """
        checkpoint = self.checkpoint_manager.load_specific_checkpoint(checkpoint_file)
        return bool(checkpoint)
        
    def clear_checkpoints(self):
        """
        Clear all checkpoints
        
        Returns:
            Number of checkpoints cleared
        """
        return self.checkpoint_manager.clear_checkpoints()
        
    def close(self):
        """
        Clean up resources
        """
        if hasattr(self, 'duck_connect') and self.duck_connect:
            self.duck_connect.close()
            
        logger.info(f"Pipeline {self.pipeline_name} resources released")

def run_pipeline(config_path=None, config=None, start_node=None, resume=True, visualize=False):
    """
    Run a pipeline from configuration
    
    Args:
        config_path: Path to pipeline configuration file
        config: Pipeline configuration dictionary
        start_node: Optional node ID to start execution from
        resume: Whether to resume from checkpoint
        visualize: Whether to visualize the pipeline
        
    Returns:
        Dictionary with execution results
    """
    # Import required modules
    import sys
    
    # Create pipeline
    pipeline = PipeDuck(config_path=config_path, config=config)
    
    # Visualize if requested
    if visualize:
        pipeline.visualize()
    
    try:
        # Execute pipeline
        results = pipeline.execute(start_node=start_node, resume=resume)
        return results
    except KeyboardInterrupt:
        logger.info("Pipeline execution interrupted by user")
        return {"status": "interrupted"}
    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}")
        return {"status": "failed", "error": str(e)}
    finally:
        # Clean up
        pipeline.close() 