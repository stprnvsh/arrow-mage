#!/usr/bin/env python3
"""
PipeLink CLI: Command-line interface for PipeLink

This is the main entry point for running PipeLink pipelines from the command line.
"""
import os
import sys
import argparse
import logging
import traceback
from pathlib import Path
from pipelink.python.pipelink import run_pipeline

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipelink.cli')

def main():
    """Main CLI entry point for PipeLink"""
    parser = argparse.ArgumentParser(description='PipeLink: Cross-Language Pipeline Orchestration')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run a pipeline')
    run_parser.add_argument('pipeline', help='Path to pipeline YAML file')
    run_parser.add_argument('--only-nodes', help='Only execute these nodes (comma-separated)', default=None)
    run_parser.add_argument('--start-from', help='Start execution from this node', default=None)
    run_parser.add_argument('--verbose', help='Enable verbose output', action='store_true')
    run_parser.add_argument('--no-daemon', help='Disable daemon processes', action='store_true')
    
    # Visualize command
    viz_parser = subparsers.add_parser('visualize', help='Visualize a pipeline')
    viz_parser.add_argument('pipeline', help='Path to pipeline YAML file')
    viz_parser.add_argument('--output', '-o', help='Output file path (PNG, PDF, SVG)', default=None)
    viz_parser.add_argument('--show', help='Show visualization immediately', action='store_true', default=True)
    viz_parser.add_argument('--no-show', help='Do not show visualization', dest='show', action='store_false')
    
    # Dashboard command
    dash_parser = subparsers.add_parser('dashboard', help='Launch pipeline monitoring dashboard')
    dash_parser.add_argument('--port', help='Port to run the dashboard on', type=int, default=5000)
    dash_parser.add_argument('--host', help='Host to run the dashboard on', default='127.0.0.1')
    
    # Daemon management commands
    daemon_parser = subparsers.add_parser('daemon', help='Manage daemon processes')
    daemon_subparsers = daemon_parser.add_subparsers(dest='daemon_command', help='Daemon command')
    
    # Start daemon command
    start_parser = daemon_subparsers.add_parser('start', help='Start language daemons')
    start_parser.add_argument('--languages', help='Languages to start daemons for (comma-separated)', 
                            default='python,r,julia')
    
    # Stop daemon command
    stop_parser = daemon_subparsers.add_parser('stop', help='Stop language daemons')
    stop_parser.add_argument('--languages', help='Languages to stop daemons for (comma-separated)',
                           default='python,r,julia')
    
    # Status daemon command
    status_parser = daemon_subparsers.add_parser('status', help='Check daemon status')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    try:
        if args.command == 'run':
            # Import pipeline runner
            try:
                from pipelink.python.pipelink import run_pipeline
            except ImportError:
                logger.error("Failed to import PipeLink. Is it installed?")
                sys.exit(1)
            
            # Parse arguments
            only_nodes = args.only_nodes.split(',') if args.only_nodes else None
            
            # Run pipeline
            run_pipeline(
                args.pipeline,
                only_nodes=only_nodes,
                start_from=args.start_from,
                verbose=args.verbose,
                use_daemon=not args.no_daemon
            )
            
        elif args.command == 'visualize':
            # Import visualizer
            try:
                from pipelink.python.pipeline_visualizer import visualize_pipeline
            except ImportError:
                logger.error("Failed to import pipeline visualizer. Is PipeLink installed?")
                sys.exit(1)
            
            # Run visualizer
            visualize_pipeline(args.pipeline, output_file=args.output, show=args.show)
            
        elif args.command == 'dashboard':
            # Launch dashboard in a subprocess to avoid import issues
            script_path = os.path.join(os.path.dirname(__file__), 'dashboard_cli.py')
            cmd = [sys.executable, script_path, '--port', str(args.port), '--host', args.host]
            
            import subprocess
            subprocess.run(cmd)
            
        elif args.command == 'daemon':
            # Import daemon manager
            try:
                from pipelink.python.language_daemon import LanguageDaemonManager
            except ImportError:
                logger.error("Failed to import daemon manager. Is PipeLink installed?")
                sys.exit(1)
                
            if args.daemon_command == 'start':
                # Start daemons
                languages = args.languages.split(',')
                daemon_manager = LanguageDaemonManager()
                
                for language in languages:
                    if language not in ['python', 'r', 'julia']:
                        logger.warning(f"Unsupported language: {language}")
                        continue
                        
                    try:
                        logger.info(f"Starting {language} daemon...")
                        daemon = daemon_manager.get_daemon(language)
                        logger.info(f"{language} daemon started successfully")
                    except Exception as e:
                        logger.error(f"Failed to start {language} daemon: {e}")
                
            elif args.daemon_command == 'stop':
                # Stop daemons
                languages = args.languages.split(',')
                daemon_manager = LanguageDaemonManager()
                
                for language in languages:
                    try:
                        if language in daemon_manager.daemons:
                            logger.info(f"Stopping {language} daemon...")
                            daemon_manager.daemons[language].shutdown()
                            logger.info(f"{language} daemon stopped successfully")
                        else:
                            logger.info(f"No running {language} daemon found")
                    except Exception as e:
                        logger.error(f"Failed to stop {language} daemon: {e}")
                
            elif args.daemon_command == 'status':
                # Check daemon status
                daemon_manager = LanguageDaemonManager()
                
                if not daemon_manager.daemons:
                    logger.info("No active daemons found")
                else:
                    logger.info("Active daemons:")
                    for language, daemon in daemon_manager.daemons.items():
                        status = "running" if daemon.running else "stopped"
                        logger.info(f"- {language}: {status}")
            else:
                daemon_parser.print_help()
                
        else:
            parser.print_help()
            
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == '__main__':
    main() 