#!/usr/bin/env python3
"""
PipeLink CLI: Command-line interface for PipeLink

This is the main entry point for running PipeLink pipelines from the command line.
"""
import os
import sys
import argparse
from pathlib import Path
from pipelink.python.pipelink import run_pipeline

def main():
    """Main entry point for PipeLink CLI"""
    parser = argparse.ArgumentParser(description='PipeLink: Cross-Language Pipeline Orchestration')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # Run command
    run_parser = subparsers.add_parser('run', help='Run a pipeline')
    run_parser.add_argument('pipeline', help='Pipeline configuration file')
    run_parser.add_argument('--only', nargs='+', help='Only run these nodes')
    run_parser.add_argument('--start-from', help='Start execution from this node')
    run_parser.add_argument('--verbose', '-v', action='store_true', help='Enable verbose output')
    
    # Dashboard command
    dashboard_parser = subparsers.add_parser('dashboard', help='Run monitoring dashboard')
    dashboard_parser.add_argument('--port', type=int, default=8050, help='Dashboard port')
    dashboard_parser.add_argument('--host', default='0.0.0.0', help='Dashboard host')
    dashboard_parser.add_argument('--debug', action='store_true', help='Enable debug mode')
    
    # Visualize pipeline command
    viz_parser = subparsers.add_parser('visualize', help='Visualize pipeline DAG')
    viz_parser.add_argument('pipeline', help='Pipeline configuration file')
    viz_parser.add_argument('--output', '-o', help='Output file for visualization')
    viz_parser.add_argument('--no-show', action='store_true', help='Do not display visualization')
    
    # Parse arguments
    args = parser.parse_args()
    
    # Process commands
    if args.command == 'run':
        # Import PipeLink module
        try:
            from pipelink.python.pipelink import run_pipeline
        except ImportError:
            # Try relative import
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
            from pipelink.python.pipelink import run_pipeline
        
        # Run pipeline
        run_pipeline(args.pipeline, only_nodes=args.only, start_from=args.start_from, 
                    verbose=args.verbose)
    
    elif args.command == 'dashboard':
        # Import dashboard module
        try:
            from pipelink.python.dashboard import run_dashboard
        except ImportError:
            # Try relative import
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
            from pipelink.python.dashboard import run_dashboard
        
        # Run dashboard
        run_dashboard(host=args.host, port=args.port, debug=args.debug)
    
    elif args.command == 'visualize':
        # Import visualizer module
        try:
            from pipelink.python.pipeline_visualizer import visualize_pipeline
        except ImportError:
            # Try relative import
            sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
            from pipelink.python.pipeline_visualizer import visualize_pipeline
        
        # Visualize pipeline
        visualize_pipeline(args.pipeline, args.output, not args.no_show)
    
    else:
        parser.print_help()

if __name__ == '__main__':
    main() 