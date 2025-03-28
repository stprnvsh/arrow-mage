"""
Run command for PipeDuck CLI

This module provides the run command for executing pipelines.
"""

import os
import sys
import json
import argparse
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck.cli.run')

def register_run_command(subparsers):
    """
    Register the run command with the argument parser
    
    Args:
        subparsers: Subparsers object from argparse
    """
    run_parser = subparsers.add_parser(
        'run',
        help='Run a pipeline',
        description='Execute a pipeline from a configuration file'
    )
    
    run_parser.add_argument(
        'config',
        help='Path to pipeline configuration file'
    )
    
    run_parser.add_argument(
        '--start-node',
        help='Node ID to start execution from'
    )
    
    run_parser.add_argument(
        '--no-resume',
        action='store_true',
        help='Do not resume from checkpoint'
    )
    
    run_parser.add_argument(
        '--visualize',
        action='store_true',
        help='Visualize pipeline before execution'
    )
    
    run_parser.add_argument(
        '--output',
        help='Output file for results'
    )
    
    run_parser.set_defaults(func=run_command)
    
def run_command(args):
    """
    Execute the run command
    
    Args:
        args: Command-line arguments
    """
    from pipeduck import run_pipeline
    
    # Check if config file exists
    if not os.path.exists(args.config):
        logger.error(f"Configuration file not found: {args.config}")
        return 1
        
    logger.info(f"Running pipeline from {args.config}")
    
    # Run pipeline
    results = run_pipeline(
        config_path=args.config,
        start_node=args.start_node,
        resume=not args.no_resume,
        visualize=args.visualize
    )
    
    # Save results if output specified
    if args.output:
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Results saved to {args.output}")
    
    # Print summary
    print("\nPipeline Execution Summary:")
    print(f"Pipeline ID: {results['pipeline_id']}")
    print(f"Start Time: {results['start_time']}")
    print(f"End Time: {results['end_time']}")
    print(f"Execution Time: {results['execution_time']:.2f} seconds")
    print(f"Successful Nodes: {len(results['successful_nodes'])}")
    print(f"Failed Nodes: {len(results['failed_nodes'])}")
    
    if results['failed_nodes']:
        return 1
    
    return 0 