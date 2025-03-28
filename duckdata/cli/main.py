#!/usr/bin/env python3

"""
DuckData command-line interface

This module provides the main command-line interface for DuckConnect and PipeDuck.
"""

import os
import sys
import argparse
import logging

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('duckdata.cli')

def main():
    """
    Main entry point for the CLI
    """
    parser = argparse.ArgumentParser(
        description='DuckData command-line interface for DuckConnect and PipeDuck'
    )
    
    # Add version argument
    parser.add_argument(
        '--version',
        action='store_true',
        help='Show version information'
    )
    
    # Create subparsers for commands
    subparsers = parser.add_subparsers(
        title='commands',
        dest='command',
        help='DuckData commands'
    )
    
    # Register PipeDuck commands
    pipeduck_parser = subparsers.add_parser(
        'pipeduck',
        help='PipeDuck pipeline orchestration',
        description='PipeDuck cross-language pipeline orchestration'
    )
    
    pipeduck_subparsers = pipeduck_parser.add_subparsers(
        title='pipeduck_commands',
        dest='pipeduck_command',
        help='PipeDuck commands'
    )
    
    # Register run command
    from cli.commands.run import register_run_command
    register_run_command(pipeduck_subparsers)
    
    # Register DuckConnect commands
    duck_connect_parser = subparsers.add_parser(
        'duck_connect',
        help='DuckConnect data sharing',
        description='DuckConnect cross-language data sharing'
    )
    
    duck_connect_subparsers = duck_connect_parser.add_subparsers(
        title='duck_connect_commands',
        dest='duck_connect_command',
        help='DuckConnect commands'
    )
    
    # Parse arguments
    args = parser.parse_args()
    
    # Show version if requested
    if args.version:
        from duck_connect import __version__ as duck_connect_version
        from pipeduck import __version__ as pipeduck_version
        
        print(f"DuckConnect v{duck_connect_version}")
        print(f"PipeDuck v{pipeduck_version}")
        return 0
    
    # Handle no command
    if not hasattr(args, 'func'):
        parser.print_help()
        return 1
    
    # Execute command
    return args.func(args)

if __name__ == '__main__':
    sys.exit(main()) 