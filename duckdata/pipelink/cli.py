#!/usr/bin/env python3
"""
PipeLink CLI: Command-line interface for PipeLink

This is the main entry point for running PipeLink pipelines from the command line.
"""
import os
import sys
import argparse
from pipelink.python.pipelink import run_pipeline

def main():
    parser = argparse.ArgumentParser(description='PipeLink: Cross-Language Pipeline Orchestration')
    parser.add_argument('pipeline_file', help='Path to pipeline YAML file')
    parser.add_argument('--only-nodes', help='Only execute these nodes (comma-separated)', default=None)
    parser.add_argument('--start-from', help='Start execution from this node', default=None)
    parser.add_argument('--verbose', help='Enable verbose output', action='store_true')
    parser.add_argument('--metrics-dir', help='Directory to store metrics', default=None)
    parser.add_argument('--generate-report', help='Generate performance report', action='store_true')
    
    args = parser.parse_args()
    
    only_nodes = args.only_nodes.split(',') if args.only_nodes else None
    
    run_pipeline(
        args.pipeline_file, 
        only_nodes=only_nodes, 
        start_from=args.start_from, 
        verbose=args.verbose,
        metrics_dir=args.metrics_dir,
        generate_report=args.generate_report
    )

if __name__ == '__main__':
    main() 