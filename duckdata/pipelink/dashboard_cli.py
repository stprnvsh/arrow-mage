#!/usr/bin/env python3
"""
Command-line script for launching the PipeLink dashboard
"""

import argparse
from pipelink.python.dashboard import run_dashboard

def main():
    parser = argparse.ArgumentParser(description='PipeLink Monitoring Dashboard')
    parser.add_argument('--host', default='127.0.0.1', help='Host to listen on')
    parser.add_argument('--port', type=int, default=5000, help='Port to listen on')
    parser.add_argument('--metrics-dir', default=None, help='Path to metrics directory')
    
    args = parser.parse_args()
    
    print(f"Starting PipeLink dashboard on http://{args.host}:{args.port}/")
    print(f"Press Ctrl+C to stop the server")
    
    run_dashboard(args.host, args.port, args.metrics_dir)

if __name__ == '__main__':
    main() 