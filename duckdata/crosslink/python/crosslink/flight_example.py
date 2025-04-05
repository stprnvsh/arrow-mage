#!/usr/bin/env python3
"""
CrossLink Flight API Example

This script demonstrates how to use the Apache Arrow Flight API in CrossLink
for distributed data sharing between nodes.
"""

import os
import sys
import time
import argparse
import pandas as pd
import pyarrow as pa
from duckdata.crosslink import get_instance, CrossLinkConfig, OperationMode

def create_sample_table():
    """Create a sample table for demonstration."""
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'value': [10.1, 20.2, 30.3, 40.4, 50.5]
    })
    return pa.Table.from_pandas(df)

def main():
    parser = argparse.ArgumentParser(description="CrossLink Flight API Example")
    parser.add_argument("--mode", choices=['server', 'client'], required=True,
                        help="Run as server or client")
    parser.add_argument("--host", default="localhost",
                        help="Host for the Flight server (default: localhost)")
    parser.add_argument("--port", type=int, default=8815,
                        help="Port for the Flight server (default: 8815)")
    args = parser.parse_args()

    if args.mode == 'server':
        # Server mode: start a Flight server and share a dataset
        print(f"Starting server on {args.host}:{args.port}")
        
        # Create a configuration for distributed mode
        config = CrossLinkConfig(
            db_path="crosslink_server.duckdb",
            debug=True,
            mode=OperationMode.DISTRIBUTED,
            flight_host=args.host,
            flight_port=args.port
        )
        
        # Initialize CrossLink
        cl = get_instance(config=config)
        
        # Start the Flight server
        if cl.start_flight_server():
            print(f"Flight server started on port {cl.flight_server_port()}")
        else:
            print("Failed to start Flight server")
            return 1
            
        try:
            # Create and share a sample table
            table = create_sample_table()
            print("\nCreated sample table:")
            print(table.to_pandas())
            
            # Push the table to local storage
            dataset_id = cl.push(table, name="sample_dataset", 
                                description="Sample dataset for Flight demo")
            print(f"Table pushed with ID: {dataset_id}")
            
            # List available datasets
            datasets = cl.list_datasets()
            print(f"Available datasets: {datasets}")
            
            print("\nWaiting for client connections...")
            print("Press Ctrl+C to stop the server")
            
            # Keep the server running until interrupted
            while True:
                time.sleep(1)
                
        except KeyboardInterrupt:
            print("\nStopping server...")
        finally:
            # Clean up
            cl.stop_flight_server()
            print("Flight server stopped")
            
    elif args.mode == 'client':
        # Client mode: connect to a Flight server and pull data
        print(f"Connecting to server at {args.host}:{args.port}")
        
        # Create a configuration for local mode
        config = CrossLinkConfig(
            db_path="crosslink_client.duckdb",
            debug=True
        )
        
        # Initialize CrossLink
        cl = get_instance(config=config)
        
        try:
            # List datasets on the remote server
            print("\nListing datasets on remote server...")
            datasets = cl.list_remote_datasets(args.host, args.port)
            print(f"Available datasets: {datasets}")
            
            if not datasets:
                print("No datasets available on the server.")
                return 0
                
            # Pull the first dataset
            dataset_id = datasets[0]
            print(f"\nPulling dataset '{dataset_id}' from remote server...")
            table = cl.flight_pull(dataset_id, args.host, args.port)
            
            print("\nPulled table:")
            print(table.to_pandas())
            
            # Create a new table to push back to the server
            new_df = pd.DataFrame({
                'id': [101, 102, 103],
                'name': ['Xavier', 'Yolanda', 'Zach'],
                'value': [99.9, 88.8, 77.7]
            })
            new_table = pa.Table.from_pandas(new_df)
            
            print("\nPushing a new table to the server...")
            print(new_df)
            
            # Push the table to the remote server
            new_dataset_id = cl.flight_push(new_table, args.host, args.port, 
                                         "client_dataset", "Dataset from client")
            print(f"Table pushed with ID: {new_dataset_id}")
            
            # List datasets again to verify the new dataset
            datasets = cl.list_remote_datasets(args.host, args.port)
            print(f"Updated datasets on server: {datasets}")
            
        except Exception as e:
            print(f"Error: {e}")
            return 1

    return 0

if __name__ == "__main__":
    sys.exit(main()) 