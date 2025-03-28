#!/usr/bin/env python3
"""
PipeDuck CLI: Command-line interface for PipeDuck framework

Provides a command-line interface for working with PipeDuck, including
pipeline execution, monitoring, and advanced DuckDB integration.
"""
import os
import sys
import json
import yaml
import argparse
import logging
import subprocess
import tempfile
import platform
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional, Union

# Import PipeDuck components
try:
    # Try direct import first (when installed as a package)
    from pipeduck import PipeDuck, run_pipeline
except ImportError:
    # Fallback to relative import
    from .pipeduck import PipeDuck, run_pipeline

# Import DuckConnect
duck_connect_imported = False

# Try different import approaches
try:
    from duck_connect.python import DuckConnect
    duck_connect_imported = True
except ImportError:
    try:
        from .duck_connect.python import DuckConnect
        duck_connect_imported = True
    except ImportError:
        try:
            from .duck_connect.core.facade import DuckConnect
            duck_connect_imported = True
        except ImportError:
            logger = logging.getLogger('pipeduck.cli')
            logger.warning("Could not import DuckConnect. Some features may be limited.")
            # Define a placeholder class that will raise an error when used
            class DuckConnect:
                def __init__(self, *args, **kwargs):
                    raise ImportError("Could not import DuckConnect. Please ensure it is installed correctly.")

# Import Python connector
try:
    from pipeduck.python.duck_connector import DuckConnector
except ImportError:
    try:
        from .python.duck_connector import DuckConnector
    except ImportError:
        logger = logging.getLogger('pipeduck.cli')
        logger.warning("DuckConnector could not be imported. Some features may be limited.")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('pipeduck.cli')

def cmd_run(args):
    """Run a pipeline from a configuration file"""
    # Run the pipeline
    result = run_pipeline(
        config_path=args.config,
        start_node=args.start_from,
        resume=args.resume,
        visualize=args.verbose
    )
    
    if result:
        logger.info("Pipeline completed successfully")
        return 0
    else:
        logger.error("Pipeline failed")
        return 1

def cmd_visualize(args):
    """Visualize a pipeline DAG"""
    try:
        pipeline = PipeDuck(args.config)
        pipeline.visualize_dag(args.output)
        pipeline.close()
        logger.info(f"Pipeline visualization saved to {args.output}")
        return 0
    except Exception as e:
        logger.error(f"Error visualizing pipeline: {e}")
        return 1

def cmd_init(args):
    """Initialize a new pipeline project"""
    name = args.name or "new-pipeline"
    directory = args.directory or name
    
    # Create project directory
    os.makedirs(directory, exist_ok=True)
    
    # Create simple pipeline configuration
    config = {
        "name": name,
        "id": f"pipeline_{int(datetime.now().timestamp())}",
        "description": "A PipeDuck pipeline",
        "db_path": f"{name}.duckdb",
        "checkpointing": {
            "enabled": True,
            "frequency": "node"
        },
        "nodes": [
            {
                "id": "load_data",
                "name": "Load Data",
                "description": "Load input data",
                "language": "python",
                "script": "nodes/load_data.py",
                "outputs": ["raw_data"]
            },
            {
                "id": "process_data",
                "name": "Process Data",
                "description": "Process and transform data",
                "language": "python",
                "script": "nodes/process_data.py",
                "inputs": ["raw_data"],
                "outputs": ["processed_data"],
                "depends_on": ["load_data"]
            },
            {
                "id": "analyze_data",
                "name": "Analyze Data",
                "description": "Analyze and generate insights",
                "language": "python",
                "script": "nodes/analyze_data.py",
                "inputs": ["processed_data"],
                "outputs": ["analysis_results"],
                "depends_on": ["process_data"]
            }
        ],
        "telemetry": {
            "enabled": False
        }
    }
    
    # Write configuration file
    config_path = os.path.join(directory, "pipeline.yaml")
    with open(config_path, 'w') as f:
        yaml.dump(config, f, sort_keys=False, default_flow_style=False)
        
    # Create nodes directory
    nodes_dir = os.path.join(directory, "nodes")
    os.makedirs(nodes_dir, exist_ok=True)
    
    # Create sample node scripts
    for node_id in ["load_data", "process_data", "analyze_data"]:
        script_path = os.path.join(nodes_dir, f"{node_id}.py")
        with open(script_path, 'w') as f:
            f.write(f"""#!/usr/bin/env python3
\"\"\"
PipeDuck Node: {node_id}
\"\"\"

import os
import sys
import json
import pandas as pd
import pyarrow as pa
from duck_connect.python.duck_connect import DuckConnect

def main():
    # Load metadata
    with open(sys.argv[1], 'r') as f:
        metadata = json.load(f)
        
    # Create DuckConnect context
    duck_connect = DuckConnect(metadata['db_path'])
    
    # Your node implementation here
    print(f"Running {metadata['node_id']} node")
    
    # Example input/output
    if 'inputs' in metadata and metadata['inputs']:
        for input_name in metadata['inputs']:
            # Example: Read input data
            # input_data = duck_connect.get_dataset(input_name)
            # print(f"Read input: {{input_name}}, shape: {{input_data.shape}}")
            pass
            
    if 'outputs' in metadata and metadata['outputs']:
        for output_name in metadata['outputs']:
            # Example: Write output data
            # sample_data = pd.DataFrame({{'value': range(10)}})
            # duck_connect.register_dataset(sample_data, output_name, 
            #     description=f"Output from {{metadata['node_id']}}")
            # print(f"Wrote output: {{output_name}}")
            pass
    
    # Success
    return 0

if __name__ == '__main__':
    sys.exit(main())
""")
    
    # Create README
    readme_path = os.path.join(directory, "README.md")
    with open(readme_path, 'w') as f:
        f.write(f"""# {name}

A PipeDuck pipeline project.

## Running the pipeline

```bash
pipeduck run pipeline.yaml
```

## Pipeline Structure

- **load_data**: Loads raw data
- **process_data**: Processes and transforms data
- **analyze_data**: Analyzes data and generates insights

## Advanced Features

This pipeline is built with PipeDuck and includes:

- Checkpointing for resilience
- Cross-language data sharing with DuckConnect
- DAG-based execution
""")
    
    logger.info(f"Initialized new pipeline project in {directory}")
    return 0

def cmd_checkpoint(args):
    """Manage pipeline checkpoints"""
    # Load pipeline
    try:
        pipeline = PipeDuck(args.config)
        
        if args.list:
            # List checkpoints
            checkpoint_dir = pipeline.checkpoint_dir
            checkpoints = []
            
            for file in os.listdir(checkpoint_dir):
                if file.startswith(f"checkpoint_{pipeline.pipeline_id}") and file.endswith(".json"):
                    if not file.endswith("_latest.json"):
                        checkpoint_path = os.path.join(checkpoint_dir, file)
                        with open(checkpoint_path, 'r') as f:
                            data = json.load(f)
                            checkpoints.append({
                                'file': file,
                                'timestamp': data.get('timestamp'),
                                'completed_nodes': len(data.get('completed_nodes', []))
                            })
            
            # Sort by timestamp
            checkpoints.sort(key=lambda x: x['timestamp'], reverse=True)
            
            # Print checkpoints
            if checkpoints:
                print(f"Checkpoints for pipeline {pipeline.pipeline_name} ({pipeline.pipeline_id}):")
                for i, cp in enumerate(checkpoints):
                    print(f"{i+1}. {cp['timestamp']} - {cp['completed_nodes']} completed nodes")
            else:
                print(f"No checkpoints found for pipeline {pipeline.pipeline_name}")
        
        elif args.clear:
            # Clear checkpoints
            checkpoint_dir = pipeline.checkpoint_dir
            count = 0
            
            for file in os.listdir(checkpoint_dir):
                if file.startswith(f"checkpoint_{pipeline.pipeline_id}"):
                    os.remove(os.path.join(checkpoint_dir, file))
                    count += 1
            
            print(f"Cleared {count} checkpoints for pipeline {pipeline.pipeline_name}")
        
        pipeline.close()
        return 0
    except Exception as e:
        logger.error(f"Error managing checkpoints: {e}")
        return 1

def cmd_duck_optimize(args):
    """Run optimizations on DuckDB database"""
    try:
        # Create optimized connection
        connector = DuckConnector(args.db_path, {
            'memory_limit': args.memory_limit,
            'threads': args.threads,
            'show_progress': True,
            'extensions': {
                'httpfs': args.httpfs,
                'fts': args.fts,
                'spatial': args.spatial
            }
        })
        
        # Vacuum and analyze
        if args.vacuum:
            print(f"Vacuuming database {args.db_path}...")
            connector.vacuum_analyze()
            
        # Create indexes
        if args.create_indexes and args.table:
            # Get columns from table
            result = connector.conn.execute(f"SELECT * FROM {args.table} LIMIT 0").fetchdf()
            columns = list(result.columns)
            
            # Ask user which columns to index
            if not args.columns:
                print(f"Available columns in {args.table}:")
                for i, col in enumerate(columns):
                    print(f"{i+1}. {col}")
                    
                col_input = input("Enter column numbers to index (comma-separated): ")
                try:
                    col_indexes = [int(i.strip()) - 1 for i in col_input.split(",")]
                    index_columns = [columns[i] for i in col_indexes if 0 <= i < len(columns)]
                except:
                    print("Invalid input, no indexes created")
                    index_columns = []
            else:
                # Use provided columns
                index_columns = [c.strip() for c in args.columns.split(",")]
                # Validate columns
                index_columns = [c for c in index_columns if c in columns]
            
            # Create indexes
            if index_columns:
                print(f"Creating indexes on {args.table} for columns: {', '.join(index_columns)}")
                for col in index_columns:
                    index_name = f"idx_{args.table}_{col}"
                    connector.conn.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {args.table}({col})")
                print(f"Created {len(index_columns)} indexes")
        
        connector.close()
        return 0
    except Exception as e:
        logger.error(f"Error optimizing database: {e}")
        return 1

def cmd_convert(args):
    """Convert data between formats using DuckDB's native converters"""
    try:
        # Create connector
        connector = DuckConnector(":memory:")
        
        # Determine input format
        input_path = args.input
        input_format = args.input_format or _detect_format(input_path)
        
        # Determine output format
        output_path = args.output
        output_format = args.output_format or _detect_format(output_path)
        
        print(f"Converting {input_format} to {output_format}...")
        
        # Load data
        if input_format == 'parquet':
            # Handle partitioned parquet
            if os.path.isdir(input_path):
                connector.load_parquet_partitioned(input_path, "input_data")
            else:
                connector.load_parquet_direct(input_path, "input_data")
        elif input_format == 'json':
            connector.load_json_direct(input_path, "input_data")
        elif input_format == 'csv':
            connector.conn.execute(f"CREATE TABLE input_data AS SELECT * FROM read_csv_auto('{input_path}')")
        else:
            raise ValueError(f"Unsupported input format: {input_format}")
            
        # Apply transformation if specified
        if args.transform:
            connector.conn.execute(f"CREATE TABLE transformed_data AS {args.transform}")
            source_table = "transformed_data"
        else:
            source_table = "input_data"
            
        # Export data
        if output_format == 'parquet':
            if args.partitioned and args.partition_column:
                # Export as partitioned parquet
                os.makedirs(output_path, exist_ok=True)
                connector.execute_partitioned_query(
                    f"SELECT * FROM {source_table}", 
                    args.partition_column, 
                    args.num_partitions or 10,
                    output_path
                )
            else:
                # Export as single parquet file
                connector.export_parquet(f"SELECT * FROM {source_table}", output_path)
        elif output_format == 'arrow':
            connector.export_arrow_ipc(f"SELECT * FROM {source_table}", output_path)
        elif output_format == 'csv':
            connector.conn.execute(f"COPY (SELECT * FROM {source_table}) TO '{output_path}' (FORMAT CSV, HEADER)")
        elif output_format == 'json':
            connector.conn.execute(f"COPY (SELECT * FROM {source_table}) TO '{output_path}' (FORMAT JSON)")
        else:
            raise ValueError(f"Unsupported output format: {output_format}")
            
        # Show summary
        count = connector.conn.execute(f"SELECT COUNT(*) FROM {source_table}").fetchone()[0]
        cols = connector.conn.execute(f"SELECT * FROM {source_table} LIMIT 0").fetchdf().columns
        
        print(f"Conversion complete: {count} rows, {len(cols)} columns")
        print(f"Output saved to {output_path}")
        
        connector.close()
        return 0
    except Exception as e:
        logger.error(f"Error converting data: {e}")
        return 1

def _detect_format(path: str) -> str:
    """Detect file format from path extension"""
    lower_path = path.lower()
    if lower_path.endswith('.parquet') or lower_path.endswith('.pq'):
        return 'parquet'
    elif lower_path.endswith('.json') or lower_path.endswith('.jsonl') or lower_path.endswith('.ndjson'):
        return 'json'
    elif lower_path.endswith('.csv'):
        return 'csv'
    elif lower_path.endswith('.arrow') or lower_path.endswith('.ipc'):
        return 'arrow'
    else:
        return 'unknown'

def cmd_install(args):
    """Install PipeDuck and its components"""
    logger.info("Installing PipeDuck and DuckConnect components...")
    
    # Determine what to install
    install_python = args.python or args.all
    install_r = args.r or args.all
    install_julia = args.julia or args.all
    
    # Get the directory of this script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    parent_dir = os.path.dirname(script_dir)
    
    # Python client installation
    if install_python:
        logger.info("Installing Python client...")
        try:
            # Get paths
            duck_dir = os.path.abspath(parent_dir)
            
            # Install the package in development mode
            cmd = [sys.executable, '-m', 'pip', 'install', '-e', duck_dir]
            result = subprocess.run(cmd, capture_output=True, text=True)
            if result.returncode != 0:
                logger.error(f"Failed to install Python client: {result.stderr}")
                if args.verbose:
                    logger.error(result.stderr)
                return 1
            else:
                logger.info("Python client installed successfully")
                if args.verbose:
                    logger.info(result.stdout)
        except Exception as e:
            logger.error(f"Error installing Python client: {e}")
            return 1
    
    # R client installation
    if install_r:
        logger.info("Installing R client...")
        try:
            # Check if R is installed
            r_result = subprocess.run(['Rscript', '--version'], capture_output=True, text=True)
            if r_result.returncode != 0:
                logger.error("R is not installed or not in PATH. Please install R before continuing.")
                return 1
            
            # Create temporary directory for R installation files
            with tempfile.TemporaryDirectory() as temp_dir:
                # Copy R package to temp dir
                r_pkg_dir = os.path.join(script_dir, 'r', 'duck_connect')
                if os.path.exists(r_pkg_dir):
                    temp_pkg_dir = os.path.join(temp_dir, 'duck_connect')
                    shutil.copytree(r_pkg_dir, temp_pkg_dir)
                    
                    # Create R installation script
                    install_script = os.path.join(temp_dir, 'install_duck_connect.R')
                    with open(install_script, 'w') as f:
                        f.write("""
                            # Install dependencies
                            install.packages(c("DBI", "R6", "jsonlite", "uuid", "yaml"), repos="https://cloud.r-project.org")
                            
                            # Install duck_connect
                            install.packages("{pkg_dir}", repos=NULL, type="source")
                            
                            # Check if installation was successful
                            if ("duck_connect" %in% installed.packages()[,"Package"]) {{
                                cat("Successfully installed duck_connect R package\\n")
                            }} else {{
                                cat("Failed to install duck_connect R package\\n")
                                quit(status=1)
                            }}
                        """.format(pkg_dir=temp_pkg_dir))
                    
                    # Run the installation script
                    r_install = subprocess.run(['Rscript', install_script], capture_output=True, text=True)
                    if r_install.returncode != 0:
                        logger.error(f"Failed to install R client: {r_install.stderr}")
                        if args.verbose:
                            logger.error(r_install.stderr)
                        if not args.continue_on_error:
                            return 1
                    else:
                        logger.info("R client installed successfully")
                        if args.verbose:
                            logger.info(r_install.stdout)
                else:
                    logger.warning("R package directory not found. Skipping R client installation.")
        except Exception as e:
            logger.error(f"Failed to install R client: {e}")
            if not args.continue_on_error:
                return 1
    
    # Julia client installation
    if install_julia:
        logger.info("Installing Julia client...")
        try:
            # Check if Julia is installed
            julia_result = subprocess.run(['julia', '--version'], capture_output=True, text=True)
            if julia_result.returncode != 0:
                logger.error("Julia is not installed or not in PATH. Please install Julia before continuing.")
                return 1
            
            # Create Julia installation script
            with tempfile.NamedTemporaryFile(suffix='.jl', delete=False) as f:
                julia_script = f.name
                f.write("""
                    # Add the package registry if needed
                    import Pkg
                    
                    # Define the package directory
                    pkg_dir = "{pkg_dir}"
                    
                    # Check if directory exists
                    if isdir(pkg_dir)
                        # Develop the local package
                        try
                            Pkg.develop(path=pkg_dir)
                            # Try loading the package to verify installation
                            @eval using DuckConnect
                            println("Successfully installed DuckConnect Julia package")
                        catch e
                            println("Error installing DuckConnect: ", e)
                            exit(1)
                        end
                    else
                        println("Julia package directory not found at: ", pkg_dir)
                        exit(1)
                    end
                """.format(pkg_dir=os.path.join(script_dir, 'julia', 'DuckConnect')).encode('utf-8'))
            
            try:
                # Run the installation script
                julia_install = subprocess.run(['julia', julia_script], capture_output=True, text=True)
                if julia_install.returncode != 0:
                    logger.error(f"Failed to install Julia client: {julia_install.stderr}")
                    if args.verbose:
                        logger.error(julia_install.stderr)
                    if not args.continue_on_error:
                        return 1
                else:
                    logger.info("Julia client installed successfully")
                    if args.verbose:
                        logger.info(julia_install.stdout)
            finally:
                os.unlink(julia_script)
        except Exception as e:
            logger.error(f"Failed to install Julia client: {e}")
            if not args.continue_on_error:
                return 1
    
    # Check if any components failed to install
    if args.continue_on_error:
        logger.warning("Installation completed with some components potentially not installed")
        return 0
    else:
        logger.info("All requested components installed successfully")
        return 0

def main():
    """Main entry point for the CLI"""
    parser = argparse.ArgumentParser(description="PipeDuck: Cross-Language Pipeline Orchestration")
    subparsers = parser.add_subparsers(dest="command", help="Command to execute")
    
    # Run subcommand
    run_parser = subparsers.add_parser("run", help="Run a pipeline")
    run_parser.add_argument("config", help="Pipeline configuration file path")
    run_parser.add_argument("--nodes", help="Comma-separated list of node IDs to run")
    run_parser.add_argument("--start-from", help="Node ID to start execution from")
    run_parser.add_argument("--resume", action="store_true", help="Resume from checkpoint if available")
    run_parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    run_parser.set_defaults(func=cmd_run)
    
    # Visualize subcommand
    viz_parser = subparsers.add_parser("visualize", help="Visualize a pipeline DAG")
    viz_parser.add_argument("config", help="Pipeline configuration file path")
    viz_parser.add_argument("--output", default="pipeline_dag.png", help="Output file path")
    viz_parser.set_defaults(func=cmd_visualize)
    
    # Init subcommand
    init_parser = subparsers.add_parser("init", help="Initialize a new pipeline project")
    init_parser.add_argument("--name", help="Pipeline name")
    init_parser.add_argument("--directory", help="Target directory for the new pipeline")
    init_parser.set_defaults(func=cmd_init)
    
    # Checkpoint subcommand
    checkpoint_parser = subparsers.add_parser("checkpoint", help="Manage pipeline checkpoints")
    checkpoint_subparsers = checkpoint_parser.add_subparsers(dest="checkpoint_command", help="Checkpoint command")
    
    # List checkpoints
    list_parser = checkpoint_subparsers.add_parser("list", help="List available checkpoints")
    list_parser.add_argument("pipeline_id", help="Pipeline ID")
    list_parser.add_argument("--checkpoint-dir", help="Checkpoint directory")
    
    # Restore checkpoint
    restore_parser = checkpoint_subparsers.add_parser("restore", help="Restore from checkpoint")
    restore_parser.add_argument("pipeline_id", help="Pipeline ID")
    restore_parser.add_argument("checkpoint", help="Checkpoint name or ID")
    restore_parser.add_argument("--checkpoint-dir", help="Checkpoint directory")
    
    # Clean checkpoints
    clean_parser = checkpoint_subparsers.add_parser("clean", help="Clean checkpoints")
    clean_parser.add_argument("pipeline_id", help="Pipeline ID")
    clean_parser.add_argument("--checkpoint-dir", help="Checkpoint directory")
    clean_parser.add_argument("--all", action="store_true", help="Clean all checkpoints")
    
    # Set default function for checkpoint
    checkpoint_parser.set_defaults(func=cmd_checkpoint)
    
    # DuckDB optimization subcommand
    duck_parser = subparsers.add_parser("duck-optimize", help="Optimize DuckDB database")
    duck_parser.add_argument("db_path", help="Path to DuckDB database")
    duck_parser.add_argument("--vacuum", action="store_true", help="Run VACUUM operation")
    duck_parser.add_argument("--analyze", action="store_true", help="Run ANALYZE on tables")
    duck_parser.add_argument("--all", action="store_true", help="Run all optimizations")
    duck_parser.set_defaults(func=cmd_duck_optimize)
    
    # Convert subcommand for data conversion
    convert_parser = subparsers.add_parser("convert", help="Convert data between formats")
    convert_parser.add_argument("input", help="Input file path")
    convert_parser.add_argument("output", help="Output file path")
    convert_parser.add_argument("--to-format", help="Output format (auto-detected from extension if not specified)")
    convert_parser.add_argument("--from-format", help="Input format (auto-detected from extension if not specified)")
    convert_parser.add_argument("--compression", choices=["gzip", "bz2", "xz", "zstd", "none"], help="Compression format")
    convert_parser.set_defaults(func=cmd_convert)
    
    # Install subcommand
    install_parser = subparsers.add_parser("install", help="Install PipeDuck components")
    install_parser.add_argument("--all", action="store_true", help="Install all components")
    install_parser.add_argument("--python", action="store_true", help="Install Python client")
    install_parser.add_argument("--r", action="store_true", help="Install R client")
    install_parser.add_argument("--julia", action="store_true", help="Install Julia client")
    install_parser.add_argument("--continue-on-error", action="store_true", help="Continue installation if some components fail")
    install_parser.add_argument("--verbose", action="store_true", help="Enable verbose output")
    install_parser.set_defaults(func=cmd_install)
    
    # Parse arguments
    args = parser.parse_args()
    
    # Set default command
    if args.command is None:
        parser.print_help()
        return 0
    
    # Handle checkpoint subcommand
    if args.command == "checkpoint" and args.checkpoint_command is None:
        checkpoint_parser.print_help()
        return 0
    
    # Execute the appropriate command
    if hasattr(args, 'func'):
        return args.func(args)
    else:
        parser.print_help()
        return 0

if __name__ == "__main__":
    sys.exit(main()) 