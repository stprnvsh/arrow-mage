# Simple Pipeline Example

This example demonstrates a simple data transformation pipeline using PipeLink with all supported languages:

1. **Python** (generate_data.py): Generates random sample data
2. **R** (transform_data.R): Transforms the data and adds new features
3. **Rust** (process_data.rs): Further enhances the data with additional computed columns
4. **Julia** (analyze_data.jl): Performs statistical analysis on the enhanced data
5. **Python** (create_report.py): Creates a final report with key metrics

## Prerequisites

Make sure you have installed PipeLink for all languages:

```bash
# Navigate to the root directory of the project
cd ../../
./install.sh
```

This will install the Python, R, Julia, and Rust packages if the respective languages are available on your system.

## Running the Pipeline

To run the pipeline:

```bash
cd examples/simple_pipeline
python -m pipelink.python.pipelink pipeline.yml
```

## Fallback Mechanism

The example scripts include fallback mechanisms to source the required modules directly if the packages are not installed:

- **Python**: Always requires the Python package to be installed
- **R**: Will try to source the R files directly if the R package is not installed
- **Julia**: Will try to include the Julia modules directly if the Julia package is not installed
- **Rust**: Requires the Rust package to be built and installed

## Expected Output

After running the pipeline, you should see output similar to:

```
INFO - pipelink - Executing pipeline: Simple Data Transformation Pipeline
INFO - pipelink - Nodes to execute: generate_data, transform_data, process_data, analyze_data, create_report
INFO - pipelink - Running node 'generate_data' (python)
Generated 1000 rows of data
INFO - pipelink - Node 'generate_data' completed successfully
INFO - pipelink - Running node 'transform_data' (r)
Transformed data: added 4 new features
INFO - pipelink - Node 'transform_data' completed successfully
INFO - pipelink - Running node 'process_data' (rust)
Processed data and added 3 new features
INFO - pipelink - Node 'process_data' completed successfully
INFO - pipelink - Running node 'analyze_data' (julia)
Analysis complete with 30+ statistics calculated
INFO - pipelink - Node 'analyze_data' completed successfully
INFO - pipelink - Running node 'create_report' (python)
Report created successfully with 15+ metrics
INFO - pipelink - Node 'create_report' completed successfully
INFO - pipelink - Pipeline 'Simple Data Transformation Pipeline' completed successfully
```

A DuckDB database file `simple_pipeline.duckdb` will be created in the current directory, containing the data shared between the pipeline nodes. 