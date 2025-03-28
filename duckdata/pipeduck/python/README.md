# PipeDuck Python Module

This directory contains the Python implementation for PipeDuck, providing high-performance data processing capabilities within Python.

## Installation

The Python module is installed automatically when you install the full DuckData package:

```bash
# Install from the repository root
pip install -e .

# Or use the installation script for all languages
./install_all.sh
```

## Usage

### Basic Usage

```python
import pipeduck
from pipeduck import duck_connect

# Create a connection to DuckDB
dc = duck_connect.DuckConnect("my_database.duckdb")

# Register a pandas DataFrame
import pandas as pd
data = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'value': [10, 20, 30, 40, 50]
})
dc.register_dataset(data, name="my_data")

# Run a SQL query
result = dc.execute_query("SELECT * FROM my_data WHERE value > 25")
print(result)
```

### Creating and Running Pipelines

```python
from pipeduck import Pipeline

# Create a pipeline
pipeline = Pipeline("my_pipeline")

# Define pipeline steps
pipeline.add_step("load_data", lambda: pd.read_csv("data.csv"))
pipeline.add_step("transform", lambda data: data.assign(doubled=data['value'] * 2))
pipeline.add_step("filter", lambda data: data[data['doubled'] > 50])

# Run the pipeline
results = pipeline.run()
```

### Cross-Language Integration

```python
import pipeduck

# Run an R script within your Python pipeline
r_result = pipeduck.run_r_script("path/to/script.R")

# Run a Julia script within your Python pipeline
julia_result = pipeduck.run_julia_script("path/to/script.jl")

# Access data shared from other languages
from pipeduck import duck_connect
dc = duck_connect.DuckConnect("shared_database.duckdb")
data_from_r = dc.get_dataset("r_processed_data")
```

## API Reference

### DuckConnect

The main class for data sharing and persistence.

- `DuckConnect(db_path)`: Create a connection to a DuckDB database
- `register_dataset(data, name, description=None)`: Register a DataFrame in the database
- `get_dataset(name)`: Retrieve a dataset by name
- `execute_query(query, params=None)`: Run a SQL query against the database
- `list_datasets()`: List all available datasets

### Pipeline

For defining and running data processing pipelines.

- `Pipeline(name)`: Create a new pipeline
- `add_step(name, function, dependencies=None)`: Add a processing step
- `run(inputs=None)`: Execute the pipeline with optional input data
- `visualize()`: Generate a visualization of the pipeline

## Examples

Check out the `examples` directory for more detailed examples of using the Python module.

## Contributing

To contribute to the Python module, please follow the project's contribution guidelines in the main README. 