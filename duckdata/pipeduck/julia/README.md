# PipeDuck Julia Module

This directory contains the Julia implementation for PipeDuck, providing high-performance data processing capabilities within Julia.

## Installation

The Julia module can be installed using one of the following methods:

### Using the installation script (recommended)

```bash
# From the repository root
./install_all.sh
```

### Manual installation

```julia
# Start Julia and run:
using Pkg
Pkg.activate("path/to/duckdata/pipeduck/duck_connect/julia")
Pkg.instantiate()
Pkg.develop(path="path/to/duckdata/pipeduck/duck_connect/julia")
```

## Usage

### Basic Usage

```julia
# Import the module
using DuckConnect

# Create a connection to DuckDB
dc = DuckConnect("my_database.duckdb")

# Create a DataFrame
using DataFrames
data = DataFrame(
    id = 1:5,
    value = [10, 20, 30, 40, 50]
)

# Register the DataFrame
register_dataset(dc, data, name="my_data")

# Run a SQL query
result = execute_query(dc, "SELECT * FROM my_data WHERE value > 25")
println(result)
```

### Creating and Running Pipelines

```julia
using DuckConnect
using DataFrames
using CSV

# Create a pipeline
pipeline = Pipeline("my_pipeline")

# Define pipeline steps
add_step!(pipeline, "load_data") do
    CSV.read("data.csv", DataFrame)
end

add_step!(pipeline, "transform") do data
    transform!(data, :value => (x -> x * 2) => :doubled)
    return data
end

add_step!(pipeline, "filter") do data
    filter(row -> row.doubled > 50, data)
end

# Run the pipeline
results = run!(pipeline)
```

### Cross-Language Integration

```julia
using DuckConnect
using DataFrames

# Access data shared from other languages
dc = DuckConnect("shared_database.duckdb")

# Get data from Python or R
python_data = get_dataset(dc, "python_processed_data")

# Process the data in Julia
processed_data = transform!(
    copy(python_data), 
    [:value, :id] => ((v, i) -> v ./ i) => :ratio
)

# Register it for other languages to use
register_dataset(dc, processed_data, name="julia_processed_data")
```

## API Reference

### Core Functions

- `DuckConnect(db_path::String)`: Create a connection to a DuckDB database
- `register_dataset(dc, data, name, description=nothing)`: Register a DataFrame in the database
- `get_dataset(dc, name)`: Retrieve a dataset by name
- `execute_query(dc, query, params=[])`: Run a SQL query against the database
- `list_datasets(dc)`: List all available datasets
- `update_dataset(dc, data, name)`: Update an existing dataset
- `delete_dataset(dc, name)`: Delete a dataset

### Pipeline Functions

- `Pipeline(name::String)`: Create a new pipeline
- `add_step!(pipeline, name, function; dependencies=nothing)`: Add a processing step
- `run!(pipeline, inputs=nothing)`: Execute the pipeline with optional input data
- `visualize(pipeline)`: Generate a visualization of the pipeline

### DuckContext

- `DuckContext(db_path::String)`: Create a duck context
- `get_input(ctx, name)`: Get an input dataset
- `set_output(ctx, data, name)`: Set an output dataset

## Examples

### Example: Data Analysis in Julia

```julia
using DuckConnect
using DataFrames
using Statistics

# Create DuckConnect instance
dc = DuckConnect("example.duckdb")

# Import data from a Python-generated source
iris = get_dataset(dc, "iris")

# Perform analysis in Julia
by_species = combine(
    groupby(iris, :Species),
    :Sepal_Length => mean => :avg_sepal_length,
    :Petal_Length => mean => :avg_petal_length,
    :Sepal_Width => std => :std_sepal_width
)

# Add new calculations using Julia's capabilities
transform!(by_species, 
    [:avg_sepal_length, :avg_petal_length] => 
    ((s, p) -> s ./ p) => :sepal_petal_ratio
)

# Register results for cross-language access
register_dataset(dc, by_species, name="julia_iris_analysis")
```

## Dependencies

The Julia module depends on the following packages:
- DuckDB
- DataFrames
- Dates
- UUIDs
- JSON
- YAML
- Statistics

## Contributing

To contribute to the Julia module, please follow the project's contribution guidelines in the main README. 