# DuckConnect - Julia Implementation

This directory contains the Julia implementation of DuckConnect, providing high-performance cross-language data sharing capabilities.

## Installation

The Julia version of DuckConnect can be installed using one of the following methods:

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
register_dataset(dc, data, name="my_dataset")

# Get a dataset
result = get_dataset(dc, "my_dataset")
println(result)

# Execute a SQL query
query_result = execute_query(dc, "SELECT * FROM my_dataset WHERE value > 25")
println(query_result)
```

### DuckContext for Pipeline Integration

```julia
using DuckConnect

# In a pipeline node
function process_node()
    # Initialize context
    ctx = DuckContext(db_path="pipeline.duckdb")
    
    # Get input data
    input_data = get_input(ctx, "input_dataset")
    
    # Process data
    processed = copy(input_data)
    processed.doubled = processed.value .* 2
    
    # Set output data
    set_output(ctx, processed, "output_dataset")
    
    return "Success"
end
```

### Working with Arrow Data (optional)

```julia
using DuckConnect
using DataFrames
using Arrow

# Create a DuckConnect instance
dc = DuckConnect("my_database.duckdb")

# Create data
data = DataFrame(
    id = 1:4,
    name = ["a", "b", "c", "d"]
)

# Save as Arrow file
Arrow.write("temp_data.arrow", data)

# Register from Arrow file (if arrow integration is available)
arrow_data = Arrow.Table("temp_data.arrow") |> DataFrame
register_dataset(dc, arrow_data, name="arrow_data")
```

### Tracking Data Lineage

```julia
using DuckConnect
using DataFrames

# Create a DuckConnect instance
dc = DuckConnect("my_database.duckdb")

# Register source datasets
register_dataset(dc, source1_df, name="source1")
register_dataset(dc, source2_df, name="source2")

# Process and create a new dataset
merged_df = process_and_merge(source1_df, source2_df)
target_id = register_dataset(dc, merged_df, name="merged_result")

# Track the lineage
register_transformation(dc,
    target_dataset_id=target_id,
    source_dataset_ids=["source1", "source2"],
    transformation="merged and aggregated"
)
```

## API Reference

### Core Functions

```julia
"""
    DuckConnect(db_path::String; debug::Bool=false)

Initialize a connection to a DuckDB database.
"""
function DuckConnect(db_path::String; debug::Bool=false)

"""
    register_dataset(dc::DuckConnect, data; 
                    name::String,
                    description::Union{String, Nothing}=nothing,
                    available_to_languages::Vector{String}=["python", "r", "julia"],
                    overwrite::Bool=false)

Register a dataset in the database.
"""
function register_dataset(dc::DuckConnect, data; name::String, ...)

"""
    get_dataset(dc::DuckConnect, id_or_name::String)

Retrieve a dataset by ID or name.
"""
function get_dataset(dc::DuckConnect, id_or_name::String)

"""
    update_dataset(dc::DuckConnect, data, id_or_name::String)

Update an existing dataset.
"""
function update_dataset(dc::DuckConnect, data, id_or_name::String)

"""
    delete_dataset(dc::DuckConnect, id_or_name::String)

Delete a dataset.
"""
function delete_dataset(dc::DuckConnect, id_or_name::String)

"""
    list_datasets(dc::DuckConnect; language::Union{String, Nothing}=nothing)

List all available datasets.
"""
function list_datasets(dc::DuckConnect; language::Union{String, Nothing}=nothing)

"""
    execute_query(dc::DuckConnect, query::String, params::Vector=[])

Execute a SQL query against the database.
"""
function execute_query(dc::DuckConnect, query::String, params::Vector=[])

"""
    register_transformation(dc::DuckConnect,
                           target_dataset_id::String,
                           source_dataset_ids::Vector{String},
                           transformation::String)

Register a transformation for lineage tracking.
"""
function register_transformation(dc::DuckConnect, ...)
```

### DuckContext

```julia
"""
    DuckContext(db_path::String)

Create a duck context for pipeline nodes.
"""
mutable struct DuckContext
    duck_connect::DuckConnect
    datasets::Dict{String, String}  # name -> id mapping
end

"""
    get_input(ctx::DuckContext, name; required=true)

Get an input dataset.
"""
function get_input(ctx::DuckContext, name; required=true)

"""
    set_output(ctx::DuckContext, data, name)

Set an output dataset.
"""
function set_output(ctx::DuckContext, data, name)
```

## Julia-Specific Features

- **Native Julia Implementation**: Designed following Julia idioms and practices
- **DataFrames.jl Integration**: Seamless integration with DataFrames
- **Multiple Dispatch Design**: Makes use of Julia's type system
- **Arrow.jl Compatibility**: Optional Arrow support for better performance
- **Modern Julia Features**: Uses Julia 1.8+ features for better performance

## Architecture

DuckConnect for Julia is organized in a modular structure:

- **core.jl**: Core connection management functionality
- **metadata.jl**: Dataset metadata handling
- **transactions.jl**: Transaction and statistics tracking
- **facade.jl**: Main DuckConnect API and DuckContext implementation

## Dependencies

- DuckDB.jl
- DataFrames.jl
- Dates
- UUIDs
- JSON.jl
- YAML.jl
- Statistics

### Optional Dependencies

- Arrow.jl

## Performance Considerations

- For large datasets, consider using Arrow tables if available
- Use SQL queries for filtering/aggregation when possible (pushdown to DuckDB)
- For complex transformations, consider registering intermediate datasets
- Julia's vectorized operations provide excellent performance for calculations

## Troubleshooting

### Common Issues

1. **Connection Issues**: If you encounter connection problems, ensure the database path is accessible and has the correct permissions.

2. **Missing Dependencies**: Make sure all required packages are installed with `Pkg.instantiate()`.

3. **Schema Mismatches**: When updating datasets, ensure the schema is compatible with the existing data.

## DataFrames.jl Integration

DuckConnect integrates well with DataFrames.jl:

```julia
using DuckConnect
using DataFrames

# Create a DuckConnect instance
dc = DuckConnect("my_database.duckdb")

# Get a dataset
data = get_dataset(dc, "my_dataset")

# Process with DataFrames
result = filter(row -> row.value > 20, data)
result = transform(result, :value => (x -> x * 2) => :doubled)
result = combine(groupby(result, :category), :value => mean => :avg_value)

# Register the result
register_dataset(dc, result, name="processed_data")
```

## Contributing

Contributions to the Julia implementation are welcome! Please see the main project README for guidelines. 