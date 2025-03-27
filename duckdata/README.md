# PipeLink: Cross-Language Pipeline Orchestration

PipeLink is a framework that enables building and executing data processing pipelines across Python, R, and Julia with seamless data sharing between steps.

## Features

- **YAML-based pipeline definition**: Simple, readable pipeline configuration
- **Cross-language data flow**: Execute pipeline steps in Python, R, and Julia
- **Dependency management**: Automatically order execution based on data dependencies
- **Consistent API**: Similar programming model across all supported languages
- **Built-in logging**: Track execution progress and debug issues
- **Selective execution**: Run specific pipeline nodes or resume from a specific step

## Installation

PipeLink consists of three packages, one for each supported language:

1. Python package (`pipelink`)
2. R package (`pipelink`)
3. Julia package (`PipeLink`)

You can install all three packages at once using the provided installation script:

```bash
# Clone the repository
git clone https://github.com/yourusername/pipelink.git
cd pipelink

# Run the installation script
./install.sh
```

The installation script will:
- Install the Python package in development mode
- Install the R package if R is available
- Install the Julia package if Julia is available

### Manual Installation

If you prefer to install the packages manually:

#### Python
```bash
pip install -e .
```

#### R
```r
install.packages("r-pipelink", repos = NULL, type = "source")
```

#### Julia
```julia
using Pkg
Pkg.develop(path="jl-pipelink")
```

### Dependencies

PipeLink has the following dependencies:

- Python 3.7+
- R 4.0+ (optional)
- Julia 1.6+ (optional)

Required packages for each language:

#### Python
```
pyyaml, pandas, duckdb, numpy, networkx
```

#### R
```
R6, duckdb, DBI, yaml, jsonlite, uuid
```

#### Julia
```
DataFrames, DuckDB, JSON, UUIDs, YAML
```

## Quick Start

### 1. Define your pipeline in YAML

Create a file named `pipeline.yml`:

```yaml
name: Simple Data Transformation Pipeline
description: A simple pipeline demonstrating cross-language data processing
db_path: crosslink.duckdb  # Path to CrossLink database
working_dir: ./            # Working directory for scripts

nodes:
  - id: generate_data
    language: python
    script: scripts/generate_data.py
    outputs:
      - raw_data
    params:
      rows: 1000
      
  - id: transform_data
    language: r
    script: scripts/transform_data.R
    inputs:
      - raw_data
    outputs:
      - transformed_data
      
  - id: analyze_data
    language: julia
    script: scripts/analyze_data.jl
    inputs:
      - transformed_data
    outputs:
      - analysis_results
      
  - id: create_report
    language: python
    script: scripts/create_report.py
    inputs:
      - transformed_data
      - analysis_results
    outputs:
      - final_report
```

### 2. Create scripts for each node

See the `examples/simple_pipeline` directory for complete examples of scripts in each language.

### 3. Run the pipeline

```bash
python -m pipelink.python.pipelink pipeline.yml
```

## API Reference

PipeLink provides a consistent API across all supported languages:

### Python

```python
from pipelink import NodeContext

with NodeContext() as ctx:
    # Get input dataset
    df = ctx.get_input("input_name")
    
    # Get parameter
    param = ctx.get_param("param_name", default_value)
    
    # Save output
    ctx.save_output("output_name", result_df, "Description")
```

### R

```r
library(pipelink)

with_node_context(function(ctx) {
    # Get input dataset
    df <- ctx$get_input("input_name")
    
    # Get parameter
    param <- ctx$get_param("param_name", default_value)
    
    # Save output
    ctx$save_output("output_name", result_df, "Description")
})
```

### Julia

```julia
using PipeLink

function main()
    ctx = NodeContext()
    try
        # Get input dataset
        df = get_input(ctx, "input_name")
        
        # Get parameter
        param = get_param(ctx, "param_name", default_value)
        
        # Save output
        save_output(ctx, "output_name", result_df, "Description")
    finally
        close(ctx)
    end
end
```

## CrossLink: The Data Sharing Layer

PipeLink includes CrossLink, a module that handles data sharing between different programming languages through a common DuckDB database.

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 