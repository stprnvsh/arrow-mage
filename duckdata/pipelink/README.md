# 🔄 PipeLink: Cross-Language Pipeline Orchestration

<div align="center">

![PipeLink Logo](https://via.placeholder.com/300x150?text=PipeLink)

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![R](https://img.shields.io/badge/R-4.0+-blue.svg)](https://www.r-project.org/)
[![Julia](https://img.shields.io/badge/julia-1.6+-blue.svg)](https://julialang.org/)

**Build and run data pipelines across Python, R, and Julia with seamless data sharing**

</div>

## 🌟 Overview

**PipeLink** is the pipeline orchestration component of the DuckData framework that enables building and executing data processing pipelines across multiple programming languages. It manages the execution flow, dependencies, and data sharing between pipeline nodes in different languages.

<div align="center">

```
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Definition                      │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                     Pipeline Execution                       │
│                                                             │
│  ┌─────────┐      ┌─────────┐      ┌─────────┐              │
│  │ Python  │      │    R    │      │  Julia  │              │
│  │  Node   │─────▶│  Node   │─────▶│  Node   │              │
│  └─────────┘      └─────────┘      └─────────┘              │
│                                                             │
└───────────────────────────────┬─────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────┐
│                      Data Management                         │
│                (DuckDB + Apache Arrow)                       │
└─────────────────────────────────────────────────────────────┘
```

</div>

## ✨ Key Features

- 🔄 **Multi-language Support**: Run pipeline nodes in Python, R, Julia, and SQL
- 📊 **Declarative Pipelines**: Define pipelines using simple YAML configuration
- 🧩 **Dependency Management**: Automatic resolution of node dependencies and execution order
- 📈 **DAG Visualization**: Visualize pipeline structure and execution flow
- 🔍 **Monitoring & Logging**: Track execution progress, performance, and resource usage
- ⏱️ **Resume Capability**: Continue pipeline execution from failure points
- 🔄 **Parameter Passing**: Pass parameters between nodes across languages
- 📋 **Execution Reports**: Generate detailed execution reports for analysis

## 🚀 Quick Start

### Define a Pipeline

Create a YAML file that defines your pipeline:

```yaml
name: My Data Pipeline
description: A sample data processing pipeline
db_path: data.duckdb
working_dir: ./

nodes:
  - id: generate_data
    language: python
    script: scripts/generate.py
    outputs:
      - raw_data
    params:
      rows: 1000
      
  - id: transform_data
    language: r
    script: scripts/transform.R
    inputs:
      - raw_data
    outputs:
      - transformed_data
      
  - id: analyze_data
    language: julia
    script: scripts/analyze.jl
    inputs:
      - transformed_data
    outputs:
      - results
```

### Run a Pipeline

```bash
# Run the entire pipeline
python -m pipelink.python.pipelink pipeline.yml

# Run specific nodes only
python -m pipelink.python.pipelink pipeline.yml --only generate_data transform_data

# Start from a specific node
python -m pipelink.python.pipelink pipeline.yml --start-from transform_data
```

### Python API

```python
from pipelink import run_pipeline

# Run a pipeline from Python code
run_pipeline("pipeline.yml")

# With custom parameters
run_pipeline("pipeline.yml", params={"data_size": 2000, "output_path": "results/"})
```

## 📋 Node Implementations

### Python Node

```python
# scripts/generate.py
from pipelink import NodeContext

def run(ctx: NodeContext):
    # Get parameters
    rows = ctx.params.get("rows", 1000)
    
    # Generate data
    import pandas as pd
    import numpy as np
    
    data = pd.DataFrame({
        'id': range(rows),
        'value': np.random.rand(rows),
        'category': np.random.choice(['A', 'B', 'C'], rows)
    })
    
    # Save output
    ctx.outputs.raw_data = data
    
    # Return success
    return True
```

### R Node

```r
# scripts/transform.R

# Load input data
raw_data <- inputs$raw_data

# Transform data
transformed <- raw_data
transformed$log_value <- log(raw_data$value + 1)
transformed$category_code <- as.integer(factor(raw_data$category))

# Save output
outputs$transformed_data <- transformed
```

### Julia Node

```julia
# scripts/analyze.jl

# Load input data
transformed_data = inputs["transformed_data"]

# Analyze data
using Statistics
results = DataFrame(
    category = unique(transformed_data.category),
    mean_value = [mean(transformed_data[transformed_data.category .== c, :].value) for c in unique(transformed_data.category)],
    count = [sum(transformed_data.category .== c) for c in unique(transformed_data.category)]
)

# Save output
outputs["results"] = results
```

## 🧩 Architecture

PipeLink consists of several core components:

- **Pipeline Manager**: Parses pipeline definitions and manages overall execution
- **Node Runner**: Executes individual pipeline nodes in their respective languages
- **Dependency Resolver**: Determines execution order based on input/output dependencies
- **Data Manager**: Handles data transfer between nodes using the CrossLink component
- **Monitoring System**: Tracks resource usage, progress, and performance metrics
- **Dashboard**: Web interface for monitoring pipeline execution and results

## 📚 Documentation

For more detailed documentation and advanced usage examples, see the [DuckData Documentation](https://github.com/yourusername/duckdata/docs).

## 🤝 Contributing

Contributions to PipeLink are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../../LICENSE) file for details.

## Performance Optimization: Language Daemons

PipeLink now includes a language daemon feature to dramatically reduce the overhead of starting new language processes for each pipeline step. This is especially beneficial for Julia, which has significant startup time.

### How It Works

Instead of starting a new process for each language node (Python, R, Julia), PipeLink can maintain persistent daemon processes for each language. When a pipeline step needs to execute code in a particular language, it sends the execution request to the appropriate daemon process instead of starting a new one.

### Benefits

- **Reduced startup overhead**: Eliminates the 4-5 second startup time for Julia and 1-2 seconds for R
- **Improved performance**: Makes cross-language pipelines perform closer to single-language ones
- **Better resource utilization**: Avoids repeatedly loading the same libraries and runtime environments

### Using Daemons

Language daemons are enabled by default when running pipelines. You can:

1. **Start daemons manually before running pipelines**:
   ```bash
   # Start all available language daemons
   pipelink daemon start
   
   # Start specific language daemons
   pipelink daemon start --languages python,julia
   
   # Check daemon status
   pipelink daemon status
   
   # Stop daemons when done
   pipelink daemon stop
   ```

2. **Use the convenience script**:
   ```bash
   ./start_daemons.sh
   ```

3. **Disable daemons if needed**:
   ```bash
   pipelink run pipeline.yml --no-daemon
   ```

### Daemon Management

Daemons will remain running until explicitly stopped or the system is restarted. They use minimal resources when idle and are automatically started when needed if not already running.

You can view the status and control daemons with:
```bash
# Check status
pipelink daemon status

# Stop all daemons
pipelink daemon stop

# Stop specific daemons
pipelink daemon stop --languages julia
``` 