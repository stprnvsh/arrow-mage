# DuckData: Cross-Language Data Processing Framework

<div align="center">
  
  ![DuckData Logo](https://via.placeholder.com/300x150?text=DuckData)
  
  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
  [![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
  [![R](https://img.shields.io/badge/R-4.0+-blue.svg)](https://www.r-project.org/)
  [![Julia](https://img.shields.io/badge/julia-1.6+-blue.svg)](https://julialang.org/)

  **Seamless cross-language data pipelines with DuckDB and Apache Arrow**
  
</div>

## 📋 Overview

**DuckData** is a powerful toolkit for building and running data processing pipelines across multiple programming languages with seamless data sharing. It combines the speed of [DuckDB](https://duckdb.org/) with the cross-language capabilities of [Apache Arrow](https://arrow.apache.org/) to create a frictionless data pipeline experience.

<div align="center">
  
```
Python ⟷ R ⟷ Julia ⟷ SQL ⟷ Rust
        all connected by
    DuckDB + Apache Arrow
```

</div>

### Key Features

- 🔄 **Cross-Language Pipelines**: Run pipeline steps in Python, R, Julia, or other languages
- 🚀 **High Performance**: Based on DuckDB and Apache Arrow for speed
- 📊 **Data Sharing**: Seamlessly share data between languages without copies
- 📝 **Simple Configuration**: Define pipelines with easy YAML files
- 📈 **Monitoring**: Track pipeline performance and resource usage
- 🔄 **Data Lineage**: Track data provenance and transformations

## 🚀 Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/yourusername/duckdata.git
cd duckdata

# Run the installation script
./duckdata/install.sh
```

### Simple Example

1. Create a pipeline configuration file `pipeline.yml`:

```yaml
name: Simple Data Pipeline
description: A simple data transformation pipeline
db_path: pipeline.duckdb
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
```

2. Run the pipeline:

```bash
python -m pipelink.python.pipelink pipeline.yml
```

## 🧩 Key Components

DuckData consists of two main components:

<details>
<summary><b>📦 PipeLink: Cross-Language Pipeline Orchestration</b></summary>

PipeLink manages the execution of data processing pipelines with nodes written in different languages.

```python
from pipelink import run_pipeline

# Run a pipeline from Python code
run_pipeline("pipeline.yml")
```

**Key Features:**
- Define pipeline nodes in YAML
- Support for Python, R, Julia nodes
- Automatic dependency resolution
- Pipeline monitoring and visualization
- Resume from failure points

</details>

<details>
<summary><b>🔄 CrossLink: Data Sharing Between Languages</b></summary>

CrossLink provides data transfer between languages using DuckDB and Arrow.

```python
from pipelink.crosslink import CrossLink

# Initialize CrossLink
cl = CrossLink(db_path="data.duckdb")

# Share data with other languages
cl.push(df, name="my_dataset", description="My data")
```

**Key Features:**
- Zero-copy data sharing when possible
- Automatic schema management
- Dataset versioning and history
- Data lineage tracking
- Performance metrics

</details>

## 📊 Data Flow Visualization

<div align="center">
  
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Python    │     │      R      │     │    Julia    │
│   Script    │────▶│   Script    │────▶│   Script    │
└─────────────┘     └─────────────┘     └─────────────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌─────────────────────────────────────────────────────┐
│                  CrossLink Database                  │
│             (DuckDB + Apache Arrow)                  │
└─────────────────────────────────────────────────────┘
```

</div>

## 📚 Documentation

### PipeLink Usage

PipeLink allows you to define and run data processing pipelines across different languages.

#### Pipeline Configuration

Create a YAML file that defines your pipeline:

```yaml
name: My Data Pipeline
description: Description of your pipeline
db_path: data.duckdb
working_dir: ./

nodes:
  - id: node_id
    language: python  # or r, julia
    script: path/to/script.py
    inputs:
      - input_dataset_name
    outputs:
      - output_dataset_name
    params:
      param1: value1
      param2: value2
```

#### Running Pipelines

```bash
# Run the entire pipeline
python -m pipelink.python.pipelink pipeline.yml

# Run specific nodes only
python -m pipelink.python.pipelink pipeline.yml --only node1 node2

# Start from a specific node
python -m pipelink.python.pipelink pipeline.yml --start-from node2
```

### CrossLink Usage

CrossLink provides seamless data sharing between languages.

#### Python

```python
from pipelink.crosslink import CrossLink

# Initialize CrossLink
cl = CrossLink(db_path="data.duckdb")

# Save data
import pandas as pd
df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
cl.push(df, name="my_dataset", description="Sample data")

# Retrieve data
retrieved_df = cl.pull("my_dataset")

# Close connection
cl.close()
```

#### R

```r
library(CrossLink)

# Initialize CrossLink
cl <- crosslink_connect("data.duckdb")

# Save data
data <- data.frame(a = c(1, 2, 3), b = c("x", "y", "z"))
push_data(cl, data, name = "my_dataset", description = "Sample data")

# Retrieve data
retrieved_data <- pull_data(cl, "my_dataset")

# Close connection
close_connection(cl)
```

#### Julia

```julia
using CrossLink

# Initialize CrossLink
cl = CrossLink("data.duckdb")

# Save data
using DataFrames
df = DataFrame(a = [1, 2, 3], b = ["x", "y", "z"])
push(cl, df, "my_dataset", "Sample data")

# Retrieve data
retrieved_df = pull(cl, "my_dataset")

# Close connection
close(cl)
```

## 📐 Architecture

<div align="center">
  
```
┌───────────────────────────────────────────────────────────────┐
│                        PipeLink                                │
│  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌──────────┐  │
│  │  Pipeline │   │   Node    │   │Dependency │   │   Task   │  │
│  │  Manager  │──▶│  Runner   │──▶│  Resolver │──▶│ Executor │  │
│  └───────────┘   └───────────┘   └───────────┘   └──────────┘  │
└───────────────────────────┬───────────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────────┐
│                        CrossLink                               │
│  ┌───────────┐   ┌───────────┐   ┌───────────┐   ┌──────────┐  │
│  │  DuckDB   │   │   Arrow   │   │  Schema   │   │ Versioning│  │
│  │ Connection│◀──▶│ Converter │◀──▶│  Manager  │◀──▶│  Manager │  │
│  └───────────┘   └───────────┘   └───────────┘   └──────────┘  │
└───────────────────────────────────────────────────────────────┘
```

</div>

## 📦 Examples

The repository includes several examples showing how to use DuckData:

<details>
<summary><b>Simple Pipeline Example</b></summary>

A basic example showing data flow between languages:

1. **Python** script generates sample data
2. **R** script transforms the data
3. **Julia** script analyzes the data

[View Simple Pipeline Example](duckdata/examples/simple_pipeline)
</details>

<details>
<summary><b>Data Connector Example</b></summary>

Demonstrates integration with external data sources:

1. Connect to databases or APIs
2. Process data through a pipeline
3. Export results

[View Data Connector Example](duckdata/examples/data_connector_example)
</details>

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👏 Acknowledgements

- [DuckDB](https://duckdb.org/) - The analytical database used for storage
- [Apache Arrow](https://arrow.apache.org/) - The cross-language data format
- All contributors who have helped shape this project 