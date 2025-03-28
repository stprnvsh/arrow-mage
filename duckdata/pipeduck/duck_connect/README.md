# DuckConnect - Cross-Language Data Sharing

DuckConnect is the core module of PipeDuck that enables seamless data sharing between Python, R, and Julia using DuckDB as an intermediate storage layer.

## Overview

DuckConnect solves the problem of inefficient data transfers between different programming languages by:

1. Using DuckDB as a shared data layer
2. Avoiding expensive serialization/deserialization operations
3. Managing metadata to track datasets across languages
4. Providing a consistent API across Python, R, and Julia

## Architecture

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│   Python    │     │      R      │     │    Julia    │
│  Application│     │ Application │     │ Application │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       ▼                   ▼                   ▼
┌──────┴──────┐     ┌──────┴──────┐     ┌──────┴──────┐
│  Python API │     │    R API    │     │  Julia API  │
└──────┬──────┘     └──────┬──────┘     └──────┬──────┘
       │                   │                   │
       └───────────┬───────┴───────────┬───────┘
                   ▼                   ▼
         ┌─────────────────────────────────────┐
         │            DuckDB Layer             │
         │                                     │
         │  ┌─────────────┐  ┌─────────────┐  │
         │  │   Metadata  │  │ Transaction │  │
         │  │    Tables   │  │    Logs     │  │
         │  └─────────────┘  └─────────────┘  │
         │                                     │
         │  ┌─────────────┐  ┌─────────────┐  │
         │  │   Lineage   │  │ Performance │  │
         │  │   Tracking  │  │     Stats   │  │
         │  └─────────────┘  └─────────────┘  │
         └─────────────────────────────────────┘
```

## Features

- **Zero-copy data sharing**: Data remains in DuckDB and is not copied between languages
- **Metadata management**: Track datasets, transformations, and lineage
- **Transactional operations**: Ensure data consistency across languages
- **Performance monitoring**: Track performance metrics for optimizations
- **Support for all common data types**: Work with tables, arrays, and specialized types

## Language Support

DuckConnect is available in all three languages with feature parity:

- **Python**: Full implementation with pandas integration
- **R**: Complete R6 class implementation
- **Julia**: Native Julia implementation

## Usage Examples

### Python

```python
from pipeduck.duck_connect import DuckConnect

# Create a connection
dc = DuckConnect("my_database.duckdb")

# Register a pandas DataFrame
import pandas as pd
data = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})
dc.register_dataset(data, name="python_data")

# Access data from other languages
r_data = dc.get_dataset("r_data")
```

### R

```r
library(pipeduck)

# Create a connection
dc <- DuckConnect$new("my_database.duckdb")

# Register a data frame
data <- data.frame(x = 1:3, y = 4:6)
dc$register_dataset(data, name = "r_data")

# Access data from other languages
python_data <- dc$get_dataset("python_data")
```

### Julia

```julia
using DuckConnect

# Create a connection
dc = DuckConnect("my_database.duckdb")

# Register a DataFrame
using DataFrames
data = DataFrame(i = 1:3, j = 4:6)
register_dataset(dc, data, name="julia_data")

# Access data from other languages
python_data = get_dataset(dc, "python_data")
```

## Core API

All three language implementations share the same core API:

| Operation | Description |
|-----------|-------------|
| Connect to database | Initialize with a DuckDB database file |
| Register dataset | Store a data structure in the database |
| Get dataset | Retrieve a dataset by name |
| Update dataset | Update an existing dataset |
| Delete dataset | Remove a dataset |
| List datasets | Show all available datasets |
| Execute query | Run SQL directly on the database |
| Register transformation | Track data lineage information |

## Advanced Features

### Metadata Management

DuckConnect maintains metadata tables to track:
- Dataset information (name, schema, source language)
- Access permissions (which languages can use each dataset)
- Creation/modification timestamps

### Lineage Tracking

Track how data flows between languages:
```python
# Python
dc.register_transformation(
    target_dataset_id="result_data",
    source_dataset_ids=["input_data1", "input_data2"],
    transformation="merge and aggregate"
)
```

### Performance Monitoring

Automatically track performance statistics:
- Data processing times
- Dataset sizes
- Query execution times

## Installation

DuckConnect is installed as part of the language-specific modules. See the individual language README files for details.

## Implementation Details

DuckConnect uses these DuckDB features:
- Memory-mapped database files for cross-process access
- Efficient columnar storage for analytical workloads
- SQL interface for data manipulation
- Schema-on-read for flexibility

## Contributing

Contributions to DuckConnect are welcome! Please see the main project README for guidelines. 