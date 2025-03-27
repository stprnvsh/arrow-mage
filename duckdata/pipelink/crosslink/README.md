# 🔄 CrossLink: Cross-Language Data Sharing

<div align="center">

![CrossLink Logo](https://via.placeholder.com/300x150?text=CrossLink)

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../../LICENSE)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![R](https://img.shields.io/badge/R-4.0+-blue.svg)](https://www.r-project.org/)
[![Julia](https://img.shields.io/badge/julia-1.6+-blue.svg)](https://julialang.org/)
[![C++](https://img.shields.io/badge/C++-17-blue.svg)](https://isocpp.org/)

**Seamlessly share data between Python, R, Julia, and C++ with DuckDB and Apache Arrow**

</div>

## 🌟 Overview

**CrossLink** is the data sharing component of the DuckData framework, enabling seamless transfer of data between different programming languages. It combines the speed of [DuckDB](https://duckdb.org/) with the cross-language capabilities of [Apache Arrow](https://arrow.apache.org/) to provide efficient, zero-copy data sharing when possible.

<div align="center">

```
┌───────────┐   ┌───────────┐   ┌───────────┐   ┌───────────┐
│  Python   │   │     R     │   │   Julia   │   │    C++    │
│  Script   │◀─▶│  Script   │◀─▶│  Script   │◀─▶│  Program  │
└─────┬─────┘   └─────┬─────┘   └─────┬─────┘   └─────┬─────┘
      │               │               │               │
      └───────────────┼───────────────┼───────────────┘
                      │               │
                ┌─────▼───────────────▼─────┐
                │      CrossLink Layer      │
                └─────┬───────────────┬─────┘
                      │               │
             ┌────────▼────┐   ┌─────▼──────┐
             │   DuckDB    │   │   Arrow    │
             │  Database   │   │  Format    │
             └─────────────┘   └────────────┘
```

</div>

## ✨ Key Features

- 🚀 **Zero-Copy Data Sharing**: Utilize zero-copy optimizations when possible
- 🔄 **Multiple Language Support**: Python, R, Julia, and C++ interfaces
- 📊 **Consistent API**: Uniform API across all supported languages
- 📝 **Schema Management**: Automatic schema inference and validation
- 🔄 **Versioning**: Track dataset versions and history
- 🔍 **Metadata**: Store and retrieve dataset descriptions and metadata
- 📈 **Performance Monitoring**: Track data transfer performance metrics
- 💾 **Storage Options**: In-memory or file-based storage

## 🚀 Quick Start

### Python

```python
from pipelink.crosslink import CrossLink

# Initialize CrossLink
cl = CrossLink(db_path="data.duckdb")

# Push data to the shared database
import pandas as pd
df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
cl.push(df, name="my_dataset", description="Sample data")

# Pull data from the shared database
retrieved_df = cl.pull("my_dataset")

# Execute a SQL query
result_df = cl.query("SELECT * FROM my_dataset WHERE a > 1")

# Close connection
cl.close()
```

### R

```r
library(CrossLink)

# Initialize CrossLink
cl <- crosslink_connect("data.duckdb")

# Push data to the shared database
data <- data.frame(a = c(1, 2, 3), b = c("x", "y", "z"))
push_data(cl, data, name = "my_dataset", description = "Sample data")

# Pull data from the shared database
retrieved_data <- pull_data(cl, "my_dataset")

# Execute a SQL query
result_data <- run_query(cl, "SELECT * FROM my_dataset WHERE a > 1")

# Close connection
close_connection(cl)
```

### Julia

```julia
using CrossLink

# Initialize CrossLink
cl = CrossLink("data.duckdb")

# Push data to the shared database
using DataFrames
df = DataFrame(a = [1, 2, 3], b = ["x", "y", "z"])
push(cl, df, "my_dataset", "Sample data")

# Pull data from the shared database
retrieved_df = pull(cl, "my_dataset")

# Execute a SQL query
result_df = query(cl, "SELECT * FROM my_dataset WHERE a > 1")

# Close connection
close(cl)
```

### C++

```cpp
#include "crosslink/crosslink.hpp"

int main() {
    // Initialize CrossLink
    CrossLink cl("data.duckdb");
    
    // Create an Arrow table
    std::shared_ptr<arrow::Table> table = CreateSampleTable();
    
    // Push data to the shared database
    cl.Push(table, "my_dataset", "Sample data");
    
    // Pull data from the shared database
    std::shared_ptr<arrow::Table> retrieved_table = cl.Pull("my_dataset");
    
    // Execute a SQL query
    std::shared_ptr<arrow::Table> result_table = 
        cl.Query("SELECT * FROM my_dataset WHERE a > 1");
    
    // Close connection
    cl.Close();
    
    return 0;
}
```

## 📋 API Reference

### Common API Across Languages

| Function | Description |
|----------|-------------|
| `connect/init` | Connect to a DuckDB database |
| `push` | Save a dataframe/table to the database |
| `pull` | Retrieve a dataset as a dataframe/table |
| `query` | Execute a SQL query and return results |
| `list_datasets` | List all available datasets |
| `get_schema` | Get the schema of a dataset |
| `get_metadata` | Retrieve metadata for a dataset |
| `close` | Close the database connection |

### Python-Specific Methods

```python
# List available datasets
datasets = cl.list_datasets()

# Get schema information
schema = cl.get_schema("my_dataset")

# Get metadata
metadata = cl.get_metadata("my_dataset")

# Create a table from a SQL query
cl.create_table("filtered_data", "SELECT * FROM my_dataset WHERE a > 1")

# Export a dataset to file
cl.export_to_parquet("my_dataset", "output.parquet")
cl.export_to_csv("my_dataset", "output.csv")

# Import from file
cl.import_from_parquet("new_dataset", "input.parquet")
cl.import_from_csv("new_dataset", "input.csv")
```

## 🧩 Architecture

CrossLink consists of several core components:

- **Connection Manager**: Handles database connections and configuration
- **Data Converter**: Translates between language-specific data structures and Arrow format
- **Schema Manager**: Maintains data schema information and validation
- **Query Engine**: Processes SQL queries against the shared database
- **Metadata Service**: Stores and retrieves dataset descriptions and metadata
- **Version Tracker**: Maintains version history of datasets

## 📚 Documentation

For more detailed documentation and advanced usage examples, see the [DuckData Documentation](https://github.com/yourusername/duckdata/docs).

## 🤝 Contributing

Contributions to CrossLink are welcome! Please feel free to submit a Pull Request.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](../../../LICENSE) file for details. 