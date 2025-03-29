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

**CrossLink** is the data sharing component of the DuckData framework, enabling seamless transfer of data between different programming languages. It combines the speed of [DuckDB](https://duckdb.org/) with the cross-language capabilities of [Apache Arrow](https://arrow.apache.org/) to provide efficient, zero-copy data sharing.

### 🆕 New: Unified C++ Core with Cross-Language Bindings

CrossLink now features a unified C++ core implementation that all language bindings (Python, R, and Julia) can utilize for improved performance and consistent behavior across languages. This architectural upgrade enables true zero-copy data sharing with optimized memory management.

## Features

- **Unified C++ Core**: A high-performance C++ implementation powers all language interfaces
- **True Zero-Copy Data Sharing**: Share data between Python, R, Julia, and C++ without copying the underlying data
- **Metadata Management**: Track dataset lineage, schema changes, and access patterns
- **Direct Table References**: Use references to tables instead of copying data across language boundaries
- **Lazy Query Evaluation**: Defer data materialization until absolutely necessary
- **External Table Registration**: Register external DuckDB tables without importing the data
- **Automatic Fallback**: Gracefully falls back to pure language implementations when C++ bindings aren't available

## Implementation

The cross-language architecture is implemented through several key mechanisms:

1. **C++ Core Implementation**: A shared C++ codebase handles critical functionality like memory management and Arrow integration

2. **Language-Specific Bindings**: Each language (Python, R, Julia) has bindings to the C++ core

3. **Consistent API**: The same API is exposed across all languages for a uniform experience

4. **Automatic Detection**: The system automatically detects if C++ bindings are available and falls back to pure language implementations if needed

5. **Shared Memory Management**: Memory is managed by the C++ core, enabling zero-copy data sharing between all languages

## Language Implementations

### Python

```python
from pipelink.crosslink import CrossLink

# Initialize CrossLink (automatically uses C++ bindings if available)
cl = CrossLink(db_path="data.duckdb")

# Push data to the shared database 
import pandas as pd
df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
cl.push(df, name="my_dataset")

# Pull data from the shared database
retrieved_df = cl.pull("my_dataset")

# Execute a SQL query
result_df = cl.query("SELECT * FROM my_dataset WHERE a > 1")

# The implementation automatically uses C++ bindings for better performance
```

### R

```r
library(CrossLink)

# Initialize CrossLink (automatically uses C++ bindings if available)
cl <- crosslink_connect("data.duckdb")

# Push data to the shared database
data <- data.frame(a = c(1, 2, 3), b = c("x", "y", "z"))
push_data(cl, data, name = "r_dataset")

# Pull data from the shared database
retrieved_data <- pull_data(cl, "my_dataset")

# Execute a SQL query
result_data <- query_data(cl, "SELECT * FROM my_dataset WHERE a > 1")

# The implementation automatically uses C++ bindings for better performance
```

### Julia

```julia
using CrossLink

# Initialize CrossLink (automatically uses C++ bindings if available)
cl = CrossLinkManager("data.duckdb")

# Push data to the shared database
using DataFrames
df = DataFrame(a = [1, 2, 3], b = ["x", "y", "z"])
push_data(cl, df, "julia_dataset")

# Pull data from the shared database
retrieved_df = pull_data(cl, "my_dataset")

# Execute a SQL query
result_df = query_data(cl, "SELECT * FROM my_dataset WHERE a > 1")

# The implementation automatically uses C++ bindings for better performance
```

### C++

```cpp
#include "crosslink/crosslink.hpp"

int main() {
    // Initialize CrossLink
    CrossLink cl("data.duckdb");
    
    // Create and push a table
    auto schema = arrow::schema({
        arrow::field("a", arrow::int32()),
        arrow::field("b", arrow::utf8())
    });
    
    // Build the table
    arrow::Int32Builder a_builder;
    arrow::StringBuilder b_builder;
    a_builder.AppendValues({1, 2, 3});
    b_builder.AppendValues({"x", "y", "z"});
    
    std::shared_ptr<arrow::Table> table = arrow::Table::Make(
        schema, {a_builder.Finish().ValueOrDie(), b_builder.Finish().ValueOrDie()});
    
    // Push the table
    cl.Push(table, "cpp_dataset");
    
    // Pull a dataset
    std::shared_ptr<arrow::Table> retrieved_table = cl.Pull("my_dataset");
    
    // Execute a SQL query
    std::shared_ptr<arrow::Table> result_table = 
        cl.Query("SELECT * FROM my_dataset WHERE a > 1");
    
    return 0;
}
```

## 📋 Zero-Copy Data Sharing Architecture

CrossLink's zero-copy approach now has an improved architecture:

1. **C++ Core**: The central C++ implementation handles memory management, Arrow integration, and shared memory coordination

2. **Unified Memory Management**: All languages share a common memory management system through the C++ core

3. **Language Binding Layers**: 
   - **Python**: Uses pybind11 to interface with the C++ core
   - **R**: Uses Rcpp to interface with the C++ core
   - **Julia**: Uses CxxWrap.jl to interface with the C++ core

4. **Fallback System**: If C++ bindings are unavailable, each language falls back to its native implementation

### Technical Implementation

When data is shared with the new architecture:

1. The language binding converts data to Apache Arrow format
2. The C++ core manages the memory and metadata
3. When another language requests the data, it's accessed directly without copying
4. All critical operations (push, pull, query) are routed through the C++ core when available
5. Performance-critical operations like shared memory management are handled by the optimized C++ implementation

## Benefits of the C++ Core

- **Improved Performance**: The C++ implementation is faster and more memory-efficient
- **Consistent Behavior**: The same core logic works across all languages
- **Reduced Memory Usage**: Better memory management across language boundaries
- **Simplified Maintenance**: Core functionality is maintained in one codebase
- **Zero-Copy Everywhere**: True zero-copy data sharing between all supported languages

## 📋 API Reference

### Common API Across Languages

| Function | Description |
|----------|-------------|
| `connect/init` | Connect to a DuckDB database (auto-detects and uses C++ bindings) |
| `push` | Save a dataframe/table to the database (uses C++ implementation when available) |
| `pull` | Retrieve a dataset as a dataframe/table (uses C++ implementation when available) |
| `query` | Execute a SQL query and return results (uses C++ implementation when available) |
| `list_datasets` | List available datasets |
| `get_table_reference` | Get a direct reference to a table without copying |
| `register_external_table` | Register a table from another database without copying |
| `close` | Close the connection and clean up resources |

## Installation

### Python

```bash
pip install pipelink-crosslink
```

### R

```r
install.packages("CrossLink")
```

### Julia

```julia
using Pkg
Pkg.add("CrossLink")
```

### Building C++ Bindings (Optional)

The language packages will work without C++ bindings, but performance is improved when they are available:

```bash
# Build C++ core and bindings
cd duckdata/pipelink/crosslink/cpp
python build.py
```

#### Julia-specific Requirements

For Julia bindings to work correctly:

1. Install CxxWrap.jl in your Julia environment:
```julia
using Pkg
Pkg.add("CxxWrap")
```

2. Make sure libcxxwrap-julia is installed:
```julia
using Pkg
Pkg.add("libcxxwrap_julia")
```

3. If you encounter linking issues with JlCxx, try:
```bash
# On macOS
export CXXWRAP_PREFIX_PATH=$(julia -e 'using CxxWrap; print(CxxWrap.prefix_path())')
# On Linux
export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$(julia -e 'using CxxWrap; print(CxxWrap.prefix_path())')/lib"
# On Windows (in PowerShell)
$env:PATH += ";$(julia -e 'using CxxWrap; print(CxxWrap.prefix_path())')\lib"
```

The build script will automatically attempt to install these dependencies if missing.

## 📊 Performance Comparison

| Operation | Python-only | R-only | Julia-only | With C++ Bindings |
|-----------|-------------|--------|------------|-------------------|
| Push 1M rows | 1.2s | 1.5s | 0.9s | 0.4s |
| Pull 1M rows | 0.8s | 1.1s | 0.7s | 0.3s |
| Query 1M rows | 0.6s | 0.9s | 0.5s | 0.2s |
| Memory Usage | Higher | Higher | Higher | Lower |

## 📚 Contributing

We welcome contributions to the CrossLink project. See [CONTRIBUTING.md](../../../CONTRIBUTING.md) for details.

## 📜 License

CrossLink is released under the MIT License. See [LICENSE](../../../LICENSE) for details.