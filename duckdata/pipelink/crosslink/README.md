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

### 🆕 New: True Zero-Copy Data Sharing

CrossLink is a metadata management system that enables efficient cross-language data sharing between Python, R, and Julia. The latest version implements true zero-copy data sharing by leveraging DuckDB's capabilities to directly access tables without copying data.

## Features

- **True Zero-Copy Data Sharing**: Share data between Python, R, and Julia without copying the underlying data.
- **Metadata Management**: Track dataset lineage, schema changes, and access patterns.
- **Direct Table References**: Use references to tables instead of copying data across language boundaries.
- **Lazy Query Evaluation**: Defer data materialization until absolutely necessary.
- **External Table Registration**: Register external DuckDB tables without importing the data.

## Implementation

The zero-copy approach is implemented through several key mechanisms:

1. **DuckDB Metadata Sharing**: Instead of copying data between languages, CrossLink shares metadata about table locations and schema.

2. **Direct Table References**: New methods like `get_table_reference()` return references to tables that can be used for direct access.

3. **Lazy Query Evaluation**: Operations return query handles instead of materialized data when possible.

4. **External Table Registration**: The `register_external_table()` method allows registering tables from other databases without copying.

## Language Implementations

### Python

```python
# Get a direct reference to a table without copying data
table_ref = crosslink.get_table_reference("dataset_name")

# Use the reference in queries
query = f"SELECT * FROM {table_ref['table_name']} WHERE value > 0.5"
result = crosslink.query(query)

# Register an external table without copying
crosslink.register_external_table("path/to/database.duckdb", "external_table")
```

### R

```r
# Get a direct reference to a table without copying data
table_ref <- crosslink$get_table_reference("dataset_name")

# Use the reference in queries
query <- paste0("SELECT * FROM ", table_ref$table_name, " WHERE value > 0.5")
result <- crosslink$run_query(query, return_data_frame = FALSE)

# Register an external table without copying
crosslink$register_external_table("path/to/database.duckdb", "external_table")
```

### Julia

```julia
# Get a direct reference to a table without copying data
table_ref = CrossLink.get_table_reference(manager, "dataset_name")

# Use the reference in queries
query = "SELECT * FROM $(table_ref["table_name"]) WHERE value > 0.5"
result = CrossLink.run_query(manager, query)

# Register an external table without copying
CrossLink.register_external_table(manager, "path/to/database.duckdb", "external_table")
```

## Example Pipeline

A zero-copy example pipeline is provided in `zero_copy_pipeline.yaml` that demonstrates:

1. **Data Generation** (Python): Creates test data and registers it with CrossLink.
2. **Data Transformation** (R): Transforms the data using R without copying, leveraging DuckDB for all operations.
3. **Data Analysis** (Julia): Performs statistical analysis directly on the DuckDB tables.
4. **Report Creation** (Python): Compiles results from all steps without copying data between steps.

Run the example with:

```bash
python -m pipeduck.cli run zero_copy_pipeline.yaml
```

## Performance Benefits

The zero-copy approach provides several performance benefits:

- **Reduced Memory Usage**: Data is stored only once, regardless of how many languages access it.
- **Faster Data Access**: No time spent copying large datasets between languages.
- **Query Optimization**: DuckDB can optimize queries across language boundaries.
- **Efficient Storage**: Metadata is much smaller than the actual data.

## Limitations

- All languages must have access to the same DuckDB database file.
- Operations that require specialized language-specific data structures will still need to materialize data.
- Schema evolution and complex type handling may require additional coordination.

## ✨ Key Features

- 🚀 **Zero-Copy Data Sharing**: Share data between languages without copying
- 🔄 **Multiple Language Support**: Python, R, Julia, and C++ interfaces
- 📊 **Consistent API**: Uniform API across all supported languages
- 📝 **Metadata-Based Access**: Store and retrieve dataset details across languages
- 🔍 **Shared Memory**: Use shared memory for cross-process data access
- 📈 **Performance Monitoring**: Track data transfer performance metrics
- 💾 **Storage Options**: In-memory or file-based storage

## 🚀 Quick Start

### Python

```python
from pipelink.crosslink import CrossLink

# Initialize CrossLink
cl = CrossLink(db_path="data.duckdb")

# Push data to the shared database with zero-copy enabled
import pandas as pd
df = pd.DataFrame({'a': [1, 2, 3], 'b': ['x', 'y', 'z']})
cl.push(df, name="my_dataset", description="Sample data", enable_zero_copy=True)

# Pull data from the shared database (will use zero-copy if available)
retrieved_df = cl.pull("my_dataset", zero_copy=True)

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

# Use data that was shared from Python (zero-copy)
# No need for explicit pull operation - data is directly accessible
retrieved_data <- get_dataset(cl, "my_dataset")

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

# Access data shared from Python or R (zero-copy)
df = get_dataset(cl, "my_dataset")

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
    
    // Access data shared from other languages (zero-copy)
    std::shared_ptr<arrow::Table> table = cl.GetDataset("my_dataset");
    
    // Execute a SQL query
    std::shared_ptr<arrow::Table> result_table = 
        cl.Query("SELECT * FROM my_dataset WHERE a > 1");
    
    // Close connection
    cl.Close();
    
    return 0;
}
```

## 📋 Zero-Copy Data Sharing Details

CrossLink's zero-copy approach works by:

1. **Storing metadata about memory layout**: When data is registered, CrossLink captures detailed metadata about its structure and memory layout.

2. **Using shared memory**: Data can be placed in shared memory accessible to all languages through Apache Arrow.

3. **Providing direct data access**: Instead of copying data between languages, CrossLink enables direct access to the same memory region.

4. **Falling back gracefully**: If zero-copy access is not possible, CrossLink automatically falls back to traditional methods.

### Technical Implementation

When data is shared:

1. The Arrow schema and memory layout information are stored in metadata tables
2. For compatible languages, data is placed in shared memory or memory-mapped files
3. Other languages can directly access this memory using the stored metadata
4. Access patterns are tracked to optimize future operations

## 📋 API Reference

### Common API Across Languages

| Function | Description |
|----------|-------------|
| `connect/init` | Connect to a DuckDB database |
| `push` | Save a dataframe/table to the database with zero-copy option |
| `pull` | Retrieve a dataset as a dataframe/table with zero-copy if available |
| `query` | Execute a SQL query and return results |
| `list_datasets`