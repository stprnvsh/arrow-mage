# 🔄 CrossLink: High-Performance Cross-Language Data Sharing

<div align="center">

<!-- Consider adding a real logo here -->
![CrossLink Placeholder Logo](https://via.placeholder.com/300x150?text=CrossLink)

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](../../../LICENSE)
[![C++](https://img.shields.io/badge/C++-17-blue.svg)](https://isocpp.org/)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![R](https://img.shields.io/badge/R-4.0+-blue.svg)](https://www.r-project.org/)
[![Julia](https://img.shields.io/badge/julia-1.6+-blue.svg)](https://julialang.org/)

**Zero-copy data sharing between C++, Python, R, and Julia using Apache Arrow**

</div>

## 🌟 Overview

CrossLink is a high-performance library for sharing data between programming languages with minimal overhead. It employs a unified C++ core with language-specific bindings that provide idiomatic access in each target language. By leveraging Apache Arrow's columnar memory format and direct memory access capabilities, CrossLink enables true zero-copy data sharing, significantly reducing the overhead traditionally associated with transferring data between language runtimes.

## 🏗️ Technical Architecture

CrossLink employs a layered architecture with four primary components:

### 1. Central C++ Core (`libcrosslink`)

The C++ core provides the fundamental functionality and is implemented in the `crosslink::CrossLink` class:

```
+-----------------------+
|  C++ Core (crosslink) |
+-----------------------+
| - Memory Management   |
| - Table Registry      |
| - Arrow Integration   |
| - DuckDB Integration  |
| - Metadata Services   |
| - Notifications       |
+-----------------------+
```

The core manages:
- **Table Registry**: Maps dataset names/IDs to Arrow table references
- **Memory Management**: Uses `std::shared_ptr<arrow::Table>` to manage table lifetimes
- **Arrow Integration**: Handles exchange of table data structures
- **DuckDB Integration**: SQL query execution across registered tables
- **Metadata Services**: Tracks dataset lineage, schema, and access patterns
- **Notification System**: Publishes data change events to subscribers

**Implementation Details:**

The core uses the pimpl idiom with the `CrossLink::Impl` class to hide implementation details:

```cpp
// Public API in crosslink.h
class CrossLink {
public:
    CrossLink(const std::string& db_path = "crosslink.duckdb", bool debug = false);
    std::string push(std::shared_ptr<arrow::Table> table, const std::string& name = "", ...);
    std::shared_ptr<arrow::Table> pull(const std::string& identifier);
    std::shared_ptr<arrow::Table> query(const std::string& sql);
    std::vector<std::string> list_datasets();
    // ... additional methods ...
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
```

The implementation manages Arrow tables through several key mechanisms:
1. **Table Sharing**: Tables pushed to CrossLink are stored as references, not copies
2. **Reference Counting**: `std::shared_ptr` ensures tables remain in memory as long as needed
3. **Metadata Database**: DuckDB stores metadata about shared tables and their schemas
4. **Potential Shared Memory**: For cross-process sharing, memory-mapped files may be used

### 2. Language Binding Layer

Each target language has a binding layer that connects to the C++ core:

```
+------------------+  +---------------+  +----------------+  +-----------------+
| Python Binding   |  | R Binding     |  | Julia Binding  |  | Direct C++ Use  |
| (pybind11)       |  | (Rcpp)        |  | (CxxWrap.jl)   |  |                 |
+------------------+  +---------------+  +----------------+  +-----------------+
| - CrossLink class|  | - crosslink_  |  | - CrossLink    |  | - crosslink::   |
|   wrapper        |  |   connect()   |  |   Manager      |  |   CrossLink     |
| - Arrow adapters |  | - push_data() |  | - push_data()  |  |                 |
| - pandas<->Arrow |  | - data.frame  |  | - DataFrame    |  |                 |
+------------------+  +---------------+  +----------------+  +-----------------+
```

Each binding provides:
- **Idiomatic Interface**: Language-appropriate naming and object patterns
- **Data Type Conversion**: Between native types and Arrow format
- **Memory Management**: Integration with language-specific GC/reference counting
- **Error Handling**: Translation between C++ exceptions and language exceptions
- **Fallback Mechanisms**: Pure-language implementations when C++ bindings unavailable

**Technical Binding Details:**

- **Python**: Uses `pybind11` to wrap C++ objects with Python bindings
- **R**: Uses `Rcpp` to create bindings between R and C++
- **Julia**: Uses `CxxWrap.jl` to create bindings to C++ libraries

Each binding also handles the conversion between:
- Python: `pandas.DataFrame` ↔ `pyarrow.Table` ↔ `arrow::Table`
- R: `data.frame` or `tibble` ↔ `arrow::Table` (R) ↔ `arrow::Table` (C++)
- Julia: `DataFrame` ↔ `Arrow.Table` ↔ `arrow::Table` (C++)

### 3. Apache Arrow Integration

Arrow provides the memory model and data structure:

```
+-----------------------------------+
|       Apache Arrow Format         |
+-----------------------------------+
| Contiguous Memory Region          |
+-----------------------------------+
| +-----------+ +--------------+    |
| | Column A  | | Column B     |    |
| | Int32     | | Utf8 (String)|    |
| +-----------+ +--------------+    |
| | Buffers:  | | Buffers:     |    |
| | - Validity| | - Validity   |    |
| | - Data    | | - Offsets    |    |
| |           | | - Data       |    |
| +-----------+ +--------------+    |
+-----------------------------------+
```

Key Arrow integration components:
- **Shared Memory Buffers**: Arrow's memory layout enables direct access without copying
- **Zero-Copy Exchange**: Pointers to buffers are shared instead of the data itself
- **Schema Preservation**: Type information is preserved across language boundaries
- **Standard Interface**: Arrow C Data Interface enables sharing buffers across languages
- **Memory Management**: Arrow's memory model works with each language's memory management

### 4. Data Persistence Layer

Optional DuckDB integration for persistence and querying:

```
+-----------------------------------+
|           DuckDB Layer            |
+-----------------------------------+
| +-----------------+ +----------+  |
| | Query Engine    | | Storage  |  |
| | - SQL parsing   | | - Page   |  |
| | - Optimization  | |   format |  |
| | - Arrow         | | - WAL    |  |
| |   integration   | |          |  |
| +-----------------+ +----------+  |
+-----------------------------------+
```

DuckDB provides:
- **File-based Storage**: Persists data to disk when needed
- **SQL Interface**: Powerful query capabilities across tables
- **Native Arrow Support**: Direct registration of Arrow tables
- **Unified Query Layer**: Common SQL interface for all languages

## ✨ Key Features

*   **High-Performance C++ Core:** Efficient data management written in C++.
*   **Zero-Copy Data Sharing:** Leverages Apache Arrow for direct memory access between languages, minimizing overhead.
*   **Native Language Bindings:** Idiomatic APIs for Python, R, and Julia.
*   **Direct C++ API:** Allows C++ applications to participate in data sharing directly.
*   **Simple Interface:** Core operations (`push`, `pull`, `query`) for easy data exchange.
*   **(Potential) Notifications:** C++ API includes hooks for data change notifications (binding support may vary).

## 🔄 Cross-Language Data Flow

When data moves between languages, CrossLink employs a specific pathway:

```
+----------------+    +------------------+    +----------------+
| Source Lang    |    | CrossLink C++    |    | Target Lang    |
| (e.g., Python) |    | Core             |    | (e.g., R)      |
+-------+--------+    +--------+---------+    +-------+--------+
        |                      |                      |
        v                      |                      |
+----------------+             |              +----------------+
| Native Format  |             |              | Native Format  |
| (DataFrame)    |             |              | (data.frame)   |
+-------+--------+             |              +-------+--------+
        |                      |                      ^
        v                      |                      |
+----------------+             |              +----------------+
| Language Arrow |             |              | Language Arrow |
| (pyarrow.Table)|             |              | (arrow::Table) |
+-------+--------+             |              +-------+--------+
        |                      |                      ^
        v                      |                      |
+-------+--------+    +--------+---------+    +-------+--------+
| Arrow C Data   |    | arrow::Table     |    | Arrow C Data   |
| Interface      +---->  std::shared_ptr +---->  Interface     |
| Struct         |    |   Memory Buffers |    | Reconstruction |
+----------------+    +------------------+    +----------------+
```

1. **Source Conversion**: Native data structures convert to language-specific Arrow objects
2. **C++ Exchange**: The Arrow buffer references are passed to the C++ core
3. **Buffer Sharing**: Memory buffers are managed by C++ core with reference counting
4. **Target Access**: Target language bindings directly access the shared Arrow buffers
5. **Target Conversion**: Arrow objects convert to native structures (only if requested)

The zero-copy nature comes from steps 2-4, where no actual data is copied - only references to memory buffers are exchanged.

## 💡 Implementation Mechanisms

### Memory Management and Data Sharing

**Core Principle**: Share pointers to memory buffers, not the data itself.

**Technical Implementation**:

1. **C++ Core**:
   ```cpp
   // In CrossLink::Impl::push
   std::string push(std::shared_ptr<arrow::Table> table, const std::string& name) {
       // Generate unique ID for the dataset
       std::string dataset_id = name.empty() ? 
           generate_uuid() : name;
       
       // Store the shared_ptr in an internal registry
       table_registry_[dataset_id] = table;
       
       // Update metadata in database
       metadata_manager_.create_dataset_metadata({
           .id = dataset_id,
           .name = dataset_id,
           .schema_json = ArrowBridge::schema_to_json(table->schema())
           // ... other metadata fields ...
       });
       
       return dataset_id;
   }
   
   // In CrossLink::Impl::pull
   std::shared_ptr<arrow::Table> pull(const std::string& identifier) {
       // Look up the table in the registry
       auto it = table_registry_.find(identifier);
       if (it != table_registry_.end()) {
           return it->second;  // Return the same shared_ptr (zero-copy)
       }
       
       // Table not in memory - might need to recreate or fail
       throw std::runtime_error("Dataset not found: " + identifier);
   }
   ```

2. **Language Bindings**:
   - **Python**:
```python
     # In Python binding wrapper
     def _wrap_arrow_table(cpp_table_ptr):
         """Convert C++ arrow::Table to pyarrow.Table without copying data"""
         # Use Arrow C Data Interface for zero-copy exchange
         c_schema, c_arrays = _cpp_table_to_c_data_interface(cpp_table_ptr)
         return pyarrow.Table._import_from_c(c_schema, c_arrays)
     
     # In CrossLink.pull method
     def pull(self, identifier):
         if self._cpp_instance:
             cpp_table = self._cpp_instance.pull(identifier)
             return _wrap_arrow_table(cpp_table)
         else:
             # Pure Python fallback implementation
             # ... (would involve more data copying) ...
     ```

   - **R**:
     ```r
     # In R binding wrapper
     wrap_arrow_table <- function(cpp_table_ptr) {
       # Use Arrow C Data Interface for zero-copy exchange
       c_schema <- cpp_table_to_c_schema(cpp_table_ptr)
       c_arrays <- cpp_table_to_c_arrays(cpp_table_ptr)
       return arrow::Table$import_from_c(c_schema, c_arrays)
     }
     
     # In pull_data function
     pull_data <- function(cl, identifier) {
       if (cl$cpp_available) {
         cpp_table <- cl$cpp_instance$pull(identifier)
         return wrap_arrow_table(cpp_table)
       } else {
         # Pure R fallback implementation
         # ... (would involve more data copying) ...
       }
     }
     ```

   - **Julia**:
     ```julia
     # In Julia binding wrapper
     function wrap_arrow_table(cpp_table_ptr)
         # Use Arrow C Data Interface for zero-copy exchange
         c_schema, c_arrays = cpp_table_to_c_interface(cpp_table_ptr)
         return Arrow.Table(c_schema, c_arrays)
     end
     
     # In pull_data function
     function pull_data(cl, identifier)
         if cl.cpp_available
             cpp_table = cl.cpp_instance.pull(identifier)
             return wrap_arrow_table(cpp_table)
         else
             # Pure Julia fallback implementation
             # ... (would involve more data copying) ...
         end
     end
     ```

### Arrow Integration Details

1. **Internal Memory Structure**:
   - Arrow tables consist of arrays of memory buffers (one per column)
   - Each buffer is a contiguous memory region with a specific layout
   - Data types are defined by Arrow's type system (int32, utf8, etc.)
   - Each buffer carries a schema describing its layout

2. **Zero-Copy Mechanism**:
   - Arrow C Data Interface used to exchange table pointers between languages
   - Memory layout is standardized across language implementations
   - Buffer ownership is tracked through reference counting
   - Garbage collection is coordinated through the C++ core

3. **Schema Translation**:
   ```
   +-------------------+    +-------------------+    +-------------------+
   | Python            |    | C++ Core          |    | R                 |
   | Field(a, int32)   +---->Field(a, int32)    +---->Field(a, int32)    |
   | Field(b, utf8)    |    | Field(b, utf8)    |    | Field(b, utf8)    |
   +-------------------+    +-------------------+    +-------------------+
   ```

### Query Execution Flow

When executing a SQL query, CrossLink uses the following flow:

```
Query: "SELECT a, b FROM dataset WHERE a > 10"
    |
    v
+-------------------+    +-------------------+    +-------------------+
| Language Binding  |    | C++ Core          |    | DuckDB Engine     |
| cl.query(sql)     +---->CrossLink::query() +---->conn.Query()       |
+-------------------+    +-------------------+    +-------------------+
                               |                        |
                               |                        v
                               |              +-------------------+
                               |              | Register tables   |
                               |              | Execute query     |
                               |              | Gather results    |
                               |              +--------+----------+
                               |                       |
                               v                       v
                         +------------------------------------+
                         | New arrow::Table with query results |
                         +------------------------------------+
                               |
                               v
+-------------------+    +-------------------+
| Language Binding  |<---+ C++ Core returns  |
| Return to caller  |    | arrow::Table      |
+-------------------+    +-------------------+
```

## 🛠️ Installation and Building

### Prerequisites

*   C++17 compatible compiler (GCC, Clang, MSVC)
*   CMake (version 3.15+)
*   Apache Arrow C++ library (including development headers) installed system-wide or locally.
*   DuckDB C++ library (if the C++ core uses it for storage/querying) installed system-wide or locally.
*   **For Python:** `pybind11`, `pyarrow`
*   **For R:** `Rcpp`, `arrow` R package
*   **For Julia:** `CxxWrap.jl`, `Arrow.jl`

## ⚙️ Technical Implementation Details by Language

### C++ Core (`libcrosslink`)

- **Namespace**: `crosslink`
- **Memory Model**: RAII with smart pointers
- **Design Pattern**: pimpl idiom (`CrossLink::Impl`)
- **Key Dependencies**:
  - **Apache Arrow** (C++): For memory model and columnar data representation
  - **DuckDB** (C++): For SQL execution and persistent storage
- **Thread Safety**: Reference counting provides basic thread safety
- **Notification System**: Callback-based with registration/unregistration

**Core Implementation Files**:
- `include/crosslink/crosslink.h`: Public API header
- `cpp/src/crosslink.cpp`: Main implementation with pimpl pattern
- `cpp/core/arrow_bridge.h`: Arrow integration
- `cpp/core/metadata_manager.h`: Dataset metadata tracking
- `cpp/core/shared_memory_manager.h`: Shared memory for cross-process access
- `cpp/core/notification_system.h`: Observer pattern for data changes

### Python Binding

- **Binding Technology**: `pybind11`
- **Key Dependencies**:
  - `pyarrow`: Python bindings for Apache Arrow
  - `pandas`: DataFrame integration
  - `duckdb-python`: DuckDB Python API (optional for fallback)
- **Module Structure**:
  - `python/crosslink.py`: Compatibility layer
  - `python/core/core.py`: Main `CrossLink` class
  - `python/shared_memory/cpp_wrapper.py`: pybind11 wrapper
  - `python/arrow_integration/arrow_integration.py`: Arrow utilities
- **Fallback Mechanism**: Pure Python implementation using DuckDB

### R Binding

- **Binding Technology**: `Rcpp`
- **Key Dependencies**:
  - `arrow` R package: R bindings for Arrow
  - `duckdb` R package: R bindings for DuckDB
  - `data.frame`/`tibble`: Data structure integration
- **Module Structure**:
  - `r/crosslink.R`: Compatibility layer
  - `r/core/core.R`: Main connection functions
  - `r/shared_memory/cpp_wrapper.R`: Rcpp wrapper
  - `r/arrow_integration/arrow_integration.R`: Arrow utilities
- **Fallback Mechanism**: Pure R implementation using duckdb

### Julia Binding

- **Binding Technology**: `CxxWrap.jl`
- **Key Dependencies**:
  - `Arrow.jl`: Julia bindings for Arrow
  - `DuckDB.jl`: Julia bindings for DuckDB
  - `DataFrames.jl`: DataFrame integration
- **Module Structure**:
  - `julia/CrossLink.jl`: Main module
  - `julia/core/core.jl`: `CrossLinkManager` struct
  - `julia/shared_memory/cpp_wrapper.jl`: CxxWrap wrapper
  - `julia/arrow_integration/arrow_integration.jl`: Arrow utilities
- **Fallback Mechanism**: Pure Julia implementation using DuckDB.jl

### Building the C++ Core

The C++ core (`libcrosslink`) requires:
- C++17 compatible compiler
- CMake 3.15+
- Apache Arrow C++ (1.0.0+)
- DuckDB (0.3.0+)

```bash
# Navigate to C++ directory
cd duckdata/pipelink/crosslink/cpp

# Configure with CMake
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release

# Build the library
cmake --build build --config Release

# Install (optional)
cmake --install build
```

This builds:
- `libcrosslink.so` / `libcrosslink.dylib` / `crosslink.dll` (shared library)
- Language-specific binding libraries 

### Language-Specific Installation

#### Python

```bash
# Install from source
cd duckdata/pipelink/crosslink/python
pip install .

# Requirements: pyarrow, pandas, duckdb
```

#### R

```r
# Install package
install.packages("duckdata/pipelink/crosslink/r", repos=NULL, type="source")

# Requirements: arrow, duckdb, jsonlite, uuid
```

#### Julia

```julia
# Add package from local path
using Pkg
Pkg.add(path="duckdata/pipelink/crosslink/julia")

# Requirements: Arrow, DuckDB, DataFrames
```

## 📊 Usage Examples

### C++ (`crosslink.h`)

```cpp
#include "crosslink/crosslink.h"
#include <arrow/api.h>
#include <arrow/builder.h>
#include <iostream>

int main() {
    try {
        // Initialize with default database path
        crosslink::CrossLink cl;
        
        // Create an Arrow table
        arrow::Int32Builder id_builder;
        arrow::StringBuilder name_builder;
        
        ARROW_RETURN_NOT_OK(id_builder.AppendValues({1, 2, 3}));
        ARROW_RETURN_NOT_OK(name_builder.AppendValues({"one", "two", "three"}));
        
        auto id_array = id_builder.Finish().ValueOrDie();
        auto name_array = name_builder.Finish().ValueOrDie();
        
    auto schema = arrow::schema({
            arrow::field("id", arrow::int32()),
            arrow::field("name", arrow::utf8())
        });
        
        auto table = arrow::Table::Make(schema, {id_array, name_array});
        
        // Share the table
        std::string dataset_id = cl.push(table, "example_table");
        std::cout << "Pushed dataset with ID: " << dataset_id << std::endl;
        
        // List available datasets
        auto datasets = cl.list_datasets();
        std::cout << "Available datasets:" << std::endl;
        for (const auto& name : datasets) {
            std::cout << "- " << name << std::endl;
        }
        
        // Pull the table back (zero-copy)
        auto retrieved_table = cl.pull("example_table");
        std::cout << "Retrieved table has " << retrieved_table->num_rows() 
                  << " rows and " << retrieved_table->num_columns() << " columns" << std::endl;
        
        // Execute a query
        auto result = cl.query("SELECT * FROM example_table WHERE id > 1");
        std::cout << "Query result has " << result->num_rows() << " rows" << std::endl;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}
```

### Python

```python
import pandas as pd
import pyarrow as pa
from pipelink.crosslink import CrossLink

# Initialize CrossLink
cl = CrossLink(db_path="crosslink.duckdb")

# Create a pandas DataFrame
df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'value': ['a', 'b', 'c', 'd', 'e']
})

# Convert to Arrow table (explicit for demonstration)
table = pa.Table.from_pandas(df)

# Push to CrossLink
dataset_id = cl.push(table, name="python_example")
print(f"Pushed dataset with ID: {dataset_id}")

# List available datasets
datasets = cl.list_datasets()
print(f"Available datasets: {datasets}")

# Pull the dataset back (zero-copy if possible)
retrieved_table = cl.pull("python_example")

# Convert to pandas if needed
retrieved_df = retrieved_table.to_pandas()
print(f"Retrieved DataFrame:\n{retrieved_df}")

# Execute a query
result = cl.query("SELECT * FROM python_example WHERE id > 3")
print(f"Query result:\n{result.to_pandas()}")
```

### R

```r
library(arrow)
library(CrossLink)

# Initialize CrossLink
cl <- crosslink_connect(db_path = "crosslink.duckdb")

# Create a data frame
df <- data.frame(
  id = 1:5,
  value = c("a", "b", "c", "d", "e")
)

# Convert to Arrow table (explicit for demonstration)
table <- arrow::as_arrow_table(df)

# Push to CrossLink
dataset_id <- push_data(cl, table, name = "r_example")
cat("Pushed dataset with ID:", dataset_id, "\n")

# List available datasets
datasets <- list_datasets(cl)
cat("Available datasets:", paste(datasets, collapse = ", "), "\n")

# Pull the dataset back (zero-copy if possible)
retrieved_table <- pull_data(cl, "r_example")

# Convert to R data frame if needed
retrieved_df <- as.data.frame(retrieved_table)
cat("Retrieved data frame:\n")
print(retrieved_df)

# Execute a query
result <- query_data(cl, "SELECT * FROM r_example WHERE id > 3")
cat("Query result:\n")
print(as.data.frame(result))
```

### Julia

```julia
using Arrow
using DataFrames
using CrossLink

# Initialize CrossLink
cl = CrossLinkManager("crosslink.duckdb")

# Create a DataFrame
df = DataFrame(
    id = 1:5,
    value = ["a", "b", "c", "d", "e"]
)

# Convert to Arrow table (explicit for demonstration)
table = Arrow.Table(df)

# Push to CrossLink
dataset_id = push_data(cl, table, "julia_example")
println("Pushed dataset with ID: $dataset_id")

# List available datasets
datasets = list_datasets(cl)
println("Available datasets: ", datasets)

# Pull the dataset back (zero-copy if possible)
retrieved_table = pull_data(cl, "julia_example")

# Convert to DataFrame if needed
retrieved_df = DataFrame(retrieved_table)
println("Retrieved DataFrame:")
println(retrieved_df)

# Execute a query
result = query_data(cl, "SELECT * FROM julia_example WHERE id > 3")
println("Query result:")
println(DataFrame(result))
```

## 📋 API Reference

### C++ Core API

| Method | Description | Parameters | Return |
|--------|-------------|------------|--------|
| `CrossLink(db_path, debug)` | Constructor | `db_path`: Database file path<br>`debug`: Enable verbose logging | `CrossLink` instance |
| `push(table, name, description)` | Share a table | `table`: Arrow table to share<br>`name`: Dataset name (optional)<br>`description`: Dataset description | Dataset ID (string) |
| `pull(identifier)` | Retrieve a shared table | `identifier`: Dataset name or ID | `std::shared_ptr<arrow::Table>` |
| `query(sql)` | Execute a SQL query | `sql`: SQL query string | `std::shared_ptr<arrow::Table>` |
| `list_datasets()` | List available datasets | None | `std::vector<std::string>` |
| `register_notification(callback)` | Register for notifications | `callback`: Function to call on events | Registration ID (string) |
| `unregister_notification(id)` | Unregister notification | `id`: Registration ID | None |
| `cleanup()` | Clean up resources | None | None |

### Python API

| Method | Description | Parameters | Return |
|--------|-------------|------------|--------|
| `CrossLink(db_path, debug)` | Constructor | `db_path`: Database file path<br>`debug`: Enable verbose logging | `CrossLink` instance |
| `push(table, name, description)` | Share a table | `table`: PyArrow table or pandas DataFrame<br>`name`: Dataset name (optional)<br>`description`: Dataset description | Dataset ID (string) |
| `pull(identifier)` | Retrieve a shared table | `identifier`: Dataset name or ID | PyArrow Table |
| `query(sql)` | Execute a SQL query | `sql`: SQL query string | PyArrow Table |
| `list_datasets()` | List available datasets | None | List of strings |
| `close()` | Clean up resources | None | None |

### R API

| Function | Description | Parameters | Return |
|----------|-------------|------------|--------|
| `crosslink_connect(db_path, debug)` | Create connection | `db_path`: Database file path<br>`debug`: Enable verbose logging | Connection object |
| `push_data(cl, table, name, description)` | Share a table | `cl`: Connection<br>`table`: Arrow table or data.frame<br>`name`: Dataset name (optional)<br>`description`: Dataset description | Dataset ID (string) |
| `pull_data(cl, identifier)` | Retrieve a shared table | `cl`: Connection<br>`identifier`: Dataset name or ID | Arrow Table |
| `query_data(cl, sql)` | Execute a SQL query | `cl`: Connection<br>`sql`: SQL query string | Arrow Table |
| `list_datasets(cl)` | List available datasets | `cl`: Connection | Character vector |
| `close_connection(cl)` | Clean up resources | `cl`: Connection | None |

### Julia API

| Function | Description | Parameters | Return |
|----------|-------------|------------|--------|
| `CrossLinkManager(db_path, debug)` | Constructor | `db_path`: Database file path<br>`debug`: Enable verbose logging | `CrossLinkManager` instance |
| `push_data(cl, table, name, description)` | Share a table | `cl`: Manager<br>`table`: Arrow.Table or DataFrame<br>`name`: Dataset name (optional)<br>`description`: Dataset description | Dataset ID (string) |
| `pull_data(cl, identifier)` | Retrieve a shared table | `cl`: Manager<br>`identifier`: Dataset name or ID | Arrow.Table |
| `query_data(cl, sql)` | Execute a SQL query | `cl`: Manager<br>`sql`: SQL query string | Arrow.Table |
| `list_datasets(cl)` | List available datasets | `cl`: Manager | Vector of strings |
| `close(cl)` | Clean up resources | `cl`: Manager | None |

## 🤝 Contributing

Contributions to CrossLink are welcome!

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](../../../LICENSE) file for details.