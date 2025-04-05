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

CrossLink now supports distributed operation using **Apache Arrow Flight**, allowing for efficient data sharing between processes running on different machines. The Flight API provides high-performance data transfer with minimal serialization overhead.

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
| - Flight Integration  |
+-----------------------+
```

The core manages:
- **Table Registry**: Maps dataset names/IDs to Arrow table references
- **Memory Management**: Uses `std::shared_ptr<arrow::Table>` to manage table lifetimes
- **Arrow Integration**: Handles exchange of table data structures
- **DuckDB Integration**: SQL query execution across registered tables
- **Metadata Services**: Tracks dataset lineage, schema, and access patterns
- **Notification System**: Publishes data change events to subscribers
- **Flight Integration**: Enables distributed data sharing across machines

**Implementation Details:**

The core uses the pimpl idiom with the `CrossLink::Impl` class to hide implementation details:

```cpp
// Public API in crosslink.h
class CrossLink {
public:
    CrossLink(const std::string& db_path = "crosslink.duckdb", bool debug = false);
    explicit CrossLink(const CrossLinkConfig& config);
    std::string push(std::shared_ptr<arrow::Table> table, const std::string& name = "", ...);
    std::shared_ptr<arrow::Table> pull(const std::string& identifier);
    
    // Flight API
    std::string flight_push(std::shared_ptr<arrow::Table> table, const std::string& remote_host, int remote_port, ...);
    std::shared_ptr<arrow::Table> flight_pull(const std::string& identifier, const std::string& remote_host, int remote_port);
    std::vector<std::string> list_remote_datasets(const std::string& remote_host, int remote_port);
    bool start_flight_server();
    bool stop_flight_server();
    
    // Other methods
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
5. **Distributed Sharing**: Apache Arrow Flight for sharing tables between machines

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
| - Flight API     |  |               |  |                |  |                 |
+------------------+  +---------------+  +----------------+  +-----------------+
```

Each binding provides:
- **Idiomatic Interface**: Language-appropriate naming and object patterns
- **Data Type Conversion**: Between native types and Arrow format
- **Memory Management**: Integration with language-specific GC/reference counting
- **Error Handling**: Translation between C++ exceptions and language exceptions
- **Fallback Mechanisms**: Pure-language implementations when C++ bindings unavailable
- **Flight API**: Access to distributed data sharing capabilities (where implemented)

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
- **Flight Protocol**: Arrow Flight RPC for efficient data transfer between processes

### 4. Data Persistence Layer

Optional DuckDB integration for persistence and querying:

```
+-----------------------------------+
|           DuckDB Layer            |
+-----------------------------------+
| +-----------------+ +----------+  |
| | Query Engine    | | Storage  |  |
| | - SQL parsing   | | - Page   |  |
| | - Optimization  | | - WAL    |  |
| | - Arrow         | |          |  |
| |   integration   | |          |  |
| +-----------------+ +----------+  |
+-----------------------------------+
```

DuckDB provides:
- **File-based Storage**: Persists data to disk when needed
- **SQL Interface**: Powerful query capabilities across tables
- **Native Arrow Support**: Direct registration of Arrow tables
- **Unified Query Layer**: Common SQL interface for all languages

## 🌐 Distributed Mode with Apache Arrow Flight

CrossLink now supports distributed operation through Apache Arrow Flight, enabling efficient data sharing between processes running on different machines.

### Flight Architecture

When operating in distributed mode:

```
+----------------+                 +----------------+
| Node A         |                 | Node B         |
|                |                 |                |
| +------------+ |                 | +------------+ |
| |CrossLink   | |                 | |CrossLink   | |
| |Core        | |   Flight RPC    | |Core        | |
| |            +<------------------>+            | |
| |Flight      | |                 | |Flight      | |
| |Server      | |                 | |Client      | |
| +------------+ |                 | +------------+ |
|                |                 |                |
+----------------+                 +----------------+
        ^                                  ^
        |                                  |
        v                                  v
+----------------+                 +----------------+
| Local Storage  |                 | Local Storage  |
| DuckDB + Arrow |                 | DuckDB + Arrow |
+----------------+                 +----------------+
```

### Configuring Distributed Mode

CrossLink can be configured for distributed operation through the `CrossLinkConfig` class:

```cpp
// C++ configuration
CrossLinkConfig config;
config.set_mode(OperationMode::DISTRIBUTED)
      .set_flight_host("localhost")
      .set_flight_port(8815)
      .set_debug(true);

CrossLink cl(config);
```

```python
# Python configuration
from duckdata.crosslink import CrossLinkConfig, OperationMode, get_instance

config = CrossLinkConfig(
    mode=OperationMode.DISTRIBUTED,
    flight_host="localhost",
    flight_port=8815,
    debug=True
)

cl = get_instance(config=config)
```

### Environment Variables

Distributed configuration can also be set through environment variables:

- `CROSSLINK_MODE`: Set to "DISTRIBUTED" for distributed mode
- `CROSSLINK_FLIGHT_HOST`: Host for the Flight server
- `CROSSLINK_FLIGHT_PORT`: Port for the Flight server
- `CROSSLINK_MOTHER_NODE`: Address of the mother node (metadata coordinator)
- `CROSSLINK_NODE_ADDRESS`: Address of this node

### Flight API Methods

CrossLink provides several methods for distributed data sharing:

- **`flight_push`**: Share a table with a remote node
- **`flight_pull`**: Retrieve a table from a remote node
- **`list_remote_datasets`**: List datasets available on a remote node
- **`start_flight_server`**: Start a Flight server for accepting connections
- **`stop_flight_server`**: Stop the Flight server

## ✨ Key Features

*   **High-Performance C++ Core:** Efficient data management written in C++.
*   **Zero-Copy Data Sharing:** Leverages Apache Arrow for direct memory access between languages, minimizing overhead.
*   **Native Language Bindings:** Idiomatic APIs for Python, R, and Julia.
*   **Direct C++ API:** Allows C++ applications to participate in data sharing directly.
*   **Simple Interface:** Core operations (`push`, `pull`, `query`) for easy data exchange.
*   **Distributed Data Sharing:** Apache Arrow Flight integration for efficient cross-machine data transfer.
*   **Notifications:** C++ API includes hooks for data change notifications (binding support may vary).

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

For distributed data flow using Flight:

```
+----------------+       +------------------+       +----------------+
| Source Node    |       | Flight Protocol  |       | Target Node    |
| (Node A)       |       |                  |       | (Node B)       |
+-------+--------+       +--------+---------+       +-------+--------+
        |                         |                         |
        v                         |                         |
+----------------+                |                 +----------------+
| CrossLink      |                |                 | CrossLink      |
| flight_push()  |                |                 | flight_pull()  |
+-------+--------+                |                 +-------+--------+
        |                         |                         ^
        v                         |                         |
+-------+--------+       +--------+---------+       +-------+--------+
| Flight Server  |       | gRPC Stream      |       | Flight Client  |
| DoGet/DoPut    +-------> Record Batches   +-------> DoGet/DoPut    |
|                |       |                  |       |                |
+----------------+       +------------------+       +----------------+
```

## 🛠️ Installation and Building

### Prerequisites

*   C++17 compatible compiler (GCC, Clang, MSVC)
*   CMake (version 3.15+)
*   Apache Arrow C++ library (including development headers) installed system-wide or locally.
*   Apache Arrow Flight C++ library (for distributed mode).
*   DuckDB C++ library (if the C++ core uses it for storage/querying) installed system-wide or locally.
*   **For Python:** `pybind11`, `pyarrow`
*   **For R:** `Rcpp`, `arrow` R package
*   **For Julia:** `CxxWrap.jl`, `Arrow.jl`

### Building with Flight Support

To build CrossLink with Arrow Flight support:

```bash
# Navigate to C++ directory
cd duckdata/crosslink/cpp

# Configure with CMake, enabling Flight
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release

# Build the library
cmake --build build --config Release

# Install (optional)
cmake --install build
```

## ⚙️ Technical Implementation Details by Language

### C++ Core (`libcrosslink`)

- **Namespace**: `crosslink`
- **Memory Model**: RAII with smart pointers
- **Design Pattern**: pimpl idiom (`CrossLink::Impl`)
- **Key Dependencies**:
  - **Apache Arrow** (C++): For memory model and columnar data representation
  - **Arrow Flight** (C++): For distributed data sharing
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
- `cpp/core/flight_client.h`: Arrow Flight client
- `cpp/core/flight_server.h`: Arrow Flight server
- `cpp/core/crosslink_config.h`: Configuration management

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

## 🤝 Contributing

Contributions to CrossLink are welcome!

## 📜 License

This project is licensed under the MIT License - see the [LICENSE](../../../LICENSE) file for details.

## 📊 Flight API Example

### Python Distributed Data Sharing

The following example demonstrates how to use CrossLink's Flight API for distributed data sharing between two Python processes:

```python
# Server process
from duckdata.crosslink import get_instance, CrossLinkConfig, OperationMode
import pandas as pd
import pyarrow as pa

# Configure the server node
config = CrossLinkConfig(
    db_path="server.duckdb",
    debug=True,
    mode=OperationMode.DISTRIBUTED,
    flight_host="localhost",
    flight_port=8815
)

# Initialize CrossLink
server = get_instance(config=config)

# Start the Flight server
if server.start_flight_server():
    print(f"Flight server started on port {server.flight_server_port()}")
else:
    print("Failed to start Flight server")
    exit(1)
    
# Create and share a sample table
df = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    'value': [10.1, 20.2, 30.3, 40.4, 50.5]
})
table = pa.Table.from_pandas(df)

# Push the table to local storage
dataset_id = server.push(table, name="sample_dataset", 
                       description="Sample dataset for Flight demo")
print(f"Table pushed with ID: {dataset_id}")

# The server continues running to serve Flight requests...
```

```python
# Client process
from duckdata.crosslink import get_instance, CrossLinkConfig
import pandas as pd
import pyarrow as pa

# Initialize CrossLink in local mode
client = get_instance()

# List datasets on the remote server
datasets = client.list_remote_datasets("localhost", 8815)
print(f"Available datasets on server: {datasets}")

# Pull a dataset from the server
table = client.flight_pull("sample_dataset", "localhost", 8815)
print("Pulled table from server:")
print(table.to_pandas())

# Create a new table to push back to the server
new_df = pd.DataFrame({
    'id': [101, 102, 103],
    'name': ['Xavier', 'Yolanda', 'Zach'],
    'value': [99.9, 88.8, 77.7]
})
new_table = pa.Table.from_pandas(new_df)

# Push the table to the remote server
new_dataset_id = client.flight_push(new_table, "localhost", 8815, 
                                  "client_dataset", "Dataset from client")
print(f"Table pushed to server with ID: {new_dataset_id}")
```

### C++ Distributed Data Sharing

```cpp
#include "crosslink/crosslink.h"
#include <arrow/api.h>
#include <arrow/table.h>
#include <iostream>

int main() {
    try {
        // Initialize distributed node A (server)
        crosslink::CrossLinkConfig config_a;
        config_a.set_mode(crosslink::OperationMode::DISTRIBUTED)
                .set_flight_host("localhost")
                .set_flight_port(8815)
                .set_debug(true);
        
        crosslink::CrossLink node_a(config_a);
        
        // Start Flight server on node A
        if (!node_a.start_flight_server()) {
            std::cerr << "Failed to start Flight server on node A" << std::endl;
            return 1;
        }
        
        // Create a sample table
        auto schema = arrow::schema({
            arrow::field("id", arrow::int64()), 
            arrow::field("name", arrow::utf8())
        });
        
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        
        id_builder.AppendValues({1, 2, 3});
        name_builder.AppendValues({"one", "two", "three"});
        
        std::shared_ptr<arrow::Array> id_array, name_array;
        id_builder.Finish(&id_array);
        name_builder.Finish(&name_array);
        
        auto table = arrow::Table::Make(schema, {id_array, name_array});
        
        // Share the table on node A
        std::string dataset_id = node_a.push(table, "sample_dataset");
        std::cout << "Table pushed to node A with ID: " << dataset_id << std::endl;

        // Initialize node B (client)
        crosslink::CrossLinkConfig config_b;
        config_b.set_debug(true);
        crosslink::CrossLink node_b(config_b);
        
        // Pull the table from node A to node B using Flight
        auto remote_table = node_b.flight_pull(dataset_id, "localhost", 8815);
        std::cout << "Table pulled by node B: " << remote_table->num_rows() << " rows, "
                 << remote_table->num_columns() << " columns" << std::endl;
        
        // Stop the Flight server when done
        node_a.stop_flight_server();
        
        return 0;
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
}
```