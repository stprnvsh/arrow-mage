# 🔄 CrossLink: High-Performance Cross-Language Data Sharing

<div align="center">

<img src="https://user-images.githubusercontent.com/5353262/172948015-d5a5c24f-832e-4ec9-95b1-1f3fce9e89cd.png" width="300" alt="CrossLink Logo">

[![License](https://img.shields.io/badge/license-Dual_License-orange.svg)](https://opensource.org/licenses/MIT)
[![C++](https://img.shields.io/badge/C++-17-blue.svg?logo=c%2B%2B)](https://isocpp.org/)
[![Python](https://img.shields.io/badge/python-3.8+-blue.svg?logo=python)](https://www.python.org/downloads/)
[![R](https://img.shields.io/badge/R-4.0+-blue.svg?logo=r)](https://www.r-project.org/)
[![Julia](https://img.shields.io/badge/julia-1.6+-blue.svg?logo=julia)](https://julialang.org/)
[![Arrow](https://img.shields.io/badge/Apache_Arrow-12.0+-blue.svg?logo=apache)](https://arrow.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-0.8.1+-blue.svg)](https://duckdb.org/)

**Zero-copy data sharing between C++, Python, R, and Julia using Apache Arrow**

[Features](#-key-features) • [Installation](#%EF%B8%8F-installation-and-building) • [Examples](#-flight-api-example) • [Architecture](#%EF%B8%8F-technical-architecture) • [Documentation](#-documentation) • [Contributing](#-contributing) • [License](#-license)

</div>

## 🌟 Overview

CrossLink is a high-performance library that enables seamless data sharing between programming languages with minimal overhead. It employs a unified C++ core with language-specific bindings that provide idiomatic access in each target language. By leveraging Apache Arrow's columnar memory format and direct memory access capabilities, CrossLink enables true zero-copy data sharing, significantly reducing the overhead traditionally associated with transferring data between language runtimes.

> **⚠️ LICENSING NOTICE: CrossLink is free for testing and evaluation purposes only. Enterprise use requires a commercial license. See [License](#-license) section for details.**

CrossLink supports both local and distributed operation modes:

- **Local Mode**: Share data between different languages on the same machine
- **Distributed Mode**: Share data between processes running on different machines using **Apache Arrow Flight**

## ✨ Key Features

- **🚀 High-Performance C++ Core**: Efficient data management with minimal overhead
- **0️⃣ Zero-Copy Data Sharing**: Direct memory access between languages via Apache Arrow
- **🔄 Native Language Bindings**: Idiomatic APIs for Python, R, and Julia
- **⚡ Direct C++ API**: First-class support for C++ applications
- **🧩 Simple Interface**: Core operations (`push`, `pull`, `query`) for easy data exchange
- **📊 DataFrame Integration**: Work with pandas, R data.frames, and Julia DataFrames
- **🌐 Distributed Sharing**: Apache Arrow Flight for efficient cross-machine data transfer
- **🔔 Change Notifications**: Subscribe to data change events
- **🔍 SQL Query Support**: Query shared datasets using SQL via DuckDB integration

## 🏗️ Technical Architecture

CrossLink employs a layered architecture with four primary components:

### 1. Central C++ Core (`libcrosslink`)

The C++ core provides the fundamental functionality and is implemented in the `crosslink::CrossLink` class:

```
┌───────────────────────────┐
│    C++ Core (crosslink)   │
├───────────────────────────┤
│ ┌─────────────────────┐   │
│ │  Memory Management  │   │
│ ├─────────────────────┤   │
│ │   Table Registry    │   │
│ ├─────────────────────┤   │
│ │  Arrow Integration  │   │
│ ├─────────────────────┤   │
│ │ DuckDB Integration  │   │
│ ├─────────────────────┤   │
│ │  Metadata Services  │   │
│ ├─────────────────────┤   │
│ │    Notifications    │   │
│ ├─────────────────────┤   │
│ │ Flight Integration  │   │
│ └─────────────────────┘   │
└───────────────────────────┘
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
    
    // Streaming API
    std::pair<std::string, std::shared_ptr<StreamWriter>> push_stream(std::shared_ptr<arrow::Schema> schema, const std::string& name = "");
    std::shared_ptr<StreamReader> pull_stream(const std::string& stream_id);
    
    // Other methods
    std::shared_ptr<arrow::Table> query(const std::string& sql);
    std::vector<std::string> list_datasets();
    // ... additional methods ...
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};
```

### 2. Language Binding Layer

Each target language has a binding layer that connects to the C++ core:

```
┌──────────────────────┐  ┌───────────────────┐  ┌────────────────────┐  ┌─────────────────────┐
│   Python Binding     │  │   R Binding       │  │  Julia Binding     │  │  Direct C++ Use     │
│   (pybind11)         │  │   (Rcpp)          │  │  (CxxWrap.jl)      │  │                     │
├──────────────────────┤  ├───────────────────┤  ├────────────────────┤  ├─────────────────────┤
│ - CrossLink class    │  │ - crosslink_      │  │ - CrossLink        │  │ - crosslink::       │
│   wrapper            │  │   connect()       │  │   Manager          │  │   CrossLink         │
│ - Arrow adapters     │  │ - push_data()     │  │ - push_data()      │  │                     │
│ - pandas<->Arrow     │  │ - data.frame      │  │ - DataFrame        │  │                     │
│ - Flight API         │  │   conversion      │  │   conversion       │  │                     │
│ - Streaming support  │  │ - Flight API      │  │ - Flight API       │  │                     │
└──────────────────────┘  └───────────────────┘  └────────────────────┘  └─────────────────────┘
```

Each binding provides:
- **Idiomatic Interface**: Language-appropriate naming and object patterns
- **Data Type Conversion**: Between native types and Arrow format
- **Memory Management**: Integration with language-specific GC/reference counting
- **Error Handling**: Translation between C++ exceptions and language exceptions
- **Fallback Mechanisms**: Pure-language implementations when C++ bindings unavailable
- **Flight API**: Access to distributed data sharing capabilities

**Technical Binding Details:**

- **Python**: Uses `pybind11` to wrap C++ objects with Python bindings
- **R**: Uses `Rcpp` to create bindings between R and C++
- **Julia**: Uses `CxxWrap.jl` to create bindings to C++ libraries

Each binding handles the conversion between:
- Python: `pandas.DataFrame` ↔ `pyarrow.Table` ↔ `arrow::Table`
- R: `data.frame` or `tibble` ↔ `arrow::Table` (R) ↔ `arrow::Table` (C++)
- Julia: `DataFrame` ↔ `Arrow.Table` ↔ `arrow::Table` (C++)

### 3. Apache Arrow Integration

Arrow provides the memory model and data structure:

```
┌─────────────────────────────────────┐
│       Apache Arrow Format           │
├─────────────────────────────────────┤
│      Contiguous Memory Region       │
├─────────────────────────────────────┤
│ ┌───────────┐   ┌────────────────┐  │
│ │ Column A  │   │ Column B       │  │
│ │ Int32     │   │ Utf8 (String)  │  │
│ ├───────────┤   ├────────────────┤  │
│ │ Buffers:  │   │ Buffers:       │  │
│ │ - Validity│   │ - Validity     │  │
│ │ - Data    │   │ - Offsets      │  │
│ │           │   │ - Data         │  │
│ └───────────┘   └────────────────┘  │
└─────────────────────────────────────┘
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
┌─────────────────────────────────────┐
│           DuckDB Layer              │
├─────────────────────────────────────┤
│ ┌─────────────────┐ ┌────────────┐  │
│ │ Query Engine    │ │ Storage    │  │
│ │ - SQL parsing   │ │ - Page     │  │
│ │ - Optimization  │ │ - WAL      │  │
│ │ - Arrow         │ │            │  │
│ │   integration   │ │            │  │
│ └─────────────────┘ └────────────┘  │
└─────────────────────────────────────┘
```

DuckDB provides:
- **File-based Storage**: Persists data to disk when needed
- **SQL Interface**: Powerful query capabilities across tables
- **Native Arrow Support**: Direct registration of Arrow tables
- **Unified Query Layer**: Common SQL interface for all languages

## 🌐 Distributed Mode with Apache Arrow Flight

CrossLink supports distributed operation through Apache Arrow Flight, enabling efficient data sharing between processes running on different machines.

### Flight Architecture

When operating in distributed mode:

```
┌────────────────────┐                 ┌────────────────────┐
│ Node A             │                 │ Node B             │
│                    │                 │                    │
│ ┌──────────────┐   │                 │ ┌──────────────┐   │
│ │CrossLink     │   │                 │ │CrossLink     │   │
│ │Core          │   │   Flight RPC    │ │Core          │   │
│ │              │◄──┼─────────────────┼─►              │   │
│ │Flight        │   │                 │ │Flight        │   │
│ │Server        │   │                 │ │Client        │   │
│ └──────────────┘   │                 │ └──────────────┘   │
│                    │                 │                    │
└────────────────────┘                 └────────────────────┘
        ▲                                      ▲
        │                                      │
        ▼                                      ▼
┌────────────────────┐                 ┌────────────────────┐
│ Local Storage      │                 │ Local Storage      │
│ DuckDB + Arrow     │                 │ DuckDB + Arrow     │
└────────────────────┘                 └────────────────────┘
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

| Variable | Description | Example |
|----------|-------------|---------|
| `CROSSLINK_MODE` | Set to "DISTRIBUTED" for distributed mode | `export CROSSLINK_MODE="DISTRIBUTED"` |
| `CROSSLINK_FLIGHT_HOST` | Host for the Flight server | `export CROSSLINK_FLIGHT_HOST="localhost"` |
| `CROSSLINK_FLIGHT_PORT` | Port for the Flight server | `export CROSSLINK_FLIGHT_PORT="8815"` |
| `CROSSLINK_MOTHER_NODE` | Address of the mother node (coordinator) | `export CROSSLINK_MOTHER_NODE="192.168.1.10:8815"` |
| `CROSSLINK_NODE_ADDRESS` | Address of this node | `export CROSSLINK_NODE_ADDRESS="192.168.1.11:8815"` |

## 🔄 Cross-Language Data Flow

When data moves between languages, CrossLink employs a specific pathway:

```
┌────────────────┐    ┌──────────────────┐    ┌────────────────┐
│ Source Lang    │    │ CrossLink C++    │    │ Target Lang    │
│ (e.g., Python) │    │ Core             │    │ (e.g., R)      │
└───────┬────────┘    └────────┬─────────┘    └───────┬────────┘
        │                      │                      │
        ▼                      │                      │
┌────────────────┐             │              ┌────────────────┐
│ Native Format  │             │              │ Native Format  │
│ (DataFrame)    │             │              │ (data.frame)   │
└───────┬────────┘             │              └───────┬────────┘
        │                      │                      ▲
        ▼                      │                      │
┌────────────────┐             │              ┌────────────────┐
│ Language Arrow │             │              │ Language Arrow │
│ (pyarrow.Table)│             │              │ (arrow::Table) │
└───────┬────────┘             │              └───────┬────────┘
        │                      │                      ▲
        ▼                      │                      │
┌───────┬────────┐    ┌────────┬─────────┐    ┌───────┬────────┐
│ Arrow C Data   │    │ arrow::Table     │    │ Arrow C Data   │
│ Interface      ├───►│  std::shared_ptr ├───►│ Interface      │
│ Struct         │    │  Memory Buffers  │    │ Reconstruction │
└────────────────┘    └──────────────────┘    └────────────────┘
```

For distributed data flow using Flight:

```
┌────────────────────┐       ┌──────────────────┐       ┌────────────────────┐
│ Source Node        │       │ Flight Protocol  │       │ Target Node        │
│ (Node A)           │       │                  │       │ (Node B)           │
└───────┬────────────┘       └────────┬─────────┘       └───────┬────────────┘
        │                             │                         │
        ▼                             │                         │
┌────────────────────┐                │                 ┌────────────────────┐
│ CrossLink          │                │                 │ CrossLink          │
│ flight_push()      │                │                 │ flight_pull()      │
└───────┬────────────┘                │                 └───────┬────────────┘
        │                             │                         ▲
        ▼                             │                         │
┌───────┬────────────┐       ┌────────┬─────────┐       ┌───────┬────────────┐
│ Flight Server      │       │ gRPC Stream      │       │ Flight Client      │
│ DoGet/DoPut        ├──────►│ Record Batches   ├──────►│ DoGet/DoPut        │
│                    │       │                  │       │                    │
└────────────────────┘       └──────────────────┘       └────────────────────┘
```

## 🛠️ Installation and Building

### Prerequisites

* C++17 compatible compiler (GCC, Clang, MSVC)
* CMake (version 3.15+)
* Apache Arrow C++ library (including development headers)
* Apache Arrow Flight C++ library (for distributed mode)
* DuckDB C++ library (for storage/querying)
* **For Python:** `pybind11`, `pyarrow`
* **For R:** `Rcpp`, `arrow` R package
* **For Julia:** `CxxWrap.jl`, `Arrow.jl`

> **Note:** By installing and using CrossLink, you agree to the [licensing terms](#-license). For enterprise/commercial use, please [obtain a license](#how-to-apply-for-an-enterprise-license) before installation.

### Building from Source

#### C++ Core Library

```bash
# Navigate to C++ directory
cd crosslink/cpp

# Configure with CMake, enabling Flight
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DCROSSLINK_ENABLE_FLIGHT=ON

# Build the library
cmake --build build --config Release

# Install (optional)
cmake --install build
```

#### Python Binding

```bash
# Install from source
cd crosslink/python
pip install -e .
```

#### R Binding

```r
# Install from source
install.packages("crosslink/r", repos=NULL, type="source")
```

#### Julia Binding

```julia
# Install from source
using Pkg
Pkg.add(path="crosslink/julia")
```

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

### Streaming API Example

CrossLink also supports streaming data with the Streaming API:

```python
from duckdata.crosslink import get_instance
import pandas as pd
import pyarrow as pa

# Initialize CrossLink
cl = get_instance()

# Create a schema for the stream
schema = pa.schema([
    pa.field('id', pa.int64()),
    pa.field('name', pa.string()),
    pa.field('timestamp', pa.timestamp('ms'))
])

# Create a stream writer
stream_id, writer = cl.push_stream(schema, name="sensor_stream")
print(f"Created stream with ID: {stream_id}")

# In another process or thread, create a stream reader
reader = cl.pull_stream(stream_id)

# Write batches to the stream
for i in range(10):
    batch_df = pd.DataFrame({
        'id': [i*10 + j for j in range(10)],
        'name': [f'sensor_{i*10+j}' for j in range(10)],
        'timestamp': pd.date_range(start='now', periods=10, freq='s')
    })
    batch = pa.RecordBatch.from_pandas(batch_df, schema=schema)
    writer.write_batch(batch)
    
# Read batches from the stream
while True:
    batch = reader.read_next_batch()
    if batch is None:
        break
    print(f"Received batch with {batch.num_rows} rows")
    print(batch.to_pandas())
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
  - `r/R/crosslink.R`: Main connection functions
  - `r/R/arrow_integration.R`: Arrow utilities
  - `r/R/data_operations.R`: Data transformation functions
  - `r/R/shared_memory.R`: Shared memory utilities
  - `r/src/cpp_wrapper.cpp`: Rcpp wrapper

### Julia Binding

- **Binding Technology**: `CxxWrap.jl`
- **Key Dependencies**:
  - `Arrow.jl`: Julia bindings for Arrow
  - `DuckDB.jl`: Julia bindings for DuckDB
  - `DataFrames.jl`: DataFrame integration
- **Module Structure**:
  - `julia/src/CrossLink.jl`: Main module
  - `julia/core/core.jl`: `CrossLinkManager` struct
  - `julia/shared_memory/cpp_wrapper.jl`: CxxWrap wrapper
  - `julia/arrow_integration/arrow_integration.jl`: Arrow utilities

## 📖 Documentation

For more detailed documentation:

- [C++ API Reference](https://crosslink.io/cpp/api) - Comprehensive C++ API documentation
- [Python API Reference](https://crosslink.io/python/api) - Python binding documentation
- [R API Reference](https://crosslink.io/r/api) - R binding documentation
- [Julia API Reference](https://crosslink.io/julia/api) - Julia binding documentation
- [User Guide](https://crosslink.io/guide) - Step-by-step guide to using CrossLink
- [Examples](https://crosslink.io/examples) - Example applications

## 🤝 Contributing

Contributions to CrossLink are welcome! Please feel free to submit a Pull Request or open an Issue.

### Development Setup

```bash
# Clone the repository
git clone https://github.com/arrow-mage/duckdata.git
cd duckdata/crosslink

# Build C++ core
cd cpp
cmake -S . -B build -DCMAKE_BUILD_TYPE=Debug -DCROSSLINK_ENABLE_TESTS=ON
cmake --build build
cd ..

# Install Python development version
cd python
pip install -e .
cd ..
```

## 📜 License

This project is available under a dual licensing model:

### Testing/Non-Commercial License
- **Free for**: Testing, evaluation, academic research, and personal non-commercial use
- **Limitations**: 
  - Not for use in production environments
  - Not for use in commercial products or services
  - No commercial support provided

### Enterprise License
- **Required for**: Commercial use, production deployments, and enterprise applications

### How to Apply for an Enterprise License
To obtain an enterprise license for CrossLink, please contact us at:
- Email: pranav.sateesh99@gmail.com


Please include information about your organization and intended use case.


