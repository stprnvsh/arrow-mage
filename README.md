# CrossLink: High-Performance Cross-Language Data Sharing

<div align="center">

  <!-- Placeholder logo - Consider creating a specific logo for CrossLink -->
  ![CrossLink Logo](https://via.placeholder.com/300x150?text=CrossLink)

  [![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) <!-- Assuming MIT license -->
  [![C++](https://img.shields.io/badge/C++-17-blue.svg)](https://isocpp.org/)
  [![Python](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
  [![R](https://img.shields.io/badge/R-4.0+-blue.svg)](https://www.r-project.org/)
  [![Julia](https://img.shields.io/badge/julia-1.6+-blue.svg)](https://julialang.org/)

  **Seamless, high-performance data sharing between C++, Python, R, and Julia using Apache Arrow and DuckDB.**

</div>

## 📋 Overview

**CrossLink** is a library designed to facilitate efficient data sharing between processes running potentially different programming languages (C++, Python, R, Julia). It leverages the power of [Apache Arrow](https://arrow.apache.org/) for in-memory data representation and efficient serialization, and uses [DuckDB](https://duckdb.org/) for managing metadata about shared datasets and enabling SQL queries across them.

The core goal is to minimize data copying when passing data between language environments, utilizing techniques like shared memory and memory-mapped files where appropriate.

<div align="center">

```
   C++ <──────┐
    ▲         │ Zero-Copy (Shared Memory / MMap)
    │         ▼            ┌─────────────────┐
 Python <─────┼───────────▶│  CrossLink Core │
    ▲         │ (Arrow IPC)│ (C++ / Arrow /  │
    │         ▼            │    DuckDB)      │
      R <─────┼───────────▶└─────────────────┘
    ▲         │
    │         ▼
  Julia <─────┘

  (Metadata & SQL via DuckDB)
```

</div>

### Key Features

-   🔄 **Cross-Language Sharing**: Share data frames/tables between C++, Python, R, and Julia.
-   🚀 **High Performance**: Built on Apache Arrow for efficient in-memory format and serialization. Uses DuckDB for fast metadata lookups and SQL queries.
-   💾 **Zero-Copy Mechanisms**: Employs shared memory and memory-mapped files (via Arrow IPC format) to avoid data duplication when possible.
-   📊 **Batch & Streaming API**: Supports sharing entire datasets (batch) or sending/receiving data incrementally (streaming).
-   🔍 **Metadata & Querying**: Stores metadata about shared datasets in DuckDB, allowing listing and SQL querying.
-   🔔 **Notification System**: Allows processes to subscribe to notifications about data updates (C++ API).

## ⚙️ Building CrossLink

CrossLink uses CMake for building its C++ core and language bindings.

### Prerequisites

-   **CMake** (>= 3.14)
-   **C++ Compiler** (C++17 compatible, e.g., GCC, Clang, MSVC)
-   **Ninja** (Recommended build system for parallelism)
-   **Apache Arrow C++ Libraries** (>= 10.0.0, including development headers). Needs components like `arrow`, `arrow_dataset`, `parquet`.
-   **DuckDB C++ Library** (>= 1.2.1, including development headers).
-   **Language-Specific Build Tools (if building bindings):**
    -   **Python:** Python (>= 3.8) development headers, `pybind11`, `pyarrow`.
    -   **R:** R (>= 4.0), R development tools (`R.home('include')`), `Rcpp` R package, `arrow` R package (with C++ headers).
    -   **Julia:** Julia (>= 1.6), `CxxWrap.jl` package.

*Note: Ensure CMake can find the installed Arrow and DuckDB libraries. You might need to set `CMAKE_PREFIX_PATH`.*

### Build Steps

1.  **Clone the repository (assuming you have it):**
    ```bash
    # cd your-crosslink-directory
    ```

2.  **Configure using CMake:** Create a build directory and run CMake.
    ```bash
    cd duckdata/crosslink/cpp # Navigate to the C++ directory containing the main CMakeLists.txt
    mkdir build
    cd build

    # Basic configuration (adjust paths/options as needed)
    # Ensure Arrow and DuckDB are findable (e.g., via CMAKE_PREFIX_PATH)
    # export CMAKE_PREFIX_PATH=/path/to/arrow;/path/to/duckdb

    cmake .. -GNinja \
          -DBUILD_PYTHON_BINDINGS=ON \
          -DBUILD_R_BINDINGS=ON \
          -DBUILD_JULIA_BINDINGS=ON
          # Add other CMake options if necessary (e.g., -DCMAKE_INSTALL_PREFIX=...)
    ```
    *   You can turn `ON`/`OFF` the `BUILD_*_BINDINGS` options depending on which languages you need.

3.  **Compile:**
    ```bash
    ninja
    ```

4.  **Install (Optional but recommended):** This copies libraries and headers to a specified location (or default system location). CMake installs the bindings directly into the language source directories (`python/crosslink`, `r/libs`, `julia/lib`) by default based on the current `CMakeLists.txt`.
    ```bash
    # This step might not be strictly necessary if using the bindings directly from the build tree
    # ninja install
    ```

## ▶️ Usage Examples

### C++

```cpp
#include <crosslink/crosslink.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <iostream>
#include <vector>

int main() {
    // Sample Arrow Table creation (replace with your actual data)
    auto schema = arrow::schema({arrow::field("a", arrow::int64()), arrow::field("b", arrow::utf8())});
    arrow::Int64Builder int_builder;
    arrow::StringBuilder str_builder;
    int_builder.AppendValues({1, 2, 3});
    str_builder.AppendValues({"x", "y", "z"});
    std::shared_ptr<arrow::Array> arr_a, arr_b;
    int_builder.Finish(&arr_a);
    str_builder.Finish(&arr_b);
    auto table = arrow::Table::Make(schema, {arr_a, arr_b});

    try {
        // 1. Initialize CrossLink
        crosslink::CrossLink cl("my_crosslink_data.duckdb"); // Specify DB path

        // 2. Push the table (batch)
        std::string dataset_id = cl.push(table, "my_cpp_table", "Table from C++");
        std::cout << "Pushed table with ID: " << dataset_id << std::endl;

        // 3. Pull the table back
        std::shared_ptr<arrow::Table> retrieved_table = cl.pull(dataset_id);
        std::cout << "Pulled table with " << retrieved_table->num_rows() << " rows." << std::endl;
        std::cout << retrieved_table->ToString() << std::endl;

        // 4. Query using SQL
        auto query_result = cl.query("SELECT b, a*2 AS a_doubled FROM " + dataset_id + " WHERE a > 1");
        std::cout << "Query Result:\n" << query_result->ToString() << std::endl;

        // 5. List datasets
        auto datasets = cl.list_datasets();
        std::cout << "Available datasets:" << std::endl;
        for(const auto& id : datasets) {
            std::cout << " - " << id << std::endl;
        }

        // 6. Streaming API Example
        // 6a. Start pushing a stream
        auto [stream_id, writer] = cl.push_stream(schema, "my_cpp_stream");
        std::cout << "Started stream with ID: " << stream_id << std::endl;

        // 6b. Write batches to the stream
        writer->write_batch(arrow::RecordBatch::FromTable(*table).ValueOrDie()); // Write first batch
        // ... write more batches ...
        writer->close(); // Signal end of stream

        // 6c. Pull the stream (can be done in another process/language)
        auto reader = cl.pull_stream(stream_id);
        std::shared_ptr<arrow::RecordBatch> batch;
        std::cout << "Reading from stream " << stream_id << ":" << std::endl;
        while ((batch = reader->read_next_batch()) != nullptr) {
             std::cout << "  Read batch with " << batch->num_rows() << " rows." << std::endl;
             // Process the batch
        }
        std::cout << "End of stream." << std::endl;
        reader->close();


    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }

    return 0;
}
```

### Python

*Make sure the `crosslink_cpp.so` (or similar) file built by CMake is available in your Python environment (e.g., installed in `duckdata/crosslink/python/crosslink` or site-packages).*

```python
import pyarrow as pa
import pandas as pd
# Assuming the package structure allows this import
# This relies on the top-level __init__.py
import duckdata.crosslink as crosslink
# Alternatively, if the C++ binding is directly importable:
# from duckdata.crosslink.python.crosslink import CrossLinkCpp as CrossLink

try:
    # 1. Get a CrossLink instance (uses C++ backend if available)
    # The top-level __init__.py handles finding the backend
    cl = crosslink.get_instance(db_path="my_crosslink_data.duckdb")
    print("CrossLink instance acquired.")

    # --- Batch API ---
    # Create a sample PyArrow Table (or use Pandas DataFrame)
    # df = pd.DataFrame({'a': [10, 20, 30], 'b': ['p', 'q', 'r']})
    # table = pa.Table.from_pandas(df)
    table = pa.table({
        'col1': pa.array([1.0, 2.5, 3.0]),
        'col2': pa.array(['apple', 'banana', 'cherry'])
    })

    # 2. Push the table
    dataset_id = cl.push(table, name="my_python_table", description="Table from Python")
    print(f"Pushed table with ID: {dataset_id}")

    # 3. Pull a table (e.g., the one pushed from C++)
    # Use the ID returned from the C++ example run
    cpp_table_id = "my_cpp_table" # Or the actual UUID generated
    try:
       retrieved_table_py = cl.pull(cpp_table_id)
       print(f"Pulled table '{cpp_table_id}':\n{retrieved_table_py.to_pandas()}")
    except Exception as e:
       print(f"Could not pull C++ table '{cpp_table_id}': {e}")


    # 4. Query using SQL
    query_result_py = cl.query(f"SELECT * FROM {dataset_id} WHERE col1 > 2.0")
    print(f"Query result:\n{query_result_py.to_pandas()}")

    # 5. List datasets
    datasets = cl.list_datasets()
    print(f"Available datasets: {datasets}")

    # --- Streaming API ---
    # 6a. Start pushing a stream
    stream_id, writer = cl.push_stream(table.schema, name="my_python_stream")
    print(f"Started stream with ID: {stream_id}")

    # 6b. Write batches
    batch1 = table.slice(0, 2)
    batch2 = table.slice(2, 1)
    writer.write_batch(batch1)
    print("Wrote batch 1")
    writer.write_batch(batch2)
    print("Wrote batch 2")
    writer.close()
    print("Closed writer")

    # 6c. Pull the stream
    reader = cl.pull_stream(stream_id)
    print(f"Reading from stream {stream_id}:")
    for i, batch in enumerate(reader): # Reader is iterable
        print(f"  Read batch {i} with {batch.num_rows} rows:")
        print(batch.to_pandas())
    print("End of stream.")
    reader.close()


except Exception as e:
    print(f"An error occurred: {e}")
    import traceback
    traceback.print_exc()

# cl.cleanup() # C++ cleanup might be called by __del__ or context manager if implemented

```

### R & Julia

*(Examples TBD - Requires inspecting R/Julia wrapper code and confirming API)*

The R and Julia packages provide similar functionality. Refer to the respective package documentation or source code (`duckdata/crosslink/r` and `duckdata/crosslink/julia`) for specific API details.

## 🏛️ Architecture

CrossLink consists of:

1.  **Core C++ Library (`libcrosslink`):** Implements the main logic using Arrow C++ and DuckDB C++ APIs. Handles data serialization (Arrow IPC), shared memory management, memory-mapped files, metadata persistence, and the notification system.
2.  **Language Bindings:** Thin C++ layers (`crosslink_cpp.so`, `crosslink_r.so`, `crosslink_jl.so`) using `pybind11`, `Rcpp`, and `CxxWrap.jl` to expose the core library's functionality to each target language.
3.  **Language Packages/Modules:** Native wrappers (Python module, R package, Julia module) that provide an idiomatic API in each language, calling the underlying binding functions.

Data sharing primarily happens via:

*   **Arrow IPC Format:** Data is serialized into the Arrow IPC format (either stream or file format).
*   **Shared Memory:** For potentially zero-copy transfer, the serialized Arrow data can be placed into shared memory segments, identified by a key stored in the metadata.
*   **Memory-Mapped Files:** Alternatively, the serialized Arrow data (IPC file format) can be written to a file which other processes can then memory-map.
*   **DuckDB:** Stores metadata (schema, source language, timestamp, sharing method (shm key/file path), etc.) and allows SQL queries over registered datasets (potentially by creating views on the underlying Arrow data).

## Contributing

*(Add contribution guidelines here)*

## License

This project is licensed under the MIT License - see the LICENSE file for details.