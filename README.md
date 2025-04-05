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

# CrossLink SoS Pipeline Benchmark

This benchmark demonstrates the performance benefits of using CrossLink for cross-language data sharing in a Script of Scripts (SoS) pipeline involving Python and R.

## Overview

In data science workflows, it's common to use multiple programming languages, each with their own strengths. For example, you might use Python for data preprocessing and R for specialized statistical analysis. Typically, these languages communicate by writing data to disk (e.g., CSV or Parquet files) and then reading it back in the other language.

CrossLink optimizes this process by:

1. Using Apache Arrow as a common in-memory data format
2. Enabling zero-copy data sharing between languages where possible
3. Using DuckDB for efficient metadata handling
4. Minimizing serialization/deserialization overhead

## Benchmark Description

This benchmark compares:

1. **Baseline**: Traditional approach where data is passed between Python and R via Parquet files on disk
2. **CrossLink**: Using CrossLink to share data between Python and R

The benchmark measures:
- Python processing time
- R processing time
- Data transfer time
- Total pipeline execution time

## Setup Requirements

- Python 3.8+ with the following packages:
  - pandas
  - numpy
  - pyarrow
  - matplotlib
  - duckdata.crosslink
- R 4.0+ with the following packages:
  - arrow
  - dplyr
  - duckdata.crosslink

## Running the Benchmark

```bash
# Make the benchmark script executable
chmod +x run_benchmark.sh

# Run the benchmark
./run_benchmark.sh
```

Results will be saved to:
- `benchmark_results.csv`: Detailed timing information
- `benchmark_comparison.png`: Visual comparison chart

## Understanding the Results

The benchmark measures several aspects of performance:

1. **Processing Time**: The time spent actually computing in each language (should be similar between baseline and CrossLink)
2. **Data Transfer Time**: Time spent on data serialization, IPC, and deserialization (should be significantly lower for CrossLink)
3. **Total Pipeline Time**: The overall execution time (should be lower for CrossLink)

The expected performance improvement with CrossLink depends on:
- Dataset size (larger datasets show greater benefits)
- Hardware setup (shared memory advantages depend on system memory architecture)
- Pipeline complexity (more inter-language exchanges show greater benefits)

## How it Works

### Baseline Pipeline
1. Generate synthetic data in Python
2. Write data to Parquet file
3. Process data in Python
4. Write processed data to Parquet file
5. Read the file in R
6. Process data in R
7. Write results to Parquet file

### CrossLink Pipeline
1. Generate synthetic data in Python
2. Share data via CrossLink
3. Process data in Python
4. Share processed data via CrossLink
5. Access shared data in R (potentially zero-copy)
6. Process data in R
7. Share results via CrossLink

## Further Optimizations

This benchmark demonstrates the basic performance improvements from CrossLink. Additional performance could be gained by:

1. Using streaming data transfer for large datasets
2. Configuring crosslink for memory-mapped file sharing
3. Optimizing the pipeline to minimize unnecessary data copying within each language

## Troubleshooting

If you encounter issues:

1. Ensure all required packages are installed
2. Check that CrossLink's C++ bindings are properly built and accessible
3. Verify that the library paths are set correctly in the environment
4. For detailed diagnosis, add the `debug=True` parameter when initializing CrossLink

# Arrow Mage Project

This project includes development work on various components, including R bindings for the CrossLink library located in `duckdata/crosslink/r/`.

## Current Status: R Bindings (`duckdata/crosslink/r/`)

Development of the R bindings has progressed through several stages of debugging:

1.  **Initial `.Call` Error:** Resolved the `"_CrossLink_crosslink_connect" not available for .Call()` error by:
    *   Adding the `@useDynLib CrossLink, .registration = TRUE` directive to the `NAMESPACE` file (via Roxygen tags).
    *   Creating a `src/Makevars` file to correctly link the R bindings against the core C++ `libcrosslink.dylib` library.

2.  **Dynamic Loading Errors:** Addressed runtime errors related to finding `libcrosslink.dylib` (`Library not loaded: @rpath/libcrosslink.dylib`) by setting an absolute RPATH in `src/Makevars`.
    *   *Note: This absolute path works locally but needs refinement for portability.* 

3.  **Rcpp Load Action Error:** Fixed an `_CrossLink_RcppExport_registerCCallable not found` error by deleting and regenerating Rcpp-related files (`R/RcppExports.R`, `src/RcppExports.cpp`).

4.  **Missing Namespace Exports:** Solved runtime errors like `'push' is not an exported object from 'namespace:CrossLink'` by adding `#' @export` Roxygen tags to the C++ functions in `src/r_binding.cpp` and regenerating/reinstalling.

5.  **Arrow C++/R Conversion Issues:** Worked through several compilation and runtime errors related to converting Arrow objects between R and C++ within the binding code (`src/r_binding.cpp`). This involved trying different approaches:
    *   Using internal `arrow` R functions (e.g., `.table_to_cpp`) - failed due to potential function removal/changes in newer `arrow` versions.
    *   Using the R6 object's `$pointer()` method - led to Rcpp conversion errors (`Not an S4 object`, `Expecting an external pointer`).
    *   Using `Rcpp::as<std::shared_ptr<...>>` directly on the pointer - led to `no matching constructor` errors.

6.  **Current Linker Error:** The latest attempt involved using the official Arrow R C++ bridge functions (`arrow::r::checked_import<Type>`). While the code compiles, the installation fails during the final linking/loading stage with:

    ```
    Error: package or namespace load failed for 'CrossLink' in dyn.load(...):
     unable to load shared object '/.../CrossLink.so':
      dlopen(/.../CrossLink.so, 0x0006): symbol not found in flat namespace '__ZN5arrow1r14checked_importINS_11RecordBatchEEENSt3__110shared_ptrIT_EEP7SEXPREC' 
    ```
    *(Symbol name corresponds to `arrow::r::checked_import<arrow::RecordBatch>(SEXPREC*)`)*

**Conclusion:**

The current blocker is a **linker error**. The build process successfully compiles the C++ code (`r_binding.cpp`, `RcppExports.cpp`) which uses `arrow::r::checked_import`, but it fails when linking the final `CrossLink.so` library because it cannot find the definition (implementation) of the `arrow::r::checked_import` function template provided by the Arrow R package's C++ library. 

This likely means that while `Rcpp::depends(arrow)` correctly provides the *headers* declaring `checked_import`, it doesn't automatically configure the linker to link against the specific Arrow R C++ library component where `checked_import` is *defined*. Further investigation is needed into how to properly configure `src/Makevars` (specifically `PKG_LIBS`) to link against the necessary Arrow R C++ object files or library component that provides these R-specific bridge functions.