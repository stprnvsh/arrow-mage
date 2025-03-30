#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>
#include <pybind11/chrono.h>
#include "../include/crosslink.h"
#include <arrow/python/pyarrow.h>
#include <utility>

namespace py = pybind11;

// === Arrow <-> PyArrow Conversion Helpers ===

// Helper function to convert C++ Arrow Table to Python
py::object cpp_table_to_python(const std::shared_ptr<arrow::Table>& table) {
    arrow::py::import_pyarrow();
    PyObject* py_obj = arrow::py::wrap_table(table);
    if (!py_obj) throw std::runtime_error("Failed to wrap arrow::Table");
    return py::reinterpret_steal<py::object>(py_obj);
}

// Helper function to convert Python Arrow Table to C++
std::shared_ptr<arrow::Table> python_table_to_cpp(py::object py_table) {
    arrow::py::import_pyarrow();
    auto result = arrow::py::unwrap_table(py_table.ptr());
    if (!result.ok()) {
        throw std::runtime_error("Failed to unwrap PyArrow Table: " + result.status().ToString());
    }
    return result.ValueUnsafe();
}

// Helper function to convert C++ Arrow Schema to Python
py::object cpp_schema_to_python(const std::shared_ptr<arrow::Schema>& schema) {
    arrow::py::import_pyarrow();
    PyObject* py_obj = arrow::py::wrap_schema(schema);
     if (!py_obj) throw std::runtime_error("Failed to wrap arrow::Schema");
    return py::reinterpret_steal<py::object>(py_obj);
}

// Helper function to convert Python Arrow Schema to C++
std::shared_ptr<arrow::Schema> python_schema_to_cpp(py::object py_schema) {
    arrow::py::import_pyarrow();
    auto result = arrow::py::unwrap_schema(py_schema.ptr());
    if (!result.ok()) {
        throw std::runtime_error("Failed to unwrap PyArrow Schema: " + result.status().ToString());
    }
    return result.ValueUnsafe();
}

// Helper function to convert C++ Arrow RecordBatch to Python
py::object cpp_batch_to_python(const std::shared_ptr<arrow::RecordBatch>& batch) {
    arrow::py::import_pyarrow();
    PyObject* py_obj = arrow::py::wrap_batch(batch);
     if (!py_obj) throw std::runtime_error("Failed to wrap arrow::RecordBatch");
    return py::reinterpret_steal<py::object>(py_obj);
}

// Helper function to convert Python Arrow RecordBatch to C++
std::shared_ptr<arrow::RecordBatch> python_batch_to_cpp(py::object py_batch) {
    arrow::py::import_pyarrow();
    auto result = arrow::py::unwrap_batch(py_batch.ptr());
    if (!result.ok()) {
        throw std::runtime_error("Failed to unwrap PyArrow RecordBatch: " + result.status().ToString());
    }
    return result.ValueUnsafe();
}

// === Pybind11 Module Definition ===

PYBIND11_MODULE(crosslink_cpp, m) {
    m.doc() = "CrossLink C++ backend for zero-copy data sharing with streaming";

    // Initialize PyArrow (must be done once)
    if (arrow::py::import_pyarrow() < 0) {
        throw std::runtime_error("Failed to initialize PyArrow integration");
    }

    // === Bind StreamWriter ===
    py::class_<crosslink::StreamWriter, std::shared_ptr<crosslink::StreamWriter>>(m, "StreamWriter")
        .def("write_batch", [](crosslink::StreamWriter& self, py::object py_batch) {
            auto batch = python_batch_to_cpp(py_batch);
            // Release GIL as this might block briefly on mutex
            py::gil_scoped_release release;
            self.write_batch(batch);
        }, py::arg("batch"), "Write a pyarrow.RecordBatch to the stream.")
        .def("close", &crosslink::StreamWriter::close,
             "Signal the end of the stream. No more batches can be written.")
        .def("schema", [](crosslink::StreamWriter& self) {
            return cpp_schema_to_python(self.schema());
        }, "Get the pyarrow.Schema for this stream.")
         .def("stream_id", &crosslink::StreamWriter::stream_id,
             "Get the unique ID for this stream.");

    // === Bind StreamReader ===
    py::class_<crosslink::StreamReader, std::shared_ptr<crosslink::StreamReader>>(m, "StreamReader")
        .def("read_next_batch", [](crosslink::StreamReader& self) -> py::object {
            std::shared_ptr<arrow::RecordBatch> batch;
            {
                // Release GIL while potentially waiting for data
                py::gil_scoped_release release;
                batch = self.read_next_batch();
            }
            if (batch) {
                return cpp_batch_to_python(batch);
            } else {
                return py::none(); // Return None when stream ends
            }
        }, py::call_guard<py::gil_scoped_acquire>(), // Re-acquire GIL before returning Python object
           "Read the next pyarrow.RecordBatch from the stream. Blocks if unavailable, returns None if stream ended.")
        .def("close", &crosslink::StreamReader::close,
             "Close the reader and release resources.")
        .def("schema", [](crosslink::StreamReader& self) {
            return cpp_schema_to_python(self.schema());
        }, "Get the pyarrow.Schema for this stream.")
        .def("stream_id", &crosslink::StreamReader::stream_id,
             "Get the unique ID for this stream.")
         // Make the reader iterable
         // __iter__ should return an iterator object. In this case, the reader itself is the iterator.
         .def("__iter__", [](crosslink::StreamReader& self) -> crosslink::StreamReader& { return self; }, py::keep_alive<0, 1>())
         // __next__ is called to get the next item
         .def("__next__", [](crosslink::StreamReader& self) {
            std::shared_ptr<arrow::RecordBatch> batch;
             {
                 py::gil_scoped_release release;
                 batch = self.read_next_batch();
             }
            if (batch) {
                return cpp_batch_to_python(batch);
            } else {
                throw py::stop_iteration();
            }
        }, py::call_guard<py::gil_scoped_acquire>());

    // === Bind CrossLink Main Class ===
    py::class_<crosslink::CrossLink>(m, "CrossLinkCpp")
        .def(py::init<const std::string&, bool>(),
             py::arg("db_path") = "crosslink.duckdb",
             py::arg("debug") = false,
             "Initialize the CrossLink C++ core.")

        // --- Batch Methods ---
        .def("push", [](crosslink::CrossLink& self, py::object py_table,
                         const std::string& name, const std::string& description) {
            auto table = python_table_to_cpp(py_table);
            return self.push(table, name, description);
        }, py::arg("table"), py::arg("name") = "", py::arg("description") = "",
           "Push a pyarrow.Table for batch sharing.")

        .def("pull", [](crosslink::CrossLink& self, const std::string& identifier) {
            auto table = self.pull(identifier);
            return cpp_table_to_python(table);
        }, py::arg("identifier"), "Pull a shared batch dataset as a pyarrow.Table.")

        .def("query", [](crosslink::CrossLink& self, const std::string& sql) {
            auto table = self.query(sql);
            // Assuming query currently returns a Table, adjust if it returns something else
            return cpp_table_to_python(table);
        }, py::arg("sql"), "Execute SQL query, return results as pyarrow.Table.")

        .def("list_datasets", &crosslink::CrossLink::list_datasets,
             "List available batch datasets.")

        // --- Streaming Methods ---
        .def("push_stream", [](crosslink::CrossLink& self, py::object py_schema, const std::string& name) {
            auto schema_cpp = python_schema_to_cpp(py_schema);
            // push_stream returns std::pair<std::string, std::shared_ptr<StreamWriter>>
            // pybind11 handles conversion of the pair to a Python tuple
            // and the shared_ptr to the bound StreamWriter Python type.
            return self.push_stream(schema_cpp, name);
        }, py::arg("schema"), py::arg("name") = "",
           "Start a new data stream, returning (stream_id, StreamWriter).")

        .def("pull_stream", [](crosslink::CrossLink& self, const std::string& stream_id) {
             // pull_stream returns std::shared_ptr<StreamReader>
             // pybind11 handles conversion to the bound StreamReader Python type.
            return self.pull_stream(stream_id);
        }, py::arg("stream_id"), "Connect to an existing data stream, returning StreamReader.")

        // --- Notification Methods ---
        .def("register_notification", &crosslink::CrossLink::register_notification,
             py::arg("callback"), "Register a callback for notifications.")
        .def("unregister_notification", &crosslink::CrossLink::unregister_notification,
             py::arg("registration_id"), "Unregister a notification callback.")

        // --- Management Methods ---
        .def("cleanup", &crosslink::CrossLink::cleanup,
             "Clean up C++ resources.");

    // Expose helper function (optional, for testing maybe?)
    m.def("is_cpp_available", []() { return true; }, "Check if C++ bindings were successfully loaded.");
} 