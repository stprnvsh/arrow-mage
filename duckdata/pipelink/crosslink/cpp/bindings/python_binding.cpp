#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>
#include "../include/crosslink.h"
#include <arrow/python/pyarrow.h>

namespace py = pybind11;

// Helper function to convert C++ Arrow table to Python
py::object cpp_arrow_to_python(const std::shared_ptr<arrow::Table>& table) {
    // Initialize PyArrow
    arrow::py::import_pyarrow();
    
    // Convert Arrow C++ table to PyArrow table
    PyObject* py_table_obj = arrow::py::wrap_table(table);
    py::object py_table = py::reinterpret_steal<py::object>(py_table_obj);
    return py_table;
}

// Helper function to convert Python Arrow table to C++
std::shared_ptr<arrow::Table> python_arrow_to_cpp(py::object py_table) {
    // Initialize PyArrow
    arrow::py::import_pyarrow();
    
    // Convert PyArrow table to Arrow C++ table
    auto result = arrow::py::unwrap_table(py_table.ptr());
    if (!result.ok()) {
        throw std::runtime_error("Failed to unwrap PyArrow table");
    }
    // Use ValueUnsafe() instead of ValueOrDie() which is deprecated in newer Arrow versions
    return result.ValueUnsafe();
}

PYBIND11_MODULE(crosslink_cpp, m) {
    m.doc() = "CrossLink C++ backend for zero-copy data sharing";
    
    // Initialize PyArrow
    if (arrow::py::import_pyarrow() < 0) {
        throw std::runtime_error("Failed to initialize PyArrow");
    }
    
    py::class_<crosslink::CrossLink>(m, "CrossLinkCpp")
        .def(py::init<const std::string&, bool>(), 
             py::arg("db_path") = "crosslink.duckdb", 
             py::arg("debug") = false)
        .def("push", [](crosslink::CrossLink& self, py::object py_table, 
                         const std::string& name, const std::string& description) {
            // Convert PyArrow table to C++ Arrow table
            auto table = python_arrow_to_cpp(py_table);
            
            // Push the table using the C++ API
            return self.push(table, name, description);
        }, py::arg("table"), py::arg("name") = "", py::arg("description") = "")
        .def("pull", [](crosslink::CrossLink& self, const std::string& identifier) {
            // Pull the table using the C++ API
            auto table = self.pull(identifier);
            
            // Convert C++ Arrow table to PyArrow table
            return cpp_arrow_to_python(table);
        }, py::arg("identifier"))
        .def("query", [](crosslink::CrossLink& self, const std::string& sql) {
            // Execute the query using the C++ API
            auto table = self.query(sql);
            
            // Convert C++ Arrow table to PyArrow table
            return cpp_arrow_to_python(table);
        }, py::arg("sql"))
        .def("list_datasets", &crosslink::CrossLink::list_datasets)
        .def("register_notification", &crosslink::CrossLink::register_notification)
        .def("unregister_notification", &crosslink::CrossLink::unregister_notification)
        .def("cleanup", &crosslink::CrossLink::cleanup);
} 