#include <Rcpp.h>
#include "crosslink.h"
#include <arrow/api.h>
// #include <arrow/r/api.h>
#include <memory>

// [[Rcpp::depends(arrow)]]

using namespace Rcpp;
// Remove dependency on arrow::r namespace
// using namespace arrow::r;

// Forward declare helper functions
std::shared_ptr<arrow::Table> r_to_cpp_table(SEXP r_table);
SEXP cpp_to_r_table(const std::shared_ptr<arrow::Table>& table);
std::shared_ptr<arrow::Schema> r_to_cpp_schema(SEXP r_schema);
SEXP cpp_to_r_schema(const std::shared_ptr<arrow::Schema>& schema);
std::shared_ptr<arrow::RecordBatch> r_to_cpp_batch(SEXP r_batch);
SEXP cpp_to_r_batch(const std::shared_ptr<arrow::RecordBatch>& batch);

// Helper function to check status and stop on error
void CheckStatus(const arrow::Status& status, const std::string& context) {
    if (!status.ok()) {
        Rcpp::stop("Arrow error in " + context + ": " + status.ToString());
    }
}

// StreamWriter wrapper for R
class RStreamWriter {
public:
    RStreamWriter(std::shared_ptr<crosslink::StreamWriter> writer) : writer_(writer) {}
    
    void write_batch(SEXP r_batch) {
        if (!writer_) Rcpp::stop("StreamWriter is not valid (likely closed)");
        auto batch = r_to_cpp_batch(r_batch);
        writer_->write_batch(batch);
    }
    
    void close() {
        if (!writer_) Rcpp::stop("StreamWriter is not valid (likely closed)");
        writer_->close();
        writer_.reset(); // Clear the pointer to prevent further use
    }
    
    SEXP schema() {
        if (!writer_) Rcpp::stop("StreamWriter is not valid (likely closed)");
        return cpp_to_r_schema(writer_->schema());
    }
    
    std::string stream_id() {
        if (!writer_) Rcpp::stop("StreamWriter is not valid (likely closed)");
        return writer_->stream_id();
    }
    
private:
    std::shared_ptr<crosslink::StreamWriter> writer_;
};

// StreamReader wrapper for R
class RStreamReader {
public:
    RStreamReader(std::shared_ptr<crosslink::StreamReader> reader) : reader_(reader) {}
    
    SEXP read_next_batch() {
        if (!reader_) Rcpp::stop("StreamReader is not valid (likely closed)");
        auto batch = reader_->read_next_batch();
        if (!batch) return R_NilValue; // Return NULL for end of stream
        return cpp_to_r_batch(batch);
    }
    
    void close() {
        if (!reader_) Rcpp::stop("StreamReader is not valid (likely closed)");
        reader_->close();
        reader_.reset(); // Clear the pointer to prevent further use
    }
    
    SEXP schema() {
        if (!reader_) Rcpp::stop("StreamReader is not valid (likely closed)");
        return cpp_to_r_schema(reader_->schema());
    }
    
    std::string stream_id() {
        if (!reader_) Rcpp::stop("StreamReader is not valid (likely closed)");
        return reader_->stream_id();
    }
    
private:
    std::shared_ptr<crosslink::StreamReader> reader_;
};

//' @title Connect to CrossLink
//' @description Establishes a connection to the CrossLink backend.
//' @param db_path Path to the DuckDB database file. Defaults to "crosslink.duckdb".
//' @param debug Enable debug logging. Defaults to FALSE.
//' @return An external pointer (XPtr) to the CrossLink connection object.
//' @export
// [[Rcpp::export]]
SEXP crosslink_connect(std::string db_path = "crosslink.duckdb", bool debug = false) {
    auto* cl = new crosslink::CrossLink(db_path, debug);
    Rcpp::XPtr<crosslink::CrossLink> ptr(cl, true);
    return ptr;
}

// Simple implementation to directly convert R Arrow objects to C++ Arrow objects
// These replace the ImportTable, ExportTable, etc. functions from arrow::r

// Helper to convert R Arrow object (R6) to C++ Arrow table via XPtr
std::shared_ptr<arrow::Table> r_to_cpp_table(SEXP r_table) {
    if (!Rf_isEnvironment(r_table)) {
        Rcpp::stop("Expected an Arrow R6 Table object.");
    }
    Rcpp::Environment table_env(r_table);
    if (!table_env.exists("pointer")) {
         Rcpp::stop("Input object does not have a $pointer() method.");
    }
    Rcpp::Function pointer_method = table_env["pointer"];
    SEXP xptr_sexp = pointer_method();
    if (TYPEOF(xptr_sexp) != EXTPTRSXP) {
        Rcpp::stop("$pointer() did not return an external pointer.");
    }
    try {
        // Assumes Arrow R stores XPtr<std::shared_ptr<arrow::Table>>
        Rcpp::XPtr<std::shared_ptr<arrow::Table>> xptr(xptr_sexp);
        return *xptr; // Return the shared_ptr held by the XPtr
    } catch (Rcpp::not_compatible& e) {
        Rcpp::stop("External pointer is not compatible with XPtr<std::shared_ptr<arrow::Table>>. Error: %s", e.what());
    } catch (...) {
        Rcpp::stop("Failed to convert external pointer from $pointer().");
    }
}

// Helper to convert C++ Arrow table to R Arrow object (R6) via XPtr
SEXP cpp_to_r_table(const std::shared_ptr<arrow::Table>& table) {
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    // Rcpp::Function factory_func; // <- Cannot default construct

    // Wrap the shared_ptr in an XPtr managed by Rcpp
    Rcpp::XPtr<std::shared_ptr<arrow::Table>> xptr(new std::shared_ptr<arrow::Table>(table));

    // Prefer a dedicated C-level import function if available
    if (arrow_env.exists("Table__import_from_c")) {
        Rcpp::Function factory_func = arrow_env["Table__import_from_c"];
        return factory_func(xptr);
    } else if (arrow_env.exists("Table")) {
        Rcpp::Environment class_env = arrow_env["Table"];
        if (class_env.exists(".unsafe_from_c")) {
             Rcpp::Function factory_func = class_env[".unsafe_from_c"];
             return factory_func(xptr);
        } else if (class_env.exists("new")) {
             Rcpp::Function factory_func = class_env["new"]; // Fallback to R6 $new
              try {
                return factory_func(xptr); // Try passing directly (might work for .unsafe_from_c)
             } catch(...) {
                try {
                    return factory_func(Rcpp::Named("xp") = xptr); // Try passing as named arg 'xp' (for $new)
                } catch (...) {
                     Rcpp::stop("Failed to call Arrow R6 Table constructor/factory with external pointer.");
                }
             }
        } else {
             Rcpp::stop("Cannot find constructor/factory method for arrow::Table R6 class.");
        }
    } else {
        Rcpp::stop("Cannot find arrow::Table R6 class generator or factory function.");
    }
    // Should not be reached if logic above is correct
    return R_NilValue;
}

// Helper to convert R Arrow Schema (R6) to C++ Arrow Schema via XPtr
std::shared_ptr<arrow::Schema> r_to_cpp_schema(SEXP r_schema) {
    if (!Rf_isEnvironment(r_schema)) {
        Rcpp::stop("Expected an Arrow R6 Schema object.");
    }
    Rcpp::Environment schema_env(r_schema);
     if (!schema_env.exists("pointer")) {
         Rcpp::stop("Input object does not have a $pointer() method.");
    }
    Rcpp::Function pointer_method = schema_env["pointer"];
    SEXP xptr_sexp = pointer_method();
    if (TYPEOF(xptr_sexp) != EXTPTRSXP) {
        Rcpp::stop("$pointer() did not return an external pointer.");
    }
     try {
        // Assumes Arrow R stores XPtr<std::shared_ptr<arrow::Schema>>
        Rcpp::XPtr<std::shared_ptr<arrow::Schema>> xptr(xptr_sexp);
        return *xptr; // Return the shared_ptr held by the XPtr
    } catch (Rcpp::not_compatible& e) {
        Rcpp::stop("External pointer is not compatible with XPtr<std::shared_ptr<arrow::Schema>>. Error: %s", e.what());
    } catch (...) {
        Rcpp::stop("Failed to convert external pointer from $pointer().");
    }
}

// Helper to convert C++ Arrow Schema to R Arrow Schema (R6) via XPtr
SEXP cpp_to_r_schema(const std::shared_ptr<arrow::Schema>& schema) {
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    // Rcpp::Function factory_func; // <- Cannot default construct

    Rcpp::XPtr<std::shared_ptr<arrow::Schema>> xptr(new std::shared_ptr<arrow::Schema>(schema));

    if (arrow_env.exists("Schema__import_from_c")) {
        Rcpp::Function factory_func = arrow_env["Schema__import_from_c"];
        return factory_func(xptr);
    } else if (arrow_env.exists("Schema")) {
        Rcpp::Environment class_env = arrow_env["Schema"];
        if (class_env.exists(".unsafe_from_c")) {
             Rcpp::Function factory_func = class_env[".unsafe_from_c"];
             return factory_func(xptr);
        } else if (class_env.exists("new")) {
             Rcpp::Function factory_func = class_env["new"];
             try {
                return factory_func(xptr);
             } catch(...) {
                try {
                    return factory_func(Rcpp::Named("xp") = xptr);
                } catch (...) {
                     Rcpp::stop("Failed to call Arrow R6 Schema constructor/factory with external pointer.");
                }
             }
        } else {
             Rcpp::stop("Cannot find constructor/factory method for arrow::Schema R6 class.");
        }
    } else {
        Rcpp::stop("Cannot find arrow::Schema R6 class generator or factory function.");
    }
    return R_NilValue;
}

// Helper to convert R Arrow RecordBatch (R6) to C++ Arrow RecordBatch via XPtr
std::shared_ptr<arrow::RecordBatch> r_to_cpp_batch(SEXP r_batch) {
    if (!Rf_isEnvironment(r_batch)) {
        Rcpp::stop("Expected an Arrow R6 RecordBatch object.");
    }
    Rcpp::Environment batch_env(r_batch);
    if (!batch_env.exists("pointer")) {
         Rcpp::stop("Input object does not have a $pointer() method.");
    }
    Rcpp::Function pointer_method = batch_env["pointer"];
    SEXP xptr_sexp = pointer_method();
     if (TYPEOF(xptr_sexp) != EXTPTRSXP) {
        Rcpp::stop("$pointer() did not return an external pointer.");
    }
    try {
        // Assumes Arrow R stores XPtr<std::shared_ptr<arrow::RecordBatch>>
        Rcpp::XPtr<std::shared_ptr<arrow::RecordBatch>> xptr(xptr_sexp);
        return *xptr; // Return the shared_ptr held by the XPtr
    } catch (Rcpp::not_compatible& e) {
        Rcpp::stop("External pointer is not compatible with XPtr<std::shared_ptr<arrow::RecordBatch>>. Error: %s", e.what());
    } catch (...) {
        Rcpp::stop("Failed to convert external pointer from $pointer().");
    }
}

// Helper to convert C++ Arrow RecordBatch to R Arrow RecordBatch (R6) via XPtr
SEXP cpp_to_r_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    // Rcpp::Function factory_func; // <- Cannot default construct

    Rcpp::XPtr<std::shared_ptr<arrow::RecordBatch>> xptr(new std::shared_ptr<arrow::RecordBatch>(batch));

    if (arrow_env.exists("RecordBatch__import_from_c")) {
        Rcpp::Function factory_func = arrow_env["RecordBatch__import_from_c"];
        return factory_func(xptr);
    } else if (arrow_env.exists("RecordBatch")) {
        Rcpp::Environment class_env = arrow_env["RecordBatch"];
        if (class_env.exists(".unsafe_from_c")) {
             Rcpp::Function factory_func = class_env[".unsafe_from_c"];
             return factory_func(xptr);
        } else if (class_env.exists("new")) {
             Rcpp::Function factory_func = class_env["new"];
             try {
                return factory_func(xptr);
             } catch(...) {
                try {
                    return factory_func(Rcpp::Named("xp") = xptr);
                } catch (...) {
                     Rcpp::stop("Failed to call Arrow R6 RecordBatch constructor/factory with external pointer.");
                }
             }
        } else {
             Rcpp::stop("Cannot find constructor/factory method for arrow::RecordBatch R6 class.");
        }
    } else {
        Rcpp::stop("Cannot find arrow::RecordBatch R6 class generator or factory function.");
    }
    return R_NilValue;
}

//' @title Push Data
//' @export
// [[Rcpp::export]]
std::string crosslink_push(SEXP cl_ptr, SEXP arrow_table, 
                          std::string name = "", 
                          std::string description = "") {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Convert R Arrow table to C++ Arrow table
    auto table = r_to_cpp_table(arrow_table);
    
    // Push the table using the C++ API
    return cl->push(table, name, description);
}

//' @title Pull Data
//' @export
// [[Rcpp::export]]
SEXP crosslink_pull(SEXP cl_ptr, std::string identifier) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Pull the table using the C++ API
    auto table = cl->pull(identifier);
    
    // Convert C++ Arrow table to R Arrow object
    return cpp_to_r_table(table);
}

//' @title Query Data
//' @export
// [[Rcpp::export]]
SEXP crosslink_query(SEXP cl_ptr, std::string sql) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Execute the query using the C++ API
    auto table = cl->query(sql);
    
    // Convert C++ Arrow table to R Arrow object
    return cpp_to_r_table(table);
}

//' @title List Datasets
//' @export
// [[Rcpp::export]]
std::vector<std::string> crosslink_list_datasets(SEXP cl_ptr) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    return cl->list_datasets();
}

// === New Streaming Methods ===

//' @title Push Stream Start
//' @export
// [[Rcpp::export]]
Rcpp::List crosslink_push_stream(SEXP cl_ptr, SEXP schema, std::string name = "") {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Convert R schema to C++ schema
    auto cpp_schema = r_to_cpp_schema(schema);
    
    // Push stream using the C++ API
    auto result = cl->push_stream(cpp_schema, name);
    std::string stream_id = result.first;
    
    // Create and return a list with the stream ID and a wrapped StreamWriter
    Rcpp::XPtr<RStreamWriter> writer_ptr(new RStreamWriter(result.second), true);
    
    Rcpp::List ret;
    ret["stream_id"] = stream_id;
    ret["writer"] = writer_ptr;
    
    return ret;
}

//' @title Pull Stream Start
//' @export
// [[Rcpp::export]]
SEXP crosslink_pull_stream(SEXP cl_ptr, std::string stream_id) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Pull stream using the C++ API
    auto reader = cl->pull_stream(stream_id);
    
    // Create and return a wrapped StreamReader
    Rcpp::XPtr<RStreamReader> reader_ptr(new RStreamReader(reader), true);
    return reader_ptr;
}

//' @title Stream Writer Write Batch
//' @export
// [[Rcpp::export]]
void crosslink_stream_writer_write_batch(SEXP writer_ptr, SEXP batch) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    writer->write_batch(batch);
}

//' @title Stream Writer Close
//' @export
// [[Rcpp::export]]
void crosslink_stream_writer_close(SEXP writer_ptr) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    writer->close();
}

//' @title Stream Writer Get Schema
//' @export
// [[Rcpp::export]]
SEXP crosslink_stream_writer_schema(SEXP writer_ptr) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    return writer->schema();
}

//' @title Stream Writer Get ID
//' @export
// [[Rcpp::export]]
std::string crosslink_stream_writer_id(SEXP writer_ptr) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    return writer->stream_id();
}

//' @title Stream Reader Read Next Batch
//' @export
// [[Rcpp::export]]
SEXP crosslink_stream_reader_read_next_batch(SEXP reader_ptr) {
    Rcpp::XPtr<RStreamReader> reader(reader_ptr);
    return reader->read_next_batch();
}

//' @title Stream Reader Close
//' @export
// [[Rcpp::export]]
void crosslink_stream_reader_close(SEXP reader_ptr) {
    Rcpp::XPtr<RStreamReader> reader(reader_ptr);
    reader->close();
}

//' @title Stream Reader Get Schema
//' @export
// [[Rcpp::export]]
SEXP crosslink_stream_reader_schema(SEXP reader_ptr) {
    Rcpp::XPtr<RStreamReader> reader(reader_ptr);
    return reader->schema();
}

//' @title Stream Reader Get ID
//' @export
// [[Rcpp::export]]
std::string crosslink_stream_reader_id(SEXP reader_ptr) {
    Rcpp::XPtr<RStreamReader> reader(reader_ptr);
    return reader->stream_id();
}

// Callback wrapper to handle R function callbacks
class RCallbackWrapper {
public:
    RCallbackWrapper(Function callback) : callback_(callback) {}
    
    void operator()(const std::string& dataset_id, const std::string& event_type) {
        try {
            callback_(dataset_id, event_type);
        } catch (const std::exception& e) {
            Rcpp::warning("Error in R callback: %s", e.what());
        }
    }
    
private:
    Function callback_;
};

// Store callback wrappers to prevent them from being garbage collected
std::unordered_map<std::string, std::shared_ptr<RCallbackWrapper>> callback_registry;

//' @title Register Notification Callback
//' @export
// [[Rcpp::export]]
std::string crosslink_register_notification(SEXP cl_ptr, Function callback) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Create a callback wrapper
    auto wrapper = std::make_shared<RCallbackWrapper>(callback);
    
    // Register with CrossLink
    std::string id = cl->register_notification(
        [wrapper](const std::string& dataset_id, const std::string& event_type) {
            (*wrapper)(dataset_id, event_type);
        }
    );
    
    // Store the wrapper to prevent garbage collection
    callback_registry[id] = wrapper;
    
    return id;
}

//' @title Unregister Notification Callback
//' @export
// [[Rcpp::export]]
void crosslink_unregister_notification(SEXP cl_ptr, std::string registration_id) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    cl->unregister_notification(registration_id);
    
    // Remove from registry
    callback_registry.erase(registration_id);
}

//' @title Cleanup CrossLink Connection
//' @export
// [[Rcpp::export]]
void crosslink_cleanup(SEXP cl_ptr) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    cl->cleanup();
} 