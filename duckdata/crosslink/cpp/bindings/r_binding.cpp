#include <Rcpp.h>
#include "../include/crosslink.h"
#include <arrow/api.h>
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

// [[Rcpp::export]]
SEXP crosslink_connect(std::string db_path = "crosslink.duckdb", bool debug = false) {
    auto* cl = new crosslink::CrossLink(db_path, debug);
    Rcpp::XPtr<crosslink::CrossLink> ptr(cl, true);
    return ptr;
}

// Simple implementation to directly convert R Arrow objects to C++ Arrow objects
// These replace the ImportTable, ExportTable, etc. functions from arrow::r

// Helper to convert R Arrow object to C++ Arrow table
std::shared_ptr<arrow::Table> r_to_cpp_table(SEXP r_table) {
    // Get the external pointer from the R object
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    Rcpp::Function as_arrow_table = arrow_env["as_arrow_table"];
    Rcpp::Function table_to_cpp = arrow_env[".table_to_cpp"];
    
    // First ensure we have an Arrow Table
    SEXP arrow_table = as_arrow_table(r_table);
    // Then extract the C++ pointer
    SEXP table_xptr = table_to_cpp(arrow_table);
    
    // Get the XPtr containing the C++ pointer
    Rcpp::XPtr<std::shared_ptr<arrow::Table>> table_ptr(table_xptr);
    
    return *table_ptr.get();
}

// Helper to convert C++ Arrow table to R Arrow object
SEXP cpp_to_r_table(const std::shared_ptr<arrow::Table>& table) {
    // Use R's Arrow package to convert C++ table to R
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    Rcpp::Function wrap_table = arrow_env["Table__from_xptr"];
    
    // Create a new pointer to wrap the table
    auto* table_ptr = new std::shared_ptr<arrow::Table>(table);
    Rcpp::XPtr<std::shared_ptr<arrow::Table>> xptr(table_ptr, true);
    
    // Call the wrap function
    return wrap_table(xptr);
}

// Helper to convert R Arrow Schema to C++ Arrow Schema
std::shared_ptr<arrow::Schema> r_to_cpp_schema(SEXP r_schema) {
    // Get the external pointer from the R object
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    Rcpp::Function schema_to_cpp = arrow_env["as_schema"];
    
    // Extract the C++ pointer
    SEXP schema_sexp = schema_to_cpp(r_schema);
    Rcpp::XPtr<std::shared_ptr<arrow::Schema>> schema_ptr(schema_sexp);
    
    return *schema_ptr.get();
}

// Helper to convert C++ Arrow Schema to R Arrow Schema
SEXP cpp_to_r_schema(const std::shared_ptr<arrow::Schema>& schema) {
    // Use R's Arrow package to convert C++ schema to R
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    Rcpp::Function wrap_schema = arrow_env["Schema__from_xptr"];
    
    // Create a new pointer to wrap the schema
    auto* schema_ptr = new std::shared_ptr<arrow::Schema>(schema);
    Rcpp::XPtr<std::shared_ptr<arrow::Schema>> xptr(schema_ptr, true);
    
    // Call the wrap function
    return wrap_schema(xptr);
}

// Helper to convert R Arrow RecordBatch to C++ Arrow RecordBatch
std::shared_ptr<arrow::RecordBatch> r_to_cpp_batch(SEXP r_batch) {
    // Get the external pointer from the R object
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    Rcpp::Function batch_to_cpp = arrow_env["as_record_batch"];
    
    // Extract the C++ pointer
    SEXP batch_sexp = batch_to_cpp(r_batch);
    Rcpp::XPtr<std::shared_ptr<arrow::RecordBatch>> batch_ptr(batch_sexp);
    
    return *batch_ptr.get();
}

// Helper to convert C++ Arrow RecordBatch to R Arrow RecordBatch
SEXP cpp_to_r_batch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Use R's Arrow package to convert C++ batch to R
    Rcpp::Environment arrow_env = Rcpp::Environment::namespace_env("arrow");
    Rcpp::Function wrap_batch = arrow_env["RecordBatch__from_xptr"];
    
    // Create a new pointer to wrap the batch
    auto* batch_ptr = new std::shared_ptr<arrow::RecordBatch>(batch);
    Rcpp::XPtr<std::shared_ptr<arrow::RecordBatch>> xptr(batch_ptr, true);
    
    // Call the wrap function
    return wrap_batch(xptr);
}

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

// [[Rcpp::export]]
SEXP crosslink_pull(SEXP cl_ptr, std::string identifier) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Pull the table using the C++ API
    auto table = cl->pull(identifier);
    
    // Convert C++ Arrow table to R Arrow object
    return cpp_to_r_table(table);
}

// [[Rcpp::export]]
SEXP crosslink_query(SEXP cl_ptr, std::string sql) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Execute the query using the C++ API
    auto table = cl->query(sql);
    
    // Convert C++ Arrow table to R Arrow object
    return cpp_to_r_table(table);
}

// [[Rcpp::export]]
std::vector<std::string> crosslink_list_datasets(SEXP cl_ptr) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    return cl->list_datasets();
}

// === New Streaming Methods ===

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

// [[Rcpp::export]]
SEXP crosslink_pull_stream(SEXP cl_ptr, std::string stream_id) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    // Pull stream using the C++ API
    auto reader = cl->pull_stream(stream_id);
    
    // Create and return a wrapped StreamReader
    Rcpp::XPtr<RStreamReader> reader_ptr(new RStreamReader(reader), true);
    return reader_ptr;
}

// [[Rcpp::export]]
void crosslink_stream_writer_write_batch(SEXP writer_ptr, SEXP batch) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    writer->write_batch(batch);
}

// [[Rcpp::export]]
void crosslink_stream_writer_close(SEXP writer_ptr) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    writer->close();
}

// [[Rcpp::export]]
SEXP crosslink_stream_writer_schema(SEXP writer_ptr) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    return writer->schema();
}

// [[Rcpp::export]]
std::string crosslink_stream_writer_id(SEXP writer_ptr) {
    Rcpp::XPtr<RStreamWriter> writer(writer_ptr);
    return writer->stream_id();
}

// [[Rcpp::export]]
SEXP crosslink_stream_reader_read_next_batch(SEXP reader_ptr) {
    Rcpp::XPtr<RStreamReader> reader(reader_ptr);
    return reader->read_next_batch();
}

// [[Rcpp::export]]
void crosslink_stream_reader_close(SEXP reader_ptr) {
    Rcpp::XPtr<RStreamReader> reader(reader_ptr);
    reader->close();
}

// [[Rcpp::export]]
SEXP crosslink_stream_reader_schema(SEXP reader_ptr) {
    Rcpp::XPtr<RStreamReader> reader(reader_ptr);
    return reader->schema();
}

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

// [[Rcpp::export]]
void crosslink_unregister_notification(SEXP cl_ptr, std::string registration_id) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    
    cl->unregister_notification(registration_id);
    
    // Remove from registry
    callback_registry.erase(registration_id);
}

// [[Rcpp::export]]
void crosslink_cleanup(SEXP cl_ptr) {
    Rcpp::XPtr<crosslink::CrossLink> cl(cl_ptr);
    cl->cleanup();
} 