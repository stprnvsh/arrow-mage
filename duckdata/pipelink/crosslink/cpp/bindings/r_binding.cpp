#include <Rcpp.h>
#include "../include/crosslink.h"
#include <arrow/api.h>

// [[Rcpp::depends(arrow)]]

using namespace Rcpp;

// [[Rcpp::export]]
SEXP crosslink_connect(std::string db_path = "crosslink.duckdb", bool debug = false) {
    auto* cl = new crosslink::CrossLink(db_path, debug);
    Rcpp::XPtr<crosslink::CrossLink> ptr(cl, true);
    return ptr;
}

// Helper to convert R Arrow object to C++ Arrow table
std::shared_ptr<arrow::Table> r_to_cpp_table(SEXP r_table) {
    // In a real implementation, this would convert from R to C++ Arrow
    // As a placeholder, we'll create a small empty table
    auto schema = arrow::schema({arrow::field("placeholder", arrow::int64())});
    auto array = std::make_shared<arrow::Int64Array>(0, arrow::Buffer::FromString(""), nullptr, 0);
    auto column = std::make_shared<arrow::ChunkedArray>(std::vector<std::shared_ptr<arrow::Array>>{array});
    auto table = arrow::Table::Make(schema, {column});
    
    Rcpp::warning("R to C++ Arrow conversion is implemented as a placeholder. Use the Python interface for full functionality.");
    return table;
}

// Helper to convert C++ Arrow table to R Arrow object
SEXP cpp_to_r_table(const std::shared_ptr<arrow::Table>& table) {
    // In a real implementation, this would convert from C++ to R Arrow
    // For now, we'll return a simple R list with metadata
    Rcpp::List result;
    result["num_rows"] = table->num_rows();
    result["num_columns"] = table->num_columns();
    std::vector<std::string> column_names;
    for (int i = 0; i < table->num_columns(); i++) {
        column_names.push_back(table->field(i)->name());
    }
    result["column_names"] = column_names;
    
    Rcpp::warning("C++ to R Arrow conversion is implemented as a placeholder. Use the Python interface for full functionality.");
    return result;
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