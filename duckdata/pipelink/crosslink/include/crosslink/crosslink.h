#pragma once
#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <arrow/api.h>

namespace crosslink {

class CrossLink {
public:
    // Connect to CrossLink database
    CrossLink(const std::string& db_path = "crosslink.duckdb", bool debug = false);
    ~CrossLink();
    
    // Share a table from C++
    std::string push(std::shared_ptr<arrow::Table> table, 
                     const std::string& name = "",
                     const std::string& description = "");
    
    // Get a table from any language
    std::shared_ptr<arrow::Table> pull(const std::string& identifier);
    
    // Execute a query and get results as Arrow table
    std::shared_ptr<arrow::Table> query(const std::string& sql);
    
    // List available datasets
    std::vector<std::string> list_datasets();
    
    // Register for notifications
    std::string register_notification(
        const std::function<void(const std::string&, const std::string&)>& callback);
    
    // Unregister notifications
    void unregister_notification(const std::string& registration_id);
    
    // Clean up resources
    void cleanup();
    
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace crosslink 