#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <memory>
#include <arrow/api.h>
#include <duckdb.hpp>
#include <mutex>

namespace crosslink {

struct DatasetMetadata {
    std::string id;
    std::string name;
    std::string source_language;
    std::string created_at;
    std::string updated_at;
    duckdb::timestamp_t created_at_ts;
    duckdb::timestamp_t updated_at_ts;
    std::string description;
    std::string schema_json;
    std::string table_name;
    bool arrow_data;
    int version;
    bool current_version;
    std::string schema_hash;
    std::vector<std::string> access_languages;
    std::string memory_map_path;
    std::string shared_memory_key;
    std::string arrow_schema_json;
    
    // Additional fields for improved shared memory management
    int64_t shared_memory_size = 0;    // Size of the shared memory region in bytes
    int64_t num_rows = 0;             // Number of rows in the table
    int64_t num_columns = 0;          // Number of columns in the table
    int64_t serialized_size = 0;       // Size of the serialized Arrow table
};

class MetadataManager {
public:
    MetadataManager(const std::string& db_path);
    ~MetadataManager();
    
    // Initialize database tables
    void init_tables();
    
    // Create dataset metadata entry
    void create_dataset_metadata(const DatasetMetadata& metadata);
    
    // Get dataset metadata
    DatasetMetadata get_dataset_metadata(const std::string& identifier);
    
    // List available datasets
    std::vector<std::string> list_datasets();
    
    // Log access
    void log_access(const std::string& dataset_id, 
                   const std::string& language,
                   const std::string& operation,
                   const std::string& access_method,
                   bool success);
    
    // Update shared memory size for a dataset
    void update_shared_memory_size(const std::string& dataset_id, size_t size);
    
    // Get shared memory size for a dataset
    size_t get_shared_memory_size(const std::string& dataset_id);
    
    // Helper to convert Arrow schema to JSON string
    std::string schema_to_json(const std::shared_ptr<arrow::Schema>& schema);
    
    // Helper to convert JSON string to Arrow schema
    std::shared_ptr<arrow::Schema> json_to_schema(const std::string& schema_json);
    
private:
    std::unique_ptr<duckdb::DuckDB> db_;
    std::unique_ptr<duckdb::Connection> conn_;
    std::unordered_map<std::string, DatasetMetadata> metadata_cache_;
    std::mutex cache_mutex_;
};

} // namespace crosslink 