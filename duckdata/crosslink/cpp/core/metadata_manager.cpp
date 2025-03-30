#include "metadata_manager.h"
#include <duckdb.hpp>
#include <arrow/json/api.h>
#include <sstream>
#include <iostream>

namespace crosslink {

MetadataManager::MetadataManager(const std::string& db_path) {
    try {
        // Open DuckDB database
        db_ = std::make_unique<duckdb::DuckDB>(db_path);
        conn_ = std::make_unique<duckdb::Connection>(*db_);
        
        // Initialize tables
        init_tables();
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to initialize metadata manager: " + std::string(e.what()));
    }
}

MetadataManager::~MetadataManager() {
    // Release resources
}

void MetadataManager::init_tables() {
    try {
        // Create datasets table if it doesn't exist
        conn_->Query(R"(
            CREATE TABLE IF NOT EXISTS datasets (
                id VARCHAR PRIMARY KEY,
                name VARCHAR,
                source_language VARCHAR,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                description VARCHAR,
                schema_json VARCHAR,
                table_name VARCHAR,
                arrow_data BOOLEAN,
                version INTEGER,
                current_version BOOLEAN,
                schema_hash VARCHAR,
                memory_map_path VARCHAR,
                shared_memory_key VARCHAR,
                arrow_schema_json VARCHAR,
                shared_memory_size BIGINT DEFAULT 0,
                num_rows BIGINT DEFAULT 0,
                num_columns INTEGER DEFAULT 0,
                serialized_size BIGINT DEFAULT 0
            )
        )");
        
        // Create access log table if it doesn't exist
        conn_->Query(R"(
            CREATE TABLE IF NOT EXISTS access_log (
                id VARCHAR PRIMARY KEY,
                dataset_id VARCHAR,
                language VARCHAR,
                operation VARCHAR,
                access_method VARCHAR,
                timestamp TIMESTAMP,
                success BOOLEAN
            )
        )");
        
        // Create dataset_access table for tracking which languages can access each dataset
        conn_->Query(R"(
            CREATE TABLE IF NOT EXISTS dataset_access (
                dataset_id VARCHAR,
                language VARCHAR,
                PRIMARY KEY (dataset_id, language)
            )
        )");
        
        // Upgrade table if it exists but doesn't have the new columns
        try {
            auto result = conn_->Query("SELECT shared_memory_size FROM datasets LIMIT 0");
        } catch (const std::exception& e) {
            // Add new columns if they don't exist
            conn_->Query("ALTER TABLE datasets ADD COLUMN shared_memory_size BIGINT DEFAULT 0");
            conn_->Query("ALTER TABLE datasets ADD COLUMN num_rows BIGINT DEFAULT 0");
            conn_->Query("ALTER TABLE datasets ADD COLUMN num_columns INTEGER DEFAULT 0");
            conn_->Query("ALTER TABLE datasets ADD COLUMN serialized_size BIGINT DEFAULT 0");
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to initialize tables: " + std::string(e.what()));
    }
}

void MetadataManager::create_dataset_metadata(const DatasetMetadata& metadata) {
    try {
        // First, insert into datasets table
        auto result = conn_->Query(
            "INSERT INTO datasets VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            metadata.id,
            metadata.name,
            metadata.source_language,
            metadata.created_at,
            metadata.updated_at,
            metadata.description,
            metadata.schema_json,
            metadata.table_name,
            metadata.arrow_data,
            metadata.version,
            metadata.current_version,
            metadata.schema_hash,
            metadata.memory_map_path,
            metadata.shared_memory_key,
            metadata.arrow_schema_json,
            metadata.shared_memory_size,
            metadata.num_rows,
            metadata.num_columns,
            metadata.serialized_size
        );
        
        // Then, insert access languages
        for (const auto& language : metadata.access_languages) {
            conn_->Query(
                "INSERT INTO dataset_access VALUES (?, ?)",
                metadata.id,
                language
            );
        }
        
        // Update cache
        metadata_cache_[metadata.id] = metadata;
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to create dataset metadata: " + std::string(e.what()));
    }
}

DatasetMetadata MetadataManager::get_dataset_metadata(const std::string& identifier) {
    // Check if it's in the cache
    auto cache_it = metadata_cache_.find(identifier);
    if (cache_it != metadata_cache_.end()) {
        return cache_it->second;
    }
    
    try {
        // Query datasets table
        auto result = conn_->Query(
            "SELECT * FROM datasets WHERE id = ? OR name = ?",
            identifier, identifier
        );
        
        if (result->HasError()) {
            throw std::runtime_error("Failed to query dataset: " + result->GetError());
        }
        
        auto &materialized = result->Cast<duckdb::MaterializedQueryResult>();
        if (materialized.RowCount() == 0) {
            throw std::runtime_error("Dataset not found: " + identifier);
        }
        
        // Extract dataset metadata
        DatasetMetadata metadata;
        
        // Access data using column indices
        metadata.id = materialized.GetValue(0, 0).ToString();
        metadata.name = materialized.GetValue(1, 0).ToString();
        metadata.source_language = materialized.GetValue(2, 0).ToString();
        metadata.created_at = materialized.GetValue(3, 0).ToString();
        metadata.updated_at = materialized.GetValue(4, 0).ToString();
        metadata.description = materialized.GetValue(5, 0).ToString();
        metadata.schema_json = materialized.GetValue(6, 0).ToString();
        metadata.table_name = materialized.GetValue(7, 0).ToString();
        metadata.arrow_data = materialized.GetValue<bool>(8, 0);
        metadata.version = materialized.GetValue<int>(9, 0);
        metadata.current_version = materialized.GetValue<bool>(10, 0);
        metadata.schema_hash = materialized.GetValue(11, 0).ToString();
        metadata.memory_map_path = materialized.GetValue(12, 0).ToString();
        metadata.shared_memory_key = materialized.GetValue(13, 0).ToString();
        metadata.arrow_schema_json = materialized.GetValue(14, 0).ToString();
        
        // Get new fields if they exist
        if (result->ColumnCount() > 15) {
            metadata.shared_memory_size = materialized.GetValue<int64_t>(15, 0);
            metadata.num_rows = materialized.GetValue<int64_t>(16, 0);
            metadata.num_columns = materialized.GetValue<int>(17, 0);
            metadata.serialized_size = materialized.GetValue<int64_t>(18, 0);
        }
        
        // Get access languages
        auto access_result = conn_->Query(
            "SELECT language FROM dataset_access WHERE dataset_id = ?",
            metadata.id
        );
        
        if (access_result->HasError()) {
            throw std::runtime_error("Failed to query dataset access: " + access_result->GetError());
        }
        
        auto &materialized_access = access_result->Cast<duckdb::MaterializedQueryResult>();
        for (size_t i = 0; i < materialized_access.RowCount(); i++) {
            metadata.access_languages.push_back(materialized_access.GetValue(0, i).ToString());
        }
        
        // Update cache
        metadata_cache_[metadata.id] = metadata;
        if (!metadata.name.empty()) {
            metadata_cache_[metadata.name] = metadata;
        }
        
        return metadata;
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to get dataset metadata: " + std::string(e.what()));
    }
}

std::vector<std::string> MetadataManager::list_datasets() {
    std::vector<std::string> dataset_ids;
    
    try {
        auto result = conn_->Query("SELECT id FROM datasets WHERE current_version = TRUE");
        
        if (result->HasError()) {
            throw std::runtime_error("Failed to query datasets: " + result->GetError());
        }
        
        auto &materialized = result->Cast<duckdb::MaterializedQueryResult>();
        for (size_t i = 0; i < materialized.RowCount(); i++) {
            dataset_ids.push_back(materialized.GetValue(0, i).ToString());
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to list datasets: " + std::string(e.what()));
    }
    
    return dataset_ids;
}

void MetadataManager::log_access(
    const std::string& dataset_id, 
    const std::string& language,
    const std::string& operation,
    const std::string& access_method,
    bool success) {
    
    try {
        // Generate a UUID for the log entry
        std::string log_id = "log-" + dataset_id + "-" + std::to_string(time(nullptr));
        
        // Insert into access_log
        conn_->Query(
            "INSERT INTO access_log VALUES (?, ?, ?, ?, ?, NOW(), ?)",
            log_id,
            dataset_id,
            language,
            operation,
            access_method,
            success
        );
    } catch (const std::exception& e) {
        // Just log the error but don't throw - access logging should not interrupt normal operation
        std::cerr << "Failed to log access: " << e.what() << std::endl;
    }
}

void MetadataManager::update_shared_memory_size(const std::string& dataset_id, size_t size) {
    try {
        // Update the shared memory size in the database
        conn_->Query(
            "UPDATE datasets SET shared_memory_size = ? WHERE id = ?",
            static_cast<int64_t>(size),
            dataset_id
        );
        
        // Update cache if present
        auto cache_it = metadata_cache_.find(dataset_id);
        if (cache_it != metadata_cache_.end()) {
            cache_it->second.shared_memory_size = size;
            
            // If name is also in cache, update that too
            auto name_it = metadata_cache_.find(cache_it->second.name);
            if (name_it != metadata_cache_.end()) {
                name_it->second.shared_memory_size = size;
            }
        }
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to update shared memory size: " + std::string(e.what()));
    }
}

size_t MetadataManager::get_shared_memory_size(const std::string& dataset_id) {
    try {
        // First check cache
        auto cache_it = metadata_cache_.find(dataset_id);
        if (cache_it != metadata_cache_.end() && cache_it->second.shared_memory_size > 0) {
            return cache_it->second.shared_memory_size;
        }
        
        // Query the database
        auto result = conn_->Query(
            "SELECT shared_memory_size FROM datasets WHERE id = ? OR name = ?",
            dataset_id, dataset_id
        );
        
        if (result->HasError()) {
            throw std::runtime_error("Failed to query dataset: " + result->GetError());
        }
        
        auto &materialized = result->Cast<duckdb::MaterializedQueryResult>();
        if (materialized.RowCount() == 0) {
            throw std::runtime_error("Dataset not found: " + dataset_id);
        }
        
        // Extract size
        size_t size = static_cast<size_t>(materialized.GetValue<int64_t>(0, 0));
        
        // Update cache if needed
        if (cache_it != metadata_cache_.end()) {
            cache_it->second.shared_memory_size = size;
        }
        
        return size;
    } catch (const std::exception& e) {
        throw std::runtime_error("Failed to get shared memory size: " + std::string(e.what()));
    }
}

std::string MetadataManager::schema_to_json(const std::shared_ptr<arrow::Schema>& schema) {
    // This is a simplified version - in a real implementation, we'd use Arrow's JSON serializer
    std::stringstream ss;
    ss << "{\"fields\":[";
    
    for (int i = 0; i < schema->num_fields(); i++) {
        auto field = schema->field(i);
        if (i > 0) ss << ",";
        
        ss << "{\"name\":\"" << field->name() << "\",\"type\":\"" 
           << field->type()->ToString() << "\",\"nullable\":" 
           << (field->nullable() ? "true" : "false") << "}";
    }
    
    ss << "]}";
    return ss.str();
}

std::shared_ptr<arrow::Schema> MetadataManager::json_to_schema(const std::string& schema_json) {
    // This is a simplified version - in a real implementation, we'd use Arrow's JSON deserializer
    // For now, we'll just return a dummy schema
    return arrow::schema({
        arrow::field("placeholder", arrow::utf8())
    });
}

} // namespace crosslink 