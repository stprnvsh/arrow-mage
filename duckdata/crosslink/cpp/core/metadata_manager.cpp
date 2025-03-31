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
    if (metadata.id.empty()) {
        throw std::runtime_error("Dataset ID cannot be empty");
    }

    // Use prepared statements for safety and potential performance benefits
    auto prepared = conn_->Prepare(R"(
        INSERT OR REPLACE INTO datasets (
            id, name, source_language, created_at, updated_at, 
            description, schema_json, table_name, arrow_data, version, 
            current_version, schema_hash, memory_map_path, shared_memory_key, 
            arrow_schema_json, shared_memory_size, num_rows, num_columns, serialized_size
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    )");

    if (!prepared->HasError()) {
        // Start Transaction
        auto begin_res = conn_->Query("BEGIN TRANSACTION");
        if (!begin_res || begin_res->HasError()) {
            throw std::runtime_error("Failed to begin transaction: " + (begin_res ? begin_res->GetError() : "Unknown error"));
        }

        try {
            // Execute the prepared statement with metadata values
            auto result = prepared->Execute(
                metadata.id,
                metadata.name,
                metadata.source_language,
                metadata.created_at_ts,
                metadata.updated_at_ts,
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
                static_cast<int64_t>(metadata.shared_memory_size),
                static_cast<int64_t>(metadata.num_rows),
                static_cast<int32_t>(metadata.num_columns),
                static_cast<int64_t>(metadata.serialized_size)
            );

            if (!result || result->HasError()) {
                throw std::runtime_error("Failed to insert/replace dataset metadata: " + (result ? result->GetError() : "Unknown execution error"));
            }

            // Commit Transaction
            auto commit_res = conn_->Query("COMMIT");
            if (!commit_res || commit_res->HasError()) {
                // Attempt rollback, though commit failure is serious
                conn_->Query("ROLLBACK"); 
                throw std::runtime_error("Failed to commit transaction: " + (commit_res ? commit_res->GetError() : "Unknown error"));
            }

            // Update cache after successful commit
            // Use R-value reference and move semantics if DatasetMetadata is movable
            // Otherwise, copy assignment is fine.
             std::lock_guard<std::mutex> lock(cache_mutex_); // Protect cache access
            metadata_cache_[metadata.id] = metadata; 
            // Also cache by name for faster lookups if name is provided and different from ID
            if (!metadata.name.empty() && metadata.name != metadata.id) {
                 metadata_cache_[metadata.name] = metadata;
            }

        } catch (const std::exception& e) {
            // Rollback on any exception during execute/commit
            conn_->Query("ROLLBACK");
            throw; // Re-throw the exception
        }
    } else {
        throw std::runtime_error("Failed to prepare dataset metadata insert statement: " + prepared->GetError());
    }

    // Update access languages table (consider transaction?)
    // Delete existing entries first
    conn_->Query("DELETE FROM dataset_access WHERE dataset_id = ?", metadata.id);
    // Insert new entries
    for (const auto& lang : metadata.access_languages) {
        conn_->Query("INSERT INTO dataset_access (dataset_id, language) VALUES (?, ?)", metadata.id, lang);
    }
}

DatasetMetadata MetadataManager::get_dataset_metadata(const std::string& identifier) {
    if (identifier.empty()) {
        throw std::runtime_error("Dataset identifier cannot be empty");
    }

    // Check cache first
    {
        std::lock_guard<std::mutex> lock(cache_mutex_); // Protect cache access
        auto it = metadata_cache_.find(identifier);
        if (it != metadata_cache_.end()) {
            return it->second; // Return cached copy
        }
    }

    // Not in cache, query the database
    auto result = conn_->Query(R"(
        SELECT 
            id, name, source_language, created_at, updated_at, 
            description, schema_json, table_name, arrow_data, version, 
            current_version, schema_hash, memory_map_path, shared_memory_key, 
            arrow_schema_json, shared_memory_size, num_rows, num_columns, serialized_size
        FROM datasets 
        WHERE id = ? OR name = ?
    )", identifier, identifier);

    if (!result || result->HasError()) {
        throw std::runtime_error("Failed to query dataset metadata: " + (result ? result->GetError() : "Query execution failed"));
    }

    auto& materialized = result->Cast<duckdb::MaterializedQueryResult>();
    if (materialized.RowCount() == 0) {
        throw std::runtime_error("Dataset not found: " + identifier);
    }
    if (materialized.RowCount() > 1) {
         // This shouldn't happen if id/name constraints are correct, but good to check
         std::cerr << "Warning: Found multiple metadata entries for identifier: " << identifier << std::endl;
    }

    // Populate metadata struct from the first row
    DatasetMetadata metadata;
    metadata.id = materialized.GetValue(0, 0).ToString();
    metadata.name = materialized.GetValue(1, 0).ToString();
    metadata.source_language = materialized.GetValue(2, 0).ToString();
    metadata.created_at_ts = materialized.GetValue<duckdb::timestamp_t>(3, 0);
    metadata.updated_at_ts = materialized.GetValue<duckdb::timestamp_t>(4, 0);
    metadata.created_at = duckdb::Timestamp::ToString(metadata.created_at_ts);
    metadata.updated_at = duckdb::Timestamp::ToString(metadata.updated_at_ts);
    metadata.description = materialized.GetValue(5, 0).ToString();
    metadata.schema_json = materialized.GetValue(6, 0).ToString();
    metadata.table_name = materialized.GetValue(7, 0).ToString();
    metadata.arrow_data = materialized.GetValue<bool>(8, 0);
    metadata.version = materialized.GetValue<int32_t>(9, 0);
    metadata.current_version = materialized.GetValue<bool>(10, 0);
    metadata.schema_hash = materialized.GetValue(11, 0).ToString();
    metadata.memory_map_path = materialized.GetValue(12, 0).ToString();
    metadata.shared_memory_key = materialized.GetValue(13, 0).ToString();
    metadata.arrow_schema_json = materialized.GetValue(14, 0).ToString();
    metadata.shared_memory_size = materialized.GetValue<int64_t>(15, 0);
    metadata.num_rows = materialized.GetValue<int64_t>(16, 0);
    metadata.num_columns = materialized.GetValue<int32_t>(17, 0);
    metadata.serialized_size = materialized.GetValue<int64_t>(18, 0);

    // Retrieve access languages
    auto access_result = conn_->Query("SELECT language FROM dataset_access WHERE dataset_id = ?", metadata.id);
    if (access_result && !access_result->HasError()) {
        auto& access_materialized = access_result->Cast<duckdb::MaterializedQueryResult>();
        for (const auto& row : access_materialized.Collection().GetRows()) {
             metadata.access_languages.push_back(row.GetValue(0).ToString());
        }
    }

    // Update cache
    {
        std::lock_guard<std::mutex> lock(cache_mutex_); // Protect cache access
        metadata_cache_[metadata.id] = metadata;
        // Also cache by name if different
        if (!metadata.name.empty() && metadata.name != metadata.id) {
            metadata_cache_[metadata.name] = metadata;
        }
    }

    return metadata;
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