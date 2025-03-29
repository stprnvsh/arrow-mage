#include "../include/crosslink.h"
#include "../core/arrow_bridge.h"
#include "../core/metadata_manager.h"
#include "../core/notification_system.h"
#include "../core/shared_memory_manager.h"
#include <duckdb.hpp>
#include <iostream>

namespace crosslink {

// Implementation class
class CrossLink::Impl {
public:
    Impl(const std::string& db_path, bool debug)
        : db_path_(db_path), debug_(debug), metadata_manager_(db_path) {
        
        if (debug_) {
            std::cout << "CrossLink initialized with database: " << db_path << std::endl;
        }
    }
    
    ~Impl() {
        cleanup();
    }
    
    std::string push(std::shared_ptr<arrow::Table> table, 
                     const std::string& name,
                     const std::string& description) {
        try {
            if (debug_) {
                std::cout << "Pushing table with " << table->num_rows() << " rows and " 
                          << table->num_columns() << " columns" << std::endl;
            }
            
            // Use ArrowBridge to share the table
            std::string dataset_id = ArrowBridge::share_arrow_table(
                db_path_, table, name, true, true);
            
            // Create and store metadata
            DatasetMetadata metadata;
            metadata.id = dataset_id;
            metadata.name = name.empty() ? "cpp_" + dataset_id.substr(0, 8) : name;
            metadata.source_language = "cpp";
            metadata.created_at = ArrowBridge::get_current_timestamp();
            metadata.updated_at = metadata.created_at;
            metadata.description = description;
            metadata.arrow_data = true;
            metadata.version = 1;
            metadata.current_version = true;
            metadata.schema_hash = ""; // Would compute schema hash in real implementation
            metadata.access_languages = {"cpp", "python", "r", "julia"};
            metadata.shared_memory_key = dataset_id;
            
            // Store schema as JSON
            metadata.arrow_schema_json = metadata_manager_.schema_to_json(table->schema());
            
            // Store metadata
            metadata_manager_.create_dataset_metadata(metadata);
            
            // Notify others about the new dataset
            NotificationEvent event;
            event.dataset_id = dataset_id;
            event.type = NotificationType::DATA_UPDATED;
            event.source_language = "cpp";
            event.timestamp = metadata.created_at;
            NotificationSystem::instance().notify(event);
            
            // Log the access
            metadata_manager_.log_access(
                dataset_id, "cpp", "push", "arrow", true
            );
            
            if (debug_) {
                std::cout << "Table pushed successfully with ID: " << dataset_id << std::endl;
            }
            
            return dataset_id;
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error pushing table: " << e.what() << std::endl;
            }
            throw;
        }
    }
    
    std::shared_ptr<arrow::Table> pull(const std::string& identifier) {
        try {
            if (debug_) {
                std::cout << "Pulling table: " << identifier << std::endl;
            }
            
            // Get metadata
            DatasetMetadata metadata = metadata_manager_.get_dataset_metadata(identifier);
            
            // Get the table
            auto table = ArrowBridge::get_arrow_table(db_path_, metadata.id);
            
            // Log the access
            metadata_manager_.log_access(
                metadata.id, "cpp", "pull", "arrow", true
            );
            
            if (debug_) {
                std::cout << "Table pulled successfully: " 
                          << table->num_rows() << " rows, "
                          << table->num_columns() << " columns" << std::endl;
            }
            
            return table;
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error pulling table: " << e.what() << std::endl;
            }
            
            // Log the failed access
            try {
                metadata_manager_.log_access(
                    identifier, "cpp", "pull", "arrow", false
                );
            } catch (...) {
                // Ignore errors from logging
            }
            
            throw;
        }
    }
    
    std::shared_ptr<arrow::Table> query(const std::string& sql) {
        try {
            if (debug_) {
                std::cout << "Executing query: " << sql << std::endl;
            }
            
            // Create a DuckDB connection
            duckdb::DuckDB db(db_path_);
            duckdb::Connection conn(db);
            
            // Execute the query
            auto result = conn.Query(sql);
            
            // Check if the query was successful
            if (!result || result->HasError()) {
                std::string error_msg = "Query failed";
                if (result && result->HasError()) {
                    error_msg += ": " + result->GetError();
                }
                throw std::runtime_error(error_msg);
            }
            
            // In a real implementation, we would convert DuckDB result to Arrow
            // For now, return an empty table as placeholder
            std::vector<std::shared_ptr<arrow::Array>> columns;
            auto schema = arrow::schema({});  // Empty schema for placeholder
            
            return arrow::Table::Make(schema, columns);
            
            if (debug_) {
                std::cout << "Query executed successfully" << std::endl;
            }
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error executing query: " << e.what() << std::endl;
            }
            throw;
        }
    }
    
    std::vector<std::string> list_datasets() {
        try {
            if (debug_) {
                std::cout << "Listing datasets" << std::endl;
            }
            
            return metadata_manager_.list_datasets();
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error listing datasets: " << e.what() << std::endl;
            }
            throw;
        }
    }
    
    std::string register_notification(
        const std::function<void(const std::string&, const std::string&)>& callback) {
        
        // Wrap the callback to convert from our internal notification format
        auto wrapped_callback = [callback](const NotificationEvent& event) {
            std::string event_type;
            switch (event.type) {
                case NotificationType::DATA_UPDATED:
                    event_type = "updated";
                    break;
                case NotificationType::DATA_DELETED:
                    event_type = "deleted";
                    break;
                case NotificationType::METADATA_CHANGED:
                    event_type = "metadata_changed";
                    break;
                default:
                    event_type = "unknown";
            }
            
            callback(event.dataset_id, event_type);
        };
        
        return NotificationSystem::instance().register_callback(wrapped_callback);
    }
    
    void unregister_notification(const std::string& registration_id) {
        NotificationSystem::instance().unregister_callback(registration_id);
    }
    
    void cleanup() {
        if (debug_) {
            std::cout << "Cleaning up CrossLink resources" << std::endl;
        }
        
        // Clean up shared memory regions
        SharedMemoryManager::instance().cleanup_all();
    }
    
private:
    std::string db_path_;
    bool debug_;
    MetadataManager metadata_manager_;
};

// CrossLink implementation that delegates to Impl
CrossLink::CrossLink(const std::string& db_path, bool debug)
    : impl_(std::make_unique<Impl>(db_path, debug)) {
}

CrossLink::~CrossLink() = default;

std::string CrossLink::push(std::shared_ptr<arrow::Table> table, 
                           const std::string& name,
                           const std::string& description) {
    return impl_->push(table, name, description);
}

std::shared_ptr<arrow::Table> CrossLink::pull(const std::string& identifier) {
    return impl_->pull(identifier);
}

std::shared_ptr<arrow::Table> CrossLink::query(const std::string& sql) {
    return impl_->query(sql);
}

std::vector<std::string> CrossLink::list_datasets() {
    return impl_->list_datasets();
}

std::string CrossLink::register_notification(
    const std::function<void(const std::string&, const std::string&)>& callback) {
    return impl_->register_notification(callback);
}

void CrossLink::unregister_notification(const std::string& registration_id) {
    impl_->unregister_notification(registration_id);
}

void CrossLink::cleanup() {
    impl_->cleanup();
}

} // namespace crosslink 