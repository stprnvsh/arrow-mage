#include "../include/crosslink.h"
#include "../core/arrow_bridge.h"
#include "../core/metadata_manager.h"
#include "../core/notification_system.h"
#include "../core/shared_memory_manager.h"
#include <duckdb.hpp>
#include <iostream>
#include <map>
#include <mutex>
#include <condition_variable>
#include <deque>
#include <atomic>
#include <stdexcept> // For std::runtime_error
#include <chrono>    // For timestamps/unique IDs (example)
#include <sstream>   // For string streams (example ID generation)

namespace crosslink {

// Helper to generate unique IDs (replace with proper UUID if available)
std::string generate_stream_id() {
    auto now = std::chrono::system_clock::now();
    auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    std::stringstream ss;
    ss << "stream_" << ms;
    return ss.str();
}

// State associated with a single active stream
struct StreamState {
    const std::string id;
    const std::shared_ptr<arrow::Schema> schema;
    std::deque<std::shared_ptr<arrow::RecordBatch>> batch_queue;
    std::mutex queue_mutex;
    std::condition_variable queue_cv;
    std::atomic<bool> end_of_stream{false};
    std::atomic<bool> writer_active{true}; // Track if writer is still held

    StreamState(std::string stream_id, std::shared_ptr<arrow::Schema> s)
        : id(std::move(stream_id)), schema(std::move(s)) {}

    // Add a batch, notify one reader
    void add_batch(std::shared_ptr<arrow::RecordBatch> batch) {
        if (!batch->schema()->Equals(*schema)) {
            throw std::runtime_error("Batch schema does not match stream schema.");
        }
        {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (end_of_stream) {
                 throw std::runtime_error("Cannot write to a closed stream.");
            }
            batch_queue.push_back(std::move(batch));
        }
        queue_cv.notify_one();
    }

    // Get next batch, wait if necessary
    std::shared_ptr<arrow::RecordBatch> get_batch() {
        std::unique_lock<std::mutex> lock(queue_mutex);
        queue_cv.wait(lock, [this] {
            return !batch_queue.empty() || end_of_stream;
        });

        if (!batch_queue.empty()) {
            auto batch = batch_queue.front();
            batch_queue.pop_front();
            return batch;
        }
        // Queue is empty and EOS is true
        return nullptr;
    }

    // Signal end of stream, notify all readers
    void signal_eos() {
         {
            std::lock_guard<std::mutex> lock(queue_mutex);
            if (end_of_stream) return; // Already closed
            end_of_stream = true;
            writer_active = false;
        }
        queue_cv.notify_all();
    }

     // Mark writer as inactive (e.g., writer object destroyed)
    void writer_closed() {
        writer_active = false;
        // If writer closes without explicit EOS, treat as EOS
        if (!end_of_stream) {
            signal_eos();
        }
    }
};

// Concrete implementation of StreamWriter
class StreamWriterImpl : public StreamWriter {
    std::shared_ptr<StreamState> state_;

public:
    StreamWriterImpl(std::shared_ptr<StreamState> state) : state_(std::move(state)) {}

    // Destructor signals writer closure if not already done
    ~StreamWriterImpl() override {
        if (state_) {
            state_->writer_closed();
        }
    }

    void write_batch(std::shared_ptr<arrow::RecordBatch> batch) override {
        if (!state_) throw std::runtime_error("StreamWriter is not valid (likely moved or closed).");
        state_->add_batch(std::move(batch));
    }

    void close() override {
        if (!state_) throw std::runtime_error("StreamWriter is not valid (likely moved or closed).");
        state_->signal_eos();
        // Optionally release shared_ptr after explicit close?
        // state_.reset(); // Or let destructor handle it
    }

    std::shared_ptr<arrow::Schema> schema() const override {
         if (!state_) throw std::runtime_error("StreamWriter is not valid.");
        return state_->schema;
    }

    const std::string& stream_id() const override {
        if (!state_) throw std::runtime_error("StreamWriter is not valid.");
        return state_->id;
    }
};

// Concrete implementation of StreamReader
class StreamReaderImpl : public StreamReader {
    std::shared_ptr<StreamState> state_;
    // Could add reader-specific state if needed (e.g., read position, but queue handles it now)

public:
    StreamReaderImpl(std::shared_ptr<StreamState> state) : state_(std::move(state)) {}

    ~StreamReaderImpl() override {
        // Optional: could notify state that a reader detached if needed
        close();
    }

    std::shared_ptr<arrow::RecordBatch> read_next_batch() override {
        if (!state_) throw std::runtime_error("StreamReader is not valid (likely closed).");
        return state_->get_batch();
    }

    std::shared_ptr<arrow::Schema> schema() const override {
        if (!state_) throw std::runtime_error("StreamReader is not valid.");
        return state_->schema;
    }

     const std::string& stream_id() const override {
        if (!state_) throw std::runtime_error("StreamReader is not valid.");
        return state_->id;
    }

    void close() override {
        // Release the reference to the state.
        // If this is the last reader/writer holding the state, it will be destroyed.
        state_.reset();
    }
};

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
    
    // === New Streaming Methods ===

    std::pair<std::string, std::shared_ptr<StreamWriter>>
    push_stream(std::shared_ptr<arrow::Schema> schema, const std::string& name) {
        auto stream_id = generate_stream_id();
        auto state = std::make_shared<StreamState>(stream_id, std::move(schema));
        auto writer = std::make_shared<StreamWriterImpl>(state);

        {
            std::lock_guard<std::mutex> lock(stream_registry_mutex_);
            if (stream_registry_.count(stream_id)) {
                // Extremely unlikely due to timestamp/randomness, but handle anyway
                throw std::runtime_error("Generated duplicate stream ID: " + stream_id);
            }
            stream_registry_[stream_id] = state;
        }

        if (debug_) {
            std::cout << "Created push stream: " << stream_id << " (named: " << name << ")" << std::endl;
        }
        // Log metadata? Similar to push for tables? TBD.

        // The return type needs to match the function signature
        return {stream_id, writer};
    }

    std::shared_ptr<StreamReader> pull_stream(const std::string& stream_id) {
         std::shared_ptr<StreamState> state;
        {
            std::lock_guard<std::mutex> lock(stream_registry_mutex_);
            auto it = stream_registry_.find(stream_id);
            if (it == stream_registry_.end()) {
                throw std::runtime_error("Stream not found: " + stream_id);
            }
             // Check if the writer is still active? Or allow reading completed streams?
             // if (!it->second->writer_active && it->second->batch_queue.empty() && it->second->end_of_stream) {
             //     throw std::runtime_error("Stream is closed and fully consumed: " + stream_id);
             // }
            state = it->second; // Get the shared state
        }

        // Create a new reader instance sharing the same state
        auto reader = std::make_shared<StreamReaderImpl>(state);

        if (debug_) {
            std::cout << "Created pull stream reader for: " << stream_id << std::endl;
        }
         // Log access?
        return reader; // Return type matches now
    }

private:
    std::string db_path_;
    bool debug_;
    MetadataManager metadata_manager_;
    // Existing members for batch/metadata etc.

    // === New Members for Streaming ===
    std::map<std::string, std::shared_ptr<StreamState>> stream_registry_;
    std::mutex stream_registry_mutex_; // Protects access to stream_registry_
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

// === Delegate Streaming Methods ===
std::pair<std::string, std::shared_ptr<StreamWriter>>
CrossLink::push_stream(std::shared_ptr<arrow::Schema> schema, const std::string& name) {
    // NOTE: Use std::move on schema if ownership is transferred
    return impl_->push_stream(schema, name);
}

std::shared_ptr<StreamReader> CrossLink::pull_stream(const std::string& stream_id) {
    return impl_->pull_stream(stream_id);
}

// === Delegate Notification Methods ===
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