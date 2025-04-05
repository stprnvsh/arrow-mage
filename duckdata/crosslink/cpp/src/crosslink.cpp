#include "../include/crosslink.h"
#include "../core/arrow_bridge.h"
#include "../core/metadata_manager.h"
#include "../core/notification_system.h"
#include "../core/shared_memory_manager.h"
#include "../core/crosslink_config.h"
#include "../core/flight_client.h"
#include "../core/flight_server.h"
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
    Impl(const CrossLinkConfig& config)
        : config_(config), db_path_(config.db_path()), debug_(config.debug()), 
          metadata_manager_(config.db_path()) {
        
        if (debug_) {
            std::cout << "CrossLink initialized with database: " << db_path_ << std::endl;
            if (config_.mode() == OperationMode::DISTRIBUTED) {
                std::cout << "CrossLink running in distributed mode" << std::endl;
                std::cout << "Flight server configured on " << config_.flight_host() 
                        << ":" << config_.flight_port() << std::endl;
                if (!config_.mother_node_address().empty()) {
                    std::cout << "Mother node at " << config_.mother_node_address() 
                            << ":" << config_.mother_node_port() << std::endl;
                }
                if (!config_.node_address().empty()) {
                    std::cout << "Node address: " << config_.node_address() << std::endl;
                }
            } else {
                std::cout << "CrossLink running in local mode" << std::endl;
            }
        }
        
        // Start the Flight server if in distributed mode
        if (config_.mode() == OperationMode::DISTRIBUTED) {
            start_flight_server();
        }
    }
    
    // Constructor overload for backward compatibility
    Impl(const std::string& db_path, bool debug)
        : Impl(CrossLinkConfig(db_path, debug)) {}
    
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
            
            // Use ArrowBridge to share the table, passing the member metadata_manager_
            std::string dataset_name = ArrowBridge::share_arrow_table(
                metadata_manager_, table, name, true, true, db_path_); // Pass metadata_manager_ and db_path_
            
            // Create and store metadata - This is now done *inside* share_arrow_table
            // Remove redundant metadata creation here:
            /*
            DatasetMetadata metadata;
            metadata.id = dataset_id; // dataset_id is no longer returned
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
            
            // Store schema as JSON
            metadata.arrow_schema_json = metadata_manager_.schema_to_json(table->schema());
            
            // Store metadata
            metadata_manager_.create_dataset_metadata(metadata);
            */
            
            // Notify others about the new dataset (use the returned name)
            NotificationEvent event;
            // Need the actual dataset ID for notification - perhaps share_arrow_table should return pair(id, name)?
            // For now, let's assume notification system can handle names or we modify later.
            // We *don't* have the dataset_id here anymore, only the name.
            // Let's retrieve the ID from metadata using the returned name for the notification.
            std::string dataset_id_for_notification;
            try {
                DatasetMetadata pushed_meta = metadata_manager_.get_dataset_metadata(dataset_name);
                dataset_id_for_notification = pushed_meta.id;
            } catch (const std::exception& meta_err) {
                std::cerr << "Warning: Could not retrieve metadata for dataset '" << dataset_name 
                          << "' immediately after push to get ID for notification: " << meta_err.what() << std::endl;
                dataset_id_for_notification = dataset_name; // Fallback to using name in notification
            }

            event.dataset_id = dataset_id_for_notification; 
            event.type = NotificationType::DATA_UPDATED;
            event.source_language = "cpp";
            // Convert timestamp_t to string for NotificationEvent
            event.timestamp = duckdb::Timestamp::ToString(ArrowBridge::get_current_duckdb_timestamp()); 
            NotificationSystem::instance().notify(event);
            
            // Log the access (using the ID retrieved for notification)
            metadata_manager_.log_access(
                dataset_id_for_notification, "cpp", "push", "arrow", true
            );
            
            if (debug_) {
                std::cout << "Table pushed successfully with Name: " << dataset_name << std::endl;
            }
            
            return dataset_name; // Return the name as intended
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
            
            // Get metadata using member manager_
            // DatasetMetadata metadata = metadata_manager_.get_dataset_metadata(identifier);
            // This lookup is now inside get_arrow_table
            
            // Get the table using ArrowBridge, passing the member metadata_manager_
            auto table = ArrowBridge::get_arrow_table(metadata_manager_, identifier, db_path_); // Pass metadata_manager_ and db_path_
            
            // Log the access - We need the actual dataset ID here.
            // Retrieve metadata again to get the ID, as identifier could be name or ID.
             std::string actual_dataset_id;
             try {
                 DatasetMetadata pulled_meta = metadata_manager_.get_dataset_metadata(identifier);
                 actual_dataset_id = pulled_meta.id;
             } catch (const std::exception& meta_err) {
                  std::cerr << "Warning: Could not retrieve metadata for dataset '" << identifier 
                            << "' during pull operation logging: " << meta_err.what() << std::endl;
                  actual_dataset_id = identifier; // Fallback to using the provided identifier
             }

            metadata_manager_.log_access(
                actual_dataset_id, "cpp", "pull", "arrow", true
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
                 // We might not have the ID if the initial metadata lookup failed.
                 // Log using the identifier provided.
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
        
        // Stop Flight server if running
        stop_flight_server();
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

    // Start the Flight server
    bool start_flight_server() {
        try {
            if (flight_server_) {
                if (debug_) {
                    std::cout << "Flight server already running" << std::endl;
                }
                return true; // Already running
            }
            
            if (debug_) {
                std::cout << "Starting Flight server on " << config_.flight_host() 
                      << ":" << config_.flight_port() << std::endl;
            }
            
            // Initialize the flight server
            auto status = FlightServer::Initialize(
                config_.flight_host(), config_.flight_port(),
                metadata_manager_, db_path_, &flight_server_);
            
            if (!status.ok()) {
                if (debug_) {
                    std::cerr << "Failed to initialize Flight server: " 
                          << status.ToString() << std::endl;
                }
                return false;
            }
            
            // Start the server
            status = flight_server_->StartAsync();
            if (!status.ok()) {
                if (debug_) {
                    std::cerr << "Failed to start Flight server: " 
                          << status.ToString() << std::endl;
                }
                flight_server_.reset();
                return false;
            }
            
            if (debug_) {
                std::cout << "Flight server started on port " << flight_server_->port() << std::endl;
            }
            
            return true;
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error starting Flight server: " << e.what() << std::endl;
            }
            return false;
        }
    }
    
    // Stop the Flight server
    bool stop_flight_server() {
        try {
            if (!flight_server_) {
                return true; // Already stopped
            }
            
            if (debug_) {
                std::cout << "Stopping Flight server" << std::endl;
            }
            
            auto status = flight_server_->Stop();
            if (!status.ok()) {
                if (debug_) {
                    std::cerr << "Failed to stop Flight server: " 
                          << status.ToString() << std::endl;
                }
                return false;
            }
            
            flight_server_.reset();
            if (debug_) {
                std::cout << "Flight server stopped" << std::endl;
            }
            
            return true;
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error stopping Flight server: " << e.what() << std::endl;
            }
            return false;
        }
    }
    
    // Get the port of the running Flight server
    int flight_server_port() const {
        return flight_server_ ? flight_server_->port() : -1;
    }
    
    // Share a table via Flight to a remote node
    std::string flight_push(std::shared_ptr<arrow::Table> table,
                          const std::string& remote_host,
                          int remote_port,
                          const std::string& name,
                          const std::string& description) {
        try {
            if (debug_) {
                std::cout << "Pushing table via Flight to " << remote_host << ":" << remote_port
                      << " (" << table->num_rows() << " rows, " 
                      << table->num_columns() << " columns)" << std::endl;
            }
            
            // Create a dataset ID for this table
            std::string dataset_id = name.empty() ? 
                ArrowBridge::generate_uuid() :
                name;
            
            // Connect to the remote Flight server
            std::unique_ptr<FlightClient> client;
            auto status = FlightClient::Initialize(remote_host, remote_port, &client);
            if (!status.ok()) {
                throw std::runtime_error("Failed to connect to Flight server: " + 
                                        status.ToString());
            }
            
            // Put the table
            status = client->PutDataset(dataset_id, table);
            if (!status.ok()) {
                throw std::runtime_error("Failed to put dataset via Flight: " + 
                                        status.ToString());
            }
            
            if (debug_) {
                std::cout << "Table pushed successfully via Flight with ID: " 
                      << dataset_id << std::endl;
            }
            
            return dataset_id;
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error pushing table via Flight: " << e.what() << std::endl;
            }
            throw;
        }
    }
    
    // Get a table via Flight from a remote node
    std::shared_ptr<arrow::Table> flight_pull(const std::string& identifier,
                                            const std::string& remote_host,
                                            int remote_port) {
        try {
            if (debug_) {
                std::cout << "Pulling table via Flight from " << remote_host << ":" 
                      << remote_port << " (ID: " << identifier << ")" << std::endl;
            }
            
            // Connect to the remote Flight server
            std::unique_ptr<FlightClient> client;
            auto status = FlightClient::Initialize(remote_host, remote_port, &client);
            if (!status.ok()) {
                throw std::runtime_error("Failed to connect to Flight server: " + 
                                        status.ToString());
            }
            
            // Get the table
            std::shared_ptr<arrow::Table> table;
            status = client->GetDataset(identifier, &table);
            if (!status.ok()) {
                throw std::runtime_error("Failed to get dataset via Flight: " + 
                                        status.ToString());
            }
            
            if (debug_) {
                std::cout << "Table pulled successfully via Flight: " 
                      << table->num_rows() << " rows, "
                      << table->num_columns() << " columns" << std::endl;
            }
            
            return table;
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error pulling table via Flight: " << e.what() << std::endl;
            }
            throw;
        }
    }
    
    // List available datasets on a remote Flight server
    std::vector<std::string> list_remote_datasets(const std::string& remote_host,
                                               int remote_port) {
        try {
            if (debug_) {
                std::cout << "Listing datasets via Flight from " << remote_host << ":" 
                      << remote_port << std::endl;
            }
            
            // Connect to the remote Flight server
            std::unique_ptr<FlightClient> client;
            auto status = FlightClient::Initialize(remote_host, remote_port, &client);
            if (!status.ok()) {
                throw std::runtime_error("Failed to connect to Flight server: " + 
                                        status.ToString());
            }
            
            // List datasets
            std::vector<std::string> dataset_ids;
            status = client->ListDatasets(&dataset_ids);
            if (!status.ok()) {
                throw std::runtime_error("Failed to list datasets via Flight: " + 
                                        status.ToString());
            }
            
            if (debug_) {
                std::cout << "Listed " << dataset_ids.size() 
                      << " datasets via Flight" << std::endl;
            }
            
            return dataset_ids;
        } catch (const std::exception& e) {
            if (debug_) {
                std::cerr << "Error listing datasets via Flight: " << e.what() << std::endl;
            }
            throw;
        }
    }
    
    // Get current configuration
    const CrossLinkConfig& config() const {
        return config_;
    }

private:
    CrossLinkConfig config_;
    std::string db_path_;
    bool debug_;
    MetadataManager metadata_manager_;
    
    // Flight components
    std::unique_ptr<FlightServer> flight_server_;
    
    // Stream registry
    std::map<std::string, std::shared_ptr<StreamState>> stream_registry_;
    std::mutex stream_registry_mutex_; // Protects access to stream_registry_
};

// CrossLink implementation that delegates to Impl
CrossLink::CrossLink(const std::string& db_path, bool debug)
    : impl_(std::make_unique<Impl>(db_path, debug)) {
}

CrossLink::CrossLink(const CrossLinkConfig& config)
    : impl_(std::make_unique<Impl>(config)) {
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

// Flight API implementations
std::string CrossLink::flight_push(std::shared_ptr<arrow::Table> table,
                                  const std::string& remote_host,
                                  int remote_port,
                                  const std::string& name,
                                  const std::string& description) {
    return impl_->flight_push(table, remote_host, remote_port, name, description);
}

std::shared_ptr<arrow::Table> CrossLink::flight_pull(const std::string& identifier,
                                                   const std::string& remote_host,
                                                   int remote_port) {
    return impl_->flight_pull(identifier, remote_host, remote_port);
}

std::vector<std::string> CrossLink::list_remote_datasets(const std::string& remote_host,
                                                       int remote_port) {
    return impl_->list_remote_datasets(remote_host, remote_port);
}

bool CrossLink::start_flight_server() {
    return impl_->start_flight_server();
}

bool CrossLink::stop_flight_server() {
    return impl_->stop_flight_server();
}

int CrossLink::flight_server_port() const {
    return impl_->flight_server_port();
}

const CrossLinkConfig& CrossLink::config() const {
    return impl_->config();
}

} // namespace crosslink 