#pragma once
#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <arrow/api.h>
#include <utility>

namespace crosslink {

// Forward declarations
class CrossLinkConfig;
enum class OperationMode;

// --- Define Abstract Base Classes ---

// Abstract interface for writing to a stream
class StreamWriter {
public:
    virtual ~StreamWriter() = default; // Virtual destructor
    virtual void write_batch(std::shared_ptr<arrow::RecordBatch> batch) = 0;
    virtual void close() = 0; // Signal end of stream
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;
    virtual const std::string& stream_id() const = 0;
};

// Abstract interface for reading from a stream
class StreamReader {
public:
    virtual ~StreamReader() = default; // Virtual destructor
    virtual std::shared_ptr<arrow::RecordBatch> read_next_batch() = 0;
    virtual std::shared_ptr<arrow::Schema> schema() const = 0;
    virtual const std::string& stream_id() const = 0;
    virtual void close() = 0; // Release resources on the reader side
};

// --- End Abstract Base Classes ---

class CrossLink {
public:
    // Connect to CrossLink database
    CrossLink(const std::string& db_path = "crosslink.duckdb", bool debug = false);
    
    // Connect with full configuration
    explicit CrossLink(const CrossLinkConfig& config);
    
    ~CrossLink();
    
    // Share a table from C++
    std::string push(std::shared_ptr<arrow::Table> table, 
                     const std::string& name = "",
                     const std::string& description = "");
    
    // Get a table from any language
    std::shared_ptr<arrow::Table> pull(const std::string& identifier);
    
    // --- Flight API ---
    // Share a table via Flight to a remote node
    std::string flight_push(std::shared_ptr<arrow::Table> table,
                            const std::string& remote_host,
                            int remote_port,
                            const std::string& name = "",
                            const std::string& description = "");
    
    // Get a table via Flight from a remote node
    std::shared_ptr<arrow::Table> flight_pull(const std::string& identifier,
                                              const std::string& remote_host,
                                              int remote_port);
    // --- End Flight API ---
    
    // --- Streaming API ---
    // Create a new stream for pushing data
    std::pair<std::string, std::shared_ptr<StreamWriter>> 
    push_stream(std::shared_ptr<arrow::Schema> schema, 
                const std::string& name = "");
    
    // Get a reader for an existing stream
    std::shared_ptr<StreamReader> pull_stream(const std::string& stream_id);
    // --- End Streaming API ---
    
    // Execute a query and get results as Arrow table
    std::shared_ptr<arrow::Table> query(const std::string& sql);
    
    // List available datasets
    std::vector<std::string> list_datasets();
    
    // List available datasets on a remote Flight server
    std::vector<std::string> list_remote_datasets(const std::string& remote_host,
                                                 int remote_port);
    
    // Register for notifications
    std::string register_notification(
        const std::function<void(const std::string&, const std::string&)>& callback);
    
    // Unregister notifications
    void unregister_notification(const std::string& registration_id);
    
    // Start Flight server (for distributed mode)
    bool start_flight_server();
    
    // Stop Flight server
    bool stop_flight_server();
    
    // Get the port of the running Flight server
    int flight_server_port() const;
    
    // Clean up resources
    void cleanup();
    
    // Get current configuration
    const CrossLinkConfig& config() const;
    
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

} // namespace crosslink 