#pragma once
#include <string>
#include <memory>
#include <vector>
#include <functional>
#include <arrow/api.h>
#include <utility>

namespace crosslink {

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
    ~CrossLink();
    
    // Share a table from C++
    std::string push(std::shared_ptr<arrow::Table> table, 
                     const std::string& name = "",
                     const std::string& description = "");
    
    // Get a table from any language
    std::shared_ptr<arrow::Table> pull(const std::string& identifier);
    
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