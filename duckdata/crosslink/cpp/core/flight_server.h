#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <arrow/api.h>
#include <arrow/flight/api.h>
#include "metadata_manager.h"

namespace crosslink {

// Forward declaration
class MetadataManager;

// Custom FlightServer implementation that serves CrossLink datasets
class FlightServer : public arrow::flight::FlightServerBase {
public:
    // Initialize a Flight server
    static arrow::Status Initialize(const std::string& host, int port,
                                    MetadataManager& metadata_manager,
                                    const std::string& db_path,
                                    std::unique_ptr<FlightServer>* server);
    
    // Start the server in a separate thread
    arrow::Status StartAsync();

    // Stop the server
    arrow::Status Stop();

    // Get the port the server is listening on
    int port() const;

    // Implementation of arrow::flight::FlightServerBase methods
    arrow::Status ListFlights(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::Criteria* criteria,
        std::unique_ptr<arrow::flight::FlightListing>* listings) override;

    arrow::Status GetFlightInfo(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::FlightDescriptor& descriptor,
        std::unique_ptr<arrow::flight::FlightInfo>* info) override;

    arrow::Status DoGet(
        const arrow::flight::ServerCallContext& context,
        const arrow::flight::Ticket& ticket,
        std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

    arrow::Status DoPut(
        const arrow::flight::ServerCallContext& context,
        std::unique_ptr<arrow::flight::FlightMessageReader> reader,
        std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) override;

private:
    // Private constructor, use Initialize instead
    FlightServer(std::unique_ptr<arrow::flight::FlightServerBase> server,
                MetadataManager& metadata_manager,
                const std::string& db_path);

    // The actual Arrow Flight server
    std::unique_ptr<arrow::flight::FlightServerBase> server_;
    
    // The metadata manager (reference to the one in CrossLink)
    MetadataManager& metadata_manager_;
    
    // Database path
    std::string db_path_;
    
    // Server port
    int port_;
    
    // Thread running the server
    std::unique_ptr<std::thread> server_thread_;
    
    // Is the server running
    bool is_running_;
    
    // Mutex to protect is_running_
    std::mutex running_mutex_;
    
    // Cached tables for PUTs that are in progress
    std::unordered_map<std::string, std::shared_ptr<arrow::Table>> pending_tables_;
    
    // Mutex to protect pending_tables_
    std::mutex pending_tables_mutex_;
};

} // namespace crosslink 