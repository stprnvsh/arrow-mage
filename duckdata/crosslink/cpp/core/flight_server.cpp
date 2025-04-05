#include "flight_server.h"
#include "arrow_bridge.h"
#include <arrow/flight/api.h>
#include <arrow/status.h>
#include <arrow/result.h>
#include <arrow/ipc/writer.h>
#include <arrow/ipc/reader.h>
#include <iostream>
#include <vector>
#include <algorithm>

namespace crosslink {

// Helper class for implementing Flight listings
class FlightListingImpl : public arrow::flight::FlightListing {
public:
    explicit FlightListingImpl(std::vector<arrow::flight::FlightInfo> flights)
        : flights_(std::move(flights)), position_(0) {}

    arrow::Result<arrow::flight::FlightInfo> Next() override {
        if (position_ >= flights_.size()) {
            return arrow::Status::Invalid("EOS");
        }
        return flights_[position_++];
    }

private:
    std::vector<arrow::flight::FlightInfo> flights_;
    size_t position_;
};

// Helper class for implementing Flight data streams
class TableDataStream : public arrow::flight::FlightDataStream {
public:
    TableDataStream(std::shared_ptr<arrow::Schema> schema,
                  std::shared_ptr<arrow::RecordBatchReader> reader)
        : schema_(std::move(schema)), reader_(std::move(reader)), counter_(0) {}

    std::shared_ptr<arrow::Schema> schema() override { return schema_; }

    arrow::Result<arrow::flight::FlightPayload> GetSchemaPayload() override {
        arrow::flight::FlightPayload payload;
        payload.descriptor.type = arrow::flight::FlightDescriptor::PATH;
        payload.ipc_message.type = arrow::ipc::MessageType::SCHEMA;
        ARROW_ASSIGN_OR_RAISE(payload.ipc_message.metadata,
                            arrow::ipc::internal::GetSchemaMessage(*schema_, 
                                                                 arrow::ipc::IpcWriteOptions::Defaults()));
        return payload;
    }

    arrow::Result<arrow::flight::FlightPayload> Next() override {
        std::shared_ptr<arrow::RecordBatch> batch;
        ARROW_ASSIGN_OR_RAISE(batch, reader_->Next());
        if (!batch) {
            arrow::flight::FlightPayload payload;
            payload.ipc_message.type = arrow::ipc::MessageType::NONE;
            return payload;
        }
        
        arrow::flight::FlightPayload payload;
        payload.descriptor.type = arrow::flight::FlightDescriptor::PATH;
        
        // Set IPC message type
        payload.ipc_message.type = arrow::ipc::MessageType::RECORD_BATCH;
        
        // Get record batch message
        ARROW_ASSIGN_OR_RAISE(payload.ipc_message.metadata,
                            arrow::ipc::internal::GetRecordBatchMessage(
                                *batch, counter_++, arrow::ipc::IpcWriteOptions::Defaults()));
        
        // Get body buffer
        ARROW_ASSIGN_OR_RAISE(payload.body, arrow::ipc::GetRecordBatchPayload(*batch));
        
        return payload;
    }

private:
    std::shared_ptr<arrow::Schema> schema_;
    std::shared_ptr<arrow::RecordBatchReader> reader_;
    int64_t counter_;
};

// Initialize a Flight server
arrow::Status FlightServer::Initialize(const std::string& host, int port,
                                      MetadataManager& metadata_manager,
                                      const std::string& db_path,
                                      std::unique_ptr<FlightServer>* server) {
    arrow::flight::Location location;
    ARROW_RETURN_NOT_OK(arrow::flight::Location::ForGrpcTcp(host, port, &location));
    
    arrow::flight::FlightServerOptions options(location);
    
    // Create the base server
    std::unique_ptr<arrow::flight::FlightServerBase> base_server;
    ARROW_ASSIGN_OR_RAISE(base_server, 
                         arrow::flight::FlightServerBase::Create(options));
    
    // Create our server implementation
    *server = std::unique_ptr<FlightServer>(
        new FlightServer(std::move(base_server), metadata_manager, db_path));
    
    return arrow::Status::OK();
}

// Constructor
FlightServer::FlightServer(std::unique_ptr<arrow::flight::FlightServerBase> server,
                           MetadataManager& metadata_manager,
                           const std::string& db_path)
    : server_(std::move(server)), 
      metadata_manager_(metadata_manager),
      db_path_(db_path),
      port_(0),
      is_running_(false) {}

// Start the server in a separate thread
arrow::Status FlightServer::StartAsync() {
    std::lock_guard<std::mutex> guard(running_mutex_);
    
    if (is_running_) {
        return arrow::Status::OK(); // Already running
    }
    
    // Get the actual port the server is bound to
    ARROW_ASSIGN_OR_RAISE(port_, server_->port());
    
    // Start the server in a new thread
    server_thread_ = std::make_unique<std::thread>([this]() {
        auto status = server_->Serve();
        if (!status.ok()) {
            std::cerr << "Flight server failed to start: " << status.ToString() << std::endl;
        }
    });
    
    // Wait a moment to ensure the server is up
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    is_running_ = true;
    return arrow::Status::OK();
}

// Stop the server
arrow::Status FlightServer::Stop() {
    std::lock_guard<std::mutex> guard(running_mutex_);
    
    if (!is_running_) {
        return arrow::Status::OK(); // Already stopped
    }
    
    // Stop the server
    ARROW_RETURN_NOT_OK(server_->Shutdown());
    
    // Wait for the server thread to finish
    if (server_thread_ && server_thread_->joinable()) {
        server_thread_->join();
        server_thread_.reset();
    }
    
    is_running_ = false;
    return arrow::Status::OK();
}

// Get the port the server is listening on
int FlightServer::port() const {
    return port_;
}

// Implement ListFlights
arrow::Status FlightServer::ListFlights(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Criteria* criteria,
    std::unique_ptr<arrow::flight::FlightListing>* listings) {
    
    try {
        // Get the list of datasets from the metadata manager
        std::vector<std::string> dataset_ids = metadata_manager_.list_datasets();
        
        // Create a flight info for each dataset
        std::vector<arrow::flight::FlightInfo> flight_infos;
        flight_infos.reserve(dataset_ids.size());
        
        for (const auto& id : dataset_ids) {
            try {
                // Get metadata for this dataset
                DatasetMetadata metadata = metadata_manager_.get_dataset_metadata(id);
                
                // Create a schema from the metadata
                std::shared_ptr<arrow::Schema> schema = metadata_manager_.json_to_schema(metadata.arrow_schema_json);
                
                // Create endpoint for this dataset
                arrow::flight::FlightEndpoint endpoint({id}, {});
                
                // Create a flight descriptor for this dataset
                arrow::flight::FlightDescriptor descriptor = 
                    arrow::flight::FlightDescriptor::Path({id});
                
                int64_t total_records = metadata.num_rows;
                int64_t total_bytes = metadata.serialized_size;
                
                // Create and add the flight info
                auto flight_info = arrow::flight::FlightInfo::Make(*schema, descriptor, 
                                                                  {endpoint}, total_records, total_bytes);
                flight_infos.push_back(*flight_info);
            } catch (const std::exception& e) {
                // Skip datasets with errors
                std::cerr << "Error getting metadata for dataset " << id << ": " << e.what() << std::endl;
                continue;
            }
        }
        
        // Create the listing
        *listings = std::unique_ptr<arrow::flight::FlightListing>(
            new FlightListingImpl(std::move(flight_infos)));
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::UnknownError("Error listing flights: ", e.what());
    }
}

// Implement GetFlightInfo
arrow::Status FlightServer::GetFlightInfo(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::FlightDescriptor& descriptor,
    std::unique_ptr<arrow::flight::FlightInfo>* info) {
    
    try {
        // Only support path-based descriptors
        if (descriptor.type != arrow::flight::FlightDescriptor::PATH || 
            descriptor.path.empty()) {
            return arrow::Status::Invalid("Invalid descriptor: PATH type with dataset ID expected");
        }
        
        // Get the dataset ID from the descriptor
        const std::string& dataset_id = descriptor.path[0];
        
        // Get metadata for this dataset
        DatasetMetadata metadata = metadata_manager_.get_dataset_metadata(dataset_id);
        
        // Create a schema from the metadata
        std::shared_ptr<arrow::Schema> schema = metadata_manager_.json_to_schema(metadata.arrow_schema_json);
        
        // Create a ticket for this dataset
        arrow::flight::Ticket ticket({dataset_id});
        
        // Create endpoint for this dataset
        arrow::flight::FlightEndpoint endpoint(ticket, {});
        
        int64_t total_records = metadata.num_rows;
        int64_t total_bytes = metadata.serialized_size;
        
        // Create the flight info
        ARROW_ASSIGN_OR_RAISE(*info, arrow::flight::FlightInfo::Make(*schema, descriptor, 
                                                                   {endpoint}, total_records, total_bytes));
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::UnknownError("Error getting flight info: ", e.what());
    }
}

// Implement DoGet
arrow::Status FlightServer::DoGet(
    const arrow::flight::ServerCallContext& context,
    const arrow::flight::Ticket& ticket,
    std::unique_ptr<arrow::flight::FlightDataStream>* stream) {
    
    try {
        // Get the dataset ID from the ticket
        const std::string& dataset_id = ticket.ticket;
        
        // Try to get the table first from pending tables (for recently PUT tables)
        std::shared_ptr<arrow::Table> table;
        {
            std::lock_guard<std::mutex> guard(pending_tables_mutex_);
            auto it = pending_tables_.find(dataset_id);
            if (it != pending_tables_.end()) {
                table = it->second;
                // Remove from pending tables after successful retrieval
                pending_tables_.erase(it);
            }
        }
        
        // If not found in pending tables, get it from storage
        if (!table) {
            table = ArrowBridge::get_arrow_table(metadata_manager_, dataset_id, db_path_);
        }
        
        // Create a record batch reader from the table
        std::shared_ptr<arrow::RecordBatchReader> reader;
        ARROW_ASSIGN_OR_RAISE(reader, arrow::RecordBatchReader::FromTable(table));
        
        // Create the data stream
        *stream = std::make_unique<TableDataStream>(table->schema(), reader);
        
        // Log the access
        metadata_manager_.log_access(dataset_id, "flight", "get", "flight", true);
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::UnknownError("Error in DoGet: ", e.what());
    }
}

// Implement DoPut
arrow::Status FlightServer::DoPut(
    const arrow::flight::ServerCallContext& context,
    std::unique_ptr<arrow::flight::FlightMessageReader> reader,
    std::unique_ptr<arrow::flight::FlightMetadataWriter> writer) {
    
    try {
        // Only support path-based descriptors
        const auto& descriptor = reader->descriptor();
        if (descriptor.type != arrow::flight::FlightDescriptor::PATH || 
            descriptor.path.empty()) {
            return arrow::Status::Invalid("Invalid descriptor: PATH type with dataset ID expected");
        }
        
        // Get the dataset ID from the descriptor
        const std::string& dataset_id = descriptor.path[0];
        
        // Read all record batches into a table
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        std::shared_ptr<arrow::RecordBatch> batch;
        
        while (true) {
            ARROW_ASSIGN_OR_RAISE(batch, reader->Next());
            if (!batch) {
                break;
            }
            batches.push_back(batch);
        }
        
        // Create table from batches
        std::shared_ptr<arrow::Table> table;
        ARROW_ASSIGN_OR_RAISE(table, arrow::Table::FromRecordBatches(reader->schema(), batches));
        
        // Store the table in pending tables
        {
            std::lock_guard<std::mutex> guard(pending_tables_mutex_);
            pending_tables_[dataset_id] = table;
        }
        
        // Use ArrowBridge to share the table
        std::string shared_dataset_id = ArrowBridge::share_arrow_table(
            metadata_manager_, table, dataset_id, true, true, db_path_);
        
        // Clean up pending tables on successful share
        {
            std::lock_guard<std::mutex> guard(pending_tables_mutex_);
            pending_tables_.erase(dataset_id);
        }
        
        // Log the access
        metadata_manager_.log_access(dataset_id, "flight", "put", "flight", true);
        
        return arrow::Status::OK();
    } catch (const std::exception& e) {
        return arrow::Status::UnknownError("Error in DoPut: ", e.what());
    }
}

} // namespace crosslink 