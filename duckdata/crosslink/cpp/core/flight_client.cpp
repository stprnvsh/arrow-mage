#include "flight_client.h"
#include <arrow/flight/api.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/buffer.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <iostream>

namespace crosslink {

// Helper method to create a dataset location descriptor
arrow::flight::FlightDescriptor MakeDatasetDescriptor(const std::string& dataset_id) {
    return arrow::flight::FlightDescriptor::Path({dataset_id});
}

// Initialize a Flight client
arrow::Status FlightClient::Initialize(const std::string& host, int port,
                                    std::unique_ptr<FlightClient>* client) {
    arrow::flight::Location location;
    ARROW_RETURN_NOT_OK(arrow::flight::Location::ForGrpcTcp(host, port, &location));
    
    std::unique_ptr<arrow::flight::FlightClient> flight_client;
    ARROW_ASSIGN_OR_RAISE(flight_client, arrow::flight::FlightClient::Connect(location));
    
    *client = std::unique_ptr<FlightClient>(new FlightClient(std::move(flight_client)));
    return arrow::Status::OK();
}

// Constructor
FlightClient::FlightClient(std::unique_ptr<arrow::flight::FlightClient> client)
    : client_(std::move(client)) {}

// Destructor
FlightClient::~FlightClient() = default;

// Get a dataset from a remote Flight server
arrow::Status FlightClient::GetDataset(const std::string& dataset_id, 
                                      std::shared_ptr<arrow::Table>* out_table) {
    // Create a flight descriptor for the dataset
    auto descriptor = MakeDatasetDescriptor(dataset_id);
    
    // Get flight info to obtain the ticket
    std::unique_ptr<arrow::flight::FlightInfo> flight_info;
    ARROW_ASSIGN_OR_RAISE(flight_info, client_->GetFlightInfo(descriptor));
    
    // Should have exactly one endpoint
    if (flight_info->endpoints().empty()) {
        return arrow::Status::Invalid("No endpoints returned for dataset: ", dataset_id);
    }
    
    // Get the ticket from the first endpoint
    const arrow::flight::FlightEndpoint& endpoint = flight_info->endpoints()[0];
    const arrow::flight::Ticket& ticket = endpoint.ticket;
    
    // Use the ticket to get a reader
    std::unique_ptr<arrow::flight::FlightStreamReader> stream;
    ARROW_ASSIGN_OR_RAISE(stream, client_->DoGet(ticket));
    
    // Read all batches and concatenate into a table
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    arrow::Result<std::shared_ptr<arrow::RecordBatch>> batch_result;
    
    while (true) {
        batch_result = stream->Next();
        if (!batch_result.ok()) {
            // Check if it's an end of stream (EOS) error
            if (batch_result.status().IsInvalid() && 
                batch_result.status().message().find("EOS") != std::string::npos) {
                break;  // End of stream
            }
            return batch_result.status();
        }
        
        std::shared_ptr<arrow::RecordBatch> batch = batch_result.ValueOrDie();
        if (batch == nullptr) {
            break;  // End of stream
        }
        
        batches.push_back(batch);
    }
    
    // Create a table from the batches
    return arrow::Table::FromRecordBatches(batches).Value(out_table);
}

// Put a dataset to a remote Flight server
arrow::Status FlightClient::PutDataset(const std::string& dataset_id,
                                      const std::shared_ptr<arrow::Table>& table) {
    // Create a flight descriptor for the dataset
    auto descriptor = MakeDatasetDescriptor(dataset_id);
    
    // Create a writer to upload the data
    std::unique_ptr<arrow::flight::FlightStreamWriter> writer;
    std::unique_ptr<arrow::flight::FlightMetadataReader> metadata_reader;
    ARROW_ASSIGN_OR_RAISE(auto put_result, client_->DoPut(descriptor, table->schema()));
    writer = std::move(put_result.first);
    metadata_reader = std::move(put_result.second);
    
    // Convert table to record batches and write them
    ARROW_ASSIGN_OR_RAISE(auto batch_reader, arrow::RecordBatchReader::FromTable(table));
    std::shared_ptr<arrow::RecordBatch> batch;
    
    while (true) {
        ARROW_ASSIGN_OR_RAISE(batch, batch_reader->Next());
        if (batch == nullptr) {
            break;
        }
        ARROW_RETURN_NOT_OK(writer->WriteRecordBatch(*batch));
    }
    
    // Close the writer to complete the put operation
    ARROW_RETURN_NOT_OK(writer->Close());
    
    return arrow::Status::OK();
}

// List available datasets on the Flight server
arrow::Status FlightClient::ListDatasets(std::vector<std::string>* dataset_ids) {
    std::unique_ptr<arrow::flight::FlightListing> listing;
    ARROW_ASSIGN_OR_RAISE(listing, client_->ListFlights());
    
    dataset_ids->clear();
    arrow::flight::FlightInfo flight_info;
    
    while (true) {
        ARROW_ASSIGN_OR_RAISE(flight_info, listing->Next());
        if (!flight_info.descriptor().path.empty()) {
            // Add the first path element as the dataset ID
            dataset_ids->push_back(flight_info.descriptor().path[0]);
        }
    }
    
    return arrow::Status::OK();
}

} // namespace crosslink 