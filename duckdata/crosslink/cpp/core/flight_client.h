#pragma once
#include <memory>
#include <string>
#include <arrow/api.h>
#include <arrow/flight/api.h>

namespace crosslink {

class FlightClient {
public:
    // Initialize a Flight client
    static arrow::Status Initialize(const std::string& host, int port,
                                   std::unique_ptr<FlightClient>* client);
    
    ~FlightClient();
    
    // Do not allow copy or move
    FlightClient(const FlightClient&) = delete;
    FlightClient& operator=(const FlightClient&) = delete;
    
    // Get a dataset from a remote Flight server
    arrow::Status GetDataset(const std::string& dataset_id, 
                             std::shared_ptr<arrow::Table>* out_table);
    
    // Put a dataset to a remote Flight server
    arrow::Status PutDataset(const std::string& dataset_id,
                             const std::shared_ptr<arrow::Table>& table);
    
    // List available datasets on the Flight server
    arrow::Status ListDatasets(std::vector<std::string>* dataset_ids);

private:
    // Private constructor, use Initialize instead
    explicit FlightClient(std::unique_ptr<arrow::flight::FlightClient> client);
    
    // The actual Arrow Flight client
    std::unique_ptr<arrow::flight::FlightClient> client_;
};

} // namespace crosslink 