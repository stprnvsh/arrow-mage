#include <crosslink/crosslink.h>
#include <arrow/api.h>
#include <arrow/table.h>
#include <iostream>
#include <vector>
#include <thread>
#include <chrono>

using namespace crosslink;

int main() {
    try {
        // 1. Initialize Node A (server node)
        std::cout << "Initializing Node A (server)..." << std::endl;
        CrossLinkConfig config_a;
        config_a.set_mode(OperationMode::DISTRIBUTED)
                .set_flight_host("localhost")
                .set_flight_port(8815)
                .set_debug(true);
        
        CrossLink node_a(config_a);
        
        // Start Flight server on Node A
        if (!node_a.start_flight_server()) {
            std::cerr << "Failed to start Flight server on Node A" << std::endl;
            return 1;
        }
        
        int server_port = node_a.flight_server_port();
        std::cout << "Node A Flight server started on port " << server_port << std::endl;
        
        // 2. Create a sample table on Node A
        std::cout << "\nCreating sample table on Node A..." << std::endl;
        auto schema = arrow::schema({arrow::field("id", arrow::int64()), 
                                    arrow::field("name", arrow::utf8())});
        
        arrow::Int64Builder id_builder;
        arrow::StringBuilder name_builder;
        
        id_builder.AppendValues({1, 2, 3, 4, 5});
        name_builder.AppendValues({"Alice", "Bob", "Charlie", "David", "Eve"});
        
        std::shared_ptr<arrow::Array> id_array, name_array;
        id_builder.Finish(&id_array);
        name_builder.Finish(&name_array);
        
        auto table = arrow::Table::Make(schema, {id_array, name_array});
        
        // 3. Push the table to Node A's local storage
        std::string dataset_id = node_a.push(table, "sample_dataset", "Sample dataset for Flight test");
        std::cout << "Table pushed to Node A with ID: " << dataset_id << std::endl;
        
        // 4. Initialize Node B (client node)
        std::cout << "\nInitializing Node B (client)..." << std::endl;
        CrossLinkConfig config_b;
        config_b.set_mode(OperationMode::LOCAL)
                .set_debug(true);
        
        CrossLink node_b(config_b);
        
        // 5. Pull the table from Node A to Node B using Flight
        std::cout << "\nNode B pulling table from Node A via Flight..." << std::endl;
        auto remote_table = node_b.flight_pull(dataset_id, "localhost", server_port);
        
        std::cout << "Table pulled by Node B: " << remote_table->num_rows() << " rows, "
                 << remote_table->num_columns() << " columns" << std::endl;
        
        // 6. List datasets on Node A from Node B
        std::cout << "\nNode B listing datasets on Node A..." << std::endl;
        auto datasets = node_b.list_remote_datasets("localhost", server_port);
        
        std::cout << "Datasets on Node A:" << std::endl;
        for (const auto& id : datasets) {
            std::cout << " - " << id << std::endl;
        }
        
        // 7. Create another table on Node B
        std::cout << "\nCreating another table on Node B..." << std::endl;
        arrow::Int64Builder id_builder2;
        arrow::StringBuilder name_builder2;
        
        id_builder2.AppendValues({101, 102, 103});
        name_builder2.AppendValues({"Xavier", "Yolanda", "Zach"});
        
        std::shared_ptr<arrow::Array> id_array2, name_array2;
        id_builder2.Finish(&id_array2);
        name_builder2.Finish(&name_array2);
        
        auto table2 = arrow::Table::Make(schema, {id_array2, name_array2});
        
        // 8. Push the table from Node B to Node A using Flight
        std::cout << "\nNode B pushing table to Node A via Flight..." << std::endl;
        std::string dataset_id2 = node_b.flight_push(table2, "localhost", server_port, 
                                                 "node_b_dataset", "Dataset from Node B");
        
        std::cout << "Table pushed from Node B to Node A with ID: " << dataset_id2 << std::endl;
        
        // 9. Pull the table back on Node A to confirm
        std::cout << "\nNode A pulling back the table from its own storage..." << std::endl;
        auto confirmed_table = node_a.pull(dataset_id2);
        
        std::cout << "Table confirmed on Node A: " << confirmed_table->num_rows() << " rows, "
                 << confirmed_table->num_columns() << " columns" << std::endl;
        
        // 10. Stop the Flight server
        std::cout << "\nStopping Node A Flight server..." << std::endl;
        node_a.stop_flight_server();
        
        std::cout << "\nFlight example completed successfully!" << std::endl;
        return 0;
        
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
} 