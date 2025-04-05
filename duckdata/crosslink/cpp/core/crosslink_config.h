#pragma once
#include <string>
#include <memory>
#include <optional>

namespace crosslink {

// Enum for the operation mode of CrossLink
enum class OperationMode {
    LOCAL,        // Local mode: data shared within a single machine
    DISTRIBUTED   // Distributed mode: data shared across machines
};

// Configuration class for CrossLink
class CrossLinkConfig {
public:
    // Constructor with default values
    CrossLinkConfig(
        const std::string& db_path = "crosslink.duckdb",
        bool debug = false,
        OperationMode mode = OperationMode::LOCAL,
        const std::string& flight_host = "localhost",
        int flight_port = 8815,
        const std::string& mother_node_address = "",
        int mother_node_port = 8815,
        const std::string& node_address = "")
        : db_path_(db_path),
          debug_(debug),
          mode_(mode),
          flight_host_(flight_host),
          flight_port_(flight_port),
          mother_node_address_(mother_node_address),
          mother_node_port_(mother_node_port),
          node_address_(node_address) {}
    
    // Getters
    const std::string& db_path() const { return db_path_; }
    bool debug() const { return debug_; }
    OperationMode mode() const { return mode_; }
    const std::string& flight_host() const { return flight_host_; }
    int flight_port() const { return flight_port_; }
    const std::string& mother_node_address() const { return mother_node_address_; }
    int mother_node_port() const { return mother_node_port_; }
    const std::string& node_address() const { return node_address_; }
    
    // Setters with chainable interface
    CrossLinkConfig& set_db_path(const std::string& db_path) {
        db_path_ = db_path;
        return *this;
    }
    
    CrossLinkConfig& set_debug(bool debug) {
        debug_ = debug;
        return *this;
    }
    
    CrossLinkConfig& set_mode(OperationMode mode) {
        mode_ = mode;
        return *this;
    }
    
    CrossLinkConfig& set_flight_host(const std::string& flight_host) {
        flight_host_ = flight_host;
        return *this;
    }
    
    CrossLinkConfig& set_flight_port(int flight_port) {
        flight_port_ = flight_port;
        return *this;
    }
    
    CrossLinkConfig& set_mother_node_address(const std::string& mother_node_address) {
        mother_node_address_ = mother_node_address;
        return *this;
    }
    
    CrossLinkConfig& set_mother_node_port(int mother_node_port) {
        mother_node_port_ = mother_node_port;
        return *this;
    }
    
    CrossLinkConfig& set_node_address(const std::string& node_address) {
        node_address_ = node_address;
        return *this;
    }
    
    // Helper to check if configuration is valid for distributed mode
    bool is_valid_distributed_config() const {
        return mode_ == OperationMode::DISTRIBUTED &&
               !mother_node_address_.empty() &&
               !node_address_.empty();
    }
    
    // Static factory method to create config from environment variables
    static CrossLinkConfig from_env();
    
private:
    std::string db_path_;             // Path to the DuckDB database
    bool debug_;                     // Debug mode flag
    OperationMode mode_;             // Operation mode
    std::string flight_host_;         // Host for the Flight server
    int flight_port_;                // Port for the Flight server
    std::string mother_node_address_; // Address of the mother node (for distributed mode)
    int mother_node_port_;           // Port of the mother node (for distributed mode)
    std::string node_address_;        // Address of this node (for distributed mode)
};

} // namespace crosslink 