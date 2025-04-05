#include "crosslink_config.h"
#include <cstdlib>
#include <iostream>

namespace crosslink {

// Helper function to get environment variable with a default value
std::string get_env_or_default(const char* env_var, const std::string& default_value) {
    const char* val = std::getenv(env_var);
    return val ? std::string(val) : default_value;
}

// Helper function to get integer environment variable with a default value
int get_env_int_or_default(const char* env_var, int default_value) {
    const char* val = std::getenv(env_var);
    if (val) {
        try {
            return std::stoi(val);
        } catch (const std::exception& e) {
            std::cerr << "Warning: Failed to parse environment variable " << env_var
                      << " as integer. Using default value " << default_value << std::endl;
        }
    }
    return default_value;
}

// Create config from environment variables
CrossLinkConfig CrossLinkConfig::from_env() {
    // Get operation mode from env var
    OperationMode mode = OperationMode::LOCAL;
    std::string mode_str = get_env_or_default("CROSSLINK_MODE", "LOCAL");
    if (mode_str == "DISTRIBUTED") {
        mode = OperationMode::DISTRIBUTED;
    }
    
    // Other environment variables
    std::string db_path = get_env_or_default("CROSSLINK_DB_PATH", "crosslink.duckdb");
    bool debug = get_env_or_default("CROSSLINK_DEBUG", "0") == "1";
    std::string flight_host = get_env_or_default("CROSSLINK_FLIGHT_HOST", "localhost");
    int flight_port = get_env_int_or_default("CROSSLINK_FLIGHT_PORT", 8815);
    std::string mother_node_address = get_env_or_default("CROSSLINK_MOTHER_NODE", "");
    int mother_node_port = get_env_int_or_default("CROSSLINK_MOTHER_NODE_PORT", 8815);
    std::string node_address = get_env_or_default("CROSSLINK_NODE_ADDRESS", "");
    
    return CrossLinkConfig(
        db_path,
        debug,
        mode,
        flight_host,
        flight_port,
        mother_node_address,
        mother_node_port,
        node_address
    );
}

} // namespace crosslink 