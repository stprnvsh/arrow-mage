#pragma once
#include <memory>
#include <string>
#include <arrow/api.h>

namespace crosslink {

class ArrowBridge {
public:
    // Share an Arrow table via shared memory and return metadata
    static std::string share_arrow_table(
        const std::string& db_path,
        std::shared_ptr<arrow::Table> table,
        const std::string& name = "",
        bool use_shared_memory = true,
        bool memory_mapped = true);
    
    // Get an Arrow table from metadata
    static std::shared_ptr<arrow::Table> get_arrow_table(
        const std::string& db_path,
        const std::string& identifier);
    
    // Create memory mapped Arrow file
    static std::string create_memory_mapped_file(
        const std::string& db_path,
        const std::string& dataset_id,
        std::shared_ptr<arrow::Table> table);
        
    // Utility function to generate a unique ID
    static std::string generate_uuid();
    
    // Get current timestamp as string
    static std::string get_current_timestamp();
};

} // namespace crosslink 