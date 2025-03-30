#include "arrow_bridge.h"
#include "shared_memory_manager.h"
#include "metadata_manager.h"
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <chrono>
#include <filesystem>
#include <random>
#include <sstream>
#include <iomanip>

namespace crosslink {

std::string ArrowBridge::generate_uuid() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    static std::uniform_int_distribution<> dis2(8, 11);

    std::stringstream ss;
    ss << std::hex;

    for (int i = 0; i < 8; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 4; i++) {
        ss << dis(gen);
    }
    ss << "-4";
    for (int i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (int i = 0; i < 3; i++) {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 12; i++) {
        ss << dis(gen);
    }

    return ss.str();
}

std::string ArrowBridge::get_current_timestamp() {
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    std::stringstream ss;
    ss << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %X");
    return ss.str();
}

std::string ArrowBridge::share_arrow_table(
    const std::string& db_path,
    std::shared_ptr<arrow::Table> table,
    const std::string& name,
    bool use_shared_memory,
    bool memory_mapped) {
    
    auto dataset_id = generate_uuid();
    
    // Initialize metadata manager
    MetadataManager metadata_manager(db_path);
    
    // Create metadata
    DatasetMetadata metadata;
    metadata.id = dataset_id;
    metadata.name = name.empty() ? "arrow_" + dataset_id.substr(0, 8) : name;
    metadata.source_language = "cpp";
    metadata.created_at = get_current_timestamp();
    metadata.updated_at = metadata.created_at;
    metadata.description = "";
    metadata.arrow_data = true;
    metadata.version = 1;
    metadata.current_version = true;
    metadata.schema_hash = ""; // Would compute schema hash in real implementation
    metadata.access_languages = {"cpp", "python", "r", "julia"};
    metadata.num_rows = table->num_rows();
    metadata.num_columns = table->num_columns();
    
    // Calculate serialized size
    std::shared_ptr<arrow::Buffer> buffer;
    auto pool = arrow::default_memory_pool();
    
    // Use the static factory method to create a BufferOutputStream
    auto result = arrow::io::BufferOutputStream::Create(4096, pool);
    if (!result.ok()) {
        throw std::runtime_error("Failed to create BufferOutputStream: " + 
                                 result.status().ToString());
    }
    auto stream = result.ValueOrDie();
    
    auto writer_result = arrow::ipc::MakeStreamWriter(stream, table->schema());
    if (!writer_result.ok()) {
        throw std::runtime_error("Failed to create Arrow stream writer: " + 
                                writer_result.status().ToString());
    }
    auto writer = writer_result.ValueOrDie();
    
    auto write_status = writer->WriteTable(*table);
    if (!write_status.ok()) {
        throw std::runtime_error("Failed to write Arrow table: " + write_status.ToString());
    }
    
    auto close_status = writer->Close();
    if (!close_status.ok()) {
        throw std::runtime_error("Failed to close writer: " + close_status.ToString());
    }
    
    auto buffer_result = stream->Finish();
    if (!buffer_result.ok()) {
        throw std::runtime_error("Failed to finish buffer: " + buffer_result.status().ToString());
    }
    buffer = buffer_result.ValueOrDie();
    metadata.serialized_size = buffer->size();
    
    // Set up shared memory if requested
    if (use_shared_memory) {
        auto& shm_manager = SharedMemoryManager::instance();
        auto region = shm_manager.create_region(dataset_id, table);
        metadata.shared_memory_key = dataset_id;
        metadata.shared_memory_size = region->size();
    }
    
    // Set up memory mapped file if requested
    if (memory_mapped) {
        metadata.memory_map_path = create_memory_mapped_file(db_path, dataset_id, table);
    }
    
    // Store schema as JSON
    metadata.arrow_schema_json = metadata_manager.schema_to_json(table->schema());
    
    // Store metadata
    metadata_manager.create_dataset_metadata(metadata);
    
    // TODO: Add notification for data update
    
    return dataset_id;
}

std::shared_ptr<arrow::Table> ArrowBridge::get_arrow_table(
    const std::string& db_path,
    const std::string& identifier) {
    
    // Retrieve metadata to determine if table is in shared memory or memory-mapped file
    MetadataManager metadata_manager(db_path);
    auto metadata = metadata_manager.get_dataset_metadata(identifier);
    
    // First try shared memory if available
    if (!metadata.shared_memory_key.empty()) {
        auto& shm_manager = SharedMemoryManager::instance();
        auto region = shm_manager.get_region(metadata.shared_memory_key);
        
        if (region) {
            // Deserialize table from shared memory
            auto buffer = std::make_shared<arrow::Buffer>(reinterpret_cast<const uint8_t*>(region->data()), region->size());
            arrow::io::BufferReader reader(buffer);
            auto reader_result = arrow::ipc::RecordBatchStreamReader::Open(&reader);
            if (!reader_result.ok()) {
                throw std::runtime_error("Failed to open IPC stream: " + 
                                        reader_result.status().ToString());
            }
            
            auto batch_reader = reader_result.ValueOrDie();
            std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
            
            arrow::Status status;
            std::shared_ptr<arrow::RecordBatch> batch;
            while ((status = batch_reader->ReadNext(&batch)).ok() && batch != nullptr) {
                batches.push_back(batch);
            }
            
            if (!status.ok()) {
                throw std::runtime_error("Error reading record batches: " + status.ToString());
            }
            
            auto table_result = arrow::Table::FromRecordBatches(batches);
            if (!table_result.ok()) {
                throw std::runtime_error("Failed to create table from batches: " + 
                                        table_result.status().ToString());
            }
            
            return table_result.ValueOrDie();
        }
    }
    
    // Try memory-mapped file if not in shared memory or shared memory failed
    if (!metadata.memory_map_path.empty() && std::filesystem::exists(metadata.memory_map_path)) {
        auto result = arrow::io::MemoryMappedFile::Open(metadata.memory_map_path, arrow::io::FileMode::READ);
        if (!result.ok()) {
            throw std::runtime_error("Failed to open memory-mapped file: " + 
                                    result.status().ToString());
        }
        
        auto mmap_file = result.ValueOrDie();
        auto reader_result = arrow::ipc::RecordBatchFileReader::Open(mmap_file);
        if (!reader_result.ok()) {
            throw std::runtime_error("Failed to open Arrow file reader: " + 
                                    reader_result.status().ToString());
        }
        
        auto reader = reader_result.ValueOrDie();
        std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
        
        for (int i = 0; i < reader->num_record_batches(); i++) {
            auto batch_result = reader->ReadRecordBatch(i);
            if (!batch_result.ok()) {
                throw std::runtime_error("Error reading record batch: " + 
                                        batch_result.status().ToString());
            }
            batches.push_back(batch_result.ValueOrDie());
        }
        
        auto table_result = arrow::Table::FromRecordBatches(batches);
        if (!table_result.ok()) {
            throw std::runtime_error("Failed to create table from batches: " + 
                                    table_result.status().ToString());
        }
        
        return table_result.ValueOrDie();
    }
    
    throw std::runtime_error("Table not found or not accessible: " + identifier);
}

std::string ArrowBridge::create_memory_mapped_file(
    const std::string& db_path,
    const std::string& dataset_id,
    std::shared_ptr<arrow::Table> table) {
    
    // Create a directory for arrow files if it doesn't exist
    std::filesystem::path db_dir = std::filesystem::path(db_path).parent_path();
    std::filesystem::path arrow_dir = db_dir / "arrow_files";
    
    if (!std::filesystem::exists(arrow_dir)) {
        std::filesystem::create_directories(arrow_dir);
    }
    
    // Create file path
    std::string file_path = (arrow_dir / (dataset_id + ".arrow")).string();
    
    // Calculate the serialized size needed for the table
    auto serialize_result = arrow::io::BufferOutputStream::Create(4096);
    if (!serialize_result.ok()) {
        throw std::runtime_error("Failed to create BufferOutputStream for size calculation: " + 
                               serialize_result.status().ToString());
    }
    auto serialize_stream = serialize_result.ValueOrDie();
    
    auto serialize_writer_result = arrow::ipc::MakeFileWriter(serialize_stream, table->schema());
    if (!serialize_writer_result.ok()) {
        throw std::runtime_error("Failed to create Arrow file writer for size calculation: " + 
                               serialize_writer_result.status().ToString());
    }
    
    auto serialize_writer = serialize_writer_result.ValueOrDie();
    auto serialize_status = serialize_writer->WriteTable(*table);
    if (!serialize_status.ok()) {
        throw std::runtime_error("Failed to write Arrow table for size calculation: " + 
                               serialize_status.ToString());
    }
    
    auto close_serialize_status = serialize_writer->Close();
    if (!close_serialize_status.ok()) {
        throw std::runtime_error("Failed to close Arrow writer for size calculation: " + 
                               close_serialize_status.ToString());
    }
    
    auto buffer_result = serialize_stream->Finish();
    if (!buffer_result.ok()) {
        throw std::runtime_error("Failed to finish buffer for size calculation: " + 
                               buffer_result.status().ToString());
    }
    
    auto serialized_buffer = buffer_result.ValueOrDie();
    size_t file_size = serialized_buffer->size();
    
    // Write table to memory-mapped file
    auto result = arrow::io::MemoryMappedFile::Create(file_path, file_size);
    if (!result.ok()) {
        throw std::runtime_error("Failed to create memory-mapped file: " + 
                                result.status().ToString());
    }
    
    auto mmap_file = result.ValueOrDie();
    auto writer_result = arrow::ipc::MakeFileWriter(mmap_file, table->schema());
    if (!writer_result.ok()) {
        throw std::runtime_error("Failed to create Arrow file writer: " + 
                                writer_result.status().ToString());
    }
    
    auto writer = writer_result.ValueOrDie();
    auto status = writer->WriteTable(*table);
    if (!status.ok()) {
        throw std::runtime_error("Failed to write Arrow table: " + status.ToString());
    }
    
    status = writer->Close();
    if (!status.ok()) {
        throw std::runtime_error("Failed to close Arrow writer: " + status.ToString());
    }
    
    return file_path;
}

} // namespace crosslink 