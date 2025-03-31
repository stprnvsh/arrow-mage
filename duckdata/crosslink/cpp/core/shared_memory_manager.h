#pragma once
#include <string>
#include <memory>
#include <unordered_map>
#include <arrow/api.h>
#include <mutex>

namespace crosslink {

class SharedMemoryRegion {
public:
    SharedMemoryRegion(const std::string& name, size_t size, bool create = true);
    ~SharedMemoryRegion();
    
    void* data();
    size_t size() const;
    std::string name() const;
    
private:
    class Impl;
    std::unique_ptr<Impl> impl_;
};

class SharedMemoryManager {
public:
    static SharedMemoryManager& instance();
    
    // Create a shared memory region for an Arrow table
    std::shared_ptr<SharedMemoryRegion> create_region(
        const std::string& dataset_id,
        std::shared_ptr<arrow::Table> table);
        
    // Get a shared memory region by ID
    std::shared_ptr<SharedMemoryRegion> get_region(
        const std::string& dataset_id, 
        const std::string& db_path);
    
    // Release a shared memory region
    void release_region(const std::string& dataset_id);
    
    // Clean up all regions
    void cleanup_all();
    
private:
    SharedMemoryManager();
    ~SharedMemoryManager();
    
    std::unordered_map<std::string, std::shared_ptr<SharedMemoryRegion>> regions_;
    std::mutex regions_mutex_;
};

} // namespace crosslink 