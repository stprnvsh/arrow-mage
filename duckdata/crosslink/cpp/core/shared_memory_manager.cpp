#include "shared_memory_manager.h"
#include "metadata_manager.h"
#include <stdexcept>
#include <iostream>

#ifdef __linux__
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#elif defined(__APPLE__)
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

#include <arrow/ipc/api.h>
#include <arrow/io/api.h>

namespace crosslink {

// Platform-specific implementation of SharedMemoryRegion
class SharedMemoryRegion::Impl {
public:
    Impl(const std::string& name, size_t size) : 
        name_(name), size_(size), data_(nullptr) {
#if defined(__linux__) || defined(__APPLE__)
        // POSIX shared memory implementation
        fd_ = shm_open(name.c_str(), O_CREAT | O_RDWR, 0666);
        if (fd_ == -1) {
            throw std::runtime_error("Failed to create shared memory: " + std::string(strerror(errno)));
        }
        
        if (ftruncate(fd_, size) == -1) {
            close(fd_);
            shm_unlink(name.c_str());
            throw std::runtime_error("Failed to set size: " + std::string(strerror(errno)));
        }
        
        data_ = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (data_ == MAP_FAILED) {
            close(fd_);
            shm_unlink(name.c_str());
            throw std::runtime_error("Failed to map memory: " + std::string(strerror(errno)));
        }
#elif defined(_WIN32)
        // Windows shared memory implementation
        handle_ = CreateFileMapping(
            INVALID_HANDLE_VALUE,
            NULL,
            PAGE_READWRITE,
            0,
            static_cast<DWORD>(size),
            name.c_str());
            
        if (handle_ == NULL) {
            throw std::runtime_error("Failed to create shared memory");
        }
        
        data_ = MapViewOfFile(
            handle_,
            FILE_MAP_ALL_ACCESS,
            0,
            0,
            size);
            
        if (data_ == NULL) {
            CloseHandle(handle_);
            throw std::runtime_error("Failed to map memory");
        }
#else
        throw std::runtime_error("Shared memory not supported on this platform");
#endif
    }
    
    ~Impl() {
#if defined(__linux__) || defined(__APPLE__)
        if (data_ != MAP_FAILED && data_ != nullptr) {
            munmap(data_, size_);
        }
        if (fd_ != -1) {
            close(fd_);
            shm_unlink(name_.c_str());
        }
#elif defined(_WIN32)
        if (data_ != nullptr) {
            UnmapViewOfFile(data_);
        }
        if (handle_ != NULL) {
            CloseHandle(handle_);
        }
#endif
    }
    
    void* data() { return data_; }
    size_t size() const { return size_; }
    std::string name() const { return name_; }
    
private:
    std::string name_;
    size_t size_;
    void* data_;
    
#if defined(__linux__) || defined(__APPLE__)
    int fd_ = -1;
#elif defined(_WIN32)
    HANDLE handle_ = NULL;
#endif
};

// SharedMemoryRegion implementation
SharedMemoryRegion::SharedMemoryRegion(const std::string& name, size_t size)
    : impl_(std::make_unique<Impl>(name, size)) {
}

SharedMemoryRegion::~SharedMemoryRegion() = default;

void* SharedMemoryRegion::data() {
    return impl_->data();
}

size_t SharedMemoryRegion::size() const {
    return impl_->size();
}

std::string SharedMemoryRegion::name() const {
    return impl_->name();
}

// SharedMemoryManager implementation
SharedMemoryManager& SharedMemoryManager::instance() {
    static SharedMemoryManager instance;
    return instance;
}

SharedMemoryManager::SharedMemoryManager() {
    // Initialize the shared memory manager
}

SharedMemoryManager::~SharedMemoryManager() {
    // Clean up all shared memory regions
    cleanup_all();
}

std::shared_ptr<SharedMemoryRegion> SharedMemoryManager::create_region(
    const std::string& dataset_id,
    std::shared_ptr<arrow::Table> table) {
    
    // Calculate the size needed for the Arrow table
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
    
    // Create a shared memory region large enough for the serialized table
    auto region = std::make_shared<SharedMemoryRegion>(dataset_id, buffer->size());
    
    // Copy the serialized table to shared memory
    std::memcpy(region->data(), buffer->data(), buffer->size());
    
    // Store the region
    regions_[dataset_id] = region;
    
    return region;
}

std::shared_ptr<SharedMemoryRegion> SharedMemoryManager::get_region(const std::string& dataset_id) {
    auto it = regions_.find(dataset_id);
    if (it != regions_.end()) {
        return it->second;
    }
    
    // Try to attach to an existing region
    try {
        // Retrieve the size from metadata
        MetadataManager metadata_manager("crosslink.duckdb");  // Default path
        try {
            auto metadata = metadata_manager.get_dataset_metadata(dataset_id);
            
            // Open the shared memory region with the correct size
            // First try to open it to see if it exists without creating it
#if defined(__linux__) || defined(__APPLE__)
            int fd = shm_open(dataset_id.c_str(), O_RDONLY, 0666);
            if (fd != -1) {
                // Get the size of the existing shared memory region
                struct stat sb;
                if (fstat(fd, &sb) != -1) {
                    close(fd);
                    
                    // Now create our region object with the correct size
                    auto region = std::make_shared<SharedMemoryRegion>(dataset_id, sb.st_size);
                    regions_[dataset_id] = region;
                    return region;
                }
                close(fd);
            }
#endif
            // If we can't get the size directly, try to use a reasonable value from metadata
            // For example, we could store the serialized size in the metadata
            // For now, fallback to using a size based on row count and column count as an estimate
            size_t estimated_size = 1024 * 1024;  // 1MB as absolute minimum
            
            // A better estimation would use actual table stats from metadata
            auto region = std::make_shared<SharedMemoryRegion>(dataset_id, estimated_size);
            regions_[dataset_id] = region;
            return region;
        } catch (const std::exception& e) {
            std::cerr << "Failed to get metadata for shared memory region sizing: " << e.what() << std::endl;
            // Fallback to a reasonable default if metadata retrieval fails
            auto region = std::make_shared<SharedMemoryRegion>(dataset_id, 1024 * 1024 * 10);  // 10MB fallback
            regions_[dataset_id] = region;
            return region;
        }
    } catch (const std::exception& e) {
        std::cerr << "Failed to attach to shared memory region: " << e.what() << std::endl;
        return nullptr;
    }
}

void SharedMemoryManager::release_region(const std::string& dataset_id) {
    regions_.erase(dataset_id);
}

void SharedMemoryManager::cleanup_all() {
    regions_.clear();
}

} // namespace crosslink 