#include "shared_memory_manager.h"
#include "metadata_manager.h"
#include <stdexcept>
#include <iostream>
#include <string>

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
#include <errno.h>
#include <string.h>
#elif defined(_WIN32)
#include <windows.h>
#endif

#include <arrow/ipc/api.h>
#include <arrow/io/api.h>
#include <arrow/buffer.h>

namespace crosslink {

// --- Helper Function for Shared Memory Name ---
// Generates a shorter name suitable for POSIX shm_open, especially on macOS.
// Prepends "/" as commonly required/recommended.
std::string generate_shm_name(const std::string& dataset_id) {
    // Max length for shm_open name on macOS can be limited (e.g., PSHMNAMLEN=30)
    // Using "/" + "cl_" + 24 chars UUID = 28 chars, safely within common limits.
    const size_t max_uuid_len = 24;
    std::string prefix = "/cl_"; // Use leading '/'
    if (dataset_id.length() <= max_uuid_len) {
        return prefix + dataset_id;
    }
    return prefix + dataset_id.substr(0, max_uuid_len);
}
// --- End Helper Function ---

// Platform-specific implementation of SharedMemoryRegion
class SharedMemoryRegion::Impl {
public:
    Impl(const std::string& name, size_t size, bool create = true) : 
        name_(name), size_(size), data_(nullptr), owns_shm_(create) {
#if defined(__linux__) || defined(__APPLE__)
        // POSIX shared memory implementation
        int flags = create ? (O_CREAT | O_RDWR) : O_RDWR; // Use O_RDWR for attaching existing
        fd_ = shm_open(name.c_str(), flags, 0666);
        if (fd_ == -1) {
             throw std::runtime_error("Failed to open/create shared memory '" + name + "': " + std::string(strerror(errno)));
        }
        
        if (create) {
            if (ftruncate(fd_, size) == -1) {
                int err = errno; // Capture errno before other calls
                close(fd_);
                shm_unlink(name.c_str()); // Clean up if truncate failed
                throw std::runtime_error("Failed to set size for shared memory '" + name + "': " + std::string(strerror(err)));
            }
        } else {
             // If attaching, query the size
            struct stat sb;
            if (fstat(fd_, &sb) == -1) {
                 int err = errno;
                 close(fd_);
                 throw std::runtime_error("Failed to fstat shared memory '" + name + "': " + std::string(strerror(err)));
            }
            // Optional: Check if sb.st_size matches expected size?
            // For now, we trust the size passed in during attach attempt (or metadata size)
            size_ = sb.st_size; // Update size_ to actual size when attaching
        }

        
        data_ = mmap(NULL, size_, PROT_READ | PROT_WRITE, MAP_SHARED, fd_, 0);
        if (data_ == MAP_FAILED) {
            int err = errno;
            close(fd_);
            if (create) shm_unlink(name.c_str()); // Only unlink if we created it
            throw std::runtime_error("Failed to map shared memory '" + name + "': " + std::string(strerror(err)));
        }
#elif defined(_WIN32)
        // Windows shared memory implementation
        // CreateFileMapping handles both create and open
        handle_ = CreateFileMapping(
            INVALID_HANDLE_VALUE,
            NULL,
            PAGE_READWRITE,
            0,
            static_cast<DWORD>(size), // This size is only used if creating
            name.c_str());
            
        if (handle_ == NULL) {
            throw std::runtime_error("Failed to create/open file mapping '" + name + "'");
        }
        
        // Check if we actually created it or just opened existing
        owns_shm_ = (GetLastError() != ERROR_ALREADY_EXISTS);

        data_ = MapViewOfFile(
            handle_,
            FILE_MAP_ALL_ACCESS,
            0,
            0,
            0); // Map entire file when opening/creating
            
        if (data_ == NULL) {
            CloseHandle(handle_);
            throw std::runtime_error("Failed to map view of file '" + name + "'");
        }
        
        // Get the actual size of the mapped region
         MEMORY_BASIC_INFORMATION info;
         if (VirtualQuery(data_, &info, sizeof(info)) == 0) {
             // Error getting size, fallback? Or use requested size?
             // Using requested size for now, but actual size might differ if attaching
         } else {
             size_ = info.RegionSize; // Use actual mapped size
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
             // Only unlink if this instance created/owned the segment
            if (owns_shm_) { 
                shm_unlink(name_.c_str());
            }
        }
#elif defined(_WIN32)
        if (data_ != nullptr) {
            UnmapViewOfFile(data_);
        }
        if (handle_ != NULL) {
            CloseHandle(handle_);
             // Windows doesn't have explicit unlink like POSIX for named mappings
             // The mapping is destroyed when the last handle is closed.
        }
#endif
    }
    
    void* data() { return data_; }
    size_t size() const { return size_; }
    std::string name() const { return name_; } // Returns the name used (short name)
    
private:
    std::string name_;
    size_t size_;
    void* data_;
    bool owns_shm_; // Track if this instance created the shm segment
    
#if defined(__linux__) || defined(__APPLE__)
    int fd_ = -1;
#elif defined(_WIN32)
    HANDLE handle_ = NULL;
#endif
};

// SharedMemoryRegion implementation
SharedMemoryRegion::SharedMemoryRegion(const std::string& name, size_t size, bool create)
    : impl_(std::make_unique<Impl>(name, size, create)) {
}

SharedMemoryRegion::~SharedMemoryRegion() = default;

void* SharedMemoryRegion::data() {
    return impl_->data();
}

size_t SharedMemoryRegion::size() const {
    return impl_->size();
}

std::string SharedMemoryRegion::name() const { // Returns the short name used
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
    
    // *** Use the helper function to generate the short name ***
    std::string shm_name = generate_shm_name(dataset_id);

    // Create a shared memory region large enough for the serialized table
    auto region = std::make_shared<SharedMemoryRegion>(shm_name, buffer->size(), /* create = */ true);
    
    // Copy the serialized table to shared memory
    std::memcpy(region->data(), buffer->data(), buffer->size());
    
    // Store the region
    regions_[dataset_id] = region;
    
    return region;
}

std::shared_ptr<SharedMemoryRegion> SharedMemoryManager::get_region(
    const std::string& dataset_id,
    const std::string& db_path) {

    // Check cache first (using full dataset_id)
    {
        std::lock_guard<std::mutex> lock(regions_mutex_);
        auto it = regions_.find(dataset_id);
        if (it != regions_.end()) {
            return it->second;
        }
    } // Release lock before potentially blocking I/O or metadata access
    
    // Region not in cache, try to attach to an existing one using metadata
    try {
        // Retrieve the metadata using the correct db_path
        MetadataManager metadata_manager(db_path); 
        DatasetMetadata metadata = metadata_manager.get_dataset_metadata(dataset_id); // Use full ID to get metadata

        // Check if metadata indicates shared memory was used
        if (metadata.shared_memory_key.empty()) {
             // This dataset wasn't stored using shared memory according to metadata
             return nullptr; 
        }

        // *** Use the short name from metadata to attach ***
        std::string shm_name = metadata.shared_memory_key; 
        size_t shm_size = metadata.shared_memory_size; // Get size from metadata

         if (shm_size == 0) {
             // Size wasn't stored correctly or is missing, cannot reliably attach
             // We could try to query size via fstat, but need the name first.
             // If name is empty, we definitely can't proceed.
             if (shm_name.empty()) {
                  throw std::runtime_error("Shared memory key is empty in metadata for dataset: " + dataset_id);
             }
             // Attempt to open and query size (more robust)
             #if defined(__linux__) || defined(__APPLE__)
                 int fd = shm_open(shm_name.c_str(), O_RDONLY, 0666);
                 if (fd == -1) {
                     throw std::runtime_error("Failed to open shared memory '" + shm_name + "' to get size: " + std::string(strerror(errno)));
                 }
                 struct stat sb;
                 if (fstat(fd, &sb) == -1) {
                     int err = errno;
                     close(fd);
                     throw std::runtime_error("Failed to fstat shared memory '" + shm_name + "': " + std::string(strerror(err)));
                 }
                 close(fd);
                 shm_size = sb.st_size;
             #elif defined(_WIN32)
                 // Getting size of existing Windows mapping is harder without opening it first
                 // Relying on metadata size is preferred. If 0, throw error.
                 throw std::runtime_error("Shared memory size is 0 or missing in metadata for Windows for dataset: " + dataset_id);
             #endif
             if (shm_size == 0) {
                 throw std::runtime_error("Failed to determine size for shared memory region: " + shm_name);
             }
         }


        // Now, create the SharedMemoryRegion object in "attach" mode (create = false)
        // Pass the SHORT name (shm_name) from metadata
        auto region = std::make_shared<SharedMemoryRegion>(shm_name, shm_size, /* create = */ false);
        
        // Add to cache (using full dataset_id as key)
        {
            std::lock_guard<std::mutex> lock(regions_mutex_);
            regions_[dataset_id] = region;
        }
        return region;

    } catch (const std::exception& e) {
        // Log the error? For now, just print to cerr if attaching failed
        std::cerr << "Failed to get or attach to shared memory region for dataset '" 
                  << dataset_id << "': " << e.what() << std::endl;
        return nullptr; // Return nullptr if region cannot be found or attached
    }
}

void SharedMemoryManager::release_region(const std::string& dataset_id) {
    std::lock_guard<std::mutex> lock(regions_mutex_);
    regions_.erase(dataset_id); // Erase using the full dataset_id key
}

void SharedMemoryManager::cleanup_all() {
    std::lock_guard<std::mutex> lock(regions_mutex_);
    // Destructors of SharedMemoryRegion in the map will handle cleanup (munmap/unlink/CloseHandle)
    regions_.clear(); 
}

} // namespace crosslink 