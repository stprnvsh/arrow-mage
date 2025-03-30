#include <jlcxx/jlcxx.hpp>
#include <jlcxx/stl.hpp>
#include <julia.h> // Include Julia C API headers
#include "../include/crosslink.h"

// Include necessary Arrow C++ and C Data Interface headers
#include <arrow/api.h> // Includes many common Arrow headers
#include <arrow/c/abi.h> // Defines ArrowArray, ArrowSchema, ArrowArrayStream
#include <arrow/c/bridge.h> // Defines Import/Export functions
#include <arrow/ipc/reader.h> // For RecordBatchReader
#include <arrow/table.h> // For Table and TableBatchReader

#include <memory> // For std::shared_ptr

// This binding uses the Arrow C Data Interface to transfer data between Julia and C++
// It requires the Arrow.jl package to be installed in Julia

// Helper function to check status and throw runtime error
void CheckArrowStatus(const arrow::Status& status, const std::string& context) {
    if (!status.ok()) {
        throw std::runtime_error("Arrow error in " + context + ": " + status.ToString());
    }
}

// Helper to import a C++ RecordBatch from Julia's C Data Interface representation (bytes)
std::shared_ptr<arrow::RecordBatch> julia_c_data_to_cpp_batch(jlcxx::ArrayRef<uint8_t> arrow_c_data) {
    if (arrow_c_data.size() < sizeof(struct ArrowArray) + sizeof(struct ArrowSchema)) {
        throw std::runtime_error("Invalid Arrow C data format: size is too small.");
    }
    // Note: The incoming data buffer holds *copies* of the ArrowArray and ArrowSchema structs.
    // We need to create actual struct instances to pass to ImportRecordBatch.
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    std::memcpy(&c_array, arrow_c_data.data(), sizeof(struct ArrowArray));
    std::memcpy(&c_schema, arrow_c_data.data() + sizeof(struct ArrowArray), sizeof(struct ArrowSchema));

    // Import using arrow::ImportRecordBatch
    // The ImportRecordBatch function *moves* ownership from the C structs.
    // It takes ownership of the data buffers pointed to by the structs
    // and sets the release callbacks to nullptr.
    auto maybe_batch = arrow::ImportRecordBatch(&c_array, &c_schema);
    CheckArrowStatus(maybe_batch.status(), "julia_c_data_to_cpp_batch");

    // IMPORTANT: After ImportRecordBatch, c_array.release and c_schema.release are nullptr.
    // We do NOT need to call release on them here. Arrow's internal mechanisms
    // tied to the lifetime of the resulting shared_ptr<RecordBatch> handle memory.

    return maybe_batch.ValueUnsafe();
}

// Helper to import a C++ Schema from Julia's C Data Interface representation (bytes)
std::shared_ptr<arrow::Schema> julia_c_data_to_cpp_schema(jlcxx::ArrayRef<uint8_t> schema_bytes) {
    if (schema_bytes.size() < sizeof(struct ArrowSchema)) {
        throw std::runtime_error("Invalid Arrow C schema format: size is too small.");
    }
    struct ArrowSchema c_schema;
    std::memcpy(&c_schema, schema_bytes.data(), sizeof(struct ArrowSchema));

    // Import using arrow::ImportSchema
    auto maybe_schema = arrow::ImportSchema(&c_schema);
    CheckArrowStatus(maybe_schema.status(), "julia_c_data_to_cpp_schema");

    // ImportSchema also moves ownership and sets release to nullptr.
    return maybe_schema.ValueUnsafe();
}

// Helper to export C++ RecordBatch to Julia's C Data Interface representation (bytes)
jlcxx::Array<uint8_t> cpp_batch_to_julia_c_data(const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Allocate ArrowArray and ArrowSchema C structs *on the stack* temporarily.
    // ExportRecordBatch will populate these structs and set their release callbacks.
    // These callbacks capture the necessary information (like the batch shared_ptr)
    // to manage the lifetime of the underlying data.
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;

    // Initialize to zero, important for release checks
    std::memset(&c_array, 0, sizeof(c_array));
    std::memset(&c_schema, 0, sizeof(c_schema));

    // Export using arrow::ExportRecordBatch
    arrow::Status status = arrow::ExportRecordBatch(*batch, &c_array, &c_schema);
    if (!status.ok()) {
        // If export fails, release callbacks *might* have been partially set.
        // Call them if they exist to clean up potential partial state.
        if (c_array.release) c_array.release(&c_array);
        if (c_schema.release) c_schema.release(&c_schema);
        CheckArrowStatus(status, "cpp_batch_to_julia_c_data (export)");
    }

    // Serialize ArrowArray and ArrowSchema structs to a Julia byte array
    size_t total_size = sizeof(struct ArrowArray) + sizeof(struct ArrowSchema);
    jlcxx::Array<uint8_t> result(total_size);

    // Copy struct *contents* (including pointers and release callbacks) to Julia array
    auto* dest_ptr = reinterpret_cast<uint8_t*>(jl_array_data(result.wrapped(), uint8_t));
    std::memcpy(dest_ptr, &c_array, sizeof(struct ArrowArray));
    std::memcpy(dest_ptr + sizeof(struct ArrowArray), &c_schema, sizeof(struct ArrowSchema));

    // Ownership of the *underlying data* is transferred to the consumer (Julia)
    // via the C struct data and release callbacks. Julia's Arrow implementation
    // must eventually call the release callbacks copied into its memory.
    // We do not own the c_array/c_schema stack variables anymore in terms of lifetime
    // management of the data they point to, that's the job of the release callback.

    return result;
}

// Helper to export C++ Schema to Julia's C Data Interface representation (bytes)
jlcxx::Array<uint8_t> cpp_schema_to_julia_c_data(const std::shared_ptr<arrow::Schema>& schema) {
    struct ArrowSchema c_schema;
    std::memset(&c_schema, 0, sizeof(c_schema)); // Initialize

    arrow::Status status = arrow::ExportSchema(*schema, &c_schema);
    if (!status.ok()) {
        if (c_schema.release) c_schema.release(&c_schema); // Cleanup on failure
        CheckArrowStatus(status, "cpp_schema_to_julia_c_data (export)");
    }

    size_t total_size = sizeof(struct ArrowSchema);
    jlcxx::Array<uint8_t> result(total_size);
    std::memcpy(reinterpret_cast<uint8_t*>(jl_array_data(result.wrapped(), uint8_t)), &c_schema, total_size);

    // Ownership transferred via the copied struct contents (incl. release callback).
    return result;
}

// Placeholder for table conversion (more complex)
// For now, we only handle batch/schema which is sufficient for streaming
std::shared_ptr<arrow::Table> julia_c_data_to_cpp_table(jlcxx::ArrayRef<uint8_t> arrow_c_data) {
    // This requires importing a potentially chunked array/table structure.
    // For simplicity, let's assume the input is a single RecordBatch C structure
    // and we create a single-batch table from it.
     if (arrow_c_data.size() < sizeof(struct ArrowArray) + sizeof(struct ArrowSchema)) {
        throw std::runtime_error("Invalid Arrow C data format for table import: size is too small.");
    }
    // Create structs from bytes
    struct ArrowArray c_array;
    struct ArrowSchema c_schema;
    std::memcpy(&c_array, arrow_c_data.data(), sizeof(struct ArrowArray));
    std::memcpy(&c_schema, arrow_c_data.data() + sizeof(struct ArrowArray), sizeof(struct ArrowSchema));

    // Import as a RecordBatch first
    auto maybe_batch = arrow::ImportRecordBatch(&c_array, &c_schema);
    CheckArrowStatus(maybe_batch.status(), "julia_c_data_to_cpp_table (import batch)");
    auto batch = maybe_batch.ValueUnsafe(); // Ownership transferred via import

    // Construct a table from this single batch
    // Table::FromRecordBatches takes a vector of shared_ptr
    auto table_result = arrow::Table::FromRecordBatches({batch});
    CheckArrowStatus(table_result.status(), "julia_c_data_to_cpp_table (create table)");

    return table_result.ValueUnsafe();
}

jlcxx::Array<uint8_t> cpp_table_to_julia_c_data(const std::shared_ptr<arrow::Table>& table) {
    // This requires exporting a potentially chunked structure or combining batches.
    // For simplicity, let's combine into one batch and export that.
    // Note: CombineChunks can be expensive for large tables.

    // 1. Combine chunks to get a single-chunk Table
    auto maybe_combined_table = table->CombineChunks();
    CheckArrowStatus(maybe_combined_table.status(), "cpp_table_to_julia_c_data (combine chunks)");
    std::shared_ptr<arrow::Table> combined_table = maybe_combined_table.ValueUnsafe();

    // 2. Create a TableBatchReader for the combined table
    arrow::TableBatchReader reader(*combined_table);

    // 3. Read the single RecordBatch from the reader
    std::shared_ptr<arrow::RecordBatch> batch;
    arrow::Status read_status = reader.ReadNext(&batch); // Pass address of shared_ptr
    CheckArrowStatus(read_status, "cpp_table_to_julia_c_data (read batch from combined table)");

    if (!batch) {
        // Handle the case where the combined table is empty (though CombineChunks might error earlier)
        throw std::runtime_error("Failed to read batch from combined table, or table was empty.");
    }

    // 4. Pass the RecordBatch to the existing helper function
    return cpp_batch_to_julia_c_data(batch);
}

// Exports a C++ RecordBatchReader to a C ArrowArrayStream struct (allocated on heap)
// and returns the pointer address as uintptr_t. The caller (Julia) owns the pointer
// and *must* call the stream's release callback.
uintptr_t cpp_reader_to_julia_stream_ptr(const std::shared_ptr<arrow::RecordBatchReader>& reader) {
    // Allocate the C stream struct on the heap.
    // The ownership of this struct is transferred to the consumer (Julia),
    // and it *must* call the release callback to free this memory.
    // Use new ArrowArrayStream{} for value initialization (zeroing).
    auto* stream_ptr = new ArrowArrayStream{};

    // Export the reader into the C struct.
    // This sets up the function pointers (get_schema, get_next, release)
    // in the struct. The release callback will typically capture the shared_ptr
    // to the reader, keeping it alive until release is called.
    arrow::Status status = arrow::ExportRecordBatchReader(reader, stream_ptr);

    if (!status.ok()) {
        // If export fails, we need to clean up the allocated struct.
        // The release callback might not be set or might be invalid.
        // Check if release is set before calling, though unlikely if Export failed early.
        // if (stream_ptr->release) stream_ptr->release(stream_ptr); // Potentially unsafe if partially initialized
        delete stream_ptr; // Clean up the heap allocation
        CheckArrowStatus(status, "cpp_reader_to_julia_stream_ptr (export)");
    }

    // Return the raw pointer address as an integer.
    // Julia will cast this back to Ptr{ArrowArrayStream}.
    return reinterpret_cast<uintptr_t>(stream_ptr);
}

JLCXX_MODULE define_julia_module(jlcxx::Module& mod) {
    // === Define the StreamWriter class ===
    mod.add_type<crosslink::StreamWriter>("StreamWriter")
        .method("write_batch", [](crosslink::StreamWriter& self, jlcxx::ArrayRef<uint8_t> arrow_c_data) {
            // Convert from Julia Arrow RecordBatch (via C bytes) to C++ RecordBatch
            auto batch = julia_c_data_to_cpp_batch(arrow_c_data);
            // Write the batch to the stream
            self.write_batch(batch);
        })
        .method("close", &crosslink::StreamWriter::close)
        .method("stream_id", &crosslink::StreamWriter::stream_id)
        .method("schema", [](crosslink::StreamWriter& self) {
            // Get C++ Arrow Schema from the stream
            auto schema = self.schema();
            // Export schema to C Data Interface bytes for Julia
            return cpp_schema_to_julia_c_data(schema);
        });
    
    // === Define the StreamReader class ===
    mod.add_type<crosslink::StreamReader>("StreamReader")
        .method("read_next_batch", [](crosslink::StreamReader& self) -> jlcxx::Array<uint8_t> {
            // Read the next batch from the stream
            auto batch = self.read_next_batch();
            
            // Return empty array if no more batches
            if (!batch) {
                // Explicitly cast 0 to size_t
                return jlcxx::Array<uint8_t>(static_cast<size_t>(0));
            }
            
            // Export the C++ batch to C Data Interface bytes for Julia
            return cpp_batch_to_julia_c_data(batch);
        })
        .method("close", &crosslink::StreamReader::close)
        .method("stream_id", &crosslink::StreamReader::stream_id)
        .method("schema", [](crosslink::StreamReader& self) {
            // Get C++ Arrow Schema from the stream
            auto schema = self.schema();
            
            // Export schema to C Data Interface bytes for Julia
            return cpp_schema_to_julia_c_data(schema);
        });

    // Define the CrossLink class
    mod.add_type<crosslink::CrossLink>("CrossLinkCpp")
        .constructor<const std::string&, bool>()
        
        // Push method
        .method("push", [](crosslink::CrossLink& self, jlcxx::ArrayRef<uint8_t> arrow_c_data,
                           const std::string& name, const std::string& description) {
             // Convert Julia C data bytes to C++ Table
            auto table = julia_c_data_to_cpp_table(arrow_c_data);
            // Push the table to CrossLink
            return self.push(table, name, description);
        })
        
        // Pull method
        .method("pull", [](crosslink::CrossLink& self, const std::string& identifier) -> uintptr_t {
            // Pull the table using the C++ API
            auto table = self.pull(identifier);
            // Create a reader for the table
            auto reader = std::make_shared<arrow::TableBatchReader>(*table);
            // Export the reader to a C stream struct and return the pointer address
            return cpp_reader_to_julia_stream_ptr(reader);
        })
        
        // Query method
        .method("query", [](crosslink::CrossLink& self, const std::string& sql) -> uintptr_t {
            // Execute the query using the C++ API
            auto table = self.query(sql);
            // Create a reader for the table
            auto reader = std::make_shared<arrow::TableBatchReader>(*table);
            // Export the reader to a C stream struct and return the pointer address
            return cpp_reader_to_julia_stream_ptr(reader);
        })
        
        .method("list_datasets", &crosslink::CrossLink::list_datasets)
        
        // === Streaming Methods ===
        
        // Push stream
        .method("push_stream", [](crosslink::CrossLink& self, jlcxx::ArrayRef<uint8_t> schema_bytes,
                                  const std::string& name) -> jl_value_t* {
            // Import C++ Schema from Julia C data bytes
            auto schema = julia_c_data_to_cpp_schema(schema_bytes);

            // Call the C++ push_stream method
            auto result_pair = self.push_stream(schema, name);
            const std::string& stream_id = result_pair.first;
            std::shared_ptr<crosslink::StreamWriter> writer = result_pair.second;

            // --- Create Julia Tuple (same as before) ---
            jl_value_t* jl_stream_id = jl_cstr_to_string(stream_id.c_str());
            jl_value_t* jl_writer = jlcxx::box<std::shared_ptr<crosslink::StreamWriter>>(writer);
            jl_value_t* string_type = (jl_value_t*)jl_string_type;
            jl_value_t* writer_type = jl_typeof(jl_writer);
            jl_value_t* types[] = {string_type, writer_type};
            jl_datatype_t* tuple_type = reinterpret_cast<jl_datatype_t*>(jl_apply_tuple_type_v(types, 2));
            jl_value_t* jl_tuple = jl_new_struct_uninit(tuple_type);
            jl_set_nth_field(jl_tuple, 0, jl_stream_id);
            jl_set_nth_field(jl_tuple, 1, jl_writer);
            return jl_tuple;
            // --- End Tuple Creation ---
        })
        
        // Pull stream
        .method("pull_stream", [](crosslink::CrossLink& self, const std::string& stream_id) -> uintptr_t {
            // Pull stream using the C++ API returns std::shared_ptr<crosslink::StreamReader>
            auto crosslink_reader = self.pull_stream(stream_id);

            // ****** ADAPTATION NEEDED HERE ******
            // We need to adapt the crosslink::StreamReader interface to the
            // arrow::RecordBatchReader interface so we can use ExportRecordBatchReader.
            // This requires creating a wrapper class.

            // Define a wrapper class that inherits from arrow::RecordBatchReader
            class CrossLinkReaderWrapper : public arrow::RecordBatchReader {
            public:
                CrossLinkReaderWrapper(std::shared_ptr<crosslink::StreamReader> reader)
                    : reader_(reader), schema_(reader->schema()) {}

                std::shared_ptr<arrow::Schema> schema() const override {
                    return schema_;
                }

                arrow::Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
                    // Handle potential blocking and GIL release if necessary, although
                    // this C++ callback might not be subject to GIL issues directly.
                    try {
                         auto next_batch = reader_->read_next_batch();
                         *batch = next_batch; // Assign shared_ptr
                         if (!next_batch) {
                             // End of stream signaled by nullptr in Arrow API
                             *batch = nullptr;
                         }
                         return arrow::Status::OK();
                    } catch (const std::exception& e) {
                         return arrow::Status::IOError("CrossLinkReader read error: ", e.what());
                    } catch (...) {
                         return arrow::Status::UnknownError("Unknown error in CrossLinkReader");
                    }
                 }

                 // Ensure the underlying reader is closed when this wrapper is destroyed
                 // The shared_ptr ensures lifetime management. If the Julia side calls
                 // release correctly, this wrapper (and the captured reader_) will be destroyed.
                 // We might want to explicitly close the reader_ in the destructor too.
                 ~CrossLinkReaderWrapper() override {
                     if(reader_) {
                         // Maybe call reader_->close() here, although shared_ptr going
                         // out of scope should handle C++ resource cleanup.
                         // Closing explicitly might be safer depending on reader_'s impl.
                         try {
                              reader_->close();
                         } catch (...) { /* Ignore errors during cleanup */ }
                     }
                 }


            private:
                std::shared_ptr<crosslink::StreamReader> reader_;
                std::shared_ptr<arrow::Schema> schema_; // Cache schema
            };

            // Create an instance of the wrapper reader
            auto arrow_reader = std::make_shared<CrossLinkReaderWrapper>(crosslink_reader);

            // Export the *wrapper* reader using the helper
            return cpp_reader_to_julia_stream_ptr(arrow_reader);
        })
        
        // Notification methods
        .method("register_notification", [](crosslink::CrossLink& self, jl_function_t* callback) {
            auto wrapped_callback = [callback](const std::string& dataset_id, const std::string& event_type) {
                jl_value_t* args[2];
                args[0] = jl_cstr_to_string(dataset_id.c_str());
                args[1] = jl_cstr_to_string(event_type.c_str());
                JL_TRY {
                    jl_call(callback, args, 2);
                }
                JL_CATCH {
                    jl_value_t *e = jl_exception_occurred();
                    jl_printf(jl_stderr_stream(), "Julia error in notification callback: ");
                    jl_static_show(jl_stderr_stream(), e);
                    jl_printf(jl_stderr_stream(), "\n");
                    jl_exception_clear();
                }
            };
            return self.register_notification(wrapped_callback);
        })
        
        .method("unregister_notification", &crosslink::CrossLink::unregister_notification)
        .method("cleanup", &crosslink::CrossLink::cleanup);

    // Explicitly apply necessary STL type wrappers
    // This is crucial for the linker to find the StlWrappers instance
    jlcxx::stl::apply_stl<std::string>(mod);
    // If CrossLink::list_streams() returns std::vector<std::string>, add:
    // mod.apply_stl_wrappers<std::vector<std::string>>(); // Incorrect name
    // jlcxx::stl::apply_stl<std::vector<std::string>>(mod);
} 