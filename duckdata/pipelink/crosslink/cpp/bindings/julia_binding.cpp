#include <jlcxx/jlcxx.hpp>
#include <jlcxx/stl.hpp>
#include "../include/crosslink.h"
#include <arrow/c/bridge.h>

// This binding uses the Arrow C Data Interface to transfer data between Julia and C++
// It requires the Arrow.jl package to be installed in Julia

// Forward declare the Arrow C Data Interface structures if not already included
extern "C" {
  struct ArrowArray;
  struct ArrowSchema;
}

// Helper functions would be needed to convert between C++ Arrow tables and Julia Arrow tables
// This is a simplified version for illustration purposes

JLCXX_MODULE define_julia_module(jlcxx::Module& mod) {
    // Define the CrossLink class
    mod.add_type<crosslink::CrossLink>("CrossLinkCpp")
        .constructor<const std::string&, bool>()
        
        // Push method - convert Julia Arrow table to C++ Arrow table via C Data Interface
        .method("push", [](crosslink::CrossLink& self, jlcxx::ArrayRef<uint8_t> arrow_c_data, 
                           const std::string& name, const std::string& description) {
            // arrow_c_data should contain serialized ArrowArray and ArrowSchema C structs
            // from Arrow.jl's to_c() function
            
            if (arrow_c_data.size() < sizeof(struct ArrowArray) + sizeof(struct ArrowSchema)) {
                throw std::runtime_error("Invalid Arrow C data format");
            }
            
            // Extract ArrowArray and ArrowSchema from the byte array
            struct ArrowArray* c_array = reinterpret_cast<struct ArrowArray*>(arrow_c_data.data());
            struct ArrowSchema* c_schema = reinterpret_cast<struct ArrowSchema*>(
                arrow_c_data.data() + sizeof(struct ArrowArray));
            
            // Convert C structs to C++ Arrow Table
            auto maybe_array = arrow::ImportArray(c_array, c_schema);
            if (!maybe_array.ok()) {
                throw std::runtime_error("Failed to import Arrow array: " + 
                                       maybe_array.status().ToString());
            }
            
            // Create table from array
            auto array = maybe_array.ValueUnsafe();
            auto schema = arrow::schema({
                arrow::field("data", array->type())
            });
            auto table = arrow::Table::Make(schema, {array});
            
            // Push the table to CrossLink
            return self.push(table, name, description);
        })
        
        // Pull method - get Arrow table and convert to C Data Interface for Julia
        .method("pull", [](crosslink::CrossLink& self, const std::string& identifier) {
            // Pull the table using the C++ API
            auto table = self.pull(identifier);
            
            // Allocate memory for ArrowArray and ArrowSchema C structs
            struct ArrowArray* c_array = new ArrowArray();
            struct ArrowSchema* c_schema = new ArrowSchema();
            
            // Export table to C Data Interface
            // For simplicity, we'll just export the first column as an array
            // In a full implementation, we'd handle full table conversion
            arrow::Status status;
            if (table->num_columns() > 0) {
                // Get the first chunk from the chunked array
                auto chunked_array = table->column(0);
                if (chunked_array->num_chunks() > 0) {
                    auto array_chunk = chunked_array->chunk(0);
                    status = arrow::ExportArray(*array_chunk, c_array, c_schema);
                } else {
                    throw std::runtime_error("Column has no chunks to export");
                }
            } else {
                throw std::runtime_error("Empty table has no columns to export");
            }
            
            if (!status.ok()) {
                delete c_array;
                delete c_schema;
                throw std::runtime_error("Failed to export Arrow array: " + status.ToString());
            }
            
            // Serialize ArrowArray and ArrowSchema to a Julia byte array
            size_t total_size = sizeof(struct ArrowArray) + sizeof(struct ArrowSchema);
            jlcxx::Array<uint8_t> result(total_size);
            
            // Create a temporary buffer and copy the data there
            std::vector<uint8_t> buffer(total_size);
            std::memcpy(buffer.data(), c_array, sizeof(struct ArrowArray));
            std::memcpy(buffer.data() + sizeof(struct ArrowArray), c_schema, sizeof(struct ArrowSchema));
            
            // Copy to Julia array
            for (size_t i = 0; i < total_size; ++i) {
                result.push_back(buffer[i]);
            }
            
            // Note: In a real implementation, we would need to ensure memory is properly 
            // cleaned up when Julia is done with the data
            
            return result;
        })
        
        // Query method - similar to pull but starts with a SQL query
        .method("query", [](crosslink::CrossLink& self, const std::string& sql) {
            // Execute the query using the C++ API
            auto table = self.query(sql);
            
            // Same conversion as pull method
            struct ArrowArray* c_array = new ArrowArray();
            struct ArrowSchema* c_schema = new ArrowSchema();
            
            arrow::Status status;
            if (table->num_columns() > 0) {
                // Get the first chunk from the chunked array
                auto chunked_array = table->column(0);
                if (chunked_array->num_chunks() > 0) {
                    auto array_chunk = chunked_array->chunk(0);
                    status = arrow::ExportArray(*array_chunk, c_array, c_schema);
                } else {
                    throw std::runtime_error("Column has no chunks to export");
                }
            } else {
                throw std::runtime_error("Empty table has no columns to export");
            }
            
            if (!status.ok()) {
                delete c_array;
                delete c_schema;
                throw std::runtime_error("Failed to export Arrow array: " + status.ToString());
            }
            
            size_t total_size = sizeof(struct ArrowArray) + sizeof(struct ArrowSchema);
            jlcxx::Array<uint8_t> result(total_size);
            
            // Create a temporary buffer and copy the data there
            std::vector<uint8_t> buffer(total_size);
            std::memcpy(buffer.data(), c_array, sizeof(struct ArrowArray));
            std::memcpy(buffer.data() + sizeof(struct ArrowArray), c_schema, sizeof(struct ArrowSchema));
            
            // Copy to Julia array
            for (size_t i = 0; i < total_size; ++i) {
                result.push_back(buffer[i]);
            }
            
            return result;
        })
        
        .method("list_datasets", &crosslink::CrossLink::list_datasets)
        
        // Improved notification callback
        .method("register_notification", [](crosslink::CrossLink& self, jl_function_t* callback) {
            // Create a Julia callback wrapper
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
} 