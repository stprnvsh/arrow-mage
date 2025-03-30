#' CrossLink: Simple cross-language data sharing with true zero-copy optimization
#' 
#' This compatibility layer maintains backward compatibility with code that imports directly
#' from crosslink.R rather than using the new modular structure.
#' 
#' @importFrom duckdb duckdb_connect duckdb_execute duckdb_fetch_arrow
#' @importFrom arrow read_arrow write_arrow Table
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom uuid UUIDgenerate
#' @importFrom digest digest

# Import from core module
source(file.path(dirname(dirname(system.file(package = "crosslink"))), "r", "core", "core.R"))

# Import from data_operations module
source(file.path(dirname(dirname(system.file(package = "crosslink"))), "r", "data_operations", "data_operations.R"))

# Import from arrow_integration module
source(file.path(dirname(dirname(system.file(package = "crosslink"))), "r", "arrow_integration", "arrow_integration.R"))

# Import from shared_memory module
source(file.path(dirname(dirname(system.file(package = "crosslink"))), "r", "shared_memory", "shared_memory.R"))

# Import from utilities module
source(file.path(dirname(dirname(system.file(package = "crosslink"))), "r", "utilities", "utilities.R"))

# Import from metadata module
source(file.path(dirname(dirname(system.file(package = "crosslink"))), "r", "metadata", "metadata.R"))

# For backward compatibility - expose the important global variables
.crosslink_connection_pool <- get(".crosslink_connection_pool", envir = asNamespace("crosslink"))
.crosslink_shared_memory_pool <- get(".crosslink_shared_memory_pool", envir = asNamespace("crosslink"))
.crosslink_mmap_table_cache <- get(".crosslink_mmap_table_cache", envir = asNamespace("crosslink"))
.shared_memory_available <- get(".shared_memory_available", envir = asNamespace("crosslink"))

# Expose public functions for backward compatibility
# Core functions
crosslink_connect <- crosslink_connect

# Data operations
push <- push_data
pull <- pull_data
get_table_reference <- get_table_reference
register_external_table <- register_external_table
list_datasets <- list_datasets
query <- query_data

# Arrow integration
share_arrow_table <- share_arrow_table
get_arrow_table <- get_arrow_table
create_duckdb_view_from_arrow <- create_duckdb_view_from_arrow

# Shared memory functions
setup_shared_memory <- setup_shared_memory
get_from_shared_memory <- get_from_shared_memory
cleanup_all_shared_memory <- cleanup_all_shared_memory

# Utility functions
cleanup_all_arrow_files <- cleanup_all_arrow_files
is_file_safe_to_delete <- is_file_safe_to_delete

# Metadata functions
get_dataset_metadata <- get_dataset_metadata
update_dataset_metadata <- update_dataset_metadata

# === New Streaming Functions ===

#' Push a data stream to CrossLink
#' 
#' Start a new stream to send data batches.
#' 
#' @param con CrossLink connection
#' @param schema Arrow schema describing the structure of batches to be sent
#' @param name Optional name for the stream
#' @return A stream writer object with methods to write batches
#' @export
push_stream <- function(con, schema, name = "") {
  # Try to use C++ implementation if available
  if (inherits(con, "crosslink_cpp_connection") || 
      (is.list(con) && con$cpp_available && !is.null(con$cpp_instance))) {
    
    # Use the appropriate connection
    cl <- if (inherits(con, "crosslink_cpp_connection")) {
      con
    } else {
      con$cpp_instance
    }
    
    # Try C++ push_stream
    result <- push_stream_via_cpp(cl, schema, name)
    if (!is.null(result)) {
      return(result)
    }
  }
  
  # Fallback to pure R implementation (not implemented yet)
  stop("Pure R implementation of streaming is not available. C++ bindings are required.")
}

#' Pull a data stream from CrossLink
#' 
#' Connect to an existing stream to receive data batches.
#' 
#' @param con CrossLink connection
#' @param stream_id ID of the stream to connect to
#' @return A stream reader object with methods to read batches
#' @export
pull_stream <- function(con, stream_id) {
  # Try to use C++ implementation if available
  if (inherits(con, "crosslink_cpp_connection") || 
      (is.list(con) && con$cpp_available && !is.null(con$cpp_instance))) {
    
    # Use the appropriate connection
    cl <- if (inherits(con, "crosslink_cpp_connection")) {
      con
    } else {
      con$cpp_instance
    }
    
    # Try C++ pull_stream
    result <- pull_stream_via_cpp(cl, stream_id)
    if (!is.null(result)) {
      return(result)
    }
  }
  
  # Fallback to pure R implementation (not implemented yet)
  stop("Pure R implementation of streaming is not available. C++ bindings are required.")
} 