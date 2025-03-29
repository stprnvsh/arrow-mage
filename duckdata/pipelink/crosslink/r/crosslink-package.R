#' CrossLink: Cross-language data sharing with true zero-copy optimization
#' 
#' This module provides R functions for working with the CrossLink system,
#' which enables seamless zero-copy data sharing between R, Python, Julia, and C++
#' 
#' @importFrom duckdb duckdb_connect duckdb_execute duckdb_fetch_arrow
#' @importFrom arrow read_arrow write_arrow Table
#' @importFrom jsonlite fromJSON toJSON
#' @importFrom uuid UUIDgenerate
#' @importFrom digest digest

# Global cache for shared memory regions
.crosslink_shared_memory_pool <- new.env()

# Global cache for memory-mapped arrow tables
.crosslink_mmap_table_cache <- new.env()

#' Check if shared memory package is available
#' @return TRUE if shared memory support is available
check_shared_memory_available <- function() {
  tryCatch({
    # Check if shared memory support is available
    # This could be either using Linux's shared memory or a specialized R package
    # Here we'll use a placeholder, but ideal implementation would use proper package for shared memory
    requireNamespace("bit64", quietly = TRUE)  # Using bit64 just to check a dependency
    return(TRUE)
  }, error = function(e) {
    return(FALSE)
  })
}

# Global variable to track if shared memory is available
.shared_memory_available <- check_shared_memory_available() 