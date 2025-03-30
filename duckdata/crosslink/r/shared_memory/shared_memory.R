#' Set up shared memory for an Arrow table
#' @param table Arrow table to share
#' @param dataset_id Dataset ID for naming
#' @param cl CrossLink connection
#' @return Shared memory information or NULL if not available
setup_shared_memory <- function(table, dataset_id, cl) {
  # Check if shared memory is available
  if (!.shared_memory_available) {
    if (cl$debug) message("Shared memory not available in this R environment")
    return(NULL)
  }
  
  tryCatch({
    if (!requireNamespace("arrow", quietly = TRUE)) {
      if (cl$debug) message("Arrow package not available for shared memory")
      return(NULL)
    }
    
    # Create a unique key for this shared memory block
    shm_name <- paste0("crosslink_", dataset_id)
    
    # In a real implementation, we would use proper shared memory
    # Since R doesn't have direct shared memory support like Python's multiprocessing.shared_memory,
    # we'll simulate it with files in a designated location
    
    # Get shared memory directory
    shm_dir <- tempdir()
    shm_file <- file.path(shm_dir, paste0(shm_name, ".arrow"))
    
    # Write Arrow data to the shared location
    arrow::write_arrow(table, shm_file)
    
    # Track this file for cleanup
    cl$shared_memory_files <- c(cl$shared_memory_files, shm_file)
    
    # Store in global registry for cross-instance access
    assign(shm_name, shm_file, envir = .crosslink_shared_memory_pool)
    
    # Return shared memory information
    return(list(
      shared_memory_key = shm_name,
      shared_memory_file = shm_file
    ))
  }, error = function(e) {
    if (cl$debug) message("Failed to set up shared memory: ", e$message)
    return(NULL)
  })
}

#' Get Arrow table from shared memory
#' @param shared_memory_key Shared memory key
#' @param cl CrossLink connection
#' @return Arrow table or NULL if failed
get_from_shared_memory <- function(shared_memory_key, cl) {
  # Handle NULL or missing shared memory key
  if (is.null(shared_memory_key) || missing(shared_memory_key)) {
    if (cl$debug) message("Shared memory key is NULL or missing, will fall back to standard queries")
    return(NULL)
  }
  
  if (!.shared_memory_available) {
    if (cl$debug) message("Shared memory not available, will fall back to standard queries")
    return(NULL)
  }
  
  tryCatch({
    if (!requireNamespace("arrow", quietly = TRUE)) {
      if (cl$debug) message("Arrow package not available, will fall back to standard queries")
      return(NULL)
    }
    
    # Check if the key exists in our registry
    if (exists(shared_memory_key, envir = .crosslink_shared_memory_pool)) {
      shm_file <- get(shared_memory_key, envir = .crosslink_shared_memory_pool)
      
      # Read the Arrow table from the file
      if (file.exists(shm_file)) {
        return(arrow::read_arrow(shm_file))
      } else {
        if (cl$debug) message("Shared memory file not found: ", shm_file, ", will fall back to standard queries")
      }
    } else {
      if (cl$debug) message("Shared memory key not found in registry: ", shared_memory_key, ", will fall back to standard queries")
    }
    
    # If not found, look in the temp directory
    shm_file <- file.path(tempdir(), paste0(shared_memory_key, ".arrow"))
    if (file.exists(shm_file)) {
      # Register for future use
      assign(shared_memory_key, shm_file, envir = .crosslink_shared_memory_pool)
      return(arrow::read_arrow(shm_file))
    } else {
      if (cl$debug) message("Shared memory file not found in temp dir: ", shm_file, ", will fall back to standard queries")
    }
    
    # If we get here, we couldn't find the shared memory
    return(NULL)
  }, error = function(e) {
    if (cl$debug) message("Failed to access shared memory: ", e$message, ", will fall back to standard queries")
    return(NULL)
  })
}

#' Fall back to DuckDB when shared memory fails
#' @param identifier Dataset identifier
#' @param cl CrossLink connection
#' @return Arrow table or NULL if failed
fallback_to_duckdb <- function(identifier, cl) {
  if (cl$debug) message("Falling back to DuckDB for identifier: ", identifier)
  
  tryCatch({
    if (!requireNamespace("DBI", quietly = TRUE) || 
        !requireNamespace("duckdb", quietly = TRUE) ||
        !requireNamespace("arrow", quietly = TRUE)) {
      if (cl$debug) message("Required packages not available for DuckDB fallback")
      return(NULL)
    }
    
    # Connect to DuckDB
    con <- DBI::dbConnect(duckdb::duckdb(), cl$db_path)
    on.exit(DBI::dbDisconnect(con, shutdown = TRUE))
    
    # Query the data
    # Safely handle identifiers with special characters
    safe_identifier <- gsub("[^a-zA-Z0-9_]", "", identifier)
    
    # Get all columns
    result <- DBI::dbGetQuery(con, paste0("SELECT * FROM ", safe_identifier))
    
    # Convert to Arrow table
    return(arrow::as_arrow_table(result))
  }, error = function(e) {
    if (cl$debug) message("Error in DuckDB fallback: ", e$message)
    return(NULL)
  })
}

#' Safely get data using shared memory with fallback to DuckDB
#' @param shared_memory_key Shared memory key
#' @param identifier Dataset identifier for fallback
#' @param cl CrossLink connection
#' @return Arrow table or NULL if all methods fail
safe_shared_memory_get <- function(shared_memory_key, identifier, cl) {
  # Try shared memory first
  result <- get_from_shared_memory(shared_memory_key, cl)
  
  # If shared memory access fails, fall back to DuckDB
  if (is.null(result)) {
    if (cl$debug) message("Shared memory access failed, trying DuckDB fallback")
    result <- fallback_to_duckdb(identifier, cl)
  }
  
  return(result)
}

#' @export
#' @title Clean up all shared memory used by CrossLink
#' @return Number of shared memory regions cleaned up
cleanup_all_shared_memory <- function() {
  if (!.shared_memory_available) {
    return(0)
  }
  
  # Get all shared memory objects from the pool
  count <- 0
  
  # In our simulation, these are just files
  for (name in ls(.crosslink_shared_memory_pool)) {
    shm_file <- get(name, envir = .crosslink_shared_memory_pool)
    
    # Try to remove the file
    if (file.exists(shm_file)) {
      tryCatch({
        unlink(shm_file)
        count <- count + 1
      }, error = function(e) {
        warning("Failed to clean up shared memory file: ", shm_file)
      })
    }
    
    # Remove from registry
    rm(list = name, envir = .crosslink_shared_memory_pool)
  }
  
  # Also look for any crosslink_*.arrow files in temp dir
  temp_files <- list.files(tempdir(), pattern = "crosslink_.*\\.arrow$", full.names = TRUE)
  for (file in temp_files) {
    tryCatch({
      unlink(file)
      count <- count + 1
    }, error = function(e) {
      warning("Failed to clean up shared memory file: ", file)
    })
  }
  
  return(count)
} 