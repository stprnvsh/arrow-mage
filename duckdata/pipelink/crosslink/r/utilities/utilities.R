#' Check if a file is safe to delete
#' @param file_path Path to file
#' @return TRUE if file is safe to delete
is_file_safe_to_delete <- function(file_path) {
  if (!file.exists(file_path)) {
    return(TRUE)
  }
  
  # Try to open the file with write access
  tryCatch({
    con <- file(file_path, "r+b")
    close(con)
    return(TRUE)
  }, error = function(e) {
    # If we can't open the file, it might be in use
    return(FALSE)
  })
}

#' @export
#' @title Close CrossLink connection and clean up resources
#' @param cl CrossLink connection
#' @return TRUE if successful
close_connection <- function(cl) {
  if (is.null(cl) || !inherits(cl, "crosslink_connection")) {
    warning("Not a valid CrossLink connection")
    return(FALSE)
  }
  
  # Remove from connection pool
  abs_path <- normalizePath(cl$db_path, mustWork = FALSE)
  if (exists(abs_path, envir = .crosslink_connection_pool)) {
    rm(list = abs_path, envir = .crosslink_connection_pool)
  }
  
  # Clean up shared memory files
  if (length(cl$shared_memory_files) > 0) {
    for (file_path in cl$shared_memory_files) {
      tryCatch({
        if (file.exists(file_path) && is_file_safe_to_delete(file_path)) {
          unlink(file_path)
          if (cl$debug) message("Removed shared memory file: ", file_path)
        }
      }, error = function(e) {
        if (cl$debug) message("Failed to clean up shared memory file: ", file_path)
      })
    }
  }
  
  # Clean up Arrow files
  if (length(cl$arrow_files) > 0) {
    for (file_path in cl$arrow_files) {
      tryCatch({
        if (file.exists(file_path) && is_file_safe_to_delete(file_path)) {
          unlink(file_path)
          if (cl$debug) message("Removed Arrow file: ", file_path)
        }
      }, error = function(e) {
        if (cl$debug) message("Failed to clean up Arrow file: ", file_path)
      })
    }
  }
  
  # Clean up CrossLink mmaps directory
  mmaps_dir <- file.path(dirname(cl$db_path), "crosslink_mmaps")
  if (dir.exists(mmaps_dir)) {
    # Try to remove all .arrow files that are safe to delete
    arrow_files <- list.files(mmaps_dir, pattern = "\\.arrow$", full.names = TRUE)
    for (file_path in arrow_files) {
      tryCatch({
        if (is_file_safe_to_delete(file_path)) {
          unlink(file_path)
          if (cl$debug) message("Removed mmapped Arrow file: ", file_path)
        }
      }, error = function(e) {
        if (cl$debug) message("Failed to clean up mmapped Arrow file: ", file_path)
      })
    }
    
    # Try to remove the directory if it's empty
    if (length(list.files(mmaps_dir)) == 0) {
      tryCatch({
        unlink(mmaps_dir, recursive = TRUE)
      }, error = function(e) {
        # Directory might still be in use
      })
    }
  }
  
  # Detach any attached databases
  if (length(cl$attached_databases) > 0) {
    for (db_name in names(cl$attached_databases)) {
      tryCatch({
        duckdb::duckdb_execute(cl$con, paste0("DETACH DATABASE ", db_name))
        if (cl$debug) message("Detached database: ", db_name)
      }, error = function(e) {
        # Database might have been detached already
      })
    }
  }
  
  # Close the connection
  tryCatch({
    duckdb::duckdb_close(cl$con)
    cl$con <- NULL
    if (cl$debug) message("Closed DuckDB connection")
    return(TRUE)
  }, error = function(e) {
    warning("Error closing connection: ", e$message)
    return(FALSE)
  })
}

#' @export
#' @title Clean up all Arrow files in the CrossLink mmaps directory
#' @param db_path Path to database (to locate mmaps directory)
#' @param force Force deletion even if files appear to be in use
#' @return Number of files removed
cleanup_all_arrow_files <- function(db_path = "crosslink.duckdb", force = FALSE) {
  # Get the mmaps directory
  mmaps_dir <- file.path(dirname(db_path), "crosslink_mmaps")
  
  # Check if directory exists
  if (!dir.exists(mmaps_dir)) {
    return(0)
  }
  
  # Count of removed files
  removed_count <- 0
  
  # Remove all .arrow files
  arrow_files <- list.files(mmaps_dir, pattern = "\\.arrow$", full.names = TRUE)
  for (file_path in arrow_files) {
    tryCatch({
      if (force || is_file_safe_to_delete(file_path)) {
        unlink(file_path)
        removed_count <- removed_count + 1
      }
    }, error = function(e) {
      warning("Failed to remove Arrow file: ", file_path)
    })
  }
  
  # Try to remove the directory if it's empty
  if (length(list.files(mmaps_dir)) == 0) {
    tryCatch({
      unlink(mmaps_dir, recursive = TRUE)
    }, error = function(e) {
      # Directory might still be in use
    })
  }
  
  return(removed_count)
} 