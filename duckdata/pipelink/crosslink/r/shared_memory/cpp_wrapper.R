#' C++ bindings wrapper for CrossLink
#' 
#' This file provides an interface to the C++ implementation of CrossLink
#' for better cross-language interoperability and performance.

# Global variables to track C++ binding availability
.crosslink_cpp_available <- FALSE
.crosslink_cpp_bindings <- NULL

#' Check if C++ bindings are available
#' @return TRUE if C++ bindings are available
is_cpp_available <- function() {
  return(.crosslink_cpp_available)
}

#' Try to load C++ bindings from various locations
#' @return TRUE if bindings were successfully loaded
try_load_cpp_bindings <- function() {
  if (isTRUE(.crosslink_cpp_available)) {
    return(TRUE)
  }
  
  tryCatch({
    # First approach: try Rcpp::sourceCpp with the binding file
    package_dir <- system.file(package = "crosslink")
    project_dir <- dirname(dirname(package_dir))
    
    # Look for the compiled library in possible locations
    lib_paths <- c(
      file.path(project_dir, "r", "libs", "crosslink_r"),
      file.path(project_dir, "cpp", "build", "crosslink_r"),
      file.path(project_dir, "libs", "crosslink_r")
    )
    
    # Add platform-specific extensions
    if (.Platform$OS.type == "windows") {
      lib_paths <- paste0(lib_paths, c(".dll"))
    } else if (Sys.info()["sysname"] == "Darwin") {
      lib_paths <- paste0(lib_paths, c(".so", ".dylib"))
    } else {
      lib_paths <- paste0(lib_paths, c(".so"))
    }
    
    # Try each path
    for (lib_path in lib_paths) {
      if (file.exists(lib_path)) {
        # Use dyn.load to load the library
        dyn.load(lib_path)
        
        # Check if the functions are now available
        if (exists("crosslink_connect", mode = "function") &&
            exists("crosslink_push", mode = "function") &&
            exists("crosslink_pull", mode = "function")) {
          .crosslink_cpp_available <<- TRUE
          return(TRUE)
        }
      }
    }
    
    # Second approach: try Rcpp::sourceCpp with the binding file directly
    cpp_file <- file.path(project_dir, "cpp", "bindings", "r_binding.cpp")
    if (file.exists(cpp_file)) {
      Rcpp::sourceCpp(cpp_file)
      .crosslink_cpp_available <<- TRUE
      return(TRUE)
    }
    
    # If we get here, we failed to load the bindings
    message("C++ bindings not found in any expected location")
    return(FALSE)
  }, error = function(e) {
    message("Error loading C++ bindings: ", e$message)
    return(FALSE)
  })
}

# Try to load C++ bindings on package load
.onLoad <- function(libname, pkgname) {
  try_load_cpp_bindings()
}

#' CrossLink C++ connection
#' @export
#' @param db_path Path to the DuckDB database file
#' @param debug Enable debug mode with verbose logging
#' @param force_native Force using native R implementation even if C++ is available
#' @return A CrossLink C++ connection object
crosslink_cpp_connect <- function(db_path = "crosslink.duckdb", debug = FALSE, force_native = FALSE) {
  if (force_native || !try_load_cpp_bindings()) {
    if (debug) message("C++ bindings are not available, falling back to native R implementation.")
    return(NULL)
  }
  
  tryCatch({
    cl_ptr <- crosslink_connect(db_path, debug)
    
    # Create a connection object
    cl <- list(
      ptr = cl_ptr,
      db_path = db_path,
      debug = debug
    )
    
    class(cl) <- "crosslink_cpp_connection"
    return(cl)
  }, error = function(e) {
    if (debug) message("Error connecting via C++ bindings: ", e$message, ", falling back to native R implementation.")
    return(NULL)
  })
}

#' Push a data frame or Arrow table to CrossLink via C++ bindings
#' @export
#' @param cl CrossLink C++ connection
#' @param data Data frame or Arrow table to push
#' @param name Optional name for the dataset
#' @param description Optional description
#' @return Dataset ID or NULL if operation failed
push_via_cpp <- function(cl, data, name = "", description = "") {
  if (!inherits(cl, "crosslink_cpp_connection")) {
    if (is.list(cl) && cl$debug) message("Not a valid CrossLink C++ connection, falling back to native R implementation.")
    return(NULL)
  }
  
  tryCatch({
    # Convert data to Arrow table if needed
    if (!inherits(data, "Table")) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        if (cl$debug) message("The arrow package is required for C++ data sharing")
        return(NULL)
      }
      data <- arrow::as_arrow_table(data)
    }
    
    # Push data using C++ bindings
    return(crosslink_push(cl$ptr, data, name, description))
  }, error = function(e) {
    if (cl$debug) message("Error in C++ push operation: ", e$message, ", falling back to native R implementation.")
    return(NULL)
  })
}

#' Pull data from CrossLink via C++ bindings
#' @export
#' @param cl CrossLink C++ connection
#' @param identifier Dataset ID or name
#' @param to_data_frame Convert result to data frame (default TRUE)
#' @return Arrow table or data frame, or NULL if operation failed
pull_via_cpp <- function(cl, identifier, to_data_frame = TRUE) {
  if (!inherits(cl, "crosslink_cpp_connection")) {
    if (is.list(cl) && cl$debug) message("Not a valid CrossLink C++ connection, falling back to native R implementation.")
    return(NULL)
  }
  
  tryCatch({
    # Pull data using C++ bindings
    arrow_table <- crosslink_pull(cl$ptr, identifier)
    
    # Convert to data frame if requested
    if (to_data_frame) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        if (cl$debug) message("The arrow package is required for C++ data sharing")
        return(NULL)
      }
      return(arrow::as.data.frame(arrow_table))
    } else {
      return(arrow_table)
    }
  }, error = function(e) {
    if (cl$debug) message("Error in C++ pull operation: ", e$message, ", falling back to native R implementation.")
    return(NULL)
  })
}

#' Query CrossLink database via C++ bindings
#' @export
#' @param cl CrossLink C++ connection
#' @param sql SQL query
#' @param to_data_frame Convert result to data frame (default TRUE)
#' @return Arrow table or data frame with query results, or NULL if operation failed
query_via_cpp <- function(cl, sql, to_data_frame = TRUE) {
  if (!inherits(cl, "crosslink_cpp_connection")) {
    if (is.list(cl) && cl$debug) message("Not a valid CrossLink C++ connection, falling back to native R implementation.")
    return(NULL)
  }
  
  tryCatch({
    # Execute query using C++ bindings
    arrow_table <- crosslink_query(cl$ptr, sql)
    
    # Convert to data frame if requested
    if (to_data_frame) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        if (cl$debug) message("The arrow package is required for C++ data sharing")
        return(NULL)
      }
      return(arrow::as.data.frame(arrow_table))
    } else {
      return(arrow_table)
    }
  }, error = function(e) {
    if (cl$debug) message("Error in C++ query operation: ", e$message, ", falling back to native R implementation.")
    return(NULL)
  })
}

#' List datasets via C++ bindings
#' @export
#' @param cl CrossLink C++ connection
#' @return Vector of dataset IDs or NULL if operation failed
list_datasets_via_cpp <- function(cl) {
  if (!inherits(cl, "crosslink_cpp_connection")) {
    if (is.list(cl) && cl$debug) message("Not a valid CrossLink C++ connection, falling back to native R implementation.")
    return(NULL)
  }
  
  tryCatch({
    # List datasets using C++ bindings
    return(crosslink_list_datasets(cl$ptr))
  }, error = function(e) {
    if (cl$debug) message("Error in C++ list_datasets operation: ", e$message, ", falling back to native R implementation.")
    return(NULL)
  })
}

#' Register notification callback via C++ bindings
#' @export
#' @param cl CrossLink C++ connection
#' @param callback Function to call when data changes
#' @return Registration ID or NULL if operation failed
register_notification_via_cpp <- function(cl, callback) {
  if (!inherits(cl, "crosslink_cpp_connection")) {
    if (is.list(cl) && cl$debug) message("Not a valid CrossLink C++ connection, falling back to native R implementation.")
    return(NULL)
  }
  
  tryCatch({
    # Register callback using C++ bindings
    return(crosslink_register_notification(cl$ptr, callback))
  }, error = function(e) {
    if (cl$debug) message("Error in C++ register_notification operation: ", e$message, ", falling back to native R implementation.")
    return(NULL)
  })
}

#' Unregister notification callback via C++ bindings
#' @export
#' @param cl CrossLink C++ connection
#' @param registration_id Registration ID from register_notification_via_cpp
#' @return TRUE if successful, FALSE otherwise
unregister_notification_via_cpp <- function(cl, registration_id) {
  if (!inherits(cl, "crosslink_cpp_connection")) {
    if (is.list(cl) && cl$debug) message("Not a valid CrossLink C++ connection, falling back to native R implementation.")
    return(FALSE)
  }
  
  tryCatch({
    # Unregister callback using C++ bindings
    crosslink_unregister_notification(cl$ptr, registration_id)
    return(TRUE)
  }, error = function(e) {
    if (cl$debug) message("Error in C++ unregister_notification operation: ", e$message, ", falling back to native R implementation.")
    return(FALSE)
  })
}

#' Clean up resources via C++ bindings
#' @export
#' @param cl CrossLink C++ connection
#' @return TRUE if successful, FALSE otherwise
cleanup_via_cpp <- function(cl) {
  if (!inherits(cl, "crosslink_cpp_connection")) {
    if (is.list(cl) && cl$debug) message("Not a valid CrossLink C++ connection, falling back to native R implementation.")
    return(FALSE)
  }
  
  tryCatch({
    # Clean up using C++ bindings
    crosslink_cleanup(cl$ptr)
    return(TRUE)
  }, error = function(e) {
    if (cl$debug) message("Error in C++ cleanup operation: ", e$message, ", falling back to native R implementation.")
    return(FALSE)
  })
}

# Try to load C++ bindings
try_load_cpp_bindings() 