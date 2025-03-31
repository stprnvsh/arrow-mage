#' @export
#' @title Push data to CrossLink using C++ bindings
#' @description Pushes an R data frame or Arrow Table to CrossLink using the compiled C++ bindings.
#' @param cl The CrossLink connection object (specifically, the XPtr to the C++ CrossLink object)
#' obtained from `crosslink_connect()`.
#' @param data The R data frame or Arrow Table to push.
#' @param name Optional character string name for the dataset.
#' @param description Optional character string description for the dataset.
#' @return Character string dataset ID.
push_data <- function(cl, data, name = "", description = "") {
  
  # Input validation (optional but recommended)
  if (!inherits(cl, "externalptr")) {
    stop("Invalid 'cl' object. Expected an external pointer from crosslink_connect().")
  }
  if (!is.data.frame(data) && !inherits(data, "Table")) {
    stop("'data' must be a data frame or an Arrow Table.")
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("The 'arrow' package is required to push data.")
  }
  
  # Ensure data is an Arrow Table (C++ binding expects this)
  if (!inherits(data, "Table")) {
    arrow_table <- arrow::as_arrow_table(data)
  } else {
    arrow_table <- data
  }
  
  # Call the Rcpp-generated C++ function directly
  dataset_id <- crosslink_push(cl, arrow_table, 
                              as.character(name), 
                              as.character(description))
                              
  return(dataset_id)
}

#' @export
#' @title Pull data from CrossLink using C++ bindings
#' @description Pulls a dataset from CrossLink using the compiled C++ bindings,
#' returning it as an Arrow Table or an R data frame.
#' @param cl The CrossLink connection object (specifically, the XPtr to the C++ CrossLink object)
#' obtained from `crosslink_connect()`.
#' @param identifier Character string dataset ID or name.
#' @param to_arrow Logical, if `TRUE`, returns an Arrow Table. If `FALSE` (default),
#' returns an R data frame.
#' @return An Arrow Table or an R data frame, depending on the `to_arrow` argument.
pull_data <- function(cl, identifier, to_arrow = FALSE) {

  # Input validation (optional but recommended)
  if (!inherits(cl, "externalptr")) {
    stop("Invalid 'cl' object. Expected an external pointer from crosslink_connect().")
  }
  if (!is.character(identifier) || length(identifier) != 1) {
    stop("'identifier' must be a single character string.")
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("The 'arrow' package is required to pull data.")
  }
  
  # Call the Rcpp-generated C++ function directly
  # This returns an R object wrapping an Arrow Table
  arrow_table <- crosslink_pull(cl, as.character(identifier))
  
  if (is.null(arrow_table)) {
      # The C++ function might return NULL if the identifier is not found
      stop("Dataset with identifier '", identifier, "' not found or failed to pull.")
  }
  
  # Convert to data frame if requested
  if (!to_arrow) {
    # Use arrow::as.data.frame for conversion
    result <- arrow::as.data.frame(arrow_table)
  } else {
    result <- arrow_table
  }
  
  return(result)
}

#' @export
#' @title Get a direct table reference without copying data
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @return A reference object containing database and table information
get_table_reference <- function(cl, identifier) {
  # Get metadata
  metadata <- get_dataset_metadata(cl, identifier)
  
  if (is.null(metadata)) {
    stop("Dataset with identifier '", identifier, "' not found")
  }
  
  # Log access as zero-copy
  log_access(cl, metadata$id, "reference", "zero-copy")
  
  # Return reference info
  return(list(
    database_path = normalizePath(cl$db_path),
    table_name = metadata$table_name,
    schema = metadata$schema,
    dataset_id = metadata$id,
    access_method = "direct_table_reference"
  ))
}

#' @export
#' @title Register an external table from another DuckDB database
#' @param cl CrossLink connection
#' @param external_db_path Path to external DuckDB database
#' @param external_table_name Table name in external database
#' @param name Optional name for the dataset
#' @param description Optional description
#' @return Dataset ID
register_external_table <- function(cl, external_db_path, external_table_name, name = NULL, description = NULL) {
  # Generate dataset ID and name if not provided
  dataset_id <- uuid::UUIDgenerate()
  if (is.null(name)) {
    name <- paste0("ext_", basename(external_db_path), "_", external_table_name)
  }
  
  # Ensure metadata tables are initialized
  setup_metadata_tables(cl)
  
  # Use absolute path for external database
  external_db_path <- normalizePath(external_db_path, mustWork = TRUE)
  attach_name <- paste0("ext_", gsub("-", "_", dataset_id))
  
  tryCatch({
    # Attach the external database
    duckdb::duckdb_execute(cl$con, sprintf("ATTACH DATABASE '%s' AS %s", external_db_path, attach_name))
    
    # Get schema information
    schema_df <- duckdb::duckdb_execute(cl$con, 
                                      sprintf("DESCRIBE %s.%s", attach_name, external_table_name))
    schema_df <- duckdb::duckdb_fetch(schema_df)
    
    # Create schema dictionary
    schema_dict <- list(
      columns = schema_df$column_name,
      dtypes = setNames(schema_df$column_type, schema_df$column_name)
    )
    
    # Create a view to the external table
    view_name <- paste0("ext_view_", gsub("-", "_", dataset_id))
    duckdb::duckdb_execute(cl$con, 
                         sprintf("CREATE VIEW %s AS SELECT * FROM %s.%s", 
                                view_name, attach_name, external_table_name))
    
    # Add to attached databases list for cleanup on close
    cl$attached_databases[[attach_name]] <- external_db_path
    
    # Store metadata
    stmt <- duckdb::duckdb_prepare(cl$con,
      "INSERT INTO crosslink_metadata (
        id, name, source_language, created_at, updated_at, description,
        schema, table_name, arrow_data, version, current_version,
        schema_hash, access_languages
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
    )
    
    duckdb::duckdb_execute(stmt, list(
      dataset_id, name, "external", Sys.time(), Sys.time(),
      if(is.null(description)) paste0("External table from ", external_db_path, ":", external_table_name) else description,
      jsonlite::toJSON(schema_dict, auto_unbox = TRUE),
      view_name, FALSE, 1, TRUE,
      digest::digest(jsonlite::toJSON(schema_dict, auto_unbox = TRUE), algo = "md5"),
      jsonlite::toJSON(c("python", "r", "julia", "cpp"), auto_unbox = TRUE)
    ))
    
    # Log operation
    if (cl$debug) {
      log_access(cl, dataset_id, "reference", "external_table")
    }
    
    return(dataset_id)
  }, error = function(e) {
    # Try to detach if we got as far as attaching
    tryCatch(
      duckdb::duckdb_execute(cl$con, paste0("DETACH DATABASE ", attach_name)),
      error = function(detach_error) { }
    )
    stop("Failed to register external table: ", e$message)
  })
}

#' @export
#' @title List datasets using C++ bindings
#' @description Lists the available datasets registered in CrossLink using the C++ bindings.
#' @param cl The CrossLink connection object (specifically, the XPtr to the C++ CrossLink object)
#' obtained from `crosslink_connect()`.
#' @return A character vector containing the identifiers (IDs or names) of the available datasets.
list_datasets <- function(cl) {

  # Input validation (optional but recommended)
  if (!inherits(cl, "externalptr")) {
    stop("Invalid 'cl' object. Expected an external pointer from crosslink_connect().")
  }
  
  # Call the Rcpp-generated C++ function directly
  # This returns a std::vector<std::string>, which Rcpp converts to a character vector
  dataset_list <- crosslink_list_datasets(cl)
  
  return(dataset_list)
}

#' @export
#' @title Execute a SQL query using C++ bindings
#' @description Executes a SQL query against the underlying DuckDB database using the
#' compiled C++ bindings, returning an Arrow Table or an R data frame.
#' @param cl The CrossLink connection object (specifically, the XPtr to the C++ CrossLink object)
#' obtained from `crosslink_connect()`.
#' @param sql Character string SQL query to execute.
#' @param to_arrow Logical, if `TRUE`, returns an Arrow Table. If `FALSE` (default),
#' returns an R data frame.
#' @return An Arrow Table or an R data frame, depending on the `to_arrow` argument.
query <- function(cl, sql, to_arrow = FALSE) {

  # Input validation (optional but recommended)
  if (!inherits(cl, "externalptr")) {
    stop("Invalid 'cl' object. Expected an external pointer from crosslink_connect().")
  }
  if (!is.character(sql) || length(sql) != 1) {
    stop("'sql' must be a single character string.")
  }
  if (!requireNamespace("arrow", quietly = TRUE)) {
      stop("The 'arrow' package is required to execute queries.")
  }
  
  # Call the Rcpp-generated C++ function directly
  # This returns an R object wrapping an Arrow Table
  arrow_table <- crosslink_query(cl, as.character(sql))
  
  if (is.null(arrow_table)) {
      # The C++ function might return NULL on error or empty result
      # Consider returning an empty data frame/table instead of stopping?
      # For now, let's assume NULL means error or truly empty result is intended.
      warning("Query returned NULL. This might indicate an error or an empty result set.")
      # Return an empty Arrow Table or data frame based on 'to_arrow'
      if (!to_arrow) {
         # Need schema for empty data frame - tricky without result.
         # Returning NULL might be better if C++ can't provide schema.
         # Let's return NULL for now, assuming C++ handles empty results properly.
         return(NULL) 
      } else {
         # Similarly, returning NULL might be the only option for Arrow Table
         return(NULL) 
      }
  }
  
  # Convert to data frame if requested
  if (!to_arrow) {
    # Use arrow::as.data.frame for conversion
    result <- arrow::as.data.frame(arrow_table)
  } else {
    result <- arrow_table
  }
  
  return(result)
}

#' Ensure Arrow extension is loaded
#' @param cl CrossLink connection
#' @return TRUE if Arrow is available
ensure_arrow_extension <- function(cl) {
  # Skip if already loaded
  if (cl$arrow_loaded) {
    return(cl$arrow_available)
  }
  
  # Mark as checked
  cl$arrow_loaded <- TRUE
  
  # Check if Arrow is actually available
  if (!requireNamespace("arrow", quietly = TRUE)) {
    cl$arrow_available <- FALSE
    return(FALSE)
  }
  
  # Try to load Arrow extension in DuckDB
  arrow_available <- tryCatch({
    duckdb::duckdb_execute(cl$con, "SELECT arrow_version()")
    TRUE
  }, error = function(e) {
    tryCatch({
      # Try to install and load
      duckdb::duckdb_execute(cl$con, "INSTALL arrow")
      duckdb::duckdb_execute(cl$con, "LOAD arrow")
      TRUE
    }, error = function(e) {
      if (cl$debug) message("Arrow extension not available: ", e$message)
      FALSE
    })
  })
  
  cl$arrow_available <- arrow_available
  return(arrow_available)
}

#' @export
#' @title Close CrossLink Connection and Cleanup Resources
#' @description Explicitly cleans up the resources associated with a specific CrossLink C++ connection object.
#' @param cl The CrossLink connection object (specifically, the XPtr to the C++ CrossLink object)
#' obtained from `crosslink_connect()`.
#' @return Invisible NULL. Called for side effects.
close_connection <- function(cl) {
  # Input validation
  if (!inherits(cl, "externalptr")) {
    stop("Invalid 'cl' object. Expected an external pointer from crosslink_connect().")
  }

  # Call the Rcpp-generated C++ cleanup function
  crosslink_cleanup(cl)

  # Optional: Mark the pointer as invalid/cleaned up if possible?
  # Rcpp XPtrs don't have a built-in way to invalidate them from R side easily
  # The C++ destructor associated with the XPtr should handle final cleanup if
  # the R object is garbage collected, but explicit cleanup is better.

  invisible(NULL)
} 