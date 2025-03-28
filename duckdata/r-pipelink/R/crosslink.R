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

#' @export
#' @title Connect to a CrossLink database
#' @param db_path Path to the DuckDB database file
#' @return A CrossLink connection object
crosslink_connect <- function(db_path = "crosslink.duckdb") {
  con <- duckdb::duckdb_connect(db_path, read_only = FALSE)
  
  # Check Arrow availability
  arrow_available <- tryCatch({
    duckdb::duckdb_execute(con, "SELECT arrow_version()")
    TRUE
  }, error = function(e) {
    message("Arrow extension not available: ", e$message)
    FALSE
  })
  
  # Create the connection object
  cl <- list(
    con = con,
    db_path = db_path,
    arrow_available = arrow_available,
    attached_databases = list()
  )
  
  class(cl) <- "crosslink_connection"
  
  return(cl)
}

#' @export
#' @title Get metadata for a dataset
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @return Metadata as a list or NULL if not found
get_dataset_metadata <- function(cl, identifier) {
  res <- duckdb::duckdb_execute(cl$con, 
    "SELECT * FROM crosslink_metadata WHERE id = ? OR name = ?", 
    list(identifier, identifier)
  )
  
  df <- duckdb::duckdb_fetch(res)
  
  if (nrow(df) == 0) {
    return(NULL)
  }
  
  # Convert to a list
  metadata <- as.list(df[1, ])
  
  # Parse JSON fields
  json_fields <- c("schema", "arrow_schema", "access_languages", "memory_layout")
  for (field in json_fields) {
    if (!is.na(metadata[[field]])) {
      metadata[[field]] <- jsonlite::fromJSON(metadata[[field]])
    }
  }
  
  return(metadata)
}

#' @export
#' @title Log dataset access
#' @param cl CrossLink connection
#' @param dataset_id Dataset ID
#' @param operation Operation (read, write, etc.)
#' @param access_method Access method (copy, zero-copy, view)
#' @param success Whether access was successful
#' @param error_message Error message if access failed
#' @return TRUE if successful
log_access <- function(cl, dataset_id, operation, access_method, 
                       success = TRUE, error_message = NULL) {
  log_id <- uuid::UUIDgenerate()
  
  duckdb::duckdb_execute(cl$con, 
    "INSERT INTO crosslink_access_log (
      id, dataset_id, language, operation, timestamp, 
      access_method, success, error_message
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
    list(
      log_id, dataset_id, "r", operation, Sys.time(),
      access_method, success, error_message
    )
  )
  
  return(TRUE)
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
#' @title Access a dataset with zero-copy when possible
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @param direct_access Whether to use direct DuckDB query access (TRUE) or data frame conversion (FALSE)
#' @return Either a direct DuckDB query result or a data frame
access_dataset <- function(cl, identifier, direct_access = TRUE) {
  # Get metadata
  metadata <- get_dataset_metadata(cl, identifier)
  
  if (is.null(metadata)) {
    stop("Dataset with identifier '", identifier, "' not found")
  }
  
  # Get table name
  table_name <- metadata$table_name
  
  if (direct_access) {
    # Create a lazy query object that can be further processed
    # This doesn't fetch the data yet, providing true zero-copy behavior
    log_access(cl, metadata$id, "read", "zero-copy")
    return(duckdb::duckdb_prepare(cl$con, paste0("SELECT * FROM ", table_name)))
  } else {
    # Return as data frame (will copy data)
    log_access(cl, metadata$id, "read", "copy")
    result <- duckdb::duckdb_execute(cl$con, paste0("SELECT * FROM ", table_name))
    return(duckdb::duckdb_fetch(result))
  }
}

#' @export
#' @title Register an external table without copying data
#' @param cl CrossLink connection
#' @param external_db_path Path to the external DuckDB database
#' @param external_table_name Name of the table in the external database
#' @param name Name for the dataset (optional)
#' @param description Description for the dataset (optional)
#' @return Dataset ID
register_external_table <- function(cl, external_db_path, external_table_name, 
                                   name = NULL, description = NULL) {
  # Generate dataset ID and name if needed
  dataset_id <- uuid::UUIDgenerate()
  if (is.null(name)) {
    name <- paste0("ext_", basename(external_db_path), "_", external_table_name)
  }
  
  # Normalize path
  external_db_path <- normalizePath(external_db_path)
  
  # Create attach name
  attach_name <- paste0("ext_", gsub("-", "_", dataset_id))
  
  tryCatch({
    # Attach the external database
    duckdb::duckdb_execute(cl$con, paste0("ATTACH DATABASE '", external_db_path, "' AS ", attach_name))
    
    # Add to attached databases list
    cl$attached_databases[[attach_name]] <- external_db_path
    
    # Get schema information
    schema_df <- duckdb::duckdb_execute(cl$con, 
      paste0("DESCRIBE ", attach_name, ".", external_table_name))
    schema_df <- duckdb::duckdb_fetch(schema_df)
    
    # Create schema dictionary
    schema_dict <- list(
      columns = schema_df$column_name,
      dtypes = setNames(schema_df$column_type, schema_df$column_name)
    )
    
    # Create view to external table
    view_name <- paste0("ext_view_", gsub("-", "_", dataset_id))
    duckdb::duckdb_execute(cl$con, 
      paste0("CREATE VIEW ", view_name, " AS SELECT * FROM ", 
             attach_name, ".", external_table_name))
    
    # Insert metadata
    duckdb::duckdb_execute(cl$con,
      "INSERT INTO crosslink_metadata (
        id, name, source_language, created_at, updated_at, description,
        schema, table_name, arrow_data, version, current_version,
        schema_hash, access_languages
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
      list(
        dataset_id, name, "external", Sys.time(), Sys.time(),
        if(is.null(description)) paste0("External table reference to ", external_db_path, ":", external_table_name) else description,
        jsonlite::toJSON(schema_dict, auto_unbox = TRUE), 
        view_name, FALSE, 1, TRUE,
        digest::digest(jsonlite::toJSON(schema_dict, auto_unbox = TRUE), algo = "md5"),
        jsonlite::toJSON(c("python", "r", "julia", "cpp"), auto_unbox = TRUE)
      )
    )
    
    # Log the operation
    log_access(cl, dataset_id, "reference", "external_table")
    
    return(dataset_id)
  }, error = function(e) {
    warning("Failed to register external table: ", e$message)
    # Try to detach the database
    tryCatch(
      duckdb::duckdb_execute(cl$con, paste0("DETACH DATABASE ", attach_name)),
      error = function(e) NULL
    )
    stop(e$message)
  })
}

#' @export
#' @title Pull data from CrossLink with zero-copy optimization when possible
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @param zero_copy Whether to use zero-copy optimization if available
#' @return Data frame or direct DuckDB query result
pull_data <- function(cl, identifier, zero_copy = TRUE) {
  if (zero_copy) {
    return(access_dataset(cl, identifier, direct_access = TRUE))
  } else {
    return(access_dataset(cl, identifier, direct_access = FALSE))
  }
}

#' @export
#' @title Push data to CrossLink
#' @param cl CrossLink connection
#' @param data Data frame to push
#' @param name Name for the dataset (optional)
#' @param description Description for the dataset (optional)
#' @param enable_zero_copy Enable zero-copy access (default: TRUE)
#' @return Dataset ID
push_data <- function(cl, data, name = NULL, description = NULL, enable_zero_copy = TRUE) {
  # Generate dataset ID and name if not provided
  dataset_id <- uuid::UUIDgenerate()
  
  if (is.null(name)) {
    name <- paste0("dataset_", as.integer(Sys.time()))
  }
  
  # Create table name (sanitized from dataset name)
  table_name <- paste0("data_", gsub("[^a-zA-Z0-9_]", "_", tolower(name)))
  
  # Create schema dictionary
  schema_dict <- list(
    columns = names(data),
    dtypes = sapply(data, function(x) class(x)[1], USE.NAMES = TRUE)
  )
  
  # Create DuckDB table directly from data frame
  duckdb::duckdb_register(cl$con, data, table_name)
  
  # Insert metadata
  duckdb::duckdb_execute(cl$con,
    "INSERT INTO crosslink_metadata (
      id, name, source_language, created_at, updated_at, description,
      schema, table_name, arrow_data, version, current_version,
      schema_hash, access_languages
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    list(
      dataset_id, name, "r", Sys.time(), Sys.time(),
      if(is.null(description)) "" else description,
      jsonlite::toJSON(schema_dict, auto_unbox = TRUE), 
      table_name, cl$arrow_available, 1, TRUE,
      digest::digest(jsonlite::toJSON(schema_dict, auto_unbox = TRUE), algo = "md5"),
      jsonlite::toJSON(c("python", "r", "julia", "cpp"), auto_unbox = TRUE)
    )
  )
  
  # Log the operation
  log_access(cl, dataset_id, "write", if(enable_zero_copy) "zero-copy" else "copy")
  
  return(dataset_id)
}

#' @export
#' @title Run SQL query
#' @param cl CrossLink connection
#' @param sql SQL query string
#' @param return_data_frame Whether to return a data frame (TRUE) or query result (FALSE)
#' @return Data frame or query result
run_query <- function(cl, sql, return_data_frame = TRUE) {
  result <- duckdb::duckdb_execute(cl$con, sql)
  
  if (return_data_frame) {
    return(duckdb::duckdb_fetch(result))
  } else {
    return(result)
  }
}

#' @export
#' @title List datasets
#' @param cl CrossLink connection
#' @return Data frame with dataset information
list_datasets <- function(cl) {
  res <- duckdb::duckdb_execute(cl$con, "
    SELECT id, name, source_language, created_at, table_name, description, version 
    FROM crosslink_metadata
    WHERE current_version = TRUE
    ORDER BY updated_at DESC
  ")
  
  return(duckdb::duckdb_fetch(res))
}

#' @export
#' @title Close connection
#' @param cl CrossLink connection
#' @return NULL
close_connection <- function(cl) {
  # Detach any attached databases
  for (attach_name in names(cl$attached_databases)) {
    tryCatch(
      duckdb::duckdb_execute(cl$con, paste0("DETACH DATABASE ", attach_name)),
      error = function(e) NULL
    )
  }
  
  duckdb::duckdb_disconnect(cl$con)
  invisible(NULL)
} 