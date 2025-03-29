#' @export
#' @title Share an Arrow table directly
#' @param cl CrossLink connection
#' @param table Arrow table to share
#' @param name Optional name for the dataset
#' @param description Optional description
#' @param shared_memory Use shared memory for ultra-fast access
#' @param memory_mapped Use memory-mapped files for disk-based zero-copy
#' @param register_in_duckdb Register in DuckDB for SQL access
#' @param access_languages Languages that can access this dataset
#' @return Dataset ID
share_arrow_table <- function(cl, table, name = NULL, description = NULL,
                             shared_memory = TRUE, memory_mapped = TRUE,
                             register_in_duckdb = TRUE,
                             access_languages = c("python", "r", "julia", "cpp")) {
  # Ensure Arrow is available
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("The arrow package is required for share_arrow_table")
  }
  
  # Check that table is an Arrow Table
  if (!inherits(table, "Table")) {
    stop("The 'table' parameter must be an Arrow Table")
  }
  
  # Ensure metadata tables are initialized
  setup_metadata_tables(cl)
  
  # Generate dataset ID and name
  dataset_id <- uuid::UUIDgenerate()
  if (is.null(name)) {
    name <- paste0("arrow_direct_", substr(dataset_id, 1, 8))
  }
  
  # Initialize storage methods
  memory_map_path <- NULL
  shared_memory_info <- NULL
  table_name <- NULL
  
  # Set up memory-mapped file if requested
  if (memory_mapped) {
    tryCatch({
      # Create directory for memory-mapped files
      mmaps_dir <- file.path(dirname(cl$db_path), "crosslink_mmaps")
      if (!dir.exists(mmaps_dir)) {
        dir.create(mmaps_dir, recursive = TRUE)
      }
      
      # Create file path
      memory_map_path <- file.path(mmaps_dir, paste0(dataset_id, ".arrow"))
      
      # Write Arrow table to file
      arrow::write_arrow(table, memory_map_path)
      
      # Track for cleanup
      cl$arrow_files <- c(cl$arrow_files, memory_map_path)
      
      if (cl$debug) message("Wrote Arrow table to ", memory_map_path)
    }, error = function(e) {
      if (cl$debug) message("Failed to create memory-mapped file: ", e$message)
      memory_map_path <- NULL
    })
  }
  
  # Set up shared memory if requested
  if (shared_memory) {
    shared_memory_info <- setup_shared_memory(table, dataset_id, cl)
  }
  
  # Register in DuckDB if requested
  if (register_in_duckdb) {
    tryCatch({
      # Create a table name
      table_name <- paste0("arrow_", gsub("-", "_", dataset_id))
      
      # Create view using arrow file or direct registration
      if (!is.null(memory_map_path)) {
        # Create view from arrow file
        duckdb::duckdb_execute(cl$con, sprintf(
          "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
          table_name, memory_map_path
        ))
      } else {
        # Need to create a temporary file
        temp_file <- tempfile(fileext = ".arrow")
        arrow::write_arrow(table, temp_file)
        
        # Add to cleanup list
        cl$arrow_files <- c(cl$arrow_files, temp_file)
        
        # Create view
        duckdb::duckdb_execute(cl$con, sprintf(
          "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
          table_name, temp_file
        ))
      }
      
      if (cl$debug) message("Created DuckDB view: ", table_name)
    }, error = function(e) {
      if (cl$debug) message("Failed to register in DuckDB: ", e$message)
      table_name <- NULL
    })
  }
  
  # Create schema dictionary
  schema_dict <- list(
    columns = names(table),
    dtypes = sapply(names(table), function(col) {
      as.character(table$schema$GetFieldByName(col)$type)
    }, USE.NAMES = TRUE)
  )
  
  # Serialize Arrow schema
  arrow_schema <- list(
    schema = as.character(table$schema),
    serialized = NULL,  # Not available directly in R Arrow
    metadata = if (length(table$schema$metadata) > 0) {
      table$schema$metadata
    } else {
      list()
    }
  )
  
  # Calculate schema hash
  schema_hash <- digest::digest(jsonlite::toJSON(schema_dict, auto_unbox = TRUE), algo = "md5")
  
  # Insert metadata
  duckdb::duckdb_execute(cl$con,
    "INSERT INTO crosslink_metadata (
      id, name, source_language, created_at, updated_at, description,
      schema, table_name, arrow_data, version, current_version,
      schema_hash, access_languages, memory_map_path, shared_memory_key, arrow_schema
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    list(
      dataset_id, name, "r", Sys.time(), Sys.time(),
      if(is.null(description)) "Direct Arrow table" else description,
      jsonlite::toJSON(schema_dict, auto_unbox = TRUE), 
      table_name, TRUE, 1, TRUE,
      schema_hash,
      jsonlite::toJSON(access_languages, auto_unbox = TRUE),
      memory_map_path,
      if(!is.null(shared_memory_info)) shared_memory_info$shared_memory_key else NULL,
      jsonlite::toJSON(arrow_schema, auto_unbox = TRUE)
    )
  )
  
  # Store in cache
  metadata <- list(
    id = dataset_id,
    name = name,
    table_name = table_name,
    schema = schema_dict,
    source_language = "r",
    arrow_data = TRUE,
    memory_map_path = memory_map_path,
    shared_memory_key = if(!is.null(shared_memory_info)) shared_memory_info$shared_memory_key else NULL,
    arrow_schema = arrow_schema
  )
  cl$metadata_cache[[dataset_id]] <- metadata
  cl$metadata_cache[[name]] <- metadata
  
  # Log access
  if (cl$debug) {
    log_access(cl, dataset_id, "write", "direct_arrow")
  }
  
  return(dataset_id)
}

#' @export
#' @title Get an Arrow table directly
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @return Arrow table
get_arrow_table <- function(cl, identifier) {
  # Ensure Arrow is available
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("The arrow package is required for get_arrow_table")
  }
  
  # Get metadata
  metadata <- cl$metadata_cache[[identifier]]
  if (is.null(metadata)) {
    metadata <- get_dataset_metadata(cl, identifier)
    if (is.null(metadata)) {
      stop("Dataset with identifier '", identifier, "' not found")
    }
  }
  
  # Try shared memory first (fastest)
  shared_memory_key <- metadata$shared_memory_key
  if (!is.null(shared_memory_key) && .shared_memory_available) {
    arrow_table <- get_from_shared_memory(shared_memory_key, cl)
    if (!is.null(arrow_table)) {
      if (cl$debug) {
        log_access(cl, metadata$id, "read", "shared_memory")
      }
      return(arrow_table)
    }
  }
  
  # Try memory-mapped file
  memory_map_path <- metadata$memory_map_path
  if (!is.null(memory_map_path) && file.exists(memory_map_path)) {
    # Check if we already have this table in our cache
    if (exists(memory_map_path, envir = .crosslink_mmap_table_cache)) {
      arrow_table <- get(memory_map_path, envir = .crosslink_mmap_table_cache)
    } else {
      # Read from file
      arrow_table <- arrow::read_arrow(memory_map_path)
      # Cache for future use
      assign(memory_map_path, arrow_table, envir = .crosslink_mmap_table_cache)
    }
    
    if (cl$debug) {
      log_access(cl, metadata$id, "read", "memory_mapped")
    }
    return(arrow_table)
  }
  
  # Try through DuckDB + Arrow
  if (!is.null(metadata$table_name) && cl$arrow_available) {
    tryCatch({
      # Try to get as Arrow table via DuckDB
      result <- duckdb::duckdb_execute(cl$con, paste0("SELECT * FROM ", metadata$table_name))
      arrow_table <- duckdb::duckdb_fetch_arrow(result)
      
      if (cl$debug) {
        log_access(cl, metadata$id, "read", "duckdb_arrow")
      }
      return(arrow_table)
    }, error = function(e) {
      if (cl$debug) message("Failed to get Arrow table via DuckDB: ", e$message)
    })
  }
  
  # Last resort - get as data frame and convert
  result <- duckdb::duckdb_execute(cl$con, paste0("SELECT * FROM ", metadata$table_name))
  df <- duckdb::duckdb_fetch(result)
  arrow_table <- arrow::as_arrow_table(df)
  
  if (cl$debug) {
    log_access(cl, metadata$id, "read", "dataframe_convert")
  }
  
  return(arrow_table)
}

#' Create DuckDB view from Arrow table
#' @param cl CrossLink connection
#' @param table Arrow table
#' @param view_name Optional name for the view
#' @return View name
create_duckdb_view_from_arrow <- function(cl, table, view_name = NULL) {
  # Ensure Arrow is available
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("The arrow package is required for create_duckdb_view_from_arrow")
  }
  
  # Generate view name if not provided
  if (is.null(view_name)) {
    view_id <- gsub("-", "_", uuid::UUIDgenerate())
    view_name <- paste0("arrow_view_", view_id)
  }
  
  # Try to create a view to query this arrow table
  
  # Method 1: Try direct registration with duckdb
  success <- FALSE
  tryCatch({
    # First try Arrow extension approach
    ensure_arrow_extension(cl)
    
    # Create a temporary arrow file
    temp_file <- tempfile(fileext = ".arrow")
    arrow::write_arrow(table, temp_file)
    
    # Add to cleanup list
    cl$arrow_files <- c(cl$arrow_files, temp_file)
    
    # Create view using the arrow file
    duckdb::duckdb_execute(cl$con, sprintf(
      "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
      view_name, temp_file
    ))
    
    success <- TRUE
    if (cl$debug) message("Created view from arrow file: ", view_name)
  }, error = function(e) {
    if (cl$debug) message("Failed to create view via arrow file: ", e$message)
    # Fall through to next method
  })
  
  # Method 2: Convert to R data frame and create view
  if (!success) {
    tryCatch({
      # Convert arrow table to data frame
      df <- arrow::as.data.frame(table)
      
      # Register with duckdb
      duckdb::duckdb_register(cl$con, paste0("__temp_", view_name), df)
      
      # Create view
      duckdb::duckdb_execute(cl$con, sprintf(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM __temp_%s",
        view_name, view_name
      ))
      
      # Clean up registration
      tryCatch({
        duckdb::duckdb_unregister(cl$con, paste0("__temp_", view_name))
      }, error = function(e) {
        # Ignore unregister errors
      })
      
      success <- TRUE
      if (cl$debug) message("Created view from data frame: ", view_name)
    }, error = function(e) {
      if (cl$debug) message("Failed to create view via data frame: ", e$message)
      # No more methods to try
    })
  }
  
  if (!success) {
    stop("Failed to create DuckDB view from Arrow table")
  }
  
  return(view_name)
} 