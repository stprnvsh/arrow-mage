#' Ensure metadata tables are set up
#' @param cl CrossLink connection
#' @return TRUE if successful
setup_metadata_tables <- function(cl) {
  # Skip if already initialized
  if (cl$tables_initialized) {
    return(TRUE)
  }
  
  # Create metadata tables
  tryCatch({
    # Main metadata table
    duckdb::duckdb_execute(cl$con, "
      CREATE TABLE IF NOT EXISTS crosslink_metadata (
        id TEXT PRIMARY KEY,
        name TEXT,
        source_language TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP,
        description TEXT,
        schema TEXT,
        table_name TEXT,
        arrow_data BOOLEAN DEFAULT FALSE,
        version INTEGER DEFAULT 1,
        current_version BOOLEAN DEFAULT TRUE,
        lineage TEXT,
        schema_hash TEXT,
        memory_map_path TEXT,
        shared_memory_key TEXT,
        arrow_schema TEXT,
        access_languages TEXT,
        memory_layout TEXT
      )
    ")
    
    # Access log table
    duckdb::duckdb_execute(cl$con, "
      CREATE TABLE IF NOT EXISTS crosslink_access_log (
        id TEXT PRIMARY KEY,
        dataset_id TEXT,
        language TEXT,
        operation TEXT,
        timestamp TIMESTAMP,
        access_method TEXT,
        success BOOLEAN,
        error_message TEXT
      )
    ")
    
    # Configure DuckDB for performance
    tryCatch({
      # Set memory limit to a reasonable amount (4GB or 80% of system memory)
      memory_limit <- "4GB"
      duckdb::duckdb_execute(cl$con, paste0("PRAGMA memory_limit='", memory_limit, "'"))
      
      # Set a reasonable number of threads
      # Leave 1 core for other processes
      num_cores <- max(1, parallel::detectCores() - 1)
      duckdb::duckdb_execute(cl$con, paste0("PRAGMA threads=", num_cores))
      
      # Enable object cache for better performance with repeated queries
      duckdb::duckdb_execute(cl$con, "PRAGMA enable_object_cache")
      
      # Additional performance optimizations
      duckdb::duckdb_execute(cl$con, "PRAGMA preserve_insertion_order=false")  # Allows reordering for memory efficiency
      duckdb::duckdb_execute(cl$con, "PRAGMA temp_directory=':memory:'")       # Use memory for temp files when possible
      duckdb::duckdb_execute(cl$con, "PRAGMA checkpoint_threshold='4GB'")      # Less frequent checkpoints
      duckdb::duckdb_execute(cl$con, "PRAGMA force_parallelism")               # Enable parallelism
      duckdb::duckdb_execute(cl$con, "PRAGMA cache_size=2048")                 # 2GB cache
      
    }, error = function(e) {
      if (cl$debug) warning("Failed to configure DuckDB performance settings: ", e$message)
    })
    
    # Mark as initialized
    cl$tables_initialized <- TRUE
    
    return(TRUE)
  }, error = function(e) {
    warning("Error setting up metadata tables: ", e$message)
    return(FALSE)
  })
}

#' @export
#' @title Get metadata for a dataset
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @return Metadata as a list or NULL if not found
get_dataset_metadata <- function(cl, identifier) {
  # Ensure tables are initialized
  setup_metadata_tables(cl)
  
  # Check cache first
  if (!is.null(cl$metadata_cache[[identifier]])) {
    return(cl$metadata_cache[[identifier]])
  }
  
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
  
  # Store in cache
  cl$metadata_cache[[identifier]] <- metadata
  if (identifier != metadata$id) {
    cl$metadata_cache[[metadata$id]] <- metadata
  }
  if (identifier != metadata$name) {
    cl$metadata_cache[[metadata$name]] <- metadata
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
  # Skip logging if debug mode is off
  if (!cl$debug) {
    return(TRUE)
  }
  
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