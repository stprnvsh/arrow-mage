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

# Connection pool for CrossLink instances
.crosslink_connection_pool <- new.env()

#' @export
#' @title Connect to a CrossLink database
#' @param db_path Path to the DuckDB database file
#' @param debug Enable debug mode with verbose logging
#' @return A CrossLink connection object
crosslink_connect <- function(db_path = "crosslink.duckdb", debug = FALSE) {
  # Normalize path for consistent lookup
  abs_path <- normalizePath(db_path, mustWork = FALSE)
  
  # Check if we already have a connection for this database
  if (exists(abs_path, envir = .crosslink_connection_pool)) {
    return(get(abs_path, envir = .crosslink_connection_pool))
  }
  
  # Create directory if it doesn't exist
  db_dir <- dirname(db_path)
  if (!file.exists(db_dir) && db_dir != ".") {
    dir.create(db_dir, recursive = TRUE)
  }
  
  # Connect to database
  con <- duckdb::duckdb_connect(db_path, read_only = FALSE)
  
  # Check Arrow availability - but don't load yet (lazy initialization)
  arrow_available <- tryCatch({
    # Just check if the arrow package is available
    requireNamespace("arrow", quietly = TRUE)
  }, error = function(e) {
    if (debug) message("Arrow package not available: ", e$message)
    FALSE
  })
  
  # Create the connection object
  cl <- list(
    con = con,
    db_path = db_path,
    arrow_available = arrow_available,
    arrow_loaded = FALSE,
    attached_databases = list(),
    metadata_cache = list(),
    tables_initialized = FALSE,
    debug = debug
  )
  
  class(cl) <- "crosslink_connection"
  
  # Store in connection pool
  assign(abs_path, cl, envir = .crosslink_connection_pool)
  
  return(cl)
}

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
#' @title Pull data from CrossLink with zero-copy optimization when possible
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @param zero_copy Whether to use zero-copy optimization if available
#' @return Data frame
pull_data <- function(cl, identifier, zero_copy = TRUE) {
  # Ensure tables are initialized
  setup_metadata_tables(cl)
  
  # Try to get metadata from cache first for better performance
  metadata <- cl$metadata_cache[[identifier]]
  
  # If not in cache, get it from database
  if (is.null(metadata)) {
    metadata <- get_dataset_metadata(cl, identifier)
    
    if (is.null(metadata)) {
      stop("Dataset with identifier '", identifier, "' not found")
    }
  }
  
  # Check if we can use Arrow for zero-copy
  if (zero_copy && !cl$arrow_loaded) {
    cl$arrow_available <- ensure_arrow_extension(cl)
  }
  
  # First try memory-mapped Arrow path if available
  memory_map_path <- metadata$memory_map_path
  
  if (zero_copy && cl$arrow_available && !is.null(memory_map_path) && file.exists(memory_map_path)) {
    tryCatch({
      if (!requireNamespace("arrow", quietly = TRUE)) {
        if (cl$debug) message("Arrow package not available for memory mapping")
      } else {
        # Read directly from memory-mapped file
        arrow_table <- arrow::read_arrow(memory_map_path)
        
        # Convert to data frame with zero-copy if possible
        if ("zero_copy_only" %in% names(formals(arrow::as.data.frame.Table))) {
          result <- arrow::as.data.frame(arrow_table, zero_copy_only = TRUE)
        } else {
          result <- arrow::as.data.frame(arrow_table)
        }
        
        # Log access if debug mode is enabled
        if (cl$debug) {
          log_access(cl, metadata$id, "read", "zero-copy-mmap")
        }
        
        return(result)
      }
    }, error = function(e) {
      if (cl$debug) message("Memory-mapped file read failed, falling back: ", e$message)
      # Fall through to standard methods
    })
  }
  
  # Get table name
  table_name <- metadata$table_name
  
  # Use different strategies based on availability
  if (zero_copy && cl$arrow_available && requireNamespace("arrow", quietly = TRUE)) {
    # Use Arrow for zero-copy when possible
    tryCatch({
      # Try to use duckdb_fetch_arrow if available
      if (exists("duckdb_fetch_arrow", where = asNamespace("duckdb"), mode = "function")) {
        # Prepare and fetch using Arrow if available
        if (exists("duckdb_prepare", where = asNamespace("duckdb"), mode = "function")) {
          stmt <- duckdb::duckdb_prepare(cl$con, paste0("SELECT * FROM ", table_name))
          result <- duckdb::duckdb_fetch_arrow(stmt)
        } else {
          # Use execute with Arrow if prepare is not available
          result <- tryCatch({
            duckdb::duckdb_fetch_arrow(
              duckdb::duckdb_execute(cl$con, paste0("SELECT * FROM ", table_name))
            )
          }, error = function(e) {
            if (cl$debug) message("Arrow fetch through execute failed: ", e$message)
            NULL  # Return NULL to trigger fallback
          })
        }
        
        # If we successfully got an Arrow table, convert it
        if (!is.null(result)) {
          # Convert to data frame with zero_copy_only=TRUE if supported
          if (zero_copy) {
            if ("zero_copy_only" %in% names(formals(arrow::as.data.frame.Table))) {
              data <- arrow::as.data.frame(result, zero_copy_only = TRUE)
            } else {
              data <- arrow::as.data.frame(result)
            }
          } else {
            data <- arrow::as.data.frame(result)
          }
          
          # Log access if debug mode is enabled
          if (cl$debug) {
            log_access(cl, metadata$id, "read", "zero-copy-arrow")
          }
          
          return(data)
        }
      }
      # If we reach here, Arrow methods were not available or failed
      # Fall through to standard method
    }, error = function(e) {
      if (cl$debug) message("Arrow fetch failed, falling back to standard method: ", e$message)
      # Fall back to standard method if Arrow fails
    })
  }
  
  # Standard query - check if prepare is available
  tryCatch({
    if (exists("duckdb_prepare", where = asNamespace("duckdb"), mode = "function")) {
      # Use prepared statement for better performance
      stmt <- duckdb::duckdb_prepare(cl$con, paste0("SELECT * FROM ", table_name))
      result <- duckdb::duckdb_execute(stmt)
      data <- duckdb::duckdb_fetch(result)
    } else {
      # Fallback to direct execute for older versions
      result <- duckdb::duckdb_execute(cl$con, paste0("SELECT * FROM ", table_name))
      data <- duckdb::duckdb_fetch(result)
    }
    
    # Log access if debug mode is enabled
    if (cl$debug) {
      log_access(cl, metadata$id, "read", "copy")
    }
    
    return(data)
  }, error = function(e) {
    # Final fallback if all else fails
    stop("Failed to read data: ", e$message)
  })
}

#' @export
#' @title Push data to CrossLink
#' @param cl CrossLink connection
#' @param data Data frame to push
#' @param name Name for the dataset (optional)
#' @param description Description for the dataset (optional)
#' @param enable_zero_copy Enable zero-copy access (default: TRUE)
#' @param memory_mapped Use memory-mapped files for zero-copy (default: TRUE)
#' @return Dataset ID
push_data <- function(cl, data, name = NULL, description = NULL, 
                      enable_zero_copy = TRUE, memory_mapped = TRUE) {
  # Ensure tables are initialized
  setup_metadata_tables(cl)
  
  # Check if we need Arrow
  if (enable_zero_copy && !cl$arrow_loaded) {
    ensure_arrow_extension(cl)
  }
  
  # Generate dataset ID and name if not provided
  dataset_id <- uuid::UUIDgenerate()
  
  if (is.null(name)) {
    name <- paste0("dataset_", as.integer(Sys.time()))
  }
  
  # Create table name (sanitized from dataset name)
  table_name <- paste0("data_", gsub("[^a-zA-Z0-9_]", "_", tolower(name)))
  
  # Check if we can use Arrow for zero-copy
  memory_map_path <- NULL
  arrow_schema <- NULL
  arrow_table <- NULL
  
  if (enable_zero_copy && cl$arrow_available && memory_mapped) {
    # Try to convert data to Arrow table
    tryCatch({
      if (!requireNamespace("arrow", quietly = TRUE)) {
        if (cl$debug) message("Arrow package not available for memory mapping")
      } else {
        # Convert data frame to Arrow table
        arrow_table <- arrow::as_arrow_table(data)
        
        # Create directory for memory-mapped files if it doesn't exist
        mmaps_dir <- file.path(dirname(cl$db_path), "crosslink_mmaps")
        if (!dir.exists(mmaps_dir)) {
          dir.create(mmaps_dir, recursive = TRUE)
        }
        
        # Create a unique file path for this dataset
        memory_map_path <- file.path(mmaps_dir, paste0(dataset_id, ".arrow"))
        
        # Write the Arrow table to the memory-mapped file
        arrow::write_arrow(arrow_table, memory_map_path)
        
        # Serialize Arrow schema
        arrow_schema <- list(
          schema = as.character(arrow_table$schema),
          metadata = if (length(arrow_table$schema$metadata) > 0) {
            arrow_table$schema$metadata
          } else {
            list()
          }
        )
        
        if (cl$debug) message("Wrote Arrow table to memory-mapped file: ", memory_map_path)
      }
    }, error = function(e) {
      if (cl$debug) message("Failed to create memory-mapped file: ", e$message)
      memory_map_path <- NULL
    })
  }
  
  # Try different methods to create the table, with fallbacks for compatibility
  success <- FALSE
  
  # Method 1: Modern register with chunk_size support
  if (!success) {
    tryCatch({
      # Check if register and unregister functions support the features we need
      register_fun <- get("duckdb_register", envir = asNamespace("duckdb"))
      unregister_fun <- get("duckdb_unregister", envir = asNamespace("duckdb"))
      
      # Use optimized method with duckdb_register
      chunk_size <- min(100000, nrow(data))  # Larger chunk size for efficiency
      
      # Check if chunk_size parameter is available
      if ("chunk_size" %in% names(formals(register_fun))) {
        duckdb::duckdb_register(cl$con, table_name, data, overwrite = TRUE, chunk_size = chunk_size)
      } else {
        duckdb::duckdb_register(cl$con, table_name, data)
      }
      
      # Create permanent table from registered view
      duckdb::duckdb_execute(cl$con, sprintf(
        "CREATE OR REPLACE TABLE %s AS SELECT * FROM %s", 
        table_name, 
        table_name
      ))
      
      # Unregister the temporary view
      duckdb::duckdb_unregister(cl$con, table_name)
      
      success <- TRUE
      if (cl$debug) message("Created table using register/unregister method")
    }, error = function(e) {
      if (cl$debug) message("Register method failed: ", e$message)
    })
  }
  
  # Method 2: Arrow method (if available and conversion worked)
  if (!success && cl$arrow_available && !is.null(arrow_table)) {
    tryCatch({
      # Use Arrow for fast bulk loading
      duckdb::duckdb_execute(cl$con, 
        sprintf("CREATE OR REPLACE TABLE %s AS SELECT * FROM arrow_table", table_name))
      
      success <- TRUE
      if (cl$debug) message("Created table using Arrow method")
    }, error = function(e) {
      if (cl$debug) message("Arrow method failed: ", e$message)
    })
  }
  
  # Method 3: Direct parameter execution
  if (!success) {
    tryCatch({
      # Use direct parameterized execution
      sql <- paste0("CREATE OR REPLACE TABLE ", table_name, " AS SELECT * FROM data")
      duckdb::duckdb_execute(cl$con, sql, list(data = data))
      
      success <- TRUE
      if (cl$debug) message("Created table using direct parameter execution")
    }, error = function(e) {
      if (cl$debug) message("Direct parameter execution failed: ", e$message)
    })
  }
  
  # Method 4: Temporary CSV approach as last resort
  if (!success) {
    tryCatch({
      # Write to temporary CSV and read back
      temp_file <- tempfile(fileext = ".csv")
      utils::write.csv(data, temp_file, row.names = FALSE)
      
      # Load from CSV
      duckdb::duckdb_execute(cl$con, sprintf(
        "CREATE OR REPLACE TABLE %s AS SELECT * FROM read_csv_auto('%s')", 
        table_name, 
        temp_file
      ))
      
      # Clean up
      unlink(temp_file)
      
      success <- TRUE
      if (cl$debug) message("Created table using CSV approach")
    }, error = function(e) {
      if (cl$debug) message("CSV approach failed: ", e$message)
    })
  }
  
  # If all methods failed, error out
  if (!success) {
    stop("Failed to create table using any available method")
  }
  
  # Create schema dictionary
  schema_dict <- list(
    columns = names(data),
    dtypes = sapply(data, function(x) class(x)[1], USE.NAMES = TRUE)
  )
  
  # Calculate schema hash
  schema_hash <- digest::digest(jsonlite::toJSON(schema_dict, auto_unbox = TRUE), algo = "md5")
  
  # Add metadata - check if prepare is available
  tryCatch({
    if (exists("duckdb_prepare", where = asNamespace("duckdb"), mode = "function")) {
      # Use prepared statement
      stmt <- duckdb::duckdb_prepare(cl$con,
        "INSERT INTO crosslink_metadata (
          id, name, source_language, created_at, updated_at, description,
          schema, table_name, arrow_data, version, current_version,
          schema_hash, access_languages, memory_map_path, arrow_schema
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      )
      
      duckdb::duckdb_execute(stmt, list(
        dataset_id, name, "r", Sys.time(), Sys.time(),
        if(is.null(description)) "" else description,
        jsonlite::toJSON(schema_dict, auto_unbox = TRUE), 
        table_name, cl$arrow_available, 1, TRUE,
        schema_hash,
        jsonlite::toJSON(c("python", "r", "julia", "cpp"), auto_unbox = TRUE),
        memory_map_path,
        if(is.null(arrow_schema)) NULL else jsonlite::toJSON(arrow_schema, auto_unbox = TRUE)
      ))
    } else {
      # Use direct execute for older versions
      duckdb::duckdb_execute(cl$con,
        "INSERT INTO crosslink_metadata (
          id, name, source_language, created_at, updated_at, description,
          schema, table_name, arrow_data, version, current_version,
          schema_hash, access_languages, memory_map_path, arrow_schema
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        list(
          dataset_id, name, "r", Sys.time(), Sys.time(),
          if(is.null(description)) "" else description,
          jsonlite::toJSON(schema_dict, auto_unbox = TRUE), 
          table_name, cl$arrow_available, 1, TRUE,
          schema_hash,
          jsonlite::toJSON(c("python", "r", "julia", "cpp"), auto_unbox = TRUE),
          memory_map_path,
          if(is.null(arrow_schema)) NULL else jsonlite::toJSON(arrow_schema, auto_unbox = TRUE)
        )
      )
    }
  }, error = function(e) {
    stop("Failed to create metadata: ", e$message)
  })
  
  # Store in cache
  metadata <- list(
    id = dataset_id,
    name = name,
    table_name = table_name,
    schema = schema_dict,
    source_language = "r",
    arrow_data = cl$arrow_available,
    memory_map_path = memory_map_path,
    arrow_schema = arrow_schema
  )
  cl$metadata_cache[[dataset_id]] <- metadata
  cl$metadata_cache[[name]] <- metadata
  
  # Log the operation if debug mode is enabled
  if (cl$debug) {
    log_access(cl, dataset_id, "write", if(enable_zero_copy) "zero-copy" else "copy")
  }
  
  return(dataset_id)
}

#' @export
#' @title Execute a SQL query on the CrossLink database
#' @param cl CrossLink connection
#' @param sql SQL query string
#' @param return_data_frame Whether to return a data frame (TRUE) or query result (FALSE)
#' @return Data frame or query result
query <- function(cl, sql, return_data_frame = TRUE) {
  # Ensure tables are initialized
  setup_metadata_tables(cl)
  
  # Check if prepare is available
  if (exists("duckdb_prepare", where = asNamespace("duckdb"), mode = "function")) {
    # Use prepared statement for better performance
    stmt <- duckdb::duckdb_prepare(cl$con, sql)
    result <- duckdb::duckdb_execute(stmt)
  } else {
    # Use direct execute for older versions
    result <- duckdb::duckdb_execute(cl$con, sql)
  }
  
  if (return_data_frame) {
    return(duckdb::duckdb_fetch(result))
  } else {
    return(result)
  }
}

# For backwards compatibility
#' @export
run_query <- function(cl, sql, return_data_frame = TRUE) {
  warning("run_query is deprecated, use query instead")
  query(cl, sql, return_data_frame)
}

#' @export
#' @title List datasets
#' @param cl CrossLink connection
#' @return Data frame with dataset information
list_datasets <- function(cl) {
  # Ensure tables are initialized
  setup_metadata_tables(cl)
  
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
  # Remove from connection pool
  abs_path <- normalizePath(cl$db_path, mustWork = FALSE)
  if (exists(abs_path, envir = .crosslink_connection_pool)) {
    rm(list = abs_path, envir = .crosslink_connection_pool)
  }
  
  # Detach any attached databases
  for (attach_name in names(cl$attached_databases)) {
    tryCatch(
      duckdb::duckdb_execute(cl$con, paste0("DETACH DATABASE ", attach_name)),
      error = function(e) NULL
    )
  }
  
  # Clean metadata cache
  cl$metadata_cache <- list()
  
  duckdb::duckdb_disconnect(cl$con)
  invisible(NULL)
} 