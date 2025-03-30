#' @export
#' @title Push data to CrossLink
#' @param cl CrossLink connection
#' @param data Data frame to push
#' @param name Optional name for the dataset
#' @param description Optional description for the dataset
#' @param use_arrow Whether to use Arrow for data transfer (default TRUE)
#' @param arrow_data Deprecated, use enable_zero_copy instead
#' @param enable_zero_copy Enable zero-copy access between languages (default TRUE)
#' @param memory_mapped Use memory-mapped files for zero-copy access (default TRUE)
#' @param shared_memory Use shared memory for zero-copy access (default FALSE)
#' @param access_languages Languages that can access this dataset (default all)
#' @return Dataset ID
push_data <- function(cl, data, name = NULL, description = NULL,
                     use_arrow = TRUE, arrow_data = NULL, enable_zero_copy = TRUE,
                     memory_mapped = TRUE, shared_memory = FALSE,
                     access_languages = c("python", "r", "julia", "cpp")) {
  # For backward compatibility: if arrow_data is provided, use it for enable_zero_copy
  if (!is.null(arrow_data)) {
    enable_zero_copy <- arrow_data
  }
  
  # If C++ implementation is available, use it
  if (inherits(cl, "crosslink_cpp_connection") || cl$cpp_available) {
    # Try to load cpp_wrapper.R if needed
    if (!exists("push_via_cpp", mode = "function")) {
      source_path <- file.path(dirname(dirname(system.file(package = "crosslink"))), 
                               "r", "shared_memory", "cpp_wrapper.R")
      tryCatch({
        source(source_path)
      }, error = function(e) {
        if (cl$debug) message("Failed to load C++ wrapper: ", e$message)
      })
    }
    
    # Check if input is already an Arrow Table
    is_arrow_table <- inherits(data, "Table")
    
    # Convert to Arrow table if needed
    if (!is_arrow_table && requireNamespace("arrow", quietly = TRUE)) {
      data <- arrow::as_arrow_table(data)
      is_arrow_table <- TRUE
    }
    
    # Use C++ implementation if data is an Arrow table
    if (is_arrow_table && exists("push_via_cpp", mode = "function")) {
      # For C++ connections, use cpp_instance
      if (inherits(cl, "crosslink_cpp_connection")) {
        return(push_via_cpp(cl$cpp_instance, data, 
                            ifelse(is.null(name), "", name), 
                            ifelse(is.null(description), "", description)))
      } else if (!is.null(cl$cpp_instance)) {
        return(push_via_cpp(cl$cpp_instance, data,
                            ifelse(is.null(name), "", name),
                            ifelse(is.null(description), "", description)))
      }
    }
  }
  
  # Check if input is already an Arrow Table
  is_arrow_table <- inherits(data, "Table")
  
  # If input is already an Arrow table and we want zero-copy, use direct Arrow path
  if (is_arrow_table && enable_zero_copy) {
    return(share_arrow_table(
      cl = cl,
      table = data,
      name = name,
      description = description,
      shared_memory = shared_memory,
      memory_mapped = memory_mapped,
      access_languages = access_languages
    ))
  }
  
  # Ensure tables are initialized
  setup_metadata_tables(cl)
  
  # Check if we need Arrow
  if (enable_zero_copy && !cl$arrow_loaded) {
    cl$arrow_available <- ensure_arrow_extension(cl)
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
  shared_memory_info <- NULL
  
  if (enable_zero_copy && cl$arrow_available) {
    # Try to convert data to Arrow table
    tryCatch({
      if (!requireNamespace("arrow", quietly = TRUE)) {
        if (cl$debug) message("Arrow package not available for memory mapping")
      } else {
        # Convert data frame to Arrow table
        if (is_arrow_table) {
          arrow_table <- data
        } else {
          arrow_table <- arrow::as_arrow_table(data)
        }
        
        # Set up memory-mapped file if requested
        if (memory_mapped) {
          # Create directory for memory-mapped files if it doesn't exist
          mmaps_dir <- file.path(dirname(cl$db_path), "crosslink_mmaps")
          if (!dir.exists(mmaps_dir)) {
            dir.create(mmaps_dir, recursive = TRUE)
          }
          
          # Create a unique file path for this dataset
          memory_map_path <- file.path(mmaps_dir, paste0(dataset_id, ".arrow"))
          
          # Write the Arrow table to the memory-mapped file
          arrow::write_arrow(arrow_table, memory_map_path)
          
          # Track this file for cleanup
          cl$arrow_files <- c(cl$arrow_files, memory_map_path)
          
          if (cl$debug) message("Wrote Arrow table to memory-mapped file: ", memory_map_path)
        }
        
        # Set up shared memory if requested
        if (shared_memory) {
          shared_memory_info <- setup_shared_memory(arrow_table, dataset_id, cl)
        }
        
        # Serialize Arrow schema
        arrow_schema <- list(
          schema = as.character(arrow_table$schema),
          metadata = if (length(arrow_table$schema$metadata) > 0) {
            arrow_table$schema$metadata
          } else {
            list()
          }
        )
      }
    }, error = function(e) {
      if (cl$debug) message("Failed to create Arrow representation: ", e$message)
      memory_map_path <- NULL
      arrow_table <- NULL
    })
  }
  
  # Try different methods to create the table, with fallbacks for compatibility
  success <- FALSE
  
  # Method 1: Arrow-based view to memory mapped file (most efficient)
  if (!success && !is.null(memory_map_path)) {
    tryCatch({
      # Create a view that reads directly from the Arrow file
      duckdb::duckdb_execute(cl$con, sprintf(
        "CREATE OR REPLACE VIEW %s AS SELECT * FROM read_parquet('%s')",
        table_name, memory_map_path
      ))
      
      success <- TRUE
      if (cl$debug) message("Created view from memory-mapped Arrow file")
    }, error = function(e) {
      if (cl$debug) message("View creation from memory-mapped file failed: ", e$message)
    })
  }
  
  # Method 2: Direct Arrow table registration if available
  if (!success && !is.null(arrow_table)) {
    tryCatch({
      # Try registering the Arrow table directly
      temp_name <- paste0("__temp_", dataset_id)
      
      # Method 2a: Try using direct Arrow table registration if available
      tryCatch({
        # Some versions of DuckDB R package can directly register Arrow tables
        duckdb::duckdb_register_arrow(cl$con, temp_name, arrow_table)
        duckdb::duckdb_execute(cl$con, sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", 
                                              table_name, temp_name))
        
        # Clean up the temporary registration
        duckdb::duckdb_unregister(cl$con, temp_name)
        
        success <- TRUE
        if (cl$debug) message("Created table using Arrow table registration")
      }, error = function(e) {
        if (cl$debug) message("Direct Arrow registration failed: ", e$message)
        
        # Method 2b: Convert to data frame and register
        df <- arrow::as.data.frame(arrow_table)
        duckdb::duckdb_register(cl$con, temp_name, df)
        duckdb::duckdb_execute(cl$con, sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", 
                                              table_name, temp_name))
        
        # Clean up the temporary registration
        duckdb::duckdb_unregister(cl$con, temp_name)
        
        success <- TRUE
        if (cl$debug) message("Created table using Arrow->DataFrame registration")
      })
    }, error = function(e) {
      if (cl$debug) message("Arrow registration methods failed: ", e$message)
    })
  }
  
  # Method 3: Modern register with chunk_size support
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
  
  # Method 4: Direct parameter execution
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
  
  # Method 5: Temporary CSV approach as last resort
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
          schema_hash, access_languages, memory_map_path, shared_memory_key, arrow_schema
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
      )
      
      duckdb::duckdb_execute(stmt, list(
        dataset_id, name, "r", Sys.time(), Sys.time(),
        if(is.null(description)) "" else description,
        jsonlite::toJSON(schema_dict, auto_unbox = TRUE), 
        table_name, cl$arrow_available, 1, TRUE,
        schema_hash,
        jsonlite::toJSON(access_languages, auto_unbox = TRUE),
        memory_map_path,
        if(!is.null(shared_memory_info)) shared_memory_info$shared_memory_key else NULL,
        if(is.null(arrow_schema)) NULL else jsonlite::toJSON(arrow_schema, auto_unbox = TRUE)
      ))
    } else {
      # Use direct execute for older versions
      duckdb::duckdb_execute(cl$con,
        "INSERT INTO crosslink_metadata (
          id, name, source_language, created_at, updated_at, description,
          schema, table_name, arrow_data, version, current_version,
          schema_hash, access_languages, memory_map_path, shared_memory_key, arrow_schema
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        list(
          dataset_id, name, "r", Sys.time(), Sys.time(),
          if(is.null(description)) "" else description,
          jsonlite::toJSON(schema_dict, auto_unbox = TRUE), 
          table_name, cl$arrow_available, 1, TRUE,
          schema_hash,
          jsonlite::toJSON(access_languages, auto_unbox = TRUE),
          memory_map_path,
          if(!is.null(shared_memory_info)) shared_memory_info$shared_memory_key else NULL,
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
    shared_memory_key = if(!is.null(shared_memory_info)) shared_memory_info$shared_memory_key else NULL,
    arrow_schema = arrow_schema
  )
  cl$metadata_cache[[dataset_id]] <- metadata
  cl$metadata_cache[[name]] <- metadata
  
  # Log the operation if debug mode is enabled
  if (cl$debug) {
    access_method <- if (is_arrow_table) "direct_arrow" else if (enable_zero_copy) "zero-copy" else "copy"
    log_access(cl, dataset_id, "write", access_method)
  }
  
  return(dataset_id)
}

#' @export
#' @title Pull data from CrossLink with zero-copy optimization when possible
#' @param cl CrossLink connection
#' @param identifier Dataset ID or name
#' @param zero_copy Whether to use zero-copy optimization if available
#' @param to_arrow Return an Arrow table instead of a data frame
#' @return Data frame
pull_data <- function(cl, identifier, zero_copy = TRUE, to_arrow = FALSE) {
  # If C++ implementation is available, use it
  if (inherits(cl, "crosslink_cpp_connection") || cl$cpp_available) {
    # Try to load cpp_wrapper.R if needed
    if (!exists("pull_via_cpp", mode = "function")) {
      source_path <- file.path(dirname(dirname(system.file(package = "crosslink"))), 
                               "r", "shared_memory", "cpp_wrapper.R")
      tryCatch({
        source(source_path)
      }, error = function(e) {
        if (cl$debug) message("Failed to load C++ wrapper: ", e$message)
      })
    }
    
    # Use C++ implementation
    if (exists("pull_via_cpp", mode = "function")) {
      tryCatch({
        # For C++ connections, use cpp_instance
        if (inherits(cl, "crosslink_cpp_connection")) {
          return(pull_via_cpp(cl$cpp_instance, identifier, !to_arrow))
        } else if (!is.null(cl$cpp_instance)) {
          return(pull_via_cpp(cl$cpp_instance, identifier, !to_arrow))
        }
      }, error = function(e) {
        if (cl$debug) message("C++ pull failed, falling back to R implementation: ", e$message)
        # Continue with regular R implementation
      })
    }
  }
  
  # Ensure tables are initialized
  setup_metadata_tables(cl)
  
  # For direct Arrow output, use the get_arrow_table function
  if (to_arrow) {
    tryCatch({
      return(get_arrow_table(cl, identifier))
    }, error = function(e) {
      if (cl$debug) message("Direct Arrow access failed, falling back: ", e$message)
      # Fall through to standard methods
    })
  }
  
  # Try to get metadata from cache first for better performance
  metadata <- cl$metadata_cache[[identifier]]
  
  # If not in cache, get it from database
  if (is.null(metadata)) {
    metadata <- get_dataset_metadata(cl, identifier)
    
    if (is.null(metadata)) {
      stop("Dataset with identifier '", identifier, "' not found")
    }
  }
  
  # Priority order for access:
  # 1. Shared memory (fastest)
  # 2. Memory mapped Arrow file (fast file-based)
  # 3. DuckDB with Arrow fetch (database with zero-copy)
  # 4. Standard data frame (fallback)
  
  # 1. Check for shared memory access first (fastest method)
  shared_memory_key <- metadata$shared_memory_key
  if (zero_copy && !is.null(shared_memory_key) && .shared_memory_available) {
    # Get from shared memory
    arrow_table <- get_from_shared_memory(shared_memory_key, cl)
    
    if (!is.null(arrow_table)) {
      # Convert to data frame with zero-copy when possible
      df <- arrow::as.data.frame(arrow_table)
      
      # Log access
      if (cl$debug) {
        log_access(cl, metadata$id, "read", "shared_memory")
      }
      
      return(df)
    }
  }
  
  # 2. Check for memory-mapped file (next fastest)
  memory_map_path <- metadata$memory_map_path
  if (zero_copy && !is.null(memory_map_path) && file.exists(memory_map_path)) {
    tryCatch({
      # Check if we have this table in cache
      if (exists(memory_map_path, envir = .crosslink_mmap_table_cache)) {
        arrow_table <- get(memory_map_path, envir = .crosslink_mmap_table_cache)
      } else {
        # Read from memory-mapped file using Arrow
        arrow_table <- arrow::read_arrow(memory_map_path)
        # Cache for future use
        assign(memory_map_path, arrow_table, envir = .crosslink_mmap_table_cache)
      }
      
      # Convert to data frame with zero-copy when possible
      df <- arrow::as.data.frame(arrow_table)
      
      # Log access
      if (cl$debug) {
        log_access(cl, metadata$id, "read", "memory_mapped")
      }
      
      return(df)
    }, error = function(e) {
      if (cl$debug) message("Failed to read from memory-mapped file: ", e$message)
      # Fall through to next method
    })
  }
  
  # 3. Try DuckDB with Arrow fetch if Arrow is available
  if (zero_copy && cl$arrow_available && !is.null(metadata$table_name)) {
    tryCatch({
      # First check if duckdb_fetch_arrow is available
      if (exists("duckdb_fetch_arrow", where = asNamespace("duckdb"), mode = "function")) {
        # Prepare and fetch using Arrow if available
        if (exists("duckdb_prepare", where = asNamespace("duckdb"), mode = "function")) {
          stmt <- duckdb::duckdb_prepare(cl$con, paste0("SELECT * FROM ", metadata$table_name))
          result <- duckdb::duckdb_execute(stmt)
          arrow_result <- duckdb::duckdb_fetch_arrow(result)
        } else {
          # Use execute with Arrow if prepare is not available
          result <- duckdb::duckdb_execute(cl$con, paste0("SELECT * FROM ", metadata$table_name))
          arrow_result <- duckdb::duckdb_fetch_arrow(result)
        }
        
        # Convert to data frame with zero-copy when possible
        df <- arrow::as.data.frame(arrow_result)
        
        # Log access
        if (cl$debug) {
          log_access(cl, metadata$id, "read", "duckdb_arrow")
        }
        
        return(df)
      }
    }, error = function(e) {
      if (cl$debug) message("Arrow fetch via DuckDB failed: ", e$message)
      # Fall through to standard method
    })
  }
  
  # 4. Standard query as last resort
  result <- duckdb::duckdb_execute(cl$con, paste0("SELECT * FROM ", metadata$table_name))
  data <- duckdb::duckdb_fetch(result)
  
  # Log access
  if (cl$debug) {
    log_access(cl, metadata$id, "read", "copy")
  }
  
  return(data)
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
#' @title List datasets
#' @param cl CrossLink connection
#' @return Data frame with dataset information
list_datasets <- function(cl) {
  # If C++ implementation is available, use it
  if (inherits(cl, "crosslink_cpp_connection") || cl$cpp_available) {
    # Try to load cpp_wrapper.R if needed
    if (!exists("list_datasets_via_cpp", mode = "function")) {
      source_path <- file.path(dirname(dirname(system.file(package = "crosslink"))), 
                               "r", "shared_memory", "cpp_wrapper.R")
      tryCatch({
        source(source_path)
      }, error = function(e) {
        if (cl$debug) message("Failed to load C++ wrapper: ", e$message)
      })
    }
    
    # Use C++ implementation
    if (exists("list_datasets_via_cpp", mode = "function")) {
      tryCatch({
        # For C++ connections, use cpp_instance
        if (inherits(cl, "crosslink_cpp_connection")) {
          return(list_datasets_via_cpp(cl$cpp_instance))
        } else if (!is.null(cl$cpp_instance)) {
          return(list_datasets_via_cpp(cl$cpp_instance))
        }
      }, error = function(e) {
        if (cl$debug) message("C++ list_datasets failed, falling back to R implementation: ", e$message)
        # Continue with regular R implementation
      })
    }
  }
  
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
#' @title Execute a SQL query on the CrossLink database
#' @param cl CrossLink connection
#' @param sql SQL query string
#' @param return_data_frame Whether to return a data frame (TRUE) or query result (FALSE)
#' @return Data frame or query result
query <- function(cl, sql, return_data_frame = TRUE) {
  # If C++ implementation is available, use it
  if (inherits(cl, "crosslink_cpp_connection") || cl$cpp_available) {
    # Try to load cpp_wrapper.R if needed
    if (!exists("query_via_cpp", mode = "function")) {
      source_path <- file.path(dirname(dirname(system.file(package = "crosslink"))), 
                               "r", "shared_memory", "cpp_wrapper.R")
      tryCatch({
        source(source_path)
      }, error = function(e) {
        if (cl$debug) message("Failed to load C++ wrapper: ", e$message)
      })
    }
    
    # Use C++ implementation
    if (exists("query_via_cpp", mode = "function")) {
      tryCatch({
        # For C++ connections, use cpp_instance
        if (inherits(cl, "crosslink_cpp_connection")) {
          return(query_via_cpp(cl$cpp_instance, sql, return_data_frame))
        } else if (!is.null(cl$cpp_instance)) {
          return(query_via_cpp(cl$cpp_instance, sql, return_data_frame))
        }
      }, error = function(e) {
        if (cl$debug) message("C++ query failed, falling back to R implementation: ", e$message)
        # Continue with regular R implementation
      })
    }
  }
  
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