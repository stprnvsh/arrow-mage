#' DuckConnect: Optimized cross-language data sharing with DuckDB
#' 
#' @description A module for optimized data sharing between R, Python, and Julia
#' using DuckDB as a shared memory database with metadata to avoid expensive
#' push/pull operations between languages.
#' @export
DuckConnect <- R6::R6Class(
  "DuckConnect",
  
  public = list(
    #' @field db_path Path to the DuckDB database file
    db_path = NULL,
    
    #' @field conn DuckDB connection
    conn = NULL,
    
    #' @field arrow_available Whether Arrow extension is available
    arrow_available = FALSE,
    
    #' @description Initialize DuckConnect with a database path
    #' @param db_path Path to the DuckDB database file
    initialize = function(db_path = "duck_connect.duckdb") {
      self$db_path <- db_path
      self$conn <- DBI::dbConnect(duckdb::duckdb(), db_path)
      private$setup_metadata_tables()
      
      # Configure DuckDB for handling large datasets
      tryCatch({
        # Set memory limit (80% of available RAM)
        DBI::dbExecute(self$conn, "PRAGMA memory_limit='80%'")
        
        # Set threads to use all available cores except one
        num_cores <- max(1, parallel::detectCores() - 1)
        DBI::dbExecute(self$conn, paste0("PRAGMA threads=", num_cores))
        
        # Enable parallelism
        DBI::dbExecute(self$conn, "PRAGMA force_parallelism")
        
        # Enable progress bar for long-running queries
        DBI::dbExecute(self$conn, "PRAGMA enable_progress_bar")
        
        # Enable object cache for better performance
        DBI::dbExecute(self$conn, "PRAGMA enable_object_cache")
      }, error = function(e) {
        warning("Failed to configure DuckDB performance settings: ", e)
      })
      
      # Load Arrow extension in DuckDB if available
      tryCatch({
        DBI::dbExecute(self$conn, "INSTALL arrow")
        DBI::dbExecute(self$conn, "LOAD arrow")
        self$arrow_available <- TRUE
      }, error = function(e) {
        warning("Arrow extension installation or loading failed: ", e)
        warning("Some Arrow functionality may not be available")
        self$arrow_available <- FALSE
      })
    },
    
    #' @description Register a dataset with DuckConnect
    #' @param data Data frame to register, or a SQL query string
    #' @param name Name for the dataset
    #' @param description Optional description
    #' @param source_language Source language (default: "r")
    #' @param available_to_languages List of languages that can access this dataset
    #' @param overwrite Whether to overwrite an existing dataset with the same name
    #' @return ID of the registered dataset
    register_dataset = function(data, name, description = NULL, 
                              source_language = "r",
                              available_to_languages = NULL,
                              overwrite = FALSE) {
      # Record start time for stats
      start_time <- Sys.time()
      
      # Check if dataset with this name already exists
      existing <- DBI::dbGetQuery(self$conn, 
        "SELECT id, table_name FROM duck_connect_metadata WHERE name = ?", 
        list(name))
      
      if (nrow(existing) > 0 && !overwrite) {
        stop(paste0("Dataset with name '", name, "' already exists. Use overwrite=TRUE to replace it."))
      }
      
      # Generate a unique ID for this dataset
      dataset_id <- uuid::UUIDgenerate()
      
      # Set default available languages if not specified
      if (is.null(available_to_languages)) {
        available_to_languages <- c("python", "r", "julia")
      }
      
      # Generate unique table name
      table_name <- paste0("duck_connect_", gsub("-", "_", dataset_id))
      
      # Process based on input type
      if (is.character(data) && length(data) == 1 && 
          grepl("^SELECT ", toupper(data))) {
        # It's a SQL query
        tryCatch({
          # Create table from query
          DBI::dbExecute(self$conn, paste0("CREATE OR REPLACE TABLE ", table_name, " AS ", data))
          
          # Get schema from created table
          schema_df <- DBI::dbGetQuery(self$conn, paste0("SELECT * FROM ", table_name, " LIMIT 0"))
          schema <- list(
            columns = colnames(schema_df),
            dtypes = sapply(schema_df, class),
            source_type = "sql_query"
          )
        }, error = function(e) {
          stop(paste0("Failed to create table from SQL query: ", e))
        })
      } else if (is.data.frame(data)) {
        # It's a data frame
        tryCatch({
          # Store in DuckDB
          DBI::dbWriteTable(self$conn, table_name, data, overwrite = TRUE)
          
          # Create schema info
          schema <- list(
            columns = colnames(data),
            dtypes = sapply(data, class),
            source_type = "r_dataframe",
            shape = dim(data)
          )
        }, error = function(e) {
          stop(paste0("Failed to store data frame in DuckDB: ", e))
        })
      } else if (is.character(data) && length(data) == 1 && 
                grepl("^duck_table:", data)) {
        # It's a reference to an existing DuckDB table
        ref_table <- gsub("^duck_table:", "", data)
        
        # Verify the table exists
        table_exists <- DBI::dbGetQuery(self$conn, 
          "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = ?", 
          list(ref_table))$count
        
        if (table_exists == 0) {
          stop(paste0("Referenced table '", ref_table, "' does not exist in DuckDB"))
        }
        
        # For references, we don't create a new table but use the existing one
        table_name <- ref_table
        
        # Get schema info
        schema_df <- DBI::dbGetQuery(self$conn, paste0("SELECT * FROM ", table_name, " LIMIT 0"))
        schema <- list(
          columns = colnames(schema_df),
          dtypes = sapply(schema_df, class),
          source_type = "table_reference"
        )
      } else {
        stop(paste0("Unsupported data type for registration"))
      }
      
      # Serialize schema to JSON
      schema_json <- jsonlite::toJSON(schema, auto_unbox = TRUE)
      
      # Serialize available languages to JSON
      languages_json <- jsonlite::toJSON(available_to_languages, auto_unbox = TRUE)
      
      # Delete existing metadata if overwriting
      if (nrow(existing) > 0 && overwrite) {
        old_id <- existing$id[1]
        old_table <- existing$table_name[1]
        
        # Delete old metadata
        DBI::dbExecute(self$conn, "DELETE FROM duck_connect_metadata WHERE id = ?", 
                      list(old_id))
        
        # Delete old table if it's different from the new one and not a reference
        if (old_table != table_name && !(is.character(data) && grepl("^duck_table:", data))) {
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", old_table))
        }
      }
      
      # Insert metadata
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_metadata 
        (id, name, source_language, created_at, updated_at, description, schema, table_name, available_to_languages)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?)
      ", list(
        dataset_id, name, source_language, description, schema_json, table_name, languages_json
      ))
      
      # Record transaction
      transaction_id <- uuid::UUIDgenerate()
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'create', ?, CURRENT_TIMESTAMP, ?)
      ", list(
        transaction_id, 
        dataset_id, 
        source_language, 
        jsonlite::toJSON(list(overwrite = overwrite), auto_unbox = TRUE)
      ))
      
      # Record statistics
      duration_ms <- as.numeric(difftime(Sys.time(), start_time, units = "secs")) * 1000
      row_count <- DBI::dbGetQuery(self$conn, paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      column_count <- length(schema$columns)
      
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'create', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
      ", list(
        uuid::UUIDgenerate(),
        dataset_id,
        source_language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
      ))
      
      message(paste0("Registered dataset '", name, "' with ID ", dataset_id))
      return(dataset_id)
    },
    
    #' @description Get a dataset by name or ID
    #' @param identifier Dataset name or ID
    #' @param language Language requesting the data (for access control and stats)
    #' @param as_arrow Whether to return as Arrow Table instead of data frame
    #' @return Data frame with the dataset
    get_dataset = function(identifier, language = "r", as_arrow = FALSE) {
      start_time <- Sys.time()
      
      # Look up metadata
      metadata <- DBI::dbGetQuery(self$conn, "
        SELECT id, table_name, schema, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
      ", list(identifier, identifier))
      
      if (nrow(metadata) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      dataset_id <- metadata$id[1]
      table_name <- metadata$table_name[1]
      schema_json <- metadata$schema[1]
      available_languages_json <- metadata$available_to_languages[1]
      
      # Check language access
      available_languages <- jsonlite::fromJSON(available_languages_json)
      if (!(language %in% available_languages)) {
        stop(paste0("Dataset '", identifier, "' is not available to language '", language, "'"))
      }
      
      # Record transaction
      transaction_id <- uuid::UUIDgenerate()
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'read', ?, CURRENT_TIMESTAMP, ?)
      ", list(
        transaction_id, 
        dataset_id, 
        language, 
        jsonlite::toJSON(list(as_arrow = as_arrow), auto_unbox = TRUE)
      ))
      
      # Retrieve data
      result <- tryCatch({
        if (as_arrow && self$arrow_available && requireNamespace("arrow", quietly = TRUE)) {
          # Load as Arrow table
          query <- paste0("SELECT * FROM ", table_name)
          DBI::dbExecute(self$conn, "LOAD arrow")
          result <- DBI::dbGetQuery(self$conn, paste0(query, " USING arrow"))
          return(result)
        } else {
          # Get as data frame
          DBI::dbGetQuery(self$conn, paste0("SELECT * FROM ", table_name))
        }
      }, error = function(e) {
        stop(paste0("Error retrieving dataset '", identifier, "': ", e))
      })
      
      # Record statistics
      duration_ms <- as.numeric(difftime(Sys.time(), start_time, units = "secs")) * 1000
      schema <- jsonlite::fromJSON(schema_json)
      row_count <- nrow(result)
      column_count <- length(schema$columns)
      
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'read', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
      ", list(
        uuid::UUIDgenerate(),
        dataset_id,
        language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
      ))
      
      return(result)
    },
    
    #' @description Update an existing dataset
    #' @param identifier Dataset name or ID
    #' @param data New data as data frame or SQL query
    #' @param language Language performing the update
    #' @param description Optional new description
    #' @return ID of the updated dataset
    update_dataset = function(identifier, data, language = "r", description = NULL) {
      start_time <- Sys.time()
      
      # Look up metadata
      metadata <- DBI::dbGetQuery(self$conn, "
        SELECT id, table_name, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
      ", list(identifier, identifier))
      
      if (nrow(metadata) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      dataset_id <- metadata$id[1]
      table_name <- metadata$table_name[1]
      available_languages_json <- metadata$available_to_languages[1]
      
      # Check language access
      available_languages <- jsonlite::fromJSON(available_languages_json)
      if (!(language %in% available_languages)) {
        stop(paste0("Dataset '", identifier, "' is not available to language '", language, "'"))
      }
      
      # Process based on input type
      if (is.character(data) && length(data) == 1 && 
          grepl("^SELECT ", toupper(data))) {
        # It's a SQL query - replace table contents
        tryCatch({
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
          DBI::dbExecute(self$conn, paste0("CREATE TABLE ", table_name, " AS ", data))
          
          # Get updated schema
          schema_df <- DBI::dbGetQuery(self$conn, paste0("SELECT * FROM ", table_name, " LIMIT 0"))
          schema <- list(
            columns = colnames(schema_df),
            dtypes = sapply(schema_df, class),
            source_type = "sql_query"
          )
        }, error = function(e) {
          stop(paste0("Failed to update table from SQL query: ", e))
        })
      } else if (is.data.frame(data)) {
        # It's a data frame
        tryCatch({
          # Replace table contents
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
          DBI::dbWriteTable(self$conn, table_name, data)
          
          # Update schema info
          schema <- list(
            columns = colnames(data),
            dtypes = sapply(data, class),
            source_type = "r_dataframe",
            shape = dim(data)
          )
        }, error = function(e) {
          stop(paste0("Failed to update data frame in DuckDB: ", e))
        })
      } else {
        stop(paste0("Unsupported data type for update"))
      }
      
      # Serialize schema to JSON
      schema_json <- jsonlite::toJSON(schema, auto_unbox = TRUE)
      
      # Update metadata
      if (!is.null(description)) {
        DBI::dbExecute(self$conn, "
          UPDATE duck_connect_metadata 
          SET updated_at = CURRENT_TIMESTAMP, description = ?, schema = ? 
          WHERE id = ?
        ", list(description, schema_json, dataset_id))
      } else {
        DBI::dbExecute(self$conn, "
          UPDATE duck_connect_metadata 
          SET updated_at = CURRENT_TIMESTAMP, schema = ? 
          WHERE id = ?
        ", list(schema_json, dataset_id))
      }
      
      # Record transaction
      transaction_id <- uuid::UUIDgenerate()
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'update', ?, CURRENT_TIMESTAMP, ?)
      ", list(
        transaction_id, 
        dataset_id, 
        language, 
        jsonlite::toJSON(list(description_updated = !is.null(description)), auto_unbox = TRUE)
      ))
      
      # Record statistics
      duration_ms <- as.numeric(difftime(Sys.time(), start_time, units = "secs")) * 1000
      row_count <- DBI::dbGetQuery(self$conn, paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      column_count <- length(schema$columns)
      
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'update', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
      ", list(
        uuid::UUIDgenerate(),
        dataset_id,
        language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
      ))
      
      message(paste0("Updated dataset '", identifier, "' (ID: ", dataset_id, ")"))
      return(dataset_id)
    },
    
    #' @description Delete a dataset
    #' @param identifier Dataset name or ID
    #' @param language Language performing the deletion
    #' @return TRUE if successful
    delete_dataset = function(identifier, language = "r") {
      # Look up metadata
      metadata <- DBI::dbGetQuery(self$conn, "
        SELECT id, table_name, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
      ", list(identifier, identifier))
      
      if (nrow(metadata) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      dataset_id <- metadata$id[1]
      table_name <- metadata$table_name[1]
      available_languages_json <- metadata$available_to_languages[1]
      
      # Check language access
      available_languages <- jsonlite::fromJSON(available_languages_json)
      if (!(language %in% available_languages)) {
        stop(paste0("Dataset '", identifier, "' is not available to language '", language, "'"))
      }
      
      # Delete the table
      DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
      
      # Delete metadata
      DBI::dbExecute(self$conn, "DELETE FROM duck_connect_metadata WHERE id = ?", 
                    list(dataset_id))
      
      # Record transaction
      transaction_id <- uuid::UUIDgenerate()
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'delete', ?, CURRENT_TIMESTAMP, ?)
      ", list(
        transaction_id, 
        dataset_id, 
        language, 
        "{}"
      ))
      
      message(paste0("Deleted dataset '", identifier, "' (ID: ", dataset_id, ")"))
      return(TRUE)
    },
    
    #' @description List all available datasets
    #' @param language Language requesting the list (for access control)
    #' @return Data frame containing dataset information
    list_datasets = function(language = "r") {
      # Get all datasets
      datasets <- DBI::dbGetQuery(self$conn, "
        SELECT id, name, source_language, created_at, updated_at, description, 
               table_name, available_to_languages
        FROM duck_connect_metadata
        ORDER BY updated_at DESC
      ")
      
      if (nrow(datasets) == 0) {
        return(datasets)
      }
      
      # Filter for language access
      filtered_datasets <- datasets[0,]  # Empty data frame with same structure
      
      for (i in 1:nrow(datasets)) {
        available_languages <- jsonlite::fromJSON(datasets$available_to_languages[i])
        if (language %in% available_languages) {
          filtered_datasets <- rbind(filtered_datasets, datasets[i,])
        }
      }
      
      return(filtered_datasets)
    },
    
    #' @description Execute a SQL query on the DuckDB database
    #' @param query SQL query to execute
    #' @param language Language executing the query
    #' @param as_arrow Whether to return as Arrow Table
    #' @return Data frame with query results
    execute_query = function(query, language = "r", as_arrow = FALSE) {
      start_time <- Sys.time()
      
      # Execute query
      result <- tryCatch({
        if (as_arrow && self$arrow_available && requireNamespace("arrow", quietly = TRUE)) {
          # Get as Arrow table
          DBI::dbExecute(self$conn, "LOAD arrow")
          DBI::dbGetQuery(self$conn, paste0(query, " USING arrow"))
        } else {
          # Get as data frame
          DBI::dbGetQuery(self$conn, query)
        }
      }, error = function(e) {
        stop(paste0("Error executing query: ", e))
      })
      
      # Record transaction
      transaction_id <- uuid::UUIDgenerate()
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'query', ?, CURRENT_TIMESTAMP, ?)
      ", list(
        transaction_id, 
        'N/A', 
        language, 
        jsonlite::toJSON(list(
          query = substring(query, 1, 1000), 
          as_arrow = as_arrow
        ), auto_unbox = TRUE)
      ))
      
      # Record statistics
      duration_ms <- as.numeric(difftime(Sys.time(), start_time, units = "secs")) * 1000
      row_count <- nrow(result)
      column_count <- ncol(result)
      
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'query', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
      ", list(
        uuid::UUIDgenerate(),
        'N/A',
        language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
      ))
      
      return(result)
    },
    
    #' @description Get a reference to a DuckDB table for efficient cross-language access
    #' @param identifier Dataset name or ID
    #' @param language Language requesting the reference
    #' @return Table reference string that can be used directly in SQL
    get_table_reference = function(identifier, language = "r") {
      # Look up metadata
      metadata <- DBI::dbGetQuery(self$conn, "
        SELECT id, table_name, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
      ", list(identifier, identifier))
      
      if (nrow(metadata) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      dataset_id <- metadata$id[1]
      table_name <- metadata$table_name[1]
      available_languages_json <- metadata$available_to_languages[1]
      
      # Check language access
      available_languages <- jsonlite::fromJSON(available_languages_json)
      if (!(language %in% available_languages)) {
        stop(paste0("Dataset '", identifier, "' is not available to language '", language, "'"))
      }
      
      # Record transaction for the reference access
      transaction_id <- uuid::UUIDgenerate()
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'reference', ?, CURRENT_TIMESTAMP, ?)
      ", list(
        transaction_id, 
        dataset_id, 
        language, 
        "{}"
      ))
      
      # Return the reference as a special string
      return(paste0("duck_table:", table_name))
    },
    
    #' @description Register a transformation between datasets for lineage tracking
    #' @param target_dataset Target dataset name or ID
    #' @param source_dataset Source dataset name or ID
    #' @param transformation Description of the transformation
    #' @param language Language that performed the transformation
    #' @return ID of the lineage record
    register_transformation = function(target_dataset, source_dataset, 
                                      transformation, language = "r") {
      # Look up target dataset
      target_metadata <- DBI::dbGetQuery(self$conn, "
        SELECT id FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
      ", list(target_dataset, target_dataset))
      
      if (nrow(target_metadata) == 0) {
        stop(paste0("Target dataset '", target_dataset, "' not found"))
      }
      
      target_id <- target_metadata$id[1]
      
      # Look up source dataset
      source_metadata <- DBI::dbGetQuery(self$conn, "
        SELECT id FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
      ", list(source_dataset, source_dataset))
      
      if (nrow(source_metadata) == 0) {
        stop(paste0("Source dataset '", source_dataset, "' not found"))
      }
      
      source_id <- source_metadata$id[1]
      
      # Generate unique ID for lineage record
      lineage_id <- uuid::UUIDgenerate()
      
      # Insert lineage record
      DBI::dbExecute(self$conn, "
        INSERT INTO duck_connect_lineage
        (id, target_dataset_id, source_dataset_id, transformation, language, timestamp)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ", list(lineage_id, target_id, source_id, transformation, language))
      
      message(paste0("Registered transformation from '", source_dataset, "' to '", target_dataset, "'"))
      return(lineage_id)
    },
    
    #' @description Close the connection to the database
    close = function() {
      if (!is.null(self$conn)) {
        DBI::dbDisconnect(self$conn, shutdown = TRUE)
        self$conn <- NULL
      }
    }
  ),
  
  private = list(
    #' @description Set up the metadata tables if they don't exist
    setup_metadata_tables = function() {
      # Create metadata tables for tracking data
      DBI::dbExecute(self$conn, "
        CREATE TABLE IF NOT EXISTS duck_connect_metadata (
          id TEXT PRIMARY KEY,
          name TEXT UNIQUE,
          source_language TEXT,
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          description TEXT,
          schema TEXT,  -- JSON schema information
          table_name TEXT,  -- Actual DuckDB table name
          available_to_languages TEXT  -- JSON array of languages
        )
      ")
      
      # Create transactions table to track language operations
      DBI::dbExecute(self$conn, "
        CREATE TABLE IF NOT EXISTS duck_connect_transactions (
          id TEXT PRIMARY KEY,
          dataset_id TEXT,
          operation TEXT,  -- 'create', 'read', 'update', 'delete'
          language TEXT,
          timestamp TIMESTAMP,
          details TEXT  -- JSON additional details
        )
      ")
      
      # Create lineage table for tracking data transformations
      DBI::dbExecute(self$conn, "
        CREATE TABLE IF NOT EXISTS duck_connect_lineage (
          id TEXT PRIMARY KEY,
          target_dataset_id TEXT,
          source_dataset_id TEXT,
          transformation TEXT,
          language TEXT,
          timestamp TIMESTAMP
        )
      ")
      
      # Create statistics table for performance tracking
      DBI::dbExecute(self$conn, "
        CREATE TABLE IF NOT EXISTS duck_connect_stats (
          id TEXT PRIMARY KEY,
          dataset_id TEXT,
          operation TEXT,
          language TEXT,
          timestamp TIMESTAMP,
          duration_ms FLOAT,
          memory_usage_mb FLOAT,
          row_count INTEGER,
          column_count INTEGER
        )
      ")
    }
  )
)

#' RDuckContext: R context manager for DuckConnect nodes
#' 
#' @description A context manager for working with DuckConnect in R node scripts
#' @export
RDuckContext <- R6::R6Class(
  "RDuckContext",
  
  public = list(
    #' @field dc DuckConnect instance
    dc = NULL,
    
    #' @field meta Metadata from the node
    meta = NULL,
    
    #' @field language Language identifier for this context
    language = "r",
    
    #' @field input_cache Cache for input datasets
    input_cache = NULL,
    
    #' @description Initialize context
    #' @param db_path Optional path to DuckDB database
    #' @param meta_path Optional path to metadata file
    initialize = function(db_path = NULL, meta_path = NULL) {
      self$meta <- private$load_metadata(meta_path)
      
      # Connect to DuckConnect
      if (is.null(db_path) && "db_path" %in% names(self$meta)) {
        db_path <- self$meta$db_path
      }
      
      self$dc <- DuckConnect$new(db_path = db_path %||% "duck_connect.duckdb")
      self$language <- "r"
      self$input_cache <- list()
    },
    
    #' @description Get input dataset by name
    #' @param name Name of the input
    #' @param required Whether this input is required
    #' @return Data frame with the data
    get_input = function(name, required = TRUE) {
      # Check if input is defined for this node
      inputs <- self$meta$inputs %||% character(0)
      if (!(name %in% inputs)) {
        if (required) {
          stop(paste0("Input '", name, "' is not defined for this node"))
        }
        return(NULL)
      }
      
      # Check cache
      if (name %in% names(self$input_cache)) {
        return(self$input_cache[[name]])
      }
      
      # Get from DuckConnect
      tryCatch({
        data <- self$dc$get_dataset(name, language = self$language)
        self$input_cache[[name]] <- data
        return(data)
      }, error = function(e) {
        if (required) {
          stop(paste0("Error retrieving input '", name, "': ", e))
        }
        return(NULL)
      })
    },
    
    #' @description Save output dataset
    #' @param name Name of the output
    #' @param data Data frame to save
    #' @param description Optional description
    #' @return Dataset ID
    save_output = function(name, data, description = NULL) {
      # Check if output is defined for this node
      outputs <- self$meta$outputs %||% character(0)
      if (!(name %in% outputs)) {
        stop(paste0("Output '", name, "' is not defined for this node"))
      }
      
      # Generate description if not provided
      if (is.null(description)) {
        node_id <- self$meta$node_id %||% "unknown"
        description <- paste0("Output from ", node_id)
      }
      
      # First check if dataset already exists
      tryCatch({
        existing <- self$dc$list_datasets(language = self$language)
        if (name %in% existing$name) {
          # Update existing dataset
          return(self$dc$update_dataset(name, data, language = self$language, description = description))
        } else {
          # Register new dataset
          return(self$dc$register_dataset(data, name, description, source_language = self$language))
        }
      }, error = function(e) {
        stop(paste0("Error saving output '", name, "': ", e))
      })
    },
    
    #' @description Get parameter value
    #' @param name Parameter name
    #' @param default Default value if not found
    #' @return Parameter value
    get_param = function(name, default = NULL) {
      params <- self$meta$params %||% list()
      return(params[[name]] %||% default)
    },
    
    #' @description Execute a SQL query
    #' @param query SQL query to execute
    #' @param as_arrow Whether to return as Arrow Table
    #' @return Data frame with query results
    execute_query = function(query, as_arrow = FALSE) {
      return(self$dc$execute_query(query, language = self$language, as_arrow = as_arrow))
    },
    
    #' @description Get node ID from metadata
    #' @return Node ID string
    get_node_id = function() {
      return(self$meta$node_id %||% "unknown")
    },
    
    #' @description Get pipeline name from metadata
    #' @return Pipeline name string
    get_pipeline_name = function() {
      return(self$meta$pipeline_name %||% "unknown")
    },
    
    #' @description Close connection to DuckConnect
    close = function() {
      if (!is.null(self$dc)) {
        self$dc$close()
      }
    }
  ),
  
  private = list(
    #' @description Load metadata from file
    #' @param meta_path Path to metadata file
    #' @return List containing metadata
    load_metadata = function(meta_path) {
      if (is.null(meta_path)) {
        # Try to get from command line arguments
        args <- commandArgs(trailingOnly = TRUE)
        if (length(args) > 0 && file.exists(args[1])) {
          meta_path <- args[1]
        } else {
          # No metadata file
          return(list())
        }
      }
      
      # Load metadata
      if (file.exists(meta_path)) {
        tryCatch({
          yaml_data <- yaml::read_yaml(meta_path)
          return(yaml_data)
        }, error = function(e) {
          warning(paste0("Failed to load metadata file: ", e))
          return(list())
        })
      }
      
      return(list())
    }
  )
)

# Helper function for NULL coalescing
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
} 