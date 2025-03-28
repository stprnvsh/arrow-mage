#' Main interface for DuckConnect in R
#'
#' This module provides the main API for the DuckConnect R client.
#' @export

#' @title DuckConnect
#' @description Main interface for cross-language data sharing with DuckDB.
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
    
    #' @field metadata_manager Metadata manager
    metadata_manager = NULL,
    
    #' @field transaction_manager Transaction manager
    transaction_manager = NULL,
    
    #' @description Initialize DuckConnect with a database path
    #' @param db_path Path to the DuckDB database file
    initialize = function(db_path = "duck_connect.duckdb") {
      # Create connection
      connection <- DuckConnection$new(db_path)
      self$conn <- connection$conn
      self$db_path <- connection$db_path
      self$arrow_available <- connection$arrow_available
      
      # Initialize managers
      self$metadata_manager <- MetadataManager$new()
      self$transaction_manager <- TransactionManager$new()
      
      # Setup metadata tables
      self$metadata_manager$create_tables(self$conn)
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
      
      # Set default available languages if not specified
      if (is.null(available_to_languages)) {
        available_to_languages <- c("python", "r", "julia")
      }
      
      # Generate a unique ID for this dataset
      dataset_id <- uuid::UUIDgenerate()
      
      # Generate unique table name
      table_name <- paste0("duck_connect_", gsub("-", "_", dataset_id))
      
      # Check if dataset with this name already exists
      existing <- DBI::dbGetQuery(self$conn, 
        "SELECT id, table_name FROM duck_connect_metadata WHERE name = ?", 
        list(name))
      
      if (nrow(existing) > 0 && !overwrite) {
        stop(paste0("Dataset with name '", name, "' already exists. Use overwrite=TRUE to replace it."))
      }
      
      # Delete existing table if overwriting
      if (nrow(existing) > 0 && overwrite) {
        old_id <- existing$id[1]
        old_table <- existing$table_name[1]
        
        # Delete the old table
        tryCatch({
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", old_table))
        }, error = function(e) {
          warning(paste0("Failed to drop old table: ", e$message))
        })
      }
      
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
      
      # Store metadata
      self$metadata_manager$store_metadata(
        self$conn, 
        dataset_id, 
        name, 
        source_language, 
        description, 
        schema, 
        table_name, 
        available_to_languages,
        overwrite
      )
      
      # Record transaction
      self$transaction_manager$record_transaction(
        self$conn,
        dataset_id,
        "create",
        source_language,
        list(overwrite = overwrite)
      )
      
      # Record statistics
      duration_ms <- as.numeric(difftime(Sys.time(), start_time, units = "secs")) * 1000
      row_count <- DBI::dbGetQuery(self$conn, paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      column_count <- length(schema$columns)
      
      self$transaction_manager$record_stats(
        self$conn,
        dataset_id,
        "create",
        source_language,
        duration_ms,
        0.0,  # Memory usage not available
        row_count,
        column_count
      )
      
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
      
      # Get metadata
      metadata <- self$metadata_manager$get_metadata(self$conn, identifier)
      
      dataset_id <- metadata$id
      table_name <- metadata$table_name
      
      # Check language access
      if (!(language %in% metadata$available_to_languages)) {
        stop(paste0("Dataset '", identifier, "' is not available to language '", language, "'"))
      }
      
      # Record transaction
      self$transaction_manager$record_transaction(
        self$conn,
        dataset_id,
        "read",
        language,
        list(as_arrow = as_arrow)
      )
      
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
      row_count <- nrow(result)
      column_count <- length(metadata$schema$columns)
      
      self$transaction_manager$record_stats(
        self$conn,
        dataset_id,
        "read",
        language,
        duration_ms,
        0.0,  # Memory usage not available
        row_count,
        column_count
      )
      
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
      
      # Get metadata
      metadata <- self$metadata_manager$get_metadata(self$conn, identifier)
      
      dataset_id <- metadata$id
      table_name <- metadata$table_name
      
      # Check language access
      if (!(language %in% metadata$available_to_languages)) {
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
      
      # Update metadata
      self$metadata_manager$update_metadata(
        self$conn,
        dataset_id,
        schema,
        description
      )
      
      # Record transaction
      self$transaction_manager$record_transaction(
        self$conn,
        dataset_id,
        "update",
        language,
        list(description_updated = !is.null(description))
      )
      
      # Record statistics
      duration_ms <- as.numeric(difftime(Sys.time(), start_time, units = "secs")) * 1000
      row_count <- DBI::dbGetQuery(self$conn, paste0("SELECT COUNT(*) as count FROM ", table_name))$count
      column_count <- length(schema$columns)
      
      self$transaction_manager$record_stats(
        self$conn,
        dataset_id,
        "update",
        language,
        duration_ms,
        0.0,  # Memory usage not available
        row_count,
        column_count
      )
      
      message(paste0("Updated dataset '", identifier, "' (ID: ", dataset_id, ")"))
      return(dataset_id)
    },
    
    #' @description Delete a dataset
    #' @param identifier Dataset name or ID
    #' @param language Language performing the deletion
    #' @return TRUE if successful
    delete_dataset = function(identifier, language = "r") {
      # Get metadata
      metadata <- self$metadata_manager$get_metadata(self$conn, identifier)
      
      dataset_id <- metadata$id
      table_name <- metadata$table_name
      
      # Check language access
      if (!(language %in% metadata$available_to_languages)) {
        stop(paste0("Dataset '", identifier, "' is not available to language '", language, "'"))
      }
      
      # Delete the table
      tryCatch({
        DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
      }, error = function(e) {
        warning(paste0("Failed to drop table: ", e$message))
      })
      
      # Delete metadata
      self$metadata_manager$delete_metadata(self$conn, dataset_id)
      
      # Record transaction
      self$transaction_manager$record_transaction(
        self$conn,
        dataset_id,
        "delete",
        language,
        NULL
      )
      
      message(paste0("Deleted dataset '", identifier, "' (ID: ", dataset_id, ")"))
      return(TRUE)
    },
    
    #' @description List all available datasets
    #' @param language Language requesting the list (for access control)
    #' @return Data frame containing dataset information
    list_datasets = function(language = "r") {
      return(self$metadata_manager$list_datasets(self$conn, language))
    },
    
    #' @description Execute a SQL query
    #' @param query SQL query to execute
    #' @param language Language performing the query (for stats)
    #' @param as_arrow Whether to return as Arrow Table
    #' @return Data frame with query results
    execute_query = function(query, language = "r", as_arrow = FALSE) {
      start_time <- Sys.time()
      
      # Execute the query
      result <- tryCatch({
        if (as_arrow && self$arrow_available && requireNamespace("arrow", quietly = TRUE)) {
          # Execute with Arrow
          DBI::dbExecute(self$conn, "LOAD arrow")
          DBI::dbGetQuery(self$conn, paste0(query, " USING arrow"))
        } else {
          # Standard execution
          DBI::dbGetQuery(self$conn, query)
        }
      }, error = function(e) {
        stop(paste0("Error executing query: ", e))
      })
      
      # Record statistics
      duration_ms <- as.numeric(difftime(Sys.time(), start_time, units = "secs")) * 1000
      row_count <- nrow(result)
      column_count <- ncol(result)
      
      self$transaction_manager$record_stats(
        self$conn,
        'N/A',
        "query",
        language,
        duration_ms,
        0.0,  # Memory usage not available
        row_count,
        column_count
      )
      
      return(result)
    },
    
    #' @description Get a reference to a DuckDB table for efficient cross-language access
    #' @param identifier Dataset name or ID
    #' @param language Language requesting the reference
    #' @return Table reference string that can be used directly in SQL
    get_table_reference = function(identifier, language = "r") {
      # Get metadata
      metadata <- self$metadata_manager$get_metadata(self$conn, identifier)
      
      dataset_id <- metadata$id
      table_name <- metadata$table_name
      
      # Check language access
      if (!(language %in% metadata$available_to_languages)) {
        stop(paste0("Dataset '", identifier, "' is not available to language '", language, "'"))
      }
      
      # Record transaction for the reference access
      self$transaction_manager$record_transaction(
        self$conn,
        dataset_id,
        "reference",
        language,
        NULL
      )
      
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
      # Get metadata for target dataset
      target_metadata <- self$metadata_manager$get_metadata(self$conn, target_dataset)
      target_id <- target_metadata$id
      
      # Get metadata for source dataset
      source_metadata <- self$metadata_manager$get_metadata(self$conn, source_dataset)
      source_id <- source_metadata$id
      
      # Record lineage
      lineage_id <- self$transaction_manager$record_lineage(
        self$conn,
        target_id,
        source_id,
        transformation,
        language
      )
      
      message(paste0("Registered transformation from '", source_dataset, "' to '", target_dataset, "'"))
      return(lineage_id)
    },
    
    #' @description Get the lineage for a dataset
    #' @param identifier Dataset name or ID
    #' @return List with ancestors and descendants
    get_lineage = function(identifier) {
      # Get metadata
      metadata <- self$metadata_manager$get_metadata(self$conn, identifier)
      dataset_id <- metadata$id
      
      # Get lineage
      return(self$transaction_manager$get_lineage(self$conn, dataset_id))
    },
    
    #' @description Get performance metrics for a dataset
    #' @param identifier Dataset name or ID
    #' @return List with performance metrics
    get_performance_metrics = function(identifier) {
      # Get metadata
      metadata <- self$metadata_manager$get_metadata(self$conn, identifier)
      dataset_id <- metadata$id
      
      # Get performance summary
      return(self$transaction_manager$get_performance_summary(self$conn, dataset_id))
    },
    
    #' @description Close the connection to the database
    close = function() {
      if (!is.null(self$conn)) {
        DBI::dbDisconnect(self$conn, shutdown = TRUE)
        self$conn <- NULL
      }
    }
  )
)

#' @title RDuckContext
#' @description R context manager for DuckConnect nodes
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
          if (requireNamespace("yaml", quietly = TRUE)) {
            yaml_data <- yaml::read_yaml(meta_path)
            return(yaml_data)
          } else if (requireNamespace("jsonlite", quietly = TRUE)) {
            # Try as JSON if YAML fails or is not available
            json_data <- jsonlite::fromJSON(meta_path, simplifyVector = FALSE)
            return(json_data)
          } else {
            warning("Neither yaml nor jsonlite package available for parsing metadata")
            return(list())
          }
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