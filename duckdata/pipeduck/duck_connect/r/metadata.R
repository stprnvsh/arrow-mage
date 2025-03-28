#' Metadata management for DuckConnect in R
#'
#' This module provides metadata management functionalities for DuckConnect.
#' @export

#' @title MetadataManager
#' @description Manages dataset metadata for DuckConnect.
#' @export
MetadataManager <- R6::R6Class(
  "MetadataManager",
  
  public = list(
    #' @description Create metadata tables if they don't exist
    #' @param conn DuckDB connection
    create_tables = function(conn) {
      # Create metadata tables for tracking data
      DBI::dbExecute(conn, "
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
      DBI::dbExecute(conn, "
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
      DBI::dbExecute(conn, "
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
      DBI::dbExecute(conn, "
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
    },
    
    #' @description Store metadata for a dataset
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @param name Dataset name
    #' @param source_language Source language
    #' @param description Dataset description
    #' @param schema Dataset schema as a list
    #' @param table_name Actual DuckDB table name
    #' @param available_to_languages Languages that can access the dataset
    #' @param overwrite Whether to overwrite existing dataset
    #' @return TRUE if successful
    store_metadata = function(conn, dataset_id, name, source_language, 
                            description, schema, table_name, 
                            available_to_languages, overwrite = FALSE) {
      # Convert schema and languages to JSON
      schema_json <- jsonlite::toJSON(schema, auto_unbox = TRUE)
      languages_json <- jsonlite::toJSON(available_to_languages, auto_unbox = TRUE)
      
      # Check if dataset with this name already exists
      existing <- DBI::dbGetQuery(conn, 
        "SELECT id, table_name FROM duck_connect_metadata WHERE name = ?", 
        list(name))
      
      if (nrow(existing) > 0) {
        if (!overwrite) {
          stop(paste0("Dataset with name '", name, "' already exists. Use overwrite=TRUE to replace it."))
        }
        
        # Delete existing metadata
        old_id <- existing$id[1]
        old_table <- existing$table_name[1]
        
        # Delete old metadata
        DBI::dbExecute(conn, "DELETE FROM duck_connect_metadata WHERE id = ?", list(old_id))
      }
      
      # Insert metadata
      DBI::dbExecute(conn, "
        INSERT INTO duck_connect_metadata 
        (id, name, source_language, created_at, updated_at, description, schema, table_name, available_to_languages)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?)
      ", list(
        dataset_id, name, source_language, description, schema_json, table_name, languages_json
      ))
      
      return(TRUE)
    },
    
    #' @description Retrieve metadata for a dataset
    #' @param conn DuckDB connection
    #' @param identifier Dataset name or ID
    #' @return List with dataset metadata
    get_metadata = function(conn, identifier) {
      # Look up metadata
      metadata <- DBI::dbGetQuery(conn, "
        SELECT * FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
      ", list(identifier, identifier))
      
      if (nrow(metadata) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      # Convert JSON back to list
      metadata$schema <- jsonlite::fromJSON(metadata$schema)
      metadata$available_to_languages <- jsonlite::fromJSON(metadata$available_to_languages)
      
      return(as.list(metadata[1,]))
    },
    
    #' @description Update metadata for a dataset
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @param schema New schema (optional)
    #' @param description New description (optional)
    #' @param available_to_languages New languages (optional)
    #' @return TRUE if successful
    update_metadata = function(conn, dataset_id, schema = NULL, description = NULL, 
                             available_to_languages = NULL) {
      # Check if dataset exists
      metadata <- DBI::dbGetQuery(conn, "
        SELECT id FROM duck_connect_metadata 
        WHERE id = ?
      ", list(dataset_id))
      
      if (nrow(metadata) == 0) {
        stop(paste0("Dataset with ID '", dataset_id, "' not found"))
      }
      
      # Build update statements
      updates <- list()
      params <- list()
      
      # Always update timestamp
      updates <- c(updates, "updated_at = CURRENT_TIMESTAMP")
      
      if (!is.null(schema)) {
        updates <- c(updates, "schema = ?")
        params <- c(params, list(jsonlite::toJSON(schema, auto_unbox = TRUE)))
      }
      
      if (!is.null(description)) {
        updates <- c(updates, "description = ?")
        params <- c(params, list(description))
      }
      
      if (!is.null(available_to_languages)) {
        updates <- c(updates, "available_to_languages = ?")
        params <- c(params, list(jsonlite::toJSON(available_to_languages, auto_unbox = TRUE)))
      }
      
      # If we have something to update
      if (length(updates) > 1) {
        update_sql <- paste0("
          UPDATE duck_connect_metadata 
          SET ", paste(updates, collapse = ", "), "
          WHERE id = ?
        ")
        
        # Add dataset_id to params
        params <- c(params, list(dataset_id))
        
        # Execute update
        DBI::dbExecute(conn, update_sql, params)
      }
      
      return(TRUE)
    },
    
    #' @description Delete metadata for a dataset
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @return TRUE if successful
    delete_metadata = function(conn, dataset_id) {
      DBI::dbExecute(conn, "DELETE FROM duck_connect_metadata WHERE id = ?", list(dataset_id))
      return(TRUE)
    },
    
    #' @description List all available datasets 
    #' @param conn DuckDB connection
    #' @param language Language requesting the list (for access control)
    #' @return Data frame containing dataset information
    list_datasets = function(conn, language = "r") {
      # Get all datasets
      datasets <- DBI::dbGetQuery(conn, "
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
    
    #' @description Check if a dataset is available to a language
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @param language Language to check
    #' @return TRUE if available
    is_available_to_language = function(conn, dataset_id, language) {
      metadata <- DBI::dbGetQuery(conn, "
        SELECT available_to_languages FROM duck_connect_metadata 
        WHERE id = ?
      ", list(dataset_id))
      
      if (nrow(metadata) == 0) {
        return(FALSE)
      }
      
      available_languages <- jsonlite::fromJSON(metadata$available_to_languages[1])
      return(language %in% available_languages)
    }
  )
) 