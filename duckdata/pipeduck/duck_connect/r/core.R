#' Core functionality for DuckConnect in R
#'
#' This module provides core connection management for DuckConnect R client.
#' @export

#' @title DuckConnection
#' @description Manages database connections and configuration for DuckConnect.
#' @export
DuckConnection <- R6::R6Class(
  "DuckConnection",
  
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
        warning("Failed to configure DuckDB performance settings: ", e$message)
      })
      
      # Load Arrow extension in DuckDB if available
      tryCatch({
        DBI::dbExecute(self$conn, "INSTALL arrow")
        DBI::dbExecute(self$conn, "LOAD arrow")
        self$arrow_available <- TRUE
      }, error = function(e) {
        warning("Arrow extension installation or loading failed: ", e$message)
        warning("Some Arrow functionality may not be available")
        self$arrow_available <- FALSE
      })
    },
    
    #' @description Setup database schema for metadata if needed
    #' @param metadata_manager Metadata manager instance to use
    setup_schema = function(metadata_manager) {
      metadata_manager$create_tables(self$conn)
    },
    
    #' @description Execute a query and return the results
    #' @param query SQL query to execute
    #' @param params Parameters for parameterized query
    #' @param as_arrow Whether to return as Arrow Table
    #' @return Data frame with query results
    execute_query = function(query, params = NULL, as_arrow = FALSE) {
      # Execute using Arrow if requested and available
      if (as_arrow && self$arrow_available && requireNamespace("arrow", quietly = TRUE)) {
        tryCatch({
          # Use Arrow for the query
          if (is.null(params)) {
            return(DBI::dbGetQuery(self$conn, paste0(query, " USING arrow")))
          } else {
            stmt <- DBI::dbSendQuery(self$conn, paste0(query, " USING arrow"))
            DBI::dbBind(stmt, params)
            result <- DBI::dbFetch(stmt)
            DBI::dbClearResult(stmt)
            return(result)
          }
        }, error = function(e) {
          warning("Arrow query failed, falling back to standard query: ", e$message)
          # Fall back to standard execution on error
        })
      }
      
      # Standard execution
      if (is.null(params)) {
        return(DBI::dbGetQuery(self$conn, query))
      } else {
        stmt <- DBI::dbSendQuery(self$conn, query)
        DBI::dbBind(stmt, params)
        result <- DBI::dbFetch(stmt)
        DBI::dbClearResult(stmt)
        return(result)
      }
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

# Helper function for NULL coalescing
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
} 