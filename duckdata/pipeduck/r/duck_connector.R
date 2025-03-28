#' Advanced DuckDB connector for efficient cross-language data sharing and processing.
#'
#' This module provides optimized connections to DuckDB with advanced features.
#' @export
DuckConnector <- R6::R6Class(
  "DuckConnector",
  
  public = list(
    #' @field db_path Path to the DuckDB database file
    db_path = NULL,
    
    #' @field conn DuckDB connection
    conn = NULL,
    
    #' @field config Configuration options
    config = NULL,
    
    #' @description Initialize DuckConnector with advanced settings.
    #' @param db_path Path to DuckDB database or ":memory:" for in-memory DB
    #' @param config Optional configuration dictionary with advanced settings
    initialize = function(db_path = ":memory:", config = NULL) {
      self$db_path <- db_path
      self$config <- config %||% list()
      
      # Create connection with optimized settings
      self$conn <- self$_create_optimized_connection()
      
      # Setup extensions
      self$_setup_extensions()
      
      # Setup advanced features
      self$_setup_advanced_features()
    },
    
    #' @description Create an optimized DuckDB connection with advanced settings
    _create_optimized_connection = function() {
      conn <- DBI::dbConnect(duckdb::duckdb(), self$db_path)
      
      # Set memory limit based on config or default to 80% of available RAM
      memory_limit <- self$config$memory_limit %||% "80%"
      DBI::dbExecute(conn, paste0("PRAGMA memory_limit='", memory_limit, "'"))
      
      # Enable MVCC for concurrent transactions
      DBI::dbExecute(conn, "PRAGMA enable_mvcc")
      
      # Set threads based on config or default to num_cores - 1
      num_cores <- max(1, parallel::detectCores() - 1)
      threads <- self$config$threads %||% num_cores
      DBI::dbExecute(conn, paste0("PRAGMA threads=", threads))
      
      # Enable parallelism for better performance
      DBI::dbExecute(conn, "PRAGMA force_parallelism")
      
      # Enable object cache
      DBI::dbExecute(conn, "PRAGMA enable_object_cache")
      
      # Enable progress bar for long-running queries
      if (self$config$show_progress %||% TRUE) {
        DBI::dbExecute(conn, "PRAGMA enable_progress_bar")
      }
      
      # External data cache - for out-of-core processing
      external_cache <- self$config$external_cache_directory
      if (!is.null(external_cache)) {
        dir.create(external_cache, showWarnings = FALSE, recursive = TRUE)
        DBI::dbExecute(conn, paste0("PRAGMA temp_directory='", external_cache, "'"))
      }
      
      # External buffer for large datasets
      if (self$config$use_external_buffer %||% TRUE) {
        DBI::dbExecute(conn, "PRAGMA external_threads=4")
        buffer_size <- self$config$external_buffer_size %||% "4GB"
        DBI::dbExecute(conn, paste0("PRAGMA memory_limit='", buffer_size, "'"))
      }
      
      return(conn)
    },
    
    #' @description Setup DuckDB extensions for advanced functionality
    _setup_extensions = function() {
      tryCatch({
        # Always install and load Arrow
        DBI::dbExecute(self$conn, "INSTALL arrow")
        DBI::dbExecute(self$conn, "LOAD arrow")
      }, error = function(e) {
        warning("Could not load arrow extension: ", e$message)
      })
      
      tryCatch({
        # JSON extension for native JSON support
        DBI::dbExecute(self$conn, "INSTALL json")
        DBI::dbExecute(self$conn, "LOAD json")
      }, error = function(e) {
        warning("Could not load json extension: ", e$message)
      })
      
      tryCatch({
        # Parquet extension for native Parquet support
        DBI::dbExecute(self$conn, "INSTALL parquet")
        DBI::dbExecute(self$conn, "LOAD parquet")
      }, error = function(e) {
        warning("Could not load parquet extension: ", e$message)
      })
      
      # Install additional extensions based on configuration
      extensions <- self$config$extensions %||% list()
      
      if (extensions$httpfs %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL httpfs")
          DBI::dbExecute(self$conn, "LOAD httpfs")
        }, error = function(e) {
          warning("Could not load httpfs extension: ", e$message)
        })
      }
      
      if (extensions$icu %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL icu")
          DBI::dbExecute(self$conn, "LOAD icu")
        }, error = function(e) {
          warning("Could not load icu extension: ", e$message)
        })
      }
      
      if (extensions$fts %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL fts")
          DBI::dbExecute(self$conn, "LOAD fts")
        }, error = function(e) {
          warning("Could not load fts extension: ", e$message)
        })
      }
      
      if (extensions$spatial %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL spatial")
          DBI::dbExecute(self$conn, "LOAD spatial")
        }, error = function(e) {
          warning("Could not load spatial extension: ", e$message)
        })
      }
      
      if (extensions$sqlite %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL sqlite")
          DBI::dbExecute(self$conn, "LOAD sqlite")
        }, error = function(e) {
          warning("Could not load sqlite extension: ", e$message)
        })
      }
      
      # Check for custom extensions
      custom_extensions <- extensions$custom %||% c()
      for (ext in custom_extensions) {
        tryCatch({
          DBI::dbExecute(self$conn, paste0("INSTALL ", ext))
          DBI::dbExecute(self$conn, paste0("LOAD ", ext))
        }, error = function(e) {
          warning(paste0("Failed to load extension ", ext, ": ", e$message))
        })
      }
    },
    
    #' @description Setup advanced features for DuckDB
    _setup_advanced_features = function() {
      # Fine tune the analyzer based on configuration
      optimizer <- self$config$optimizer %||% list()
      
      if (optimizer$join_order %||% FALSE) {
        DBI::dbExecute(self$conn, "PRAGMA default_join_type='adaptive'")
      }
      
      if (optimizer$filter_pushdown %||% TRUE) {
        DBI::dbExecute(self$conn, "PRAGMA enable_filter_pushdown")
      }
      
      if (optimizer$lazy_analyze %||% TRUE) {
        DBI::dbExecute(self$conn, "PRAGMA explain_output='all'")
      }
      
      # Set up optimizer for out-of-core processing
      out_of_core <- self$config$out_of_core %||% list()
      if (out_of_core$enabled %||% TRUE) {
        temp_dir <- "/tmp/duckdb_tmp"
        dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
        DBI::dbExecute(self$conn, paste0("PRAGMA temp_directory='", temp_dir, "'"))
        DBI::dbExecute(self$conn, "PRAGMA memory_limit='8GB'")
        DBI::dbExecute(self$conn, "PRAGMA threads=8")
      }
    },
    
    #' @description Create a table with advanced indexing for fast lookups
    #' @param table_name Name of the table to create
    #' @param data Data to insert (data.frame or SQL query)
    #' @param index_columns Columns to create indexes on
    #' @param index_type Type of index (art, btree, etc.)
    #' @param replace Whether to replace an existing table
    #' @return TRUE if successful
    create_indexed_table = function(table_name, data, 
                                  index_columns = NULL,
                                  index_type = "art",
                                  replace = FALSE) {
      # Handle different input types
      if (is.data.frame(data)) {
        # Create from R data.frame
        if (replace) {
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
        }
        DBI::dbWriteTable(self$conn, table_name, data, overwrite = replace)
      } else if (is.character(data) && length(data) == 1) {
        # Create from SQL query
        if (replace) {
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
        }
        DBI::dbExecute(self$conn, paste0("CREATE TABLE ", table_name, " AS ", data))
      } else if (requireNamespace("arrow", quietly = TRUE) && 
                inherits(data, "Table")) {
        # Create from Arrow Table
        if (replace) {
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
        }
        arrow_name <- paste0("arrow_", table_name)
        arrow::to_duckdb(data, conn = self$conn, table_name = arrow_name)
        DBI::dbExecute(self$conn, paste0("CREATE TABLE ", table_name, 
                                        " AS SELECT * FROM ", arrow_name))
        DBI::dbExecute(self$conn, paste0("DROP VIEW IF EXISTS ", arrow_name))
      } else {
        stop(paste0("Unsupported data type: ", class(data)[1]))
      }
      
      # Create indexes for specified columns
      if (!is.null(index_columns)) {
        for (col in index_columns) {
          index_name <- paste0("idx_", table_name, "_", col)
          DBI::dbExecute(self$conn, paste0(
            "CREATE INDEX ", index_name, 
            " ON ", table_name, "(", col, ") ",
            "USING ", index_type
          ))
        }
      }
      
      return(TRUE)
    },
    
    #' @description Execute multiple queries in a single MVCC transaction for atomic operations
    #' @param queries List of SQL queries to execute
    #' @return TRUE if transaction successful
    execute_mvcc_transaction = function(queries) {
      # Start transaction
      DBI::dbExecute(self$conn, "BEGIN TRANSACTION")
      
      tryCatch({
        # Execute each query
        for (query in queries) {
          DBI::dbExecute(self$conn, query)
        }
        
        # Commit transaction
        DBI::dbExecute(self$conn, "COMMIT")
        return(TRUE)
      }, error = function(e) {
        # Rollback on error
        DBI::dbExecute(self$conn, "ROLLBACK")
        warning(paste0("Transaction failed: ", e$message))
        stop(e)
      })
    },
    
    #' @description Load data directly from Parquet file with zero copy where possible
    #' @param parquet_file Path to Parquet file
    #' @param table_name Optional table name to create
    #' @return Data frame or table name if table created
    load_parquet_direct = function(parquet_file, table_name = NULL) {
      # Use native Parquet support to efficiently load data
      query <- paste0("SELECT * FROM read_parquet('", parquet_file, "')")
      
      # Either create a table or return as data frame
      if (!is.null(table_name)) {
        create_query <- paste0(
          "CREATE OR REPLACE TABLE ", table_name, 
          " AS SELECT * FROM read_parquet('", parquet_file, "')"
        )
        DBI::dbExecute(self$conn, create_query)
        return(table_name)
      } else {
        return(DBI::dbGetQuery(self$conn, query))
      }
    },
    
    #' @description Load a directory of partitioned Parquet files
    #' @param parquet_dir Directory containing partitioned Parquet files
    #' @param table_name Table name to create
    #' @param partition_column Optional column name that defines partitions
    #' @return Table name
    load_parquet_partitioned = function(parquet_dir, table_name, 
                                      partition_column = NULL) {
      query <- if (is.null(partition_column)) {
        paste0(
          "CREATE OR REPLACE TABLE ", table_name, 
          " AS SELECT * FROM parquet_scan('", parquet_dir, "/*')"
        )
      } else {
        paste0(
          "CREATE OR REPLACE TABLE ", table_name, 
          " AS SELECT * FROM parquet_scan('", parquet_dir, "/*', ",
          "HIVE_PARTITIONING=1)"
        )
      }
      
      DBI::dbExecute(self$conn, query)
      return(table_name)
    },
    
    #' @description Load data directly from JSON file
    #' @param json_file Path to JSON file
    #' @param table_name Optional table name to create
    #' @param auto_detect Whether to auto-detect schema
    #' @return Data frame or table name if table created
    load_json_direct = function(json_file, table_name = NULL, auto_detect = TRUE) {
      # Use native JSON support to efficiently load data
      auto_param <- if(auto_detect) ", AUTO_DETECT=TRUE" else ""
      query <- paste0("SELECT * FROM read_json('", json_file, "'", auto_param, ")")
      
      # Either create a table or return as data frame
      if (!is.null(table_name)) {
        create_query <- paste0(
          "CREATE OR REPLACE TABLE ", table_name, 
          " AS ", query
        )
        DBI::dbExecute(self$conn, create_query)
        return(table_name)
      } else {
        return(DBI::dbGetQuery(self$conn, query))
      }
    },
    
    #' @description Execute a query and return as Arrow table if arrow package is available
    #' @param query SQL query to execute
    #' @return Data frame or Arrow table if available
    execute_arrow_query = function(query) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        warning("Arrow package not available, returning as data.frame")
        return(DBI::dbGetQuery(self$conn, query))
      }
      
      # Use Arrow mode for query execution
      DBI::dbExecute(self$conn, "PRAGMA threads=8")
      DBI::dbExecute(self$conn, "LOAD arrow")
      return(DBI::dbGetQuery(self$conn, paste0(query, " USING arrow")))
    },
    
    #' @description Create a table optimized for out-of-core processing with large datasets
    #' @param table_name Name of the table to create
    #' @param data_source Data source (file path or query)
    #' @param source_type Type of source ('parquet', 'csv', 'json', 'query')
    #' @param replace Whether to replace an existing table
    #' @return TRUE if successful
    create_out_of_core_table = function(table_name, data_source, 
                                      source_type = "parquet", 
                                      replace = FALSE) {
      # Create the table
      if (replace) {
        DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
      }
      
      # Set up for out-of-core processing
      DBI::dbExecute(self$conn, "PRAGMA external_threads=8")
      DBI::dbExecute(self$conn, "PRAGMA memory_limit='4GB'")
      
      # Create table based on source type
      if (source_type == "parquet") {
        DBI::dbExecute(self$conn, paste0(
          "CREATE TABLE ", table_name, 
          " AS SELECT * FROM read_parquet('", data_source, "')"
        ))
      } else if (source_type == "csv") {
        DBI::dbExecute(self$conn, paste0(
          "CREATE TABLE ", table_name, 
          " AS SELECT * FROM read_csv('", data_source, "', ",
          "AUTO_DETECT=TRUE, SAMPLE_SIZE=1000)"
        ))
      } else if (source_type == "json") {
        DBI::dbExecute(self$conn, paste0(
          "CREATE TABLE ", table_name, 
          " AS SELECT * FROM read_json('", data_source, "', ",
          "AUTO_DETECT=TRUE)"
        ))
      } else if (source_type == "query") {
        DBI::dbExecute(self$conn, paste0(
          "CREATE TABLE ", table_name, 
          " AS ", data_source
        ))
      } else {
        stop(paste0("Unsupported source type: ", source_type))
      }
      
      return(TRUE)
    },
    
    #' @description Optimize database by vacuuming and analyzing tables
    #' @param table_name Optional table to analyze, or NULL for all tables
    vacuum_analyze = function(table_name = NULL) {
      # Run vacuum to reclaim space
      DBI::dbExecute(self$conn, "VACUUM")
      
      # Analyze tables for query optimization
      if (is.null(table_name)) {
        # Get all table names
        tables <- DBI::dbGetQuery(self$conn, "
          SELECT name FROM sqlite_master 
          WHERE type='table' AND name NOT LIKE 'sqlite_%'
        ")$name
        
        # Analyze each table
        for (tbl in tables) {
          DBI::dbExecute(self$conn, paste0("ANALYZE ", tbl))
        }
      } else {
        DBI::dbExecute(self$conn, paste0("ANALYZE ", table_name))
      }
    },
    
    #' @description Create a user-defined function in DuckDB
    #' @param function_name Name of the function to create
    #' @param function R function to register
    #' @param return_type Return type of the function
    #' @param parameter_types Vector of parameter types
    create_user_defined_function = function(function_name, function, 
                                         return_type, parameter_types) {
      duckdb::duckdb_register_r(
        self$conn, 
        function_name, 
        function, 
        return_type, 
        parameter_types
      )
    },
    
    #' @description Export query results to Arrow IPC file
    #' @param query SQL query to execute
    #' @param output_file Path to output file
    #' @return Path to created file
    export_arrow_ipc = function(query, output_file) {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("Arrow package required for Arrow IPC export")
      }
      
      # Execute query and get results as Arrow
      result <- self$execute_arrow_query(query)
      
      # Write to IPC file
      arrow::write_ipc_file(result, output_file)
      return(output_file)
    },
    
    #' @description Export query results to Parquet file
    #' @param query SQL query to execute
    #' @param output_file Path to output file
    #' @param compression Compression method
    #' @return Path to created file
    export_parquet = function(query, output_file, compression = "snappy") {
      if (!requireNamespace("arrow", quietly = TRUE)) {
        stop("Arrow package required for Parquet export")
      }
      
      # Execute query and get results as Arrow
      result <- self$execute_arrow_query(query)
      
      # Write to Parquet file
      arrow::write_parquet(result, output_file, compression = compression)
      return(output_file)
    },
    
    #' @description Execute a query in partitions for large dataset processing
    #' @param query SQL query to execute
    #' @param partition_column Column to partition by
    #' @param num_partitions Number of partitions
    #' @param output_dir Directory for output files
    #' @return Path to output directory
    execute_partitioned_query = function(query, partition_column, 
                                       num_partitions = 8, 
                                       output_dir = tempdir()) {
      # Create output directory
      dir.create(output_dir, showWarnings = FALSE, recursive = TRUE)
      
      # Extract the base table from the query
      # This is a simplified approach - in practice, more robust SQL parsing might be needed
      from_parts <- regmatches(
        query,
        regexec("FROM\\s+([^\\s]+)", query, ignore.case = TRUE)
      )[[1]]
      
      if (length(from_parts) < 2) {
        stop("Could not extract table name from query")
      }
      
      base_table <- from_parts[2]
      
      # Generate partition ranges
      min_max <- DBI::dbGetQuery(
        self$conn, 
        paste0("SELECT MIN(", partition_column, ") as min_val, ",
              "MAX(", partition_column, ") as max_val ",
              "FROM ", base_table)
      )
      
      min_val <- min_max$min_val[1]
      max_val <- min_max$max_val[1]
      
      if (is.na(min_val) || is.na(max_val)) {
        stop("Could not determine partition ranges")
      }
      
      step <- (max_val - min_val) / num_partitions
      
      # Execute each partition and save to a file
      partition_files <- character(num_partitions)
      
      for (i in 1:num_partitions) {
        lower_bound <- min_val + (i - 1) * step
        upper_bound <- if (i == num_partitions) max_val else min_val + i * step
        
        partition_query <- paste0(
          query, 
          " WHERE ", partition_column, " >= ", lower_bound,
          " AND ", partition_column, " < ", upper_bound
        )
        
        output_file <- file.path(output_dir, paste0("partition_", i, ".parquet"))
        self$export_parquet(partition_query, output_file)
        partition_files[i] <- output_file
      }
      
      return(output_dir)
    },
    
    #' @description Create a temporary column store for efficient access
    #' @param data Data frame or Arrow table to store
    #' @param name Optional name for the store (auto-generated if NULL)
    #' @return Name of the created store
    create_temporary_column_store = function(data, name = NULL) {
      # Generate random name if not provided
      if (is.null(name)) {
        name <- paste0("temp_store_", gsub("-", "_", uuid::UUIDgenerate()))
      }
      
      # Create temporary table
      if (is.data.frame(data)) {
        DBI::dbWriteTable(self$conn, name, data, temporary = TRUE)
      } else if (requireNamespace("arrow", quietly = TRUE) && 
                inherits(data, "Table")) {
        arrow::to_duckdb(data, conn = self$conn, table_name = name)
      } else {
        stop(paste0("Unsupported data type: ", class(data)[1]))
      }
      
      return(name)
    },
    
    #' @description Close the connection and release resources
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