#' Transaction management for DuckConnect in R
#'
#' This module provides transaction tracking and statistics for DuckConnect.
#' @export

#' @title TransactionManager
#' @description Manages transactions and statistics for DuckConnect.
#' @export
TransactionManager <- R6::R6Class(
  "TransactionManager",
  
  public = list(
    #' @description Record a transaction
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @param operation Operation type ('create', 'read', 'update', 'delete')
    #' @param language Language performing the operation
    #' @param details Additional details as a list
    #' @return Transaction ID
    record_transaction = function(conn, dataset_id, operation, language, details = NULL) {
      transaction_id <- uuid::UUIDgenerate()
      
      # Convert details to JSON if present
      details_json <- if (is.null(details)) "{}" else jsonlite::toJSON(details, auto_unbox = TRUE)
      
      # Insert transaction record
      DBI::dbExecute(conn, "
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
      ", list(
        transaction_id, 
        dataset_id, 
        operation, 
        language, 
        details_json
      ))
      
      return(transaction_id)
    },
    
    #' @description Record statistics for an operation
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @param operation Operation type
    #' @param language Language performing the operation
    #' @param duration_ms Duration in milliseconds
    #' @param memory_usage_mb Memory usage in MB
    #' @param row_count Number of rows processed
    #' @param column_count Number of columns processed
    #' @return Statistics ID
    record_stats = function(conn, dataset_id, operation, language, 
                          duration_ms, memory_usage_mb = 0, 
                          row_count = 0, column_count = 0) {
      stats_id <- uuid::UUIDgenerate()
      
      # Insert statistics record
      DBI::dbExecute(conn, "
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
      ", list(
        stats_id,
        dataset_id,
        operation,
        language,
        duration_ms,
        memory_usage_mb,
        row_count,
        column_count
      ))
      
      return(stats_id)
    },
    
    #' @description Get transaction history for a dataset
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @param limit Maximum number of transactions to return
    #' @return Data frame with transaction history
    get_transaction_history = function(conn, dataset_id, limit = 100) {
      transactions <- DBI::dbGetQuery(conn, "
        SELECT * FROM duck_connect_transactions
        WHERE dataset_id = ?
        ORDER BY timestamp DESC
        LIMIT ?
      ", list(dataset_id, limit))
      
      # Parse details JSON if there are any transactions
      if (nrow(transactions) > 0) {
        for (i in 1:nrow(transactions)) {
          transactions$details[i] <- jsonlite::prettify(transactions$details[i])
        }
      }
      
      return(transactions)
    },
    
    #' @description Get performance statistics for a dataset
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @param operation Optional operation type to filter by
    #' @param language Optional language to filter by
    #' @return Data frame with statistics
    get_stats = function(conn, dataset_id, operation = NULL, language = NULL) {
      # Build query with optional filters
      query <- "
        SELECT * FROM duck_connect_stats
        WHERE dataset_id = ?
      "
      params <- list(dataset_id)
      
      if (!is.null(operation)) {
        query <- paste0(query, " AND operation = ?")
        params <- c(params, list(operation))
      }
      
      if (!is.null(language)) {
        query <- paste0(query, " AND language = ?")
        params <- c(params, list(language))
      }
      
      query <- paste0(query, " ORDER BY timestamp DESC")
      
      # Execute query
      return(DBI::dbGetQuery(conn, query, params))
    },
    
    #' @description Get performance summary for a dataset
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @return List with performance summary
    get_performance_summary = function(conn, dataset_id) {
      summary_query <- "
        SELECT 
          operation,
          COUNT(*) as operation_count,
          AVG(duration_ms) as avg_duration_ms,
          MIN(duration_ms) as min_duration_ms,
          MAX(duration_ms) as max_duration_ms,
          SUM(row_count) as total_rows_processed,
          MAX(row_count) as max_rows
        FROM duck_connect_stats
        WHERE dataset_id = ?
        GROUP BY operation
      "
      
      summary <- DBI::dbGetQuery(conn, summary_query, list(dataset_id))
      
      # Get top users (languages)
      users_query <- "
        SELECT 
          language,
          COUNT(*) as operation_count
        FROM duck_connect_stats
        WHERE dataset_id = ?
        GROUP BY language
        ORDER BY operation_count DESC
      "
      
      top_users <- DBI::dbGetQuery(conn, users_query, list(dataset_id))
      
      # Get trend over time (last 10 operations)
      trend_query <- "
        SELECT 
          operation,
          language,
          timestamp,
          duration_ms,
          row_count
        FROM duck_connect_stats
        WHERE dataset_id = ?
        ORDER BY timestamp DESC
        LIMIT 10
      "
      
      trend <- DBI::dbGetQuery(conn, trend_query, list(dataset_id))
      
      return(list(
        summary = summary,
        top_users = top_users,
        recent_trend = trend
      ))
    },
    
    #' @description Get aggregated performance metrics across all datasets
    #' @param conn DuckDB connection
    #' @return List with performance metrics
    get_system_metrics = function(conn) {
      # Operations by language
      lang_ops_query <- "
        SELECT 
          language,
          operation,
          COUNT(*) as operation_count,
          AVG(duration_ms) as avg_duration_ms
        FROM duck_connect_stats
        GROUP BY language, operation
        ORDER BY language, operation
      "
      
      lang_ops <- DBI::dbGetQuery(conn, lang_ops_query)
      
      # Dataset usage
      dataset_usage_query <- "
        SELECT 
          d.name as dataset_name,
          COUNT(s.id) as operation_count,
          MAX(s.timestamp) as last_used
        FROM duck_connect_metadata d
        LEFT JOIN duck_connect_stats s ON d.id = s.dataset_id
        GROUP BY d.name
        ORDER BY operation_count DESC
      "
      
      dataset_usage <- DBI::dbGetQuery(conn, dataset_usage_query)
      
      # System performance over time (daily)
      daily_perf_query <- "
        SELECT 
          DATE(timestamp) as date,
          COUNT(*) as operation_count,
          AVG(duration_ms) as avg_duration_ms,
          SUM(row_count) as rows_processed
        FROM duck_connect_stats
        GROUP BY DATE(timestamp)
        ORDER BY date DESC
        LIMIT 30
      "
      
      daily_perf <- DBI::dbGetQuery(conn, daily_perf_query)
      
      return(list(
        language_operations = lang_ops,
        dataset_usage = dataset_usage,
        daily_performance = daily_perf
      ))
    },
    
    #' @description Record a lineage relationship between datasets
    #' @param conn DuckDB connection
    #' @param target_dataset_id Target dataset ID
    #' @param source_dataset_id Source dataset ID
    #' @param transformation Description of the transformation
    #' @param language Language that performed the transformation
    #' @return Lineage ID
    record_lineage = function(conn, target_dataset_id, source_dataset_id, 
                            transformation, language) {
      lineage_id <- uuid::UUIDgenerate()
      
      # Insert lineage record
      DBI::dbExecute(conn, "
        INSERT INTO duck_connect_lineage
        (id, target_dataset_id, source_dataset_id, transformation, language, timestamp)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
      ", list(lineage_id, target_dataset_id, source_dataset_id, transformation, language))
      
      return(lineage_id)
    },
    
    #' @description Get lineage for a dataset (both ancestors and descendants)
    #' @param conn DuckDB connection
    #' @param dataset_id Dataset ID
    #' @return List with ancestors and descendants
    get_lineage = function(conn, dataset_id) {
      # Get ancestors (sources)
      ancestors_query <- "
        SELECT 
          l.id as lineage_id,
          l.source_dataset_id as dataset_id,
          m.name as dataset_name,
          l.transformation,
          l.language,
          l.timestamp
        FROM duck_connect_lineage l
        JOIN duck_connect_metadata m ON l.source_dataset_id = m.id
        WHERE l.target_dataset_id = ?
        ORDER BY l.timestamp DESC
      "
      
      ancestors <- DBI::dbGetQuery(conn, ancestors_query, list(dataset_id))
      
      # Get descendants (targets)
      descendants_query <- "
        SELECT 
          l.id as lineage_id,
          l.target_dataset_id as dataset_id,
          m.name as dataset_name,
          l.transformation,
          l.language,
          l.timestamp
        FROM duck_connect_lineage l
        JOIN duck_connect_metadata m ON l.target_dataset_id = m.id
        WHERE l.source_dataset_id = ?
        ORDER BY l.timestamp DESC
      "
      
      descendants <- DBI::dbGetQuery(conn, descendants_query, list(dataset_id))
      
      return(list(
        ancestors = ancestors,
        descendants = descendants
      ))
    }
  )
) 