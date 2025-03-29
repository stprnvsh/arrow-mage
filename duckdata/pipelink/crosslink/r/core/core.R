#' Connection pool for CrossLink instances
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
  
  # Try to use C++ implementation if available
  cpp_available <- FALSE
  cpp_instance <- NULL
  
  # Source shared_memory/cpp_wrapper.R to ensure C++ bindings are available
  source_path <- file.path(dirname(dirname(system.file(package = "crosslink"))), 
                          "r", "shared_memory", "cpp_wrapper.R")
  tryCatch({
    source(source_path)
    cpp_available <- is_cpp_available()
    if (cpp_available) {
      cpp_instance <- crosslink_cpp_connect(db_path, debug)
    }
  }, error = function(e) {
    if (debug) message("Could not load C++ bindings: ", e$message)
    cpp_available <- FALSE
  })
  
  # If C++ implementation available, create lightweight wrapper
  if (cpp_available && !is.null(cpp_instance)) {
    cl <- list(
      cpp_instance = cpp_instance,
      db_path = db_path,
      cpp_available = TRUE,
      debug = debug
    )
    class(cl) <- c("crosslink_connection", "crosslink_cpp_connection")
    
    # Store in connection pool
    assign(abs_path, cl, envir = .crosslink_connection_pool)
    
    return(cl)
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
    arrow_files = list(),  # Track created arrow files for cleanup
    shared_memory_files = list(),  # Track created shared memory files for cleanup
    cpp_available = FALSE,
    debug = debug
  )
  
  class(cl) <- "crosslink_connection"
  
  # Store in connection pool
  assign(abs_path, cl, envir = .crosslink_connection_pool)
  
  return(cl)
} 