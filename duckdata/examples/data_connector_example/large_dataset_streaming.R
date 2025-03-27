#!/usr/bin/env Rscript
#
# Example demonstrating R integration with large datasets in CrossLink
#
# This script shows how to:
# 1. Access large datasets created in Python from R
# 2. Process data in chunks to avoid memory issues
# 3. Analyze data directly in DuckDB without loading to memory
#

# Load libraries
library(duckdb)
suppressWarnings(library(arrow))

# Try to load pipelink R package
tryCatch({
  library(pipelink)
}, error = function(e) {
  # If package isn't installed, source directly for demo purposes
  cat("pipelink R package not found, sourcing R scripts directly...\n")
  source_file <- function(file_path) {
    if (file.exists(file_path)) {
      source(file_path)
      return(TRUE)
    }
    return(FALSE)
  }
  
  # Try to find the CrossLink R file in various locations
  crosslink_paths <- c(
    file.path(dirname(getwd()), "r-pipelink", "R", "crosslink.R"),
    file.path(dirname(getwd()), "..", "r-pipelink", "R", "crosslink.R"),
    file.path(dirname(getwd()), "..", "pipelink", "crosslink", "r", "crosslink.R")
  )
  
  found <- FALSE
  for (path in crosslink_paths) {
    cat("Trying to source:", path, "\n")
    if (source_file(path)) {
      found <- TRUE
      break
    }
  }
  
  if (!found) {
    stop("Could not find CrossLink R implementation. Please install the pipelink R package.")
  }
})

# Set the path to the database file (should be the same as used in Python example)
db_path <- file.path(dirname(commandArgs(trailingOnly=FALSE)[file.exists(commandArgs(trailingOnly=FALSE))]), 
                    "large_data_example.duckdb")

cat(paste("Opening database:", db_path, "\n"))

# Initialize CrossLink connection
if (exists("CrossLink")) {
  # If the R package loaded correctly
  cl <- CrossLink$new(db_path)
} else {
  # Fall back to standalone function for direct sourcing case
  cl <- crosslink_connect(db_path)
}

# Main function
main <- function() {
  cat("R CrossLink Large Dataset Example\n")
  cat("=================================\n\n")
  
  # Check if the large dataset exists
  datasets <- cl$list_datasets()
  if (!"large_dataset" %in% datasets$name) {
    cat("Error: 'large_dataset' not found. Please run the Python example first to create it.\n")
    return()
  }
  
  cat("Found large dataset created by Python. Getting metadata...\n")
  
  # Connect to DuckDB directly to get row count
  con <- dbConnect(duckdb::duckdb(), db_path)
  row_count <- dbGetQuery(con, "SELECT COUNT(*) as count FROM large_dataset")$count
  cat(paste("Dataset has", format(row_count, big.mark=","), "rows\n\n"))
  
  # Step 1: Stream data in chunks and compute summary statistics
  cat("Processing data in chunks...\n")
  chunk_size <- 50000
  num_chunks <- ceiling(row_count / chunk_size)
  
  # Initialize accumulators for column stats
  value_sums <- numeric()
  value_counts <- numeric()
  
  # Process in chunks
  for (i in 1:min(num_chunks, 10)) {  # Limit to 10 chunks for demo
    cat(paste("Processing chunk", i, "of", min(num_chunks, 10), "...\n"))
    
    # Use DuckDB to read a chunk directly (more efficient than CrossLink for this task)
    offset <- (i - 1) * chunk_size
    query <- paste0("SELECT * FROM large_dataset LIMIT ", chunk_size, " OFFSET ", offset)
    chunk <- dbGetQuery(con, query)
    
    # Calculate stats for numeric columns
    numeric_cols <- sapply(chunk, is.numeric)
    if (length(value_sums) == 0) {
      value_sums <- colSums(chunk[, numeric_cols], na.rm = TRUE)
      value_counts <- colSums(!is.na(chunk[, numeric_cols]))
    } else {
      value_sums <- value_sums + colSums(chunk[, numeric_cols], na.rm = TRUE)
      value_counts <- value_counts + colSums(!is.na(chunk[, numeric_cols]))
    }
    
    # Print progress
    if (i %% 2 == 0 || i == min(num_chunks, 10)) {
      cat(paste("  Processed", format(i * chunk_size, big.mark=","), "rows so far\n"))
    }
  }
  
  # Calculate overall means
  value_means <- value_sums / value_counts
  
  cat("\nSummary statistics for numeric columns:\n")
  stats_df <- data.frame(
    column = names(value_means),
    mean = value_means
  )
  print(stats_df)
  
  # Step 2: Use DuckDB to perform efficient analytics without loading all data
  cat("\nRunning analytics directly in DuckDB...\n")
  
  # Example: Calculate hourly statistics
  query <- "
  SELECT
    date_trunc('hour', timestamp) as hour,
    COUNT(*) as count,
    AVG(value_0) as avg_value_0,
    MIN(value_0) as min_value_0,
    MAX(value_0) as max_value_0,
    STDDEV(value_0) as stddev_value_0
  FROM large_dataset
  GROUP BY hour
  ORDER BY hour
  LIMIT 24
  "
  
  hourly_stats <- dbGetQuery(con, query)
  cat("\nHourly statistics (first 24 hours):\n")
  print(head(hourly_stats, 5))
  
  # Step 3: Create a new dataset in R and store it back in CrossLink
  cat("\nCreating a new dataset in R and storing in CrossLink...\n")
  
  # Calculate daily summaries
  query <- "
  SELECT
    date_trunc('day', timestamp) as day,
    COUNT(*) as count,
    AVG(value_0) as avg_value_0,
    AVG(value_1) as avg_value_1,
    AVG(value_2) as avg_value_2
  FROM large_dataset
  GROUP BY day
  ORDER BY day
  "
  
  daily_summary <- dbGetQuery(con, query)
  cat(paste("Created daily summary with", nrow(daily_summary), "rows\n"))
  
  # Store this smaller dataset back to CrossLink
  cl$push(
    daily_summary,
    name = "daily_summary",
    description = "Daily summary statistics calculated in R"
  )
  
  cat("Daily summary stored in CrossLink as 'daily_summary'\n")
  
  # Step 4: Show all datasets
  cat("\nAll datasets in CrossLink:\n")
  datasets <- cl$list_datasets()
  print(datasets[, c("name", "source_language", "created_at")])
  
  # Cleanup
  dbDisconnect(con)
  cat("\nR example completed successfully\n")
}

# Run the main function
tryCatch({
  main()
}, error = function(e) {
  cat(paste("Error:", e$message, "\n"))
}, finally = {
  # Clean up
  if (exists("cl") && !is.null(cl)) {
    if (is.function(cl$close)) {
      cl$close()
    } else if (exists("crosslink_disconnect")) {
      crosslink_disconnect(cl)
    }
  }
}) 