#' Transform data for the PipeLink example pipeline.
#'
#' This is the second node in the pipeline that transforms the raw data
#' using zero-copy data sharing directly with DuckDB.

# Load required libraries
library(DBI)
library(duckdb)
library(uuid)
library(jsonlite)
library(digest)

# Main function for zero-copy processing
process_data <- function() {
  # Get the metadata file path from command line args
  args <- commandArgs(trailingOnly = TRUE)
  if (length(args) < 1) {
    stop("No metadata file provided. This script should be run by PipeLink.")
  }
  
  # Read metadata file
  meta_path <- args[1]
  cat("Using metadata file:", meta_path, "\n")
  
  # Extract database path from metadata
  meta_content <- readLines(meta_path)
  db_path_line <- grep("db_path:", meta_content, value = TRUE)
  db_path <- sub("db_path:\\s*", "", db_path_line)
  cat("Using db_path:", db_path, "\n")
  
  # Connect directly to the database for zero-copy access
  con <- dbConnect(duckdb(), db_path)
  
  # Find raw_data table 
  all_tables <- dbGetQuery(con, "SHOW TABLES")
  cat("Available tables:\n")
  print(all_tables)
  
  # Find the raw_data table
  raw_data_table <- all_tables[grep("raw_data", all_tables$name), "name"]
  cat("Raw data table:", raw_data_table, "\n")
  
  # Query the data directly - zero copy
  query <- paste0("SELECT * FROM ", raw_data_table)
  data <- dbGetQuery(con, query)
  
  cat("Data received - dimensions:", nrow(data), "rows x", ncol(data), "columns\n")
  cat("Column names:", paste(names(data), collapse=", "), "\n")
  
  # Perform transformations
  transformed_data <- data
  
  # Add new features
  transformed_data$abs_value <- abs(transformed_data$value)
  transformed_data$log_value <- log(abs(transformed_data$value) + 1)
  transformed_data$scaled_value <- transformed_data$value / max(abs(transformed_data$value))
  transformed_data$category_code <- as.integer(as.factor(transformed_data$category))
  
  # Create a table for the transformed data
  result_table_name <- "data_transformed_data"
  
  # Register and create the transformed data table
  dbWriteTable(con, "temp_transformed", transformed_data, overwrite = TRUE)
  dbExecute(con, paste0("CREATE OR REPLACE TABLE ", result_table_name, " AS SELECT * FROM temp_transformed"))
  
  # Register this table in the metadata for zero-copy access
  dataset_id <- uuid::UUIDgenerate()
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  
  # Create JSON schema
  schema_json <- jsonlite::toJSON(list(
    columns = names(transformed_data),
    dtypes = sapply(transformed_data, function(x) class(x)[1])
  ), auto_unbox = TRUE)
  
  # Insert metadata for zero-copy access
  dbExecute(con, "
    INSERT INTO crosslink_metadata (
      id, name, source_language, created_at, updated_at, description,
      schema, table_name, arrow_data, version, current_version, 
      schema_hash, access_languages
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
  ", list(
    dataset_id, 
    "transformed_data", 
    "r", 
    timestamp, 
    timestamp,
    "Transformed data with additional features (zero-copy)",
    schema_json,
    result_table_name,
    TRUE,  # arrow_data = TRUE for zero-copy
    1,
    TRUE,
    digest::digest(schema_json),
    jsonlite::toJSON(c("python", "r", "julia", "cpp"))
  ))
  
  # Clean up
  dbDisconnect(con)
  
  cat("Transformed data: added", ncol(transformed_data) - ncol(data), "new features with zero-copy data sharing\n")
  cat("Dataset registered with ID:", dataset_id, "\n")
}

# Run the process
process_data() 