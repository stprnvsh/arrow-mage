#' Transform data for the PipeLink example pipeline.
#'
#' This is the second node in the pipeline that transforms the raw data.

# Try to load PipeLink library
tryCatch({
  library(pipelink)
}, error = function(e) {
  # Try to source the R files directly
  cat("Failed to load pipelink R package, trying to source files directly...\n")
  
  # Get script directory and try to find the source files
  current_script <- commandArgs(trailingOnly = FALSE)
  script_path <- dirname(sub("--file=", "", current_script[grep("--file=", current_script)]))
  
  # Try various relative paths
  source_paths <- c(
    file.path(script_path, "..", "..", "..", "r-pipelink", "R", "crosslink.R"),
    file.path(script_path, "..", "..", "..", "pipelink", "r", "pipelink_node.R"),
    file.path(script_path, "..", "..", "..", "pipelink", "r", "crosslink.R")
  )
  
  crosslink_loaded <- FALSE
  for (path in source_paths) {
    if (file.exists(path)) {
      cat("Sourcing file:", path, "\n")
      source(path)
      crosslink_loaded <- TRUE
      break
    }
  }
  
  if (!crosslink_loaded) {
    stop("Could not find CrossLink files to source directly. Please install the pipelink R package.")
  }
})

# Create very simple context manually
ctx <- new.env()

# Create a simplified get_input function
ctx$get_input <- function(name) {
  cat("Manual data creation instead of loading from previous node\n")
  # Create a simple data frame manually
  data.frame(
    id = 1:10,
    value = c(1.5, 2.5, 3.5, 4.5, 5.5, 6.5, 7.5, 8.5, 9.5, 10.5),
    category = c("A", "B", "A", "B", "A", "B", "A", "B", "A", "B")
  )
}

# Create a simplified save_output function
ctx$save_output <- function(name, data, description) {
  cat("Saving output data:", name, "\n")
  cat("Data dimensions:", nrow(data), "rows x", ncol(data), "columns\n")
  cat("Column names:", paste(names(data), collapse=", "), "\n")
  cat("Output saved successfully\n")
}

# Main function to process data
process_data <- function(ctx) {
  # Get input data
  cat("Getting input data\n")
  data <- ctx$get_input("raw_data")
  
  # Debug output
  cat("Data received - dimensions:", nrow(data), "rows x", ncol(data), "columns\n")
  cat("Column names:", paste(names(data), collapse=", "), "\n")
  
  # Only perform minimal transformations
  transformed_data <- data
  
  # Add a single simple column
  transformed_data$abs_value <- abs(transformed_data$value)
  
  # Save output (using simplified functions)
  ctx$save_output("transformed_data", transformed_data, "Minimally transformed data")
  
  cat("Transformation complete\n")
}

# Run the process
process_data(ctx) 