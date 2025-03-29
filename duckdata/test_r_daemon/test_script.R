#!/usr/bin/env Rscript
# Simple test script for the R daemon

# Load necessary libraries
library(jsonlite)

# Print some diagnostics
cat("Running test R script\n")
cat("R version:", R.version.string, "\n")

# Read the metadata file (passed as command line argument)
args <- commandArgs(trailingOnly = TRUE)
if (length(args) > 0) {
  meta_path <- args[1]
  cat("Metadata file:", meta_path, "\n")
  
  # Try to read the metadata if it exists
  if (file.exists(meta_path)) {
    tryCatch({
      meta <- yaml::read_yaml(meta_path)
      cat("Pipeline ID:", meta$pipeline_id, "\n")
      cat("Node ID:", meta$node_id, "\n")
    }, error = function(e) {
      cat("Error reading metadata:", e$message, "\n")
    })
  } else {
    cat("Metadata file does not exist\n")
  }
} else {
  cat("No metadata file provided\n")
}

# Generate some sample results
result <- data.frame(
  x = 1:10,
  y = rnorm(10)
)

# Print the result
cat("Generated sample data:\n")
print(result)

cat("Test script completed successfully!\n") 