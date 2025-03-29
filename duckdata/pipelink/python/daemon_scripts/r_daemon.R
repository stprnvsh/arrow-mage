#!/usr/bin/env Rscript
# R daemon server for PipeLink using Rserve
# Install Rserve if not already installed
if (!requireNamespace("Rserve", quietly = TRUE)) {
  install.packages("Rserve", repos="https://cloud.r-project.org")
}

# Load required packages
library(jsonlite)
library(R6)
cat("Starting Rserve daemon for PipeLink...
")

# Create a file to indicate the server is running
socket_path <- commandArgs(trailingOnly = TRUE)[1]
file.create(socket_path)

# Try to load Rserve
rserve_loaded <- tryCatch({
  library(Rserve)
  TRUE
}, error = function(e) {
  cat("Error loading Rserve:", e$message, "
")
  FALSE
})

if (rserve_loaded) {
  # Start Rserve with custom configuration
  # Note: Rserve will detach from the terminal and run in the background
  Rserve::Rserve(debug = FALSE, port = 6311, args = "--vanilla --slave")
  
  # This point will only be reached if Rserve fails to start
  cat("Error: Failed to start Rserve
")
  if (file.exists(socket_path)) {
    file.remove(socket_path)
  }
} else {
  cat("Rserve not available, using simple file-based communication
")
  # Keep the socket file to indicate we're running
}
