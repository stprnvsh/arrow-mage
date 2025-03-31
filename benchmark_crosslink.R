#!/usr/bin/env Rscript

# CrossLink R benchmark support script
# This script is designed to be called by the Python benchmark script
# It demonstrates using CrossLink from R to access data shared by Python

# Check if arrow is installed, if not use basic R data operations
if (!requireNamespace("arrow", quietly = TRUE)) {
  cat("Warning: 'arrow' package not installed. Using basic R data operations instead.\n")
  has_arrow <- FALSE
} else {
  library(arrow)
  has_arrow <- TRUE
}

# Check if dplyr is installed
if (!requireNamespace("dplyr", quietly = TRUE)) {
  cat("Warning: 'dplyr' package not installed. Using base R operations instead.\n")
  has_dplyr <- FALSE
} else {
  library(dplyr)
  has_dplyr <- TRUE
}

# Simulating CrossLink functionality since we don't have the actual package
cat("Simulating CrossLink functionality for benchmark purposes\n")

# Define simulated CrossLink functions
crosslink_connect <- function(db_path) {
  cat(paste("Connected to simulated CrossLink database at", db_path, "\n"))
  return(list(db_path = db_path))
}

crosslink_list_datasets <- function(cl) {
  return(c("input_data", "python_output"))
}

crosslink_pull <- function(cl, dataset_id) {
  cat(paste("Pulling dataset", dataset_id, "from simulated CrossLink\n"))
  
  # Generate synthetic data similar to what Python would have shared
  set.seed(123)  # For reproducibility
  n <- 1000000
  df <- data.frame(
    id = 1:n,
    value1 = rnorm(n),
    value2 = runif(n, -100, 100),
    category = sample(LETTERS[1:4], n, replace = TRUE),
    value1_squared = (rnorm(n))^2,
    value2_normalized = runif(n)
  )
  
  return(df)
}

crosslink_push <- function(cl, data, name, description = "") {
  cat(paste("Pushing dataset", name, "to simulated CrossLink\n"))
  return(name)
}

# Connect to CrossLink (simulated)
cl <- crosslink_connect(db_path = "crosslink_benchmark.duckdb")

# List available datasets
datasets <- crosslink_list_datasets(cl)
cat("Available datasets in simulated CrossLink:\n")
print(datasets)

# Try to find and pull the Python dataset
python_dataset_id <- "python_output"
if (python_dataset_id %in% datasets) {
  cat(paste("\nPulling dataset:", python_dataset_id, "\n"))
  
  # Start timer
  start_time <- Sys.time()
  
  # Pull the dataset
  df <- crosslink_pull(cl, python_dataset_id)
  
  # Process data
  if (has_dplyr) {
    result <- df %>%
      mutate(
        value1_cubed = value1^3,
        value2_log = log(abs(value2) + 1),
        flag = ifelse(value1 > 0 & value2 > 0, "positive", "other")
      )
    
    # Calculate statistics
    stats <- result %>%
      group_by(flag) %>%
      summarize(
        count = n(),
        avg_value1 = mean(value1),
        avg_value2 = mean(value2)
      )
  } else {
    # Use base R operations
    result <- df
    result$value1_cubed <- result$value1^3
    result$value2_log <- log(abs(result$value2) + 1)
    result$flag <- ifelse(result$value1 > 0 & result$value2 > 0, "positive", "other")
    
    # Calculate statistics
    stats <- aggregate(
      cbind(count = 1, avg_value1 = value1, avg_value2 = value2) ~ flag, 
      data = result, 
      FUN = function(x) if(is.numeric(x)) mean(x) else length(x)
    )
  }
  
  # Show summary of results
  cat("\nProcessed data summary:\n")
  print(summary(result))
  
  cat("\nStatistics by flag:\n")
  print(stats)
  
  # Push results back to CrossLink
  crosslink_push(cl, result, name = "r_output", description = "R processing results")
  
  # Stop timer
  end_time <- Sys.time()
  duration <- end_time - start_time
  
  cat(paste("\nR processing completed in", format(duration, digits = 4), "seconds\n"))
} else {
  cat(paste("\nError: Could not find dataset", python_dataset_id, "\n"))
  cat("Available datasets:\n")
  print(datasets)
} 