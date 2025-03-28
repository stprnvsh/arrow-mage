#!/usr/bin/env Rscript

# PipeDuck Node: Process Sales Data
# This node processes the raw sales data and creates aggregated datasets

# Load required libraries
required_packages <- c("dplyr")
for (pkg in required_packages) {
  if (!require(pkg, character.only = TRUE, quietly = TRUE)) {
    install.packages(pkg)
    library(pkg, character.only = TRUE)
  }
}

# Load PipeDuck library
library(pipeduck)
cat("Using PipeDuck library\n")

main <- function() {
  cat("Executing node: Process Sales Data\n")
  
  # Get metadata path from command-line arguments (used by PipeDuck)
  args <- commandArgs(trailingOnly = TRUE)
  meta_path <- if (length(args) > 0) args[1] else NULL
  
  # Create DuckContext to manage database connections
  ctx <- DuckContext$new(meta_path = meta_path)
  
  # Get the raw sales data from the previous node
  cat("Loading raw sales data...\n")
  raw_sales <- ctx$get_input("raw_sales")
  cat(sprintf("Loaded %d records\n", nrow(raw_sales)))
  
  # Process 1: Daily Sales Aggregation
  cat("Creating daily sales aggregation...\n")
  daily_sales <- raw_sales %>%
    mutate(date = as.Date(date)) %>%
    group_by(date) %>%
    summarize(
      transaction_count = n(),
      total_quantity = sum(quantity),
      total_revenue = sum(revenue),
      avg_price = mean(price),
      .groups = "drop"
    ) %>%
    arrange(date)
  
  cat(sprintf("Created daily sales summary with %d rows\n", nrow(daily_sales)))
  
  # Process 2: Store Performance Analysis
  cat("Creating store performance analysis...\n")
  store_performance <- raw_sales %>%
    group_by(store_id) %>%
    summarize(
      transaction_count = n(),
      total_quantity = sum(quantity),
      total_revenue = sum(revenue),
      avg_revenue_per_transaction = mean(revenue),
      product_variety = n_distinct(product_id),
      .groups = "drop"
    ) %>%
    arrange(desc(total_revenue))
  
  cat(sprintf("Created store performance summary with %d rows\n", nrow(store_performance)))
  
  # Add some derived metrics
  store_performance <- store_performance %>%
    mutate(
      revenue_per_product = total_revenue / product_variety,
      revenue_per_quantity = total_revenue / total_quantity
    )
  
  # Save the results for the next node in the pipeline
  ctx$set_output(daily_sales, "daily_sales")
  ctx$set_output(store_performance, "store_performance")
  
  cat("Node execution complete\n")
  return(0)
}

# Run the main function
exit_code <- main()
quit(save = "no", status = exit_code) 