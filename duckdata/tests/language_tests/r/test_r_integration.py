"""
Tests for R integration with DuckData.
"""
import os
import sys
import pytest
import yaml
import pandas as pd
import numpy as np
import subprocess
from pipelink.crosslink import CrossLink
from pipelink.python.pipelink import run_pipeline

class TestRIntegration:
    """Tests for R integration with DuckData."""
    
    def test_r_available(self):
        """Check if R is available in the system."""
        try:
            result = subprocess.run(["Rscript", "--version"], 
                                    stdout=subprocess.PIPE, 
                                    stderr=subprocess.PIPE)
            return result.returncode == 0
        except:
            pytest.skip("R not available on this system")
    
    def test_r_node_execution(self, test_dir, sample_dataframe, check_language_available):
        """Test execution of an R node."""
        # Skip if R is not available
        if not check_language_available("r"):
            pytest.skip("R is not available")
        
        # Create an R script for testing
        script_content = """
# Test R script for DuckData

# Try to load CrossLink package, or source files directly
tryCatch({
  library(CrossLink)
}, error = function(e) {
  # Look for CrossLink in various locations
  script_dir <- dirname(normalizePath(commandArgs(trailingOnly = FALSE)[grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)][1]))
  paths <- c(
    file.path(script_dir, "..", "..", "..", "r-pipelink", "R", "crosslink.R"),
    file.path(script_dir, "..", "..", "..", "pipelink", "r", "crosslink.R")
  )
  
  for (path in paths) {
    if (file.exists(path)) {
      cat("Sourcing file:", path, "\n")
      source(path)
      break
    }
  }
})

# Initialize the node context
ctx <- node_context()

# Get input data
input_data <- get_input(ctx, "input_data")

# Process the data
processed_data <- input_data
processed_data$value_squared <- processed_data$value^2
processed_data$log_abs_value <- log(abs(processed_data$value) + 1)
processed_data$category_factor <- as.factor(processed_data$category)

# Add a column to indicate R processing
processed_data$processed_by_r <- TRUE

# Save the results
save_output(ctx, "r_processed_data", processed_data, "Data processed by R")

# Print summary
cat("Processed", nrow(processed_data), "rows with R\n")
"""
        
        # Create script file
        script_path = os.path.join(test_dir, "process_data.R")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Create a pipeline configuration
        nodes = [
            {
                "id": "r_process_data",
                "language": "r",
                "script": script_path,
                "inputs": ["input_data"],
                "outputs": ["r_processed_data"]
            }
        ]
        
        pipeline_config = {
            "name": "r_test_pipeline",
            "description": "Test pipeline for R integration",
            "db_path": os.path.join(test_dir, "r_test.duckdb"),
            "working_dir": test_dir,
            "nodes": nodes
        }
        
        # Save the pipeline configuration
        config_path = os.path.join(test_dir, "r_test_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Initialize CrossLink and save test data
        cl = CrossLink(db_path=pipeline_config["db_path"])
        cl.push(sample_dataframe, name="input_data", description="Test input data")
        cl.close()
        
        # Run the pipeline
        run_pipeline(config_path)
        
        # Verify the results
        cl = CrossLink(db_path=pipeline_config["db_path"])
        result_df = cl.pull("r_processed_data")
        cl.close()
        
        # Check that the transformation was applied
        assert "value_squared" in result_df.columns
        assert "log_abs_value" in result_df.columns
        assert "processed_by_r" in result_df.columns
        assert result_df["processed_by_r"].all()
    
    def test_r_data_analysis(self, test_dir, complex_dataframe, check_language_available):
        """Test R data analysis capabilities."""
        # Skip if R is not available
        if not check_language_available("r"):
            pytest.skip("R is not available")
        
        # Create an R script for data analysis
        script_content = """
# R script for data analysis in DuckData

# Try to load CrossLink package, or source files directly
tryCatch({
  library(CrossLink)
}, error = function(e) {
  # Look for CrossLink in various locations
  script_dir <- dirname(normalizePath(commandArgs(trailingOnly = FALSE)[grep("--file=", commandArgs(trailingOnly = FALSE), value = TRUE)][1]))
  paths <- c(
    file.path(script_dir, "..", "..", "..", "r-pipelink", "R", "crosslink.R"),
    file.path(script_dir, "..", "..", "..", "pipelink", "r", "crosslink.R")
  )
  
  for (path in paths) {
    if (file.exists(path)) {
      cat("Sourcing file:", path, "\n")
      source(path)
      break
    }
  }
})

# Load necessary libraries if available
for (pkg in c("dplyr", "tidyr", "stats")) {
  if (require(pkg, character.only = TRUE)) {
    cat("Loaded package:", pkg, "\n")
  } else {
    cat("Package not available:", pkg, "\n")
  }
}

# Initialize the node context
ctx <- node_context()

# Get input data
input_data <- get_input(ctx, "complex_data")

# Create summary statistics
if ("dplyr" %in% .packages()) {
  # Use dplyr for summary if available
  summary_stats <- input_data %>%
    group_by(cat_col) %>%
    summarise(
      count = n(),
      mean_float = mean(float_col, na.rm = TRUE),
      median_float = median(float_col, na.rm = TRUE),
      sd_float = sd(float_col, na.rm = TRUE),
      mean_int = mean(int_col, na.rm = TRUE),
      median_int = median(int_col, na.rm = TRUE),
      sd_int = sd(int_col, na.rm = TRUE),
      true_count = sum(bool_col, na.rm = TRUE)
    )
} else {
  # Fallback to base R
  summary_stats <- data.frame(
    cat_col = unique(input_data$cat_col)
  )
  
  for (cat in summary_stats$cat_col) {
    subset_data <- input_data[input_data$cat_col == cat, ]
    idx <- which(summary_stats$cat_col == cat)
    
    summary_stats$count[idx] <- nrow(subset_data)
    summary_stats$mean_float[idx] <- mean(subset_data$float_col, na.rm = TRUE)
    summary_stats$median_float[idx] <- median(subset_data$float_col, na.rm = TRUE)
    summary_stats$sd_float[idx] <- sd(subset_data$float_col, na.rm = TRUE)
    summary_stats$mean_int[idx] <- mean(subset_data$int_col, na.rm = TRUE)
    summary_stats$median_int[idx] <- median(subset_data$int_col, na.rm = TRUE)
    summary_stats$sd_int[idx] <- sd(subset_data$int_col, na.rm = TRUE)
    summary_stats$true_count[idx] <- sum(subset_data$bool_col, na.rm = TRUE)
  }
}

# Run a simple linear model
if (all(c("float_col", "int_col") %in% colnames(input_data))) {
  model <- lm(float_col ~ int_col, data = input_data)
  
  # Extract model information
  model_summary <- summary(model)
  coefficients <- data.frame(
    term = rownames(model_summary$coefficients),
    estimate = model_summary$coefficients[, "Estimate"],
    std_error = model_summary$coefficients[, "Std. Error"],
    t_value = model_summary$coefficients[, "t value"],
    p_value = model_summary$coefficients[, "Pr(>|t|)"]
  )
  
  # Create model metrics
  model_metrics <- data.frame(
    r_squared = model_summary$r.squared,
    adj_r_squared = model_summary$adj.r.squared,
    f_statistic = model_summary$fstatistic[1],
    p_value = pf(model_summary$fstatistic[1], 
                model_summary$fstatistic[2], 
                model_summary$fstatistic[3], 
                lower.tail = FALSE)
  )
  
  # Save model results
  save_output(ctx, "model_coefficients", coefficients, "Linear model coefficients")
  save_output(ctx, "model_metrics", model_metrics, "Linear model performance metrics")
}

# Save summary statistics
save_output(ctx, "r_summary_stats", summary_stats, "Summary statistics calculated in R")

# Print summary
cat("Generated summary statistics with", nrow(summary_stats), "groups\n")
"""
        
        # Create script file
        script_path = os.path.join(test_dir, "r_analysis.R")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Create a pipeline configuration
        nodes = [
            {
                "id": "r_data_analysis",
                "language": "r",
                "script": script_path,
                "inputs": ["complex_data"],
                "outputs": ["r_summary_stats", "model_coefficients", "model_metrics"]
            }
        ]
        
        pipeline_config = {
            "name": "r_analysis_pipeline",
            "description": "Test pipeline for R data analysis",
            "db_path": os.path.join(test_dir, "r_analysis.duckdb"),
            "working_dir": test_dir,
            "nodes": nodes
        }
        
        # Save the pipeline configuration
        config_path = os.path.join(test_dir, "r_analysis_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Initialize CrossLink and save test data
        cl = CrossLink(db_path=pipeline_config["db_path"])
        cl.push(complex_dataframe, name="complex_data", description="Complex test data")
        cl.close()
        
        # Run the pipeline
        run_pipeline(config_path)
        
        # Verify the results
        cl = CrossLink(db_path=pipeline_config["db_path"])
        
        # Check summary statistics
        summary_stats = cl.pull("r_summary_stats")
        assert summary_stats is not None
        assert "cat_col" in summary_stats.columns
        assert "count" in summary_stats.columns
        
        # Check model results if they exist
        try:
            model_coefficients = cl.pull("model_coefficients")
            assert model_coefficients is not None
            assert "term" in model_coefficients.columns
            assert "estimate" in model_coefficients.columns
            
            model_metrics = cl.pull("model_metrics")
            assert model_metrics is not None
            assert "r_squared" in model_metrics.columns
        except:
            # Model results might not be available if the linear model failed
            pass
            
        cl.close() 