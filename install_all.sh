#!/bin/bash
set -e

# Display header
echo "========================================================"
echo "       DuckData Multilanguage Installation Script       "
echo "========================================================"
echo "This script will install the DuckData packages for Python, R, and Julia"
echo "to enable multilanguage pipelines."
echo ""

# Get the absolute path of the project root directory
PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

# Check for required dependencies
echo "Checking for required dependencies..."

# Check for Python
if ! command -v python3 &> /dev/null; then
    echo "Python 3 is not installed. Please install Python 3 first."
    exit 1
fi
echo "✅ Python 3 found: $(python3 --version)"

# Check for R
if ! command -v R &> /dev/null; then
    echo "R is not installed. Please install R first."
    exit 1
fi
echo "✅ R found: $(R --version | head -n 1)"

# Check for Julia
if ! command -v julia &> /dev/null; then
    echo "Julia is not installed. Please install Julia first."
    exit 1
fi
echo "✅ Julia found: $(julia --version)"

# Install Python package
echo ""
echo "Installing Python package..."
cd "$PROJECT_ROOT/duckdata"
pip install -e .
echo "✅ Python package installed successfully."

# Install R package
echo ""
echo "Checking for existing .Rprofile..."
if [ -f ~/.Rprofile ]; then
    echo "Backing up existing .Rprofile to ~/.Rprofile.bak"
    mv ~/.Rprofile ~/.Rprofile.bak
fi

echo "Installing R package dependencies..."
Rscript -e 'if (!require("R6")) install.packages("R6", repos="https://cloud.r-project.org")
if (!require("duckdb")) install.packages("duckdb", repos="https://cloud.r-project.org")
if (!require("DBI")) install.packages("DBI", repos="https://cloud.r-project.org")
if (!require("arrow")) install.packages("arrow", repos="https://cloud.r-project.org")
if (!require("jsonlite")) install.packages("jsonlite", repos="https://cloud.r-project.org")
if (!require("yaml")) install.packages("yaml", repos="https://cloud.r-project.org")
if (!require("parallel")) install.packages("parallel", repos="https://cloud.r-project.org")'

echo "Creating patched version of duck_connector.R to fix syntax error..."
R_SOURCE_DIR="$PROJECT_ROOT/duckdata/pipeduck/r/R"
cat > "$R_SOURCE_DIR/duck_connector.R.fixed" << 'EOF'
# Helper function for null coalescing
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Advanced DuckDB connector for efficient cross-language data sharing and processing.
#'
#' This module provides optimized connections to DuckDB with advanced features.
#' @export
DuckConnector <- R6::R6Class(
  "DuckConnector",
  
  public = list(
    #' @field db_path Path to the DuckDB database file
    db_path = NULL,
    
    #' @field conn DuckDB connection
    conn = NULL,
    
    #' @field config Configuration options
    config = NULL,
    
    #' @description Initialize DuckConnector with advanced settings.
    #' @param db_path Path to DuckDB database or ":memory:" for in-memory DB
    #' @param config Optional configuration dictionary with advanced settings
    initialize = function(db_path = ":memory:", config = NULL) {
      self$db_path <- db_path
      self$config <- config %||% list()
      
      # Create connection with optimized settings
      self$conn <- self$_create_optimized_connection()
      
      # Setup extensions
      self$_setup_extensions()
      
      # Setup advanced features
      self$_setup_advanced_features()
    },
    
    #' @description Create an optimized DuckDB connection with advanced settings
    _create_optimized_connection = function() {
      conn <- DBI::dbConnect(duckdb::duckdb(), self$db_path)
      
      # Set memory limit based on config or default to 80% of available RAM
      memory_limit <- self$config$memory_limit %||% "80%"
      DBI::dbExecute(conn, paste0("PRAGMA memory_limit='", memory_limit, "'"))
      
      # Enable MVCC for concurrent transactions
      DBI::dbExecute(conn, "PRAGMA enable_mvcc")
      
      # Set threads based on config or default to num_cores - 1
      num_cores <- max(1, parallel::detectCores() - 1)
      threads <- self$config$threads %||% num_cores
      DBI::dbExecute(conn, paste0("PRAGMA threads=", threads))
      
      # Enable parallelism for better performance
      DBI::dbExecute(conn, "PRAGMA force_parallelism")
      
      # Enable object cache
      DBI::dbExecute(conn, "PRAGMA enable_object_cache")
      
      # Enable progress bar for long-running queries
      if (self$config$show_progress %||% TRUE) {
        DBI::dbExecute(conn, "PRAGMA enable_progress_bar")
      }
      
      # External data cache - for out-of-core processing
      external_cache <- self$config$external_cache_directory
      if (!is.null(external_cache)) {
        dir.create(external_cache, showWarnings = FALSE, recursive = TRUE)
        DBI::dbExecute(conn, paste0("PRAGMA temp_directory='", external_cache, "'"))
      }
      
      # External buffer for large datasets
      if (self$config$use_external_buffer %||% TRUE) {
        DBI::dbExecute(conn, "PRAGMA external_threads=4")
        buffer_size <- self$config$external_buffer_size %||% "4GB"
        DBI::dbExecute(conn, paste0("PRAGMA memory_limit='", buffer_size, "'"))
      }
      
      return(conn)
    },
    
    #' @description Setup DuckDB extensions for advanced functionality
    _setup_extensions = function() {
      tryCatch({
        # Always install and load Arrow
        DBI::dbExecute(self$conn, "INSTALL arrow")
        DBI::dbExecute(self$conn, "LOAD arrow")
      }, error = function(e) {
        warning("Could not load arrow extension: ", e$message)
      })
      
      tryCatch({
        # JSON extension for native JSON support
        DBI::dbExecute(self$conn, "INSTALL json")
        DBI::dbExecute(self$conn, "LOAD json")
      }, error = function(e) {
        warning("Could not load json extension: ", e$message)
      })
      
      tryCatch({
        # Parquet extension for native Parquet support
        DBI::dbExecute(self$conn, "INSTALL parquet")
        DBI::dbExecute(self$conn, "LOAD parquet")
      }, error = function(e) {
        warning("Could not load parquet extension: ", e$message)
      })
      
      # Install additional extensions based on configuration
      extensions <- self$config$extensions %||% list()
      
      if (extensions$httpfs %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL httpfs")
          DBI::dbExecute(self$conn, "LOAD httpfs")
        }, error = function(e) {
          warning("Could not load httpfs extension: ", e$message)
        })
      }
      
      if (extensions$icu %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL icu")
          DBI::dbExecute(self$conn, "LOAD icu")
        }, error = function(e) {
          warning("Could not load icu extension: ", e$message)
        })
      }
      
      if (extensions$fts %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL fts")
          DBI::dbExecute(self$conn, "LOAD fts")
        }, error = function(e) {
          warning("Could not load fts extension: ", e$message)
        })
      }
      
      if (extensions$spatial %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL spatial")
          DBI::dbExecute(self$conn, "LOAD spatial")
        }, error = function(e) {
          warning("Could not load spatial extension: ", e$message)
        })
      }
      
      if (extensions$sqlite %||% FALSE) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL sqlite")
          DBI::dbExecute(self$conn, "LOAD sqlite")
        }, error = function(e) {
          warning("Could not load sqlite extension: ", e$message)
        })
      }
      
      # Check for custom extensions
      custom_extensions <- extensions$custom %||% c()
      for (ext in custom_extensions) {
        tryCatch({
          DBI::dbExecute(self$conn, paste0("INSTALL ", ext))
          DBI::dbExecute(self$conn, paste0("LOAD ", ext))
        }, error = function(e) {
          warning(paste0("Failed to load extension ", ext, ": ", e$message))
        })
      }
    },
    
    #' @description Setup advanced features for DuckDB
    _setup_advanced_features = function() {
      # Fine tune the analyzer based on configuration
      optimizer <- self$config$optimizer %||% list()
      
      if (optimizer$join_order %||% FALSE) {
        DBI::dbExecute(self$conn, "PRAGMA default_join_type='adaptive'")
      }
      
      if (optimizer$filter_pushdown %||% TRUE) {
        DBI::dbExecute(self$conn, "PRAGMA enable_filter_pushdown")
      }
      
      if (optimizer$lazy_analyze %||% TRUE) {
        DBI::dbExecute(self$conn, "PRAGMA explain_output='all'")
      }
      
      # Set up optimizer for out-of-core processing
      out_of_core <- self$config$out_of_core %||% list()
      if (out_of_core$enabled %||% TRUE) {
        temp_dir <- "/tmp/duckdb_tmp"
        dir.create(temp_dir, showWarnings = FALSE, recursive = TRUE)
        DBI::dbExecute(self$conn, paste0("PRAGMA temp_directory='", temp_dir, "'"))
        DBI::dbExecute(self$conn, "PRAGMA memory_limit='8GB'")
        DBI::dbExecute(self$conn, "PRAGMA threads=8")
      }
    }
  )
)
EOF

# Replace the broken file with the fixed one
mv "$R_SOURCE_DIR/duck_connector.R.fixed" "$R_SOURCE_DIR/duck_connector.R"

echo "Installing R package..."
# Create a temporary directory for the package
TEMP_PKG_DIR=$(mktemp -d)
mkdir -p "$TEMP_PKG_DIR/pipeduck/R"

# Copy R package files
cd "$PROJECT_ROOT/duckdata/pipeduck/r"
cp -r R/* "$TEMP_PKG_DIR/pipeduck/R/"
cp DESCRIPTION "$TEMP_PKG_DIR/pipeduck/"
cp NAMESPACE "$TEMP_PKG_DIR/pipeduck/"
if [ -f LICENSE ]; then
  cp LICENSE "$TEMP_PKG_DIR/pipeduck/"
fi

# Install the R package from the temporary directory
cd "$TEMP_PKG_DIR"
R CMD INSTALL pipeduck

# Clean up
rm -rf "$TEMP_PKG_DIR"

# Verify installation
echo "Verifying R package installation..."
if Rscript -e 'if(require("pipeduck")) cat("✅ R package verified!\n") else stop("Package installation failed")'; then
    echo "✅ R package installed successfully."
else
    echo "❌ R package installation failed."
    exit 1
fi

# Install Julia package
echo ""
echo "Installing Julia package dependencies..."
julia -e '
    using Pkg
    # Install required packages
    Pkg.add("DuckDB")
    Pkg.add("DataFrames")
    Pkg.add("JSON")
    Pkg.add("YAML")
    Pkg.add("UUIDs")
'

echo "Installing Julia package..."
# Create temporary directory for package development
JULIA_PKG_DIR="$HOME/.julia/dev/DuckConnect"
mkdir -p "$JULIA_PKG_DIR/src"

# Copy Julia files
cp -r "$PROJECT_ROOT/duckdata/pipeduck/duck_connect/julia/src/"* "$JULIA_PKG_DIR/src/"
cp "$PROJECT_ROOT/duckdata/pipeduck/duck_connect/julia/Project.toml" "$JULIA_PKG_DIR/"

# Register the package in development mode
julia -e "
    using Pkg
    # Develop the package from our custom location
    Pkg.develop(path=\"$JULIA_PKG_DIR\")
    # Test loading the package
    using DuckConnect
    println(\"✅ Julia package installed successfully.\")
"

echo ""
echo "========================================================"
echo "       DuckData Installation Complete!                  "
echo "========================================================"
echo ""
echo "You can now run multilanguage pipelines with DuckData."
echo ""
echo "Usage examples:"
echo "- Python: import pipeduck"
echo "- R: library(pipeduck)"
echo "- Julia: using DuckConnect"
echo "" 