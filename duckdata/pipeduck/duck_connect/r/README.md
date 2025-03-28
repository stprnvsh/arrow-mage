# DuckConnect - R Implementation

This directory contains the R implementation of DuckConnect, providing high-performance cross-language data sharing capabilities.

## Installation

The R version of DuckConnect is installed as part of the pipeduck package:

```bash
# Using the installation script (recommended)
./install_all.sh

# Or manually from R
R CMD INSTALL duckdata

# Or from within R
install.packages("path/to/duckdata", repos = NULL, type = "source")
```

## Usage

### Basic Usage

```r
# Load the package
library(pipeduck)

# Create a DuckConnect instance
dc <- DuckConnect$new("my_database.duckdb")

# Register a data frame
data <- data.frame(
  id = 1:5,
  value = c(10, 20, 30, 40, 50)
)
dc$register_dataset(data, name = "my_dataset")

# Get a dataset
result <- dc$get_dataset("my_dataset")
print(result)

# Execute a SQL query
query_result <- dc$execute_query("SELECT * FROM my_dataset WHERE value > 25")
print(query_result)
```

### DuckContext for Pipeline Integration

```r
library(pipeduck)

# In a pipeline node
process_node <- function() {
  # Initialize context
  ctx <- DuckContext$new(db_path = "pipeline.duckdb")
  
  # Get input data
  input_data <- ctx$get_input("input_dataset")
  
  # Process data
  processed <- input_data
  processed$doubled <- processed$value * 2
  
  # Set output data
  ctx$set_output(processed, "output_dataset")
  
  return("Success")
}
```

### Working with Arrow Data (optional)

```r
library(pipeduck)
library(arrow)

# Create a DuckConnect instance
dc <- DuckConnect$new("my_database.duckdb")

# Use Arrow Table if arrow package is available
if (requireNamespace("arrow", quietly = TRUE)) {
  # Create an Arrow Table
  array1 <- arrow::Array$create(1:4)
  array2 <- arrow::Array$create(c("a", "b", "c", "d"))
  table <- arrow::Table$create(
    numbers = array1,
    letters = array2
  )
  
  # Register an Arrow Table
  dc$register_dataset(table, name = "arrow_data")
}
```

### Tracking Data Lineage

```r
library(pipeduck)

# Create a DuckConnect instance
dc <- DuckConnect$new("my_database.duckdb")

# Register source datasets
dc$register_dataset(source1_df, name = "source1")
dc$register_dataset(source2_df, name = "source2")

# Process and create a new dataset
merged_df <- process_and_merge(source1_df, source2_df)
target_id <- dc$register_dataset(merged_df, name = "merged_result")

# Track the lineage
dc$register_transformation(
  target_dataset_id = target_id,
  source_dataset_ids = c("source1", "source2"),
  transformation = "merged and aggregated"
)
```

## API Reference

### DuckConnect Class

```r
DuckConnect <- R6::R6Class("DuckConnect",
  public = list(
    initialize = function(db_path, debug = FALSE) {
      # Initialize DuckConnect with a database file
    },
    
    register_dataset = function(data, name, description = NULL, 
                               available_to_languages = c("python", "r", "julia"), 
                               overwrite = FALSE) {
      # Register a dataset in the database
    },
    
    get_dataset = function(id_or_name) {
      # Retrieve a dataset by ID or name
    },
    
    update_dataset = function(data, id_or_name) {
      # Update an existing dataset
    },
    
    delete_dataset = function(id_or_name) {
      # Delete a dataset
    },
    
    list_datasets = function(language = NULL) {
      # List all available datasets
    },
    
    execute_query = function(query, params = NULL) {
      # Execute a SQL query against the database
    },
    
    register_transformation = function(target_dataset_id, source_dataset_ids, 
                                     transformation) {
      # Register a transformation for lineage tracking
    }
  )
)
```

### DuckContext Class

```r
DuckContext <- R6::R6Class("DuckContext",
  public = list(
    initialize = function(db_path = NULL, meta_path = NULL) {
      # Initialize a DuckContext for pipeline nodes
    },
    
    get_input = function(name, required = TRUE) {
      # Get an input dataset
    },
    
    set_output = function(data, name) {
      # Set an output dataset
    }
  )
)
```

## R-Specific Features

- **R6 Class Design**: Modern object-oriented interface
- **Data.frame Native Support**: Seamless integration with standard R data frames
- **tibble Compatibility**: Works with tidyverse packages
- **dplyr Integration**: Can be used in dplyr pipelines
- **Arrow Integration**: Optional support for Apache Arrow

## Dependencies

- duckdb
- R6
- DBI
- jsonlite
- uuid

### Optional Dependencies

- arrow
- yaml
- tibble

## Performance Considerations

- For large datasets, consider using arrow tables if available
- Use SQL queries for filtering/aggregation when possible (pushdown to DuckDB)
- For complex transformations, consider registering intermediate datasets

## Tidyverse Integration

DuckConnect works well with the tidyverse ecosystem:

```r
library(pipeduck)
library(dplyr)
library(tidyr)

# Create a DuckConnect instance
dc <- DuckConnect$new("my_database.duckdb")

# Get a dataset
data <- dc$get_dataset("my_dataset")

# Process with dplyr
result <- data %>%
  filter(value > 20) %>%
  mutate(doubled = value * 2) %>%
  group_by(category) %>%
  summarize(avg_value = mean(value))

# Register the result
dc$register_dataset(result, name = "processed_data")
```

## Troubleshooting

### Common Issues

1. **Connection Issues**: If you encounter connection problems, ensure the database path is accessible and has the correct permissions.

2. **Missing Dependencies**: Make sure all required packages are installed.

3. **Schema Mismatches**: When updating datasets, ensure the schema is compatible with the existing data.

## Contributing

Contributions to the R implementation are welcome! Please see the main project README for guidelines. 