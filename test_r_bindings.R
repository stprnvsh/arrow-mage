# test_r_bindings.R
# Load necessary libraries
library(arrow)
# Assuming 'crosslink' is the name of your installed package
# If it's not installed as a package, you might need to source('path/to/crosslink.R') instead
library(CrossLink) # Changed from lowercase 'crosslink'
library(duckdb) # Might be needed implicitly by crosslink or for connection checks

# Define a unique database path for this test
DB_PATH <- "test_r_bindings.duckdb"
TEST_SUCCESS <- FALSE # Flag to track overall success

# --- Test Execution ---
tryCatch({
    print("--- Starting CrossLink R Bindings Test ---")

    # Clean up previous test database if it exists
    if (file.exists(DB_PATH)) {
        print(paste("Removing existing database:", DB_PATH))
        file.remove(DB_PATH)
    }
    if (file.exists(paste0(DB_PATH, ".wal"))) {
      print(paste("Removing existing WAL file:", paste0(DB_PATH, ".wal")))
      file.remove(paste0(DB_PATH, ".wal"))
    }

    # 1. Instantiate CrossLink Connection
    cl_con <- NULL
    print(paste("Instantiating CrossLink connection with db_path='", DB_PATH, "'...", sep=""))
    # Use namespace prefix, assuming roxygen2 export works
    cl_con <- CrossLink::crosslink_connect(db_path = DB_PATH, debug = TRUE) 
    print("CrossLink connection object created.")
    
    # Optional: Check if connection seems to be using C++ backend
    # This depends on how your R package exposes this information.
    # Example (adjust based on actual implementation):
    # if (!is.null(cl_con$cpp_available) && cl_con$cpp_available) {
    #     print("Connection seems to be using C++ backend (based on connection object).")
    # } else if (inherits(cl_con, "crosslink_cpp_connection")) {
    #     print("Connection seems to be using C++ backend (based on class).")
    # } else {
    #     print("Connection might be using R backend or C++ status unknown.")
    # }


    # 2. Create Sample Data (Arrow Table)
    print("Creating sample R data.frame...")
    r_df <- data.frame(
        col1 = c(10L, 20L, 30L, 40L), # Integer type
        col2 = c("apple", "banana", "cherry", "date"),
        stringsAsFactors = FALSE
    )
    print("Converting data.frame to arrow::Table...")
    original_table <- arrow::arrow_table(r_df)
    print("Sample Table:")
    print(original_table)

    # 3. Push Data
    dataset_id <- NULL
    print("Pushing table to CrossLink...")
    dataset_name <- "my_r_test_table"
    # Use the correct Rcpp exported function name
    dataset_id <- CrossLink::crosslink_push(cl_con, original_table, name = dataset_name) 
    print(paste("Table pushed successfully. Dataset ID/Name:", dataset_id))
    stopifnot(dataset_id == dataset_name) # Check if the returned ID matches the name

    # 4. Pull Data
    pulled_table <- NULL
    print(paste("Pulling table with identifier:", dataset_id, "..."))
    # Use the correct Rcpp exported function name
    pulled_table <- CrossLink::crosslink_pull(cl_con, dataset_id) # Remove as_data_frame if C++ returns Arrow
    print("Table pulled successfully.")
    print("Pulled Table:")
    print(pulled_table)

    # 5. Verify Data
    print("Verifying pulled data...")
    if (!is.null(pulled_table) && inherits(pulled_table, "Table") && original_table$Equals(pulled_table)) {
        print("SUCCESS: Pulled table matches the original table.")
        TEST_SUCCESS <- TRUE # Mark test as successful
    } else if (is.null(pulled_table)) {
        print("FAILURE: Pulled table is NULL.")
    } else if (!inherits(pulled_table, "Table")) {
        print(paste("FAILURE: Pulled object is not an arrow Table. Class:", class(pulled_table)))
    } else {
        print("FAILURE: Pulled table does NOT match the original table.")
        print("Original Schema:")
        print(original_table$schema)
        print("Pulled Schema:")
        print(pulled_table$schema)
        # Potentially print data for comparison if small
        # print("Original Data:")
        # print(as.data.frame(original_table))
        # print("Pulled Data:")
        # print(as.data.frame(pulled_table))
    }

}, error = function(e) {
    # Error handling
    print(paste("An error occurred:", e$message))
    print("Stack trace:")
    print(sys.calls()) # Print call stack for debugging
    
}, finally = {
    # 6. Cleanup
    print("Closing CrossLink connection...")
    if (!is.null(cl_con)) {
        # Use the correct Rcpp exported cleanup function
        tryCatch({
             CrossLink::crosslink_cleanup(cl_con) 
             print("Connection closed.")
        }, error = function(e_close) {
             print(paste("Error closing connection:", e_close$message))
        })
    }

    # Optional: remove the test database file after test only if successful
    # if (TEST_SUCCESS && file.exists(DB_PATH)) {
    #     print(paste("Removing test database:", DB_PATH))
    #     file.remove(DB_PATH)
    #     if (file.exists(paste0(DB_PATH, ".wal"))) {
    #         file.remove(paste0(DB_PATH, ".wal"))
    #     }
    # } else if (!TEST_SUCCESS) {
    #     print(paste("Test failed, leaving database file for inspection:", DB_PATH))
    # }

    print("--- CrossLink R Bindings Test Finished ---")
    if (!TEST_SUCCESS) {
        stop("Test failed.") # Exit with non-zero status on failure
    }
}) 