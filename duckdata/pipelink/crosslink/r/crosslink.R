#' CrossLink: Simple cross-language data sharing
#' 
#' @description A class to easily share data between R, Python, and Julia
#' @export
CrossLink <- R6::R6Class(
  "CrossLink",
  
  public = list(
    #' @field db_path Path to the DuckDB database file
    db_path = NULL,
    
    #' @field conn DuckDB connection
    conn = NULL,
    
    #' @field arrow_available Whether Arrow extension is available
    arrow_available = FALSE,
    
    #' @description Initialize CrossLink with a database path
    #' @param db_path Path to the DuckDB database file
    initialize = function(db_path = "crosslink.duckdb") {
      self$db_path <- db_path
      self$conn <- DBI::dbConnect(duckdb::duckdb(), db_path)
      private$setup_metadata_tables()
      
      # Configure DuckDB for handling large datasets
      tryCatch({
        # Set memory limit (use absolute value instead of percentage)
        # Use a cross-platform approach to get system memory
        memory_limit <- "4GB"  # Default conservative value
        
        if (Sys.info()["sysname"] == "Linux") {
          # Linux: try to use free command
          tryCatch({
            available_memory <- as.numeric(system("free -m", intern = TRUE)[2])
            if (!is.na(available_memory)) {
              memory_limit <- paste0((available_memory * 0.8), "MB")
            }
          }, error = function(e) {
            # Fallback to default if free command fails
          })
        } else if (Sys.info()["sysname"] == "Darwin") {
          # macOS: use hw.memsize via sysctl
          tryCatch({
            mem_str <- system("sysctl -n hw.memsize", intern = TRUE)
            if (length(mem_str) > 0) {
              mem_bytes <- as.numeric(mem_str)
              if (!is.na(mem_bytes)) {
                memory_limit <- paste0(round(mem_bytes * 0.8 / (1024^3), 2), "GB")
              }
            }
          }, error = function(e) {
            # Fallback to default if sysctl command fails
          })
        } else if (Sys.info()["sysname"] == "Windows") {
          # Windows: use memory.limit() which is specific to R on Windows
          tryCatch({
            # memory.limit() returns in MB on Windows
            win_mem <- memory.limit()
            if (!is.na(win_mem) && win_mem > 0) {
              memory_limit <- paste0(round(win_mem * 0.8), "MB")
            }
          }, error = function(e) {
            # Fallback to default
          })
        }
        
        # Set memory limit with error handling for each pragma
        tryCatch({
          DBI::dbExecute(self$conn, paste0("PRAGMA memory_limit='", memory_limit, "'"))
        }, error = function(e) {
          warning("Failed to set memory_limit: ", e$message)
        })
        
        # Set threads to a reasonable number (adjust based on system capabilities)
        tryCatch({
          num_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for other processes
          DBI::dbExecute(self$conn, paste0("PRAGMA threads=", num_cores))
        }, error = function(e) {
          warning("Failed to set threads: ", e$message)
        })
        
        # Try verify_parallelism instead of force_parallelism
        tryCatch({
          DBI::dbExecute(self$conn, "PRAGMA verify_parallelism")
        }, error = function(e) {
          # If verify_parallelism fails, try force_parallelism as fallback for newer DuckDB versions
          tryCatch({
            DBI::dbExecute(self$conn, "PRAGMA force_parallelism")
          }, error = function(e2) {
            warning("Failed to set parallelism pragma: ", e2$message)
          })
        })
        
        # Enable progress bar for long-running queries (with error handling)
        tryCatch({
          DBI::dbExecute(self$conn, "PRAGMA enable_progress_bar")
        }, error = function(e) {
          warning("Failed to enable progress bar: ", e$message)
        })
        
        # Enable object cache for better performance (with error handling)
        tryCatch({
          DBI::dbExecute(self$conn, "PRAGMA enable_object_cache")
        }, error = function(e) {
          warning("Failed to enable object cache: ", e$message)
        })
      }, error = function(e) {
        warning("Failed to configure DuckDB performance settings: ", e$message)
      })
      
      # Enable external IO if possible
      tryCatch({
        DBI::dbExecute(self$conn, "INSTALL httpfs")
        DBI::dbExecute(self$conn, "LOAD httpfs")
      }, error = function(e) {
        warning("httpfs extension not available. Remote file access will be limited.")
      })
      
      # Load the arrow extension in DuckDB
      tryCatch({
        DBI::dbExecute(self$conn, "INSTALL arrow;")
        DBI::dbExecute(self$conn, "LOAD arrow;")
        self$arrow_available <- TRUE
      }, error = function(e) {
        warning("Failed to load Arrow extension: ", e$message, ". Some functionality may be limited.")
        self$arrow_available <- FALSE
      })
    },
    
    #' @description List all available datasets in the CrossLink registry
    #' @return A data frame with dataset information
    list_datasets = function() {
      result <- DBI::dbGetQuery(self$conn, "
        SELECT id, name, source_language, created_at, table_name, description, version 
        FROM crosslink_metadata
        WHERE current_version = TRUE
        ORDER BY updated_at DESC
      ")
      
      return(result)
    },
    
    #' @description List all versions of a dataset
    #' @param identifier Dataset ID or name
    #' @return A data frame with version information
    list_dataset_versions = function(identifier) {
      result <- DBI::dbGetQuery(self$conn, "
        SELECT id, name, version, created_at, updated_at, schema_hash, current_version
        FROM crosslink_metadata 
        WHERE id = ? OR name = ?
        ORDER BY version DESC
      ", list(identifier, identifier))
      
      if (nrow(result) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      return(result)
    },
    
    #' @description Get schema evolution history for a dataset
    #' @param identifier Dataset ID or name
    #' @return A data frame with schema history
    get_schema_history = function(identifier) {
      # First get the ID if name was provided
      dataset_id_query <- DBI::dbGetQuery(self$conn, "
        SELECT id FROM crosslink_metadata
        WHERE id = ? OR name = ?
        LIMIT 1
      ", list(identifier, identifier))
      
      if (nrow(dataset_id_query) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      dataset_id <- dataset_id_query$id[1]
      
      result <- DBI::dbGetQuery(self$conn, "
        SELECT id, version, schema, schema_hash, changed_at, change_type, changes
        FROM crosslink_schema_history
        WHERE id = ?
        ORDER BY version
      ", list(dataset_id))
      
      return(result)
    },
    
    #' @description Get data lineage information for a dataset
    #' @param identifier Dataset ID or name
    #' @return A data frame with lineage information
    get_lineage = function(identifier) {
      # First get the ID if name was provided
      dataset_id_query <- DBI::dbGetQuery(self$conn, "
        SELECT id FROM crosslink_metadata
        WHERE id = ? OR name = ?
        LIMIT 1
      ", list(identifier, identifier))
      
      if (nrow(dataset_id_query) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      dataset_id <- dataset_id_query$id[1]
      
      result <- DBI::dbGetQuery(self$conn, "
        SELECT dataset_id, source_dataset_id, source_dataset_version, transformation, created_at
        FROM crosslink_lineage
        WHERE dataset_id = ?
      ", list(dataset_id))
      
      return(result)
    },
    
    #' @description Push a data frame to the CrossLink registry
    #' @param df Data frame to share
    #' @param name Optional name for the dataset
    #' @param description Optional description of the dataset
    #' @param use_arrow Whether to use Arrow for data storage (default: TRUE)
    #' @param sources List of source dataset IDs or names that were used to create this dataset
    #' @param transformation Description of the transformation applied to create this dataset
    #' @param force_new_version Whether to create a new version even if schema hasn't changed
    #' @param chunk_size Size of chunks when handling large datasets (NULL = auto-detect based on data size)
    #' @param use_disk_spilling Whether to allow DuckDB to spill to disk for large datasets
    #' @return The ID of the registered dataset
    push = function(df, name = NULL, description = NULL, use_arrow = TRUE, 
                   sources = NULL, transformation = NULL, force_new_version = FALSE,
                   chunk_size = NULL, use_disk_spilling = TRUE) {
      # Use the provided name or generate one
      if (is.null(name)) {
        name <- paste0("r_data_", as.integer(Sys.time()))
      }
      
      # Enable disk spilling if requested
      if (use_disk_spilling) {
        tryCatch({
          # Enable memory spilling to disk for large datasets (use absolute value)
          # Use a cross-platform approach to get system memory
          memory_limit <- "4GB"  # Default conservative value
          
          if (Sys.info()["sysname"] == "Linux") {
            # Linux: try to use free command
            tryCatch({
              available_memory <- as.numeric(system("free -m", intern = TRUE)[2])
              if (!is.na(available_memory)) {
                memory_limit <- paste0((available_memory * 0.8), "MB")
              }
            }, error = function(e) {
              # Fallback to default if free command fails
            })
          } else if (Sys.info()["sysname"] == "Darwin") {
            # macOS: use hw.memsize via sysctl
            tryCatch({
              mem_str <- system("sysctl -n hw.memsize", intern = TRUE)
              if (length(mem_str) > 0) {
                mem_bytes <- as.numeric(mem_str)
                if (!is.na(mem_bytes)) {
                  memory_limit <- paste0(round(mem_bytes * 0.8 / (1024^3), 2), "GB")
                }
              }
            }, error = function(e) {
              # Fallback to default if sysctl command fails
            })
          } else if (Sys.info()["sysname"] == "Windows") {
            # Windows: use memory.limit() which is specific to R on Windows
            tryCatch({
              # memory.limit() returns in MB on Windows
              win_mem <- memory.limit()
              if (!is.na(win_mem) && win_mem > 0) {
                memory_limit <- paste0(round(win_mem * 0.8), "MB")
              }
            }, error = function(e) {
              # Fallback to default
            })
          }
          
          # Set memory limit with error handling
          tryCatch({
            DBI::dbExecute(self$conn, paste0("PRAGMA memory_limit='", memory_limit, "'"))
          }, error = function(e) {
            warning("Failed to set memory_limit: ", e$message)
          })
          
          # Set threads with error handling - use parallelism more effectively
          tryCatch({
            # Use parallel processing for better performance
            num_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for other processes
            DBI::dbExecute(self$conn, paste0("PRAGMA threads=", num_cores))
          }, error = function(e) {
            warning("Failed to set threads: ", e$message)
          })
          
          # Try verify_parallelism instead of force_parallelism
          tryCatch({
            DBI::dbExecute(self$conn, "PRAGMA verify_parallelism")
          }, error = function(e) {
            # If verify_parallelism fails, try force_parallelism as fallback for newer DuckDB versions
            tryCatch({
              DBI::dbExecute(self$conn, "PRAGMA force_parallelism")
            }, error = function(e2) {
              warning("Failed to set parallelism pragma: ", e2$message)
            })
          })
        }, error = function(e) {
          warning(paste0("Failed to configure DuckDB memory settings: ", e$message))
        })
      }
      
      # Check if dataset with this name already exists
      existing <- DBI::dbGetQuery(self$conn, "
        SELECT id, schema, version FROM crosslink_metadata 
        WHERE name = ? AND current_version = TRUE
      ", list(name))
      
      dataset_id <- NULL
      create_new_version <- TRUE
      old_version <- 1
      
      if (nrow(existing) > 0) {
        dataset_id <- existing$id[1]
        old_schema <- existing$schema[1]
        old_version <- existing$version[1]
        
        # Detect schema changes
        schema_hash <- private$compute_schema_hash(df)
        schema_changes <- private$detect_schema_changes(old_schema, df)
        
        # Only create new version if schema changed or forced
        if (schema_changes$change_type == "no_change" && !force_new_version) {
          create_new_version <- FALSE
        }
      } else {
        # New dataset
        dataset_id <- uuid::UUIDgenerate()
        old_schema <- NULL
        schema_changes <- list(change_type = "initial_schema")
      }
      
      # Generate unique table name for this version
      new_version <- ifelse(create_new_version, old_version + 1, old_version)
      table_name <- paste0("crosslink_data_", gsub("-", "_", dataset_id), "_", new_version)
      
      # Calculate schema hash and JSON
      schema_json <- jsonlite::toJSON(list(
        columns = colnames(df),
        dtypes = sapply(df, class),
        is_arrow = ifelse(use_arrow && self$arrow_available, TRUE, FALSE)
      ))
      schema_hash <- private$compute_schema_hash(df)
      
      # Determine if we should use chunking
      use_chunking <- FALSE
      estimated_size_bytes <- sum(vapply(df, function(col) object.size(col), numeric(1)))
      
      if (is.null(chunk_size)) {
        # Auto-detect: if dataset is larger than 1GB, use chunking (align with Python)
        use_chunking <- estimated_size_bytes > 1000 * 1024 * 1024  # 1GB
        chunk_size <- ifelse(use_chunking, 500000, NULL)  # Default chunk size of 500k rows (align with Python)
      } else {
        use_chunking <- chunk_size > 0
      }
      
      # Store data using Arrow if possible, otherwise fall back to direct table creation
      arrow_stored <- FALSE
      
      if (use_arrow && self$arrow_available) {
        tryCatch({
          if (use_chunking) {
            # Create empty table with schema first
            sample_df <- df[1:min(1, nrow(df)), , drop=FALSE]
            arrow_table_sample <- arrow::as_arrow_table(sample_df)
            
            if (create_new_version) {
              # Create a new table for the new version with schema only
              duckdb::duckdb_register_arrow(self$conn, "arrow_schema", arrow_table_sample)
              DBI::dbExecute(self$conn, paste0("CREATE TABLE ", table_name, " AS SELECT * FROM arrow_schema WHERE 1=0"))
              DBI::dbExecute(self$conn, "DROP VIEW IF EXISTS arrow_schema")
              
              # Insert data in chunks
              chunk_count <- ceiling(nrow(df) / chunk_size)
              for (i in 1:chunk_count) {
                start_idx <- ((i-1) * chunk_size) + 1
                end_idx <- min(i * chunk_size, nrow(df))
                chunk <- df[start_idx:end_idx, , drop=FALSE]
                
                arrow_chunk <- arrow::as_arrow_table(chunk)
                duckdb::duckdb_register_arrow(self$conn, "arrow_chunk", arrow_chunk)
                DBI::dbExecute(self$conn, paste0("INSERT INTO ", table_name, " SELECT * FROM arrow_chunk"))
                DBI::dbExecute(self$conn, "DROP VIEW IF EXISTS arrow_chunk")
                
                # Release memory explicitly
                rm(chunk, arrow_chunk)
                gc()
              }
            } else {
              # Update existing table
              old_table_name <- paste0("crosslink_data_", gsub("-", "_", dataset_id), "_", old_version)
              DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", old_table_name))
              
              # Create empty table with schema
              duckdb::duckdb_register_arrow(self$conn, "arrow_schema", arrow_table_sample)
              DBI::dbExecute(self$conn, paste0("CREATE TABLE ", old_table_name, " AS SELECT * FROM arrow_schema WHERE 1=0"))
              DBI::dbExecute(self$conn, "DROP VIEW IF EXISTS arrow_schema")
              
              # Insert data in chunks
              chunk_count <- ceiling(nrow(df) / chunk_size)
              for (i in 1:chunk_count) {
                start_idx <- ((i-1) * chunk_size) + 1
                end_idx <- min(i * chunk_size, nrow(df))
                chunk <- df[start_idx:end_idx, , drop=FALSE]
                
                arrow_chunk <- arrow::as_arrow_table(chunk)
                duckdb::duckdb_register_arrow(self$conn, "arrow_chunk", arrow_chunk)
                DBI::dbExecute(self$conn, paste0("INSERT INTO ", old_table_name, " SELECT * FROM arrow_chunk"))
                DBI::dbExecute(self$conn, "DROP VIEW IF EXISTS arrow_chunk")
                
                # Release memory explicitly 
                rm(chunk, arrow_chunk)
                gc()
              }
              
              table_name <- old_table_name
            }
            
            # Mark as successfully stored using Arrow
            arrow_stored <- TRUE
          } else {
            # Non-chunked approach for smaller datasets
            # Convert data frame to Arrow table
            arrow_table <- arrow::as_arrow_table(df)
            
            if (create_new_version) {
              # Create a new table for the new version
              duckdb::duckdb_register_arrow(self$conn, "arrow_table", arrow_table)
              DBI::dbExecute(self$conn, paste0("CREATE TABLE ", table_name, " AS SELECT * FROM arrow_table"))
              DBI::dbExecute(self$conn, "DROP VIEW IF EXISTS arrow_table")
            } else {
              # Update existing table
              old_table_name <- paste0("crosslink_data_", gsub("-", "_", dataset_id), "_", old_version)
              DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", old_table_name))
              duckdb::duckdb_register_arrow(self$conn, "arrow_table", arrow_table)
              DBI::dbExecute(self$conn, paste0("CREATE TABLE ", old_table_name, " AS SELECT * FROM arrow_table"))
              DBI::dbExecute(self$conn, "DROP VIEW IF EXISTS arrow_table")
              table_name <- old_table_name
            }
            
            # Mark as successfully stored using Arrow
            arrow_stored <- TRUE
          }
        }, error = function(e) {
          warning("Failed to use Arrow for data storage: ", e$message)
          warning("Falling back to direct DuckDB table creation")
          arrow_stored <- FALSE
        })
      }
      
      if (!arrow_stored) {
        # Direct table creation without Arrow
        tryCatch({
          if (use_chunking) {
            if (create_new_version) {
              # Create empty table with schema
              sample_df <- df[1:min(1, nrow(df)), , drop=FALSE]
              DBI::dbWriteTable(self$conn, table_name, sample_df, overwrite = TRUE)
              DBI::dbExecute(self$conn, paste0("DELETE FROM ", table_name))
              
              # Insert data in chunks
              chunk_count <- ceiling(nrow(df) / chunk_size)
              for (i in 1:chunk_count) {
                start_idx <- ((i-1) * chunk_size) + 1
                end_idx <- min(i * chunk_size, nrow(df))
                chunk <- df[start_idx:end_idx, , drop=FALSE]
                
                DBI::dbAppendTable(self$conn, table_name, chunk)
                
                # Release memory explicitly
                rm(chunk)
                gc()
              }
            } else {
              # Update existing table
              old_table_name <- paste0("crosslink_data_", gsub("-", "_", dataset_id), "_", old_version)
              DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", old_table_name))
              
              # Create empty table with schema
              sample_df <- df[1:min(1, nrow(df)), , drop=FALSE]
              DBI::dbWriteTable(self$conn, old_table_name, sample_df, overwrite = TRUE)
              DBI::dbExecute(self$conn, paste0("DELETE FROM ", old_table_name))
              
              # Insert data in chunks
              chunk_count <- ceiling(nrow(df) / chunk_size)
              for (i in 1:chunk_count) {
                start_idx <- ((i-1) * chunk_size) + 1
                end_idx <- min(i * chunk_size, nrow(df))
                chunk <- df[start_idx:end_idx, , drop=FALSE]
                
                DBI::dbAppendTable(self$conn, old_table_name, chunk)
                
                # Release memory explicitly
                rm(chunk)
                gc()
              }
              
              table_name <- old_table_name
            }
          } else {
            # Non-chunked approach for smaller datasets
            if (create_new_version) {
              DBI::dbWriteTable(self$conn, table_name, df, overwrite = TRUE)
            } else {
              old_table_name <- paste0("crosslink_data_", gsub("-", "_", dataset_id), "_", old_version)
              DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", old_table_name))
              DBI::dbWriteTable(self$conn, old_table_name, df, overwrite = TRUE)
              table_name <- old_table_name
            }
          }
        }, error = function(e) {
          # If we still have memory issues, try with a smaller chunk size (similar to Python)
          if (!use_chunking || chunk_size > 10000) {
            smaller_chunk <- ifelse(!use_chunking, 10000, chunk_size %/% 2)
            warning(paste0("Retrying with smaller chunk size (", smaller_chunk, ")"))
            return(self$push(df, name, description, use_arrow, sources, transformation, 
                           force_new_version, smaller_chunk, use_disk_spilling))
          } else {
            stop(paste0("Failed to store data: ", e$message))
          }
        })
      }
      
      # If creating a new version, mark old versions as not current
      if (create_new_version && nrow(existing) > 0) {
        DBI::dbExecute(self$conn, "
          UPDATE crosslink_metadata SET current_version = FALSE
          WHERE id = ?
        ", list(dataset_id))
        
        # Record schema history
        DBI::dbExecute(self$conn, "
          INSERT INTO crosslink_schema_history 
          (id, version, schema, schema_hash, changed_at, change_type, changes)
          VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
        ", list(
          dataset_id, 
          new_version, 
          schema_json, 
          schema_hash, 
          schema_changes$change_type, 
          jsonlite::toJSON(schema_changes$changes, auto_unbox = TRUE)
        ))
      }
      
      # Record lineage information
      if (!is.null(sources) && create_new_version) {
        if (!is.list(sources) && !is.vector(sources)) {
          sources <- list(sources)
        }
        
        for (source in sources) {
          # Get source dataset ID and version
          source_metadata <- DBI::dbGetQuery(self$conn, "
            SELECT id, version FROM crosslink_metadata 
            WHERE (id = ? OR name = ?) AND current_version = TRUE
          ", list(source, source))
          
          if (nrow(source_metadata) > 0) {
            source_id <- source_metadata$id[1]
            source_version <- source_metadata$version[1]
            
            # Record lineage
            DBI::dbExecute(self$conn, "
              INSERT OR REPLACE INTO crosslink_lineage
              (dataset_id, source_dataset_id, source_dataset_version, transformation, created_at)
              VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            ", list(dataset_id, source_id, source_version, ifelse(is.null(transformation), "unknown", transformation)))
          }
        }
      }
      
      # Insert or update metadata
      if (create_new_version) {
        # Insert new metadata record
        DBI::dbExecute(self$conn, "
          INSERT INTO crosslink_metadata 
          (id, name, source_language, created_at, updated_at, description, schema, table_name, arrow_data, version, current_version, schema_hash)
          VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, TRUE, ?)
        ", list(
          dataset_id, name, "r", description, schema_json, table_name, arrow_stored, new_version, schema_hash
        ))
      } else {
        # Update existing metadata record
        DBI::dbExecute(self$conn, "
          UPDATE crosslink_metadata SET
          updated_at = CURRENT_TIMESTAMP,
          description = COALESCE(?, description),
          arrow_data = ?
          WHERE id = ? AND version = ?
        ", list(description, arrow_stored, dataset_id, old_version))
      }
      
      return(dataset_id)
    },
    
    #' @description Pull a dataset from the CrossLink registry
    #' @param identifier Dataset ID or name
    #' @param to_pandas Whether to return as data frame (default: TRUE) (name kept for API consistency with Python)
    #' @param use_arrow Whether to use Arrow for data retrieval when available (default: TRUE)
    #' @param version Specific version to retrieve (default: latest version)
    #' @param chunk_size Size of chunks when retrieving large datasets (only used when stream=TRUE)
    #' @param stream Whether to return an iterator over chunks instead of the full dataset
    #' @param use_disk_spilling Whether to allow DuckDB to spill to disk for large operations
    #' @return If stream=FALSE (default): DataFrame or Arrow Table with the dataset, If stream=TRUE: Iterator over DataFrame or Arrow Table chunks
    pull = function(identifier, to_pandas = TRUE, use_arrow = TRUE, version = NULL,
                   chunk_size = NULL, stream = FALSE, use_disk_spilling = TRUE) {
      # Enable disk spilling if requested
      if (use_disk_spilling) {
        tryCatch({
          # Enable memory spilling to disk for large datasets (use absolute value)
          # Use a cross-platform approach to get system memory
          memory_limit <- "4GB"  # Default conservative value
          
          if (Sys.info()["sysname"] == "Linux") {
            # Linux: try to use free command
            tryCatch({
              available_memory <- as.numeric(system("free -m", intern = TRUE)[2])
              if (!is.na(available_memory)) {
                memory_limit <- paste0((available_memory * 0.8), "MB")
              }
            }, error = function(e) {
              # Fallback to default if free command fails
            })
          } else if (Sys.info()["sysname"] == "Darwin") {
            # macOS: use hw.memsize via sysctl
            tryCatch({
              mem_str <- system("sysctl -n hw.memsize", intern = TRUE)
              if (length(mem_str) > 0) {
                mem_bytes <- as.numeric(mem_str)
                if (!is.na(mem_bytes)) {
                  memory_limit <- paste0(round(mem_bytes * 0.8 / (1024^3), 2), "GB")
                }
              }
            }, error = function(e) {
              # Fallback to default if sysctl command fails
            })
          } else if (Sys.info()["sysname"] == "Windows") {
            # Windows: use memory.limit() which is specific to R on Windows
            tryCatch({
              # memory.limit() returns in MB on Windows
              win_mem <- memory.limit()
              if (!is.na(win_mem) && win_mem > 0) {
                memory_limit <- paste0(round(win_mem * 0.8), "MB")
              }
            }, error = function(e) {
              # Fallback to default
            })
          }
          
          # Set memory limit with error handling
          tryCatch({
            DBI::dbExecute(self$conn, paste0("PRAGMA memory_limit='", memory_limit, "'"))
          }, error = function(e) {
            warning("Failed to set memory_limit: ", e$message)
          })
          
          # Set threads with error handling - use parallelism more effectively
          tryCatch({
            # Use parallel processing for better performance
            num_cores <- max(1, parallel::detectCores() - 1)  # Leave 1 core for other processes
            DBI::dbExecute(self$conn, paste0("PRAGMA threads=", num_cores))
          }, error = function(e) {
            warning("Failed to set threads: ", e$message)
          })
          
          # Try verify_parallelism instead of force_parallelism
          tryCatch({
            DBI::dbExecute(self$conn, "PRAGMA verify_parallelism")
          }, error = function(e) {
            # If verify_parallelism fails, try force_parallelism as fallback for newer DuckDB versions
            tryCatch({
              DBI::dbExecute(self$conn, "PRAGMA force_parallelism")
            }, error = function(e2) {
              warning("Failed to set parallelism pragma: ", e2$message)
            })
          })
        }, error = function(e) {
          warning(paste0("Failed to configure DuckDB memory settings: ", e$message))
        })
      }
      
      # Try to find by ID/name with version condition
      version_condition <- ""
      params <- list(identifier, identifier)
      
      if (!is.null(version)) {
        version_condition <- "AND version = ?"
        params <- c(params, version)
      } else {
        version_condition <- "AND current_version = TRUE"
      }
      
      query <- paste0("
        SELECT table_name, schema, arrow_data, 
               (SELECT COUNT(*) FROM crosslink_metadata cm 
                WHERE (id = ? OR name = ?) ", version_condition, ") as row_count
        FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) ", version_condition)
      
      metadata <- DBI::dbGetQuery(self$conn, query, c(list(identifier, identifier), params))
      
      if (nrow(metadata) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' and specified version not found"))
      }
      
      table_name <- metadata$table_name[1]
      schema_json <- metadata$schema[1]
      arrow_data <- metadata$arrow_data[1]
      dataset_size <- metadata$row_count[1]
      
      # Parse schema to check if it's Arrow data
      schema <- NULL
      if (!is.na(schema_json)) {
        schema <- jsonlite::fromJSON(schema_json)
      } else {
        schema <- list()
      }
      
      is_arrow <- schema$is_arrow || (!is.na(arrow_data) && arrow_data)
      
      # Get row count to determine if we should use chunking
      row_count_query <- paste0("SELECT COUNT(*) FROM ", table_name)
      row_count <- DBI::dbGetQuery(self$conn, row_count_query)[1, 1]
      
      # Auto-determine chunk size if not specified and streaming
      if (stream && is.null(chunk_size)) {
        # Default chunk size: 100k rows for regular tables, 10k rows for wide tables
        col_count <- length(schema$columns)
        if (!is.null(col_count) && col_count > 100) {  # Wide table
          chunk_size <- 10000
        } else {
          chunk_size <- 100000
        }
      }
      
      # Define a generator for streaming data
      data_stream_generator <- function() {
        # Calculate number of chunks
        num_chunks <- ceiling(row_count / chunk_size)
        
        # Create an environment to track state
        env <- new.env()
        env$chunk_idx <- 0
        env$has_next <- num_chunks > 0
        
        # Define nextElem function
        nextElem <- function() {
          if (!env$has_next) {
            stop("StopIteration")
          }
          
          offset <- env$chunk_idx * chunk_size
          limit <- chunk_size
          
          # Query for this chunk
          chunk_query <- paste0("SELECT * FROM ", table_name, " LIMIT ", limit, " OFFSET ", offset)
          
          # Use Arrow for retrieval if available and requested
          if (use_arrow && self$arrow_available && is_arrow) {
            tryCatch({
              # Try to use native Arrow retrieval
              chunk_result <- NULL
              
              if (requireNamespace("arrow", quietly = TRUE)) {
                # Use DuckDB Arrow integration if available
                con_info <- duckdb::duckdb_connection_info(self$conn)
                
                if ("arrow" %in% names(con_info) && con_info$arrow) {
                  # Direct Arrow retrieval
                  query_df <- DBI::dbSendQuery(self$conn, chunk_query)
                  chunk_result <- duckdb::duckdb_fetch_record_batch(query_df)
                  DBI::dbClearResult(query_df)
                  
                  # Convert to data frame if requested
                  if (to_pandas) {
                    chunk_result <- arrow::as_data_frame(chunk_result)
                  }
                } else {
                  # Standard retrieval and convert manually
                  chunk_result <- DBI::dbGetQuery(self$conn, chunk_query)
                  if (!to_pandas) {
                    chunk_result <- arrow::as_arrow_table(chunk_result)
                  }
                }
              } else {
                # Fall back to standard retrieval
                chunk_result <- DBI::dbGetQuery(self$conn, chunk_query)
                if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
                  chunk_result <- arrow::as_arrow_table(chunk_result)
                }
              }
              
              return(chunk_result)
            }, error = function(e) {
              warning("Arrow retrieval failed, falling back to standard: ", e$message)
              # Standard DuckDB retrieval as fallback
              chunk_result <- DBI::dbGetQuery(self$conn, chunk_query)
              if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
                chunk_result <- arrow::as_arrow_table(chunk_result)
              }
              return(chunk_result)
            })
          } else {
            # Standard DuckDB retrieval
            chunk_result <- DBI::dbGetQuery(self$conn, chunk_query)
            
            if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
              # Convert to Arrow Table if requested
              chunk_result <- arrow::as_arrow_table(chunk_result)
            }
            
            return(chunk_result)
          }
          
          # Increment chunk index and check if we have more chunks
          env$chunk_idx <- env$chunk_idx + 1
          env$has_next <- env$chunk_idx < num_chunks
        }
        
        # Return an iterator-like object
        structure(
          list(
            nextElem = nextElem,
            hasNext = function() env$has_next
          ),
          class = c("abstractiter", "iter")
        )
      }
      
      # Return a streaming iterator if requested
      if (stream) {
        return(data_stream_generator())
      }
      
      # For non-streaming mode, we still use chunking internally for large datasets
      if (row_count > 1000000) {  # For datasets with more than 1M rows
        # Use chunking internally for memory efficiency
        internal_chunk_size <- 500000  # 500k rows per chunk
        
        # Decide on container based on output type
        all_chunks <- list()
        
        # Stream data in chunks
        for (i in seq(0, row_count - 1, by = internal_chunk_size)) {
          chunk_query <- paste0("SELECT * FROM ", table_name, " LIMIT ", internal_chunk_size, " OFFSET ", i)
          
          chunk <- NULL
          if (use_arrow && self$arrow_available && is_arrow) {
            tryCatch({
              # Try to use native Arrow retrieval
              if (requireNamespace("arrow", quietly = TRUE)) {
                # Use DuckDB Arrow integration if available
                con_info <- duckdb::duckdb_connection_info(self$conn)
                
                if ("arrow" %in% names(con_info) && con_info$arrow) {
                  # Direct Arrow retrieval
                  query_df <- DBI::dbSendQuery(self$conn, chunk_query)
                  chunk <- duckdb::duckdb_fetch_record_batch(query_df)
                  DBI::dbClearResult(query_df)
                  
                  # Convert to data frame if requested
                  if (to_pandas) {
                    chunk <- arrow::as_data_frame(chunk)
                  }
                } else {
                  # Standard retrieval and convert manually
                  chunk <- DBI::dbGetQuery(self$conn, chunk_query)
                  if (!to_pandas) {
                    chunk <- arrow::as_arrow_table(chunk)
                  }
                }
              } else {
                # Fall back to standard retrieval
                chunk <- DBI::dbGetQuery(self$conn, chunk_query)
                if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
                  chunk <- arrow::as_arrow_table(chunk)
                }
              }
            }, error = function(e) {
              warning("Arrow retrieval failed, falling back to standard: ", e$message)
              # Standard DuckDB retrieval as fallback
              chunk <- DBI::dbGetQuery(self$conn, chunk_query)
              if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
                chunk <- arrow::as_arrow_table(chunk)
              }
            })
          } else {
            # Standard DuckDB retrieval
            chunk <- DBI::dbGetQuery(self$conn, chunk_query)
            if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
              chunk <- arrow::as_arrow_table(chunk)
            }
          }
          
          all_chunks[[length(all_chunks) + 1]] <- chunk
        }
        
        # Combine all chunks
        if (length(all_chunks) > 0) {
          if (to_pandas || !requireNamespace("arrow", quietly = TRUE)) {
            if (is.data.frame(all_chunks[[1]])) {
              return(do.call(rbind, all_chunks))
            } else {
              # Convert Arrow tables to data frames first then combine
              df_chunks <- lapply(all_chunks, arrow::as_data_frame)
              return(do.call(rbind, df_chunks))
            }
          } else {
            # Check if we have Arrow tables
            if (inherits(all_chunks[[1]], "arrow_table") || 
                inherits(all_chunks[[1]], "ArrowTabular") ||
                inherits(all_chunks[[1]], "Table")) {
              # Combine Arrow tables
              if (requireNamespace("arrow", quietly = TRUE)) {
                return(arrow::concat_tables(all_chunks))
              } else {
                # Convert to data frames if arrow package not available
                df_chunks <- lapply(all_chunks, function(chunk) {
                  if (is.data.frame(chunk)) return(chunk)
                  return(as.data.frame(chunk))
                })
                return(do.call(rbind, df_chunks))
              }
            } else {
              # Convert to Arrow tables then combine
              arrow_chunks <- lapply(all_chunks, function(chunk) {
                if (is.data.frame(chunk)) {
                  return(arrow::as_arrow_table(chunk))
                }
                return(chunk)
              })
              return(arrow::concat_tables(arrow_chunks))
            }
          }
        }
        
        # Return empty result
        if (to_pandas) {
          return(data.frame())
        } else if (requireNamespace("arrow", quietly = TRUE)) {
          return(arrow::arrow_table(data.frame()))
        } else {
          return(data.frame())
        }
      }
      
      # Standard retrieval for smaller datasets
      query <- paste0("SELECT * FROM ", table_name)
      
      # Use Arrow for retrieval if available and requested
      if (use_arrow && self$arrow_available && is_arrow) {
        tryCatch({
          # Try to use native Arrow retrieval
          result <- NULL
          
          if (requireNamespace("arrow", quietly = TRUE)) {
            # Use DuckDB Arrow integration if available
            con_info <- duckdb::duckdb_connection_info(self$conn)
            
            if ("arrow" %in% names(con_info) && con_info$arrow) {
              # Direct Arrow retrieval
              query_df <- DBI::dbSendQuery(self$conn, query)
              result <- duckdb::duckdb_fetch_record_batch(query_df)
              DBI::dbClearResult(query_df)
              
              # Convert to data frame if requested
              if (to_pandas) {
                result <- arrow::as_data_frame(result)
              }
              
              return(result)
            }
          }
          
          # Fall back to standard retrieval and manual conversion
          result <- DBI::dbGetQuery(self$conn, query)
          
          if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
            # Return as Arrow Table
            return(arrow::as_arrow_table(result))
          } else {
            # Already a data frame
            return(result)
          }
        }, error = function(e) {
          warning("Arrow retrieval failed, falling back to standard: ", e$message)
          # Standard DuckDB retrieval as fallback
          result <- DBI::dbGetQuery(self$conn, query)
          
          if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
            # Return as Arrow Table
            return(arrow::as_arrow_table(result))
          } else {
            # Already a data frame
            return(result)
          }
        })
      } else {
        # Standard DuckDB retrieval
        result <- DBI::dbGetQuery(self$conn, query)
        
        if (!to_pandas && requireNamespace("arrow", quietly = TRUE)) {
          # Return as Arrow Table
          return(arrow::as_arrow_table(result))
        } else {
          # Return as data frame
          return(result)
        }
      }
    },
    
    #' @description Delete a dataset from the CrossLink registry
    #' @param identifier Dataset ID or name
    #' @param version Specific version to delete (default: all versions)
    delete = function(identifier, version = NULL) {
      # Get metadata for the dataset
      version_condition <- ""
      params <- list(identifier, identifier)
      
      if (!is.null(version)) {
        version_condition <- "AND version = ?"
        params <- c(params, version)
      }
      
      query <- paste0("
        SELECT id, table_name, version FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) ", version_condition)
      
      metadata_results <- DBI::dbGetQuery(self$conn, query, params)
      
      if (nrow(metadata_results) == 0) {
        stop(paste0("Dataset with identifier '", identifier, "' not found"))
      }
      
      dataset_id <- metadata_results$id[1]
      
      for (i in 1:nrow(metadata_results)) {
        table_name <- metadata_results$table_name[i]
        ver <- metadata_results$version[i]
        
        # Delete the data table
        DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", table_name))
        
        # Delete schema history
        DBI::dbExecute(self$conn, "
          DELETE FROM crosslink_schema_history WHERE id = ? AND version = ?
        ", list(dataset_id, ver))
        
        # Delete the metadata
        DBI::dbExecute(self$conn, "
          DELETE FROM crosslink_metadata WHERE id = ? AND version = ?
        ", list(dataset_id, ver))
      }
      
      # If deleting all versions, also clean up lineage
      if (is.null(version)) {
        DBI::dbExecute(self$conn, "
          DELETE FROM crosslink_lineage WHERE dataset_id = ? OR source_dataset_id = ?
        ", list(dataset_id, dataset_id))
      }
    },
    
    #' @description Check if two datasets have compatible schemas
    #' @param source_id Source dataset ID or name
    #' @param target_id Target dataset ID or name
    #' @return Compatibility information
    check_compatibility = function(source_id, target_id) {
      source_metadata <- DBI::dbGetQuery(self$conn, "
        SELECT schema FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) AND current_version = TRUE
      ", list(source_id, source_id))
      
      target_metadata <- DBI::dbGetQuery(self$conn, "
        SELECT schema FROM crosslink_metadata 
        WHERE (id = ? OR name = ?) AND current_version = TRUE
      ", list(target_id, target_id))
      
      if (nrow(source_metadata) == 0 || nrow(target_metadata) == 0) {
        missing <- c()
        if (nrow(source_metadata) == 0) {
          missing <- c(missing, paste0("Source dataset '", source_id, "' not found"))
        }
        if (nrow(target_metadata) == 0) {
          missing <- c(missing, paste0("Target dataset '", target_id, "' not found"))
        }
        return(list(compatible = FALSE, reason = paste(missing, collapse = "; ")))
      }
      
      source_schema <- jsonlite::fromJSON(source_metadata$schema[1])
      target_schema <- jsonlite::fromJSON(target_metadata$schema[1])
      
      source_cols <- source_schema$columns
      target_cols <- target_schema$columns
      
      missing_cols <- setdiff(source_cols, target_cols)
      if (length(missing_cols) > 0) {
        return(list(
          compatible = FALSE,
          reason = paste0("Target schema missing columns: ", paste(missing_cols, collapse = ", "))
        ))
      }
      
      common_cols <- intersect(source_cols, target_cols)
      source_dtypes <- source_schema$dtypes
      target_dtypes <- target_schema$dtypes
      
      dtype_mismatches <- c()
      for (col in common_cols) {
        source_type <- source_dtypes[[col]]
        target_type <- target_dtypes[[col]]
        if (!identical(source_type, target_type)) {
          dtype_mismatches <- c(dtype_mismatches, paste0(col, ": ", source_type, " vs ", target_type))
        }
      }
      
      if (length(dtype_mismatches) > 0) {
        return(list(
          compatible = FALSE,
          reason = paste0("Type mismatches: ", paste(dtype_mismatches, collapse = "; "))
        ))
      }
      
      return(list(compatible = TRUE))
    },
    
    #' @description Close the connection to the database
    close = function() {
      DBI::dbDisconnect(self$conn)
    },
    
    #' @description Read data from remote files like S3, similar to the Python implementation
    #' @param remote_path Path to remote file or glob pattern (e.g., 's3://bucket/path/*.parquet')
    #' @param format File format (e.g., 'parquet', 'csv', etc.)
    #' @param columns Specific columns to read
    #' @param filters Filters to apply at scan time
    #' @param use_arrow Whether to use Arrow for reading
    #' @param use_parallel Whether to parallelize reading (increases threads)
    #' @param cache_locally Whether to cache remote data locally
    #' @param local_cache_path Path for local caching
    #' @return Data frame or Arrow table with the remote data
    read_remote = function(remote_path, format = "parquet", columns = NULL, filters = NULL,
                          use_arrow = TRUE, use_parallel = TRUE, cache_locally = FALSE,
                          local_cache_path = NULL) {
      # Check if httpfs extension is loaded
      is_httpfs_loaded <- tryCatch({
        httpfs_check <- DBI::dbGetQuery(self$conn, "SELECT loaded FROM pragma_database_list WHERE name='httpfs'")
        if (nrow(httpfs_check) > 0) {
          return(httpfs_check$loaded[1] == 1)
        }
        return(FALSE)
      }, error = function(e) {
        return(FALSE)
      })
      
      # Try to load httpfs if not already loaded
      if (!is_httpfs_loaded) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL httpfs")
          DBI::dbExecute(self$conn, "LOAD httpfs")
        }, error = function(e) {
          warning("Failed to load httpfs extension. Remote file access may be limited: ", e$message)
        })
      }
      
      # Configure DuckDB for remote file access
      if (use_parallel) {
        tryCatch({
          # For remote files, use more threads than CPU cores
          # This helps with parallelism when reading remote files since I/O is the bottleneck
          num_cores <- max(1, parallel::detectCores() - 1)
          thread_count <- num_cores * 3  # Use 3x the cores for better network parallelism
          DBI::dbExecute(self$conn, paste0("PRAGMA threads=", thread_count))
        }, error = function(e) {
          warning("Failed to set threads for remote file access: ", e$message)
        })
      }
      
      # Determine appropriate SQL query based on format and options
      columns_sql <- "*"
      if (!is.null(columns) && length(columns) > 0) {
        columns_sql <- paste(columns, collapse = ", ")
      }
      
      # Format-specific handling
      if (format == "parquet") {
        # Handle parquet files
        query <- paste0("SELECT ", columns_sql, " FROM read_parquet('", remote_path, "'")
        
        # Add filter conditions if present
        if (!is.null(filters) && length(filters) > 0) {
          if (is.character(filters)) {
            query <- paste0(query, ", HIVE_PARTITIONING=1, FILENAME=1) WHERE ", filters)
          } else if (is.list(filters)) {
            # Convert list of conditions to SQL WHERE clause
            where_parts <- c()
            for (i in seq_along(filters)) {
              cond <- filters[[i]]
              if (length(cond) >= 3) {
                col <- cond[[1]]
                op <- cond[[2]]
                val <- cond[[3]]
                
                # Handle different value types
                if (is.character(val)) {
                  val <- paste0("'", val, "'") 
                }
                
                where_parts <- c(where_parts, paste0(col, " ", op, " ", val))
              }
            }
            
            if (length(where_parts) > 0) {
              where_clause <- paste(where_parts, collapse = " AND ")
              query <- paste0(query, ", HIVE_PARTITIONING=1, FILENAME=1) WHERE ", where_clause)
            } else {
              query <- paste0(query, ")")
            }
          } else {
            query <- paste0(query, ")")
          }
        } else {
          query <- paste0(query, ")")
        }
      } else if (format == "csv") {
        # Handle CSV files
        query <- paste0("SELECT ", columns_sql, " FROM read_csv('", remote_path, 
                       "', AUTO_DETECT=TRUE, SAMPLE_SIZE=1000, FILENAME=1)")
        
        # Add filter conditions if present
        if (!is.null(filters) && length(filters) > 0 && is.character(filters)) {
          query <- paste0(query, " WHERE ", filters)
        }
      } else if (format == "json") {
        # Handle JSON files
        query <- paste0("SELECT ", columns_sql, " FROM read_json('", remote_path, 
                       "', AUTO_DETECT=TRUE, FILENAME=1)")
        
        # Add filter conditions if present
        if (!is.null(filters) && length(filters) > 0 && is.character(filters)) {
          query <- paste0(query, " WHERE ", filters)
        }
      } else {
        stop(paste0("Unsupported format: ", format))
      }
      
      # Execute query with the appropriate output format
      result <- NULL
      
      if (use_arrow && self$arrow_available) {
        tryCatch({
          if (requireNamespace("arrow", quietly = TRUE)) {
            # Use DuckDB Arrow integration if available
            con_info <- duckdb::duckdb_connection_info(self$conn)
            
            if ("arrow" %in% names(con_info) && con_info$arrow) {
              # Direct Arrow retrieval
              query_df <- DBI::dbSendQuery(self$conn, query)
              result <- duckdb::duckdb_fetch_record_batch(query_df)
              DBI::dbClearResult(query_df)
            } else {
              # Standard retrieval and convert manually
              result <- DBI::dbGetQuery(self$conn, query)
              result <- arrow::as_arrow_table(result)
            }
          } else {
            # Fall back to standard retrieval
            result <- DBI::dbGetQuery(self$conn, query)
          }
        }, error = function(e) {
          warning("Arrow retrieval failed, falling back to standard: ", e$message)
          # Standard DuckDB retrieval as fallback
          result <- DBI::dbGetQuery(self$conn, query)
        })
      } else {
        # Standard DuckDB retrieval
        result <- DBI::dbGetQuery(self$conn, query)
      }
      
      # Cache data locally if requested
      if (cache_locally && !is.null(result)) {
        tryCatch({
          if (is.null(local_cache_path)) {
            # Generate a local path based on remote path
            remote_file_name <- gsub(".*[/\\\\]", "", remote_path)
            remote_file_name <- gsub("[*?]", "_", remote_file_name)  # Replace glob chars
            local_cache_path <- file.path(tempdir(), paste0("crosslink_cache_", remote_file_name))
          }
          
          if (inherits(result, "arrow_table") || 
              inherits(result, "ArrowTabular") || 
              inherits(result, "Table")) {
            if (requireNamespace("arrow", quietly = TRUE)) {
              if (format == "parquet") {
                arrow::write_parquet(result, local_cache_path)
              } else if (format == "csv") {
                arrow::write_csv_arrow(result, local_cache_path)
              } else {
                # Default to parquet for other formats
                arrow::write_parquet(result, local_cache_path)
              }
            } else {
              # Convert to data frame and write using base R
              df_result <- arrow::as_data_frame(result)
              utils::write.csv(df_result, local_cache_path, row.names = FALSE)
            }
          } else {
            # Standard data frame
            utils::write.csv(result, local_cache_path, row.names = FALSE)
          }
          
          message(paste0("Cached remote data to: ", local_cache_path))
        }, error = function(e) {
          warning("Failed to cache data locally: ", e$message)
        })
      }
      
      return(result)
    },
    
    #' @description Configure S3 credentials and connection settings
    #' @param access_key_id AWS access key ID
    #' @param secret_access_key AWS secret access key
    #' @param region AWS region
    #' @param endpoint_url Custom endpoint URL (for non-AWS S3 services)
    #' @param session_token AWS session token (optional)
    #' @param use_ssl Whether to use SSL (default: TRUE)
    #' @param verify_certificates Whether to verify SSL certificates (default: TRUE)
    #' @return TRUE if successful
    configure_s3 = function(access_key_id, secret_access_key, region = NULL, 
                           endpoint_url = NULL, session_token = NULL,
                           use_ssl = TRUE, verify_certificates = TRUE) {
      # Check if httpfs extension is loaded
      is_httpfs_loaded <- tryCatch({
        httpfs_check <- DBI::dbGetQuery(self$conn, "SELECT loaded FROM pragma_database_list WHERE name='httpfs'")
        if (nrow(httpfs_check) > 0) {
          return(httpfs_check$loaded[1] == 1)
        }
        return(FALSE)
      }, error = function(e) {
        return(FALSE)
      })
      
      # Try to load httpfs if not already loaded
      if (!is_httpfs_loaded) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL httpfs")
          DBI::dbExecute(self$conn, "LOAD httpfs")
        }, error = function(e) {
          stop("Failed to load httpfs extension. S3 access is not available: ", e$message)
        })
      }
      
      # Configure S3 settings
      tryCatch({
        # Set AWS credentials
        DBI::dbExecute(self$conn, paste0("SET s3_access_key_id='", access_key_id, "'"))
        DBI::dbExecute(self$conn, paste0("SET s3_secret_access_key='", secret_access_key, "'"))
        
        # Set optional parameters if provided
        if (!is.null(region)) {
          DBI::dbExecute(self$conn, paste0("SET s3_region='", region, "'"))
        }
        
        if (!is.null(endpoint_url)) {
          DBI::dbExecute(self$conn, paste0("SET s3_endpoint='", endpoint_url, "'"))
        }
        
        if (!is.null(session_token)) {
          DBI::dbExecute(self$conn, paste0("SET s3_session_token='", session_token, "'"))
        }
        
        # Set SSL options
        DBI::dbExecute(self$conn, paste0("SET s3_use_ssl=", ifelse(use_ssl, "true", "false")))
        DBI::dbExecute(self$conn, paste0("SET s3_verify_certificates=", ifelse(verify_certificates, "true", "false")))
        
        return(TRUE)
      }, error = function(e) {
        stop("Failed to configure S3 settings: ", e$message)
      })
    },
    
    #' @description Configure remote storage connection settings
    #' @param storage_type Type of remote storage ('s3', 'azure', 'gcs', etc.)
    #' @param settings Named list of settings specific to the storage type
    #' @return TRUE if successful
    configure_storage = function(storage_type, settings) {
      # Check if httpfs extension is loaded
      is_httpfs_loaded <- tryCatch({
        httpfs_check <- DBI::dbGetQuery(self$conn, "SELECT loaded FROM pragma_database_list WHERE name='httpfs'")
        if (nrow(httpfs_check) > 0) {
          return(httpfs_check$loaded[1] == 1)
        }
        return(FALSE)
      }, error = function(e) {
        return(FALSE)
      })
      
      # Try to load httpfs if not already loaded
      if (!is_httpfs_loaded) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL httpfs")
          DBI::dbExecute(self$conn, "LOAD httpfs")
        }, error = function(e) {
          stop("Failed to load httpfs extension. Remote storage access is not available: ", e$message)
        })
      }
      
      # Configure remote storage settings
      tryCatch({
        if (storage_type == "s3") {
          # Handle S3 configuration
          if (!is.null(settings$access_key_id)) {
            DBI::dbExecute(self$conn, paste0("SET s3_access_key_id='", settings$access_key_id, "'"))
          }
          
          if (!is.null(settings$secret_access_key)) {
            DBI::dbExecute(self$conn, paste0("SET s3_secret_access_key='", settings$secret_access_key, "'"))
          }
          
          if (!is.null(settings$region)) {
            DBI::dbExecute(self$conn, paste0("SET s3_region='", settings$region, "'"))
          }
          
          if (!is.null(settings$endpoint_url)) {
            DBI::dbExecute(self$conn, paste0("SET s3_endpoint='", settings$endpoint_url, "'"))
          }
          
          if (!is.null(settings$session_token)) {
            DBI::dbExecute(self$conn, paste0("SET s3_session_token='", settings$session_token, "'"))
          }
          
          # Set SSL options
          if (!is.null(settings$use_ssl)) {
            DBI::dbExecute(self$conn, paste0("SET s3_use_ssl=", ifelse(settings$use_ssl, "true", "false")))
          }
          
          if (!is.null(settings$verify_certificates)) {
            DBI::dbExecute(self$conn, paste0("SET s3_verify_certificates=", 
                                            ifelse(settings$verify_certificates, "true", "false")))
          }
        } else if (storage_type == "azure") {
          # Handle Azure storage configuration
          if (!is.null(settings$account_name)) {
            DBI::dbExecute(self$conn, paste0("SET azure_storage_connection_string='DefaultEndpointsProtocol=https;",
                                           "AccountName=", settings$account_name, ";",
                                           "AccountKey=", settings$account_key, ";",
                                           "EndpointSuffix=core.windows.net'"))
          }
        } else if (storage_type == "gcs") {
          # Handle Google Cloud Storage configuration
          if (!is.null(settings$credentials_json)) {
            # Use a JSON credentials file
            DBI::dbExecute(self$conn, paste0("SET gcs_key_json='", settings$credentials_json, "'"))
          } else if (!is.null(settings$credentials_file)) {
            # Read credentials from file
            tryCatch({
              creds_content <- readLines(settings$credentials_file, warn = FALSE)
              creds_json <- paste(creds_content, collapse = "\n")
              DBI::dbExecute(self$conn, paste0("SET gcs_key_json='", creds_json, "'"))
            }, error = function(e) {
              stop("Failed to read GCS credentials file: ", e$message)
            })
          }
        } else {
          stop(paste0("Unsupported storage type: ", storage_type))
        }
        
        return(TRUE)
      }, error = function(e) {
        stop(paste0("Failed to configure ", storage_type, " settings: ", e$message))
      })
    },
    
    #' @description Export data directly to remote storage
    #' @param data Data frame or identifier of an existing dataset
    #' @param remote_path Path to remote location (e.g., 's3://bucket/path/file.parquet')
    #' @param format File format (e.g., 'parquet', 'csv', etc.)
    #' @param partitions List of columns to partition by
    #' @param compression Compression method ('snappy', 'gzip', etc.)
    #' @param options Additional options for the specific format
    #' @return TRUE if successful
    export_to_remote = function(data, remote_path, format = "parquet", 
                               partitions = NULL, compression = "snappy",
                               options = list()) {
      # Check if data is a data frame or a dataset identifier
      if (is.character(data) && length(data) == 1) {
        # It's a dataset identifier, retrieve it
        dataset <- self$pull(data)
        if (is.null(dataset)) {
          stop(paste0("Dataset with identifier '", data, "' not found"))
        }
      } else if (is.data.frame(data)) {
        # It's already a data frame
        dataset <- data
      } else {
        stop("Data must be either a data frame or a dataset identifier")
      }
      
      # Check if httpfs extension is loaded
      is_httpfs_loaded <- tryCatch({
        httpfs_check <- DBI::dbGetQuery(self$conn, "SELECT loaded FROM pragma_database_list WHERE name='httpfs'")
        if (nrow(httpfs_check) > 0) {
          return(httpfs_check$loaded[1] == 1)
        }
        return(FALSE)
      }, error = function(e) {
        return(FALSE)
      })
      
      # Try to load httpfs if not already loaded
      if (!is_httpfs_loaded) {
        tryCatch({
          DBI::dbExecute(self$conn, "INSTALL httpfs")
          DBI::dbExecute(self$conn, "LOAD httpfs")
        }, error = function(e) {
          stop("Failed to load httpfs extension. Remote file access is not available: ", e$message)
        })
      }
      
      # Create a temporary table with the data
      temp_table_name <- paste0("temp_export_", format(Sys.time(), "%Y%m%d%H%M%S"), "_", 
                               as.integer(runif(1, 1000, 9999)))
      
      tryCatch({
        # Create temporary table
        DBI::dbWriteTable(self$conn, temp_table_name, dataset, temporary = TRUE)
        
        # Construct export query based on format
        if (format == "parquet") {
          # Parquet export
          base_query <- paste0("COPY ", temp_table_name, " TO '", remote_path, "' (FORMAT PARQUET")
          
          # Add compression if specified
          if (!is.null(compression)) {
            base_query <- paste0(base_query, ", COMPRESSION '", compression, "'")
          }
          
          # Add partitioning if specified
          if (!is.null(partitions) && length(partitions) > 0) {
            partition_cols <- paste(partitions, collapse = ",")
            base_query <- paste0(base_query, ", PARTITION_BY (", partition_cols, ")")
          }
          
          # Add additional options
          for (opt_name in names(options)) {
            opt_value <- options[[opt_name]]
            if (is.character(opt_value)) {
              # Add quotes for string values
              base_query <- paste0(base_query, ", ", opt_name, " '", opt_value, "'")
            } else {
              # No quotes for numeric or boolean values
              base_query <- paste0(base_query, ", ", opt_name, " ", opt_value)
            }
          }
          
          # Close the query
          base_query <- paste0(base_query, ")")
          
          # Execute the export
          DBI::dbExecute(self$conn, base_query)
          
        } else if (format == "csv") {
          # CSV export
          base_query <- paste0("COPY ", temp_table_name, " TO '", remote_path, "' (FORMAT CSV")
          
          # Add header option (default to true)
          header_val <- ifelse(is.null(options$header) || options$header, "true", "false")
          base_query <- paste0(base_query, ", HEADER ", header_val)
          
          # Add delimiter if specified
          if (!is.null(options$delimiter)) {
            base_query <- paste0(base_query, ", DELIMITER '", options$delimiter, "'")
          }
          
          # Add quote character if specified
          if (!is.null(options$quote)) {
            base_query <- paste0(base_query, ", QUOTE '", options$quote, "'")
          }
          
          # Add additional options
          for (opt_name in names(options)) {
            # Skip options we've already handled
            if (opt_name %in% c("header", "delimiter", "quote")) {
              next
            }
            
            opt_value <- options[[opt_name]]
            if (is.character(opt_value)) {
              # Add quotes for string values
              base_query <- paste0(base_query, ", ", opt_name, " '", opt_value, "'")
            } else {
              # No quotes for numeric or boolean values
              base_query <- paste0(base_query, ", ", opt_name, " ", 
                                 ifelse(is.logical(opt_value), 
                                        ifelse(opt_value, "true", "false"), 
                                        opt_value))
            }
          }
          
          # Close the query
          base_query <- paste0(base_query, ")")
          
          # Execute the export
          DBI::dbExecute(self$conn, base_query)
          
        } else if (format == "json") {
          # JSON export
          base_query <- paste0("COPY ", temp_table_name, " TO '", remote_path, "' (FORMAT JSON")
          
          # Add additional options
          for (opt_name in names(options)) {
            opt_value <- options[[opt_name]]
            if (is.character(opt_value)) {
              # Add quotes for string values
              base_query <- paste0(base_query, ", ", opt_name, " '", opt_value, "'")
            } else {
              # No quotes for numeric or boolean values
              base_query <- paste0(base_query, ", ", opt_name, " ", 
                                 ifelse(is.logical(opt_value), 
                                        ifelse(opt_value, "true", "false"), 
                                        opt_value))
            }
          }
          
          # Close the query
          base_query <- paste0(base_query, ")")
          
          # Execute the export
          DBI::dbExecute(self$conn, base_query)
        } else {
          stop(paste0("Unsupported export format: ", format))
        }
        
        return(TRUE)
      }, error = function(e) {
        stop(paste0("Failed to export data: ", e$message))
      }, finally = {
        # Clean up temporary table
        tryCatch({
          DBI::dbExecute(self$conn, paste0("DROP TABLE IF EXISTS ", temp_table_name))
        }, error = function(e) {
          warning("Failed to clean up temporary table: ", e$message)
        })
      })
    }
  ),
  
  private = list(
    #' @description Set up the metadata tables if they don't exist
    setup_metadata_tables = function() {
      # Main metadata table for current versions
      DBI::dbExecute(self$conn, "
        CREATE TABLE IF NOT EXISTS crosslink_metadata (
          id TEXT PRIMARY KEY,
          name TEXT,
          source_language TEXT,
          created_at TIMESTAMP,
          updated_at TIMESTAMP,
          description TEXT,
          schema TEXT,
          table_name TEXT,
          arrow_data BOOLEAN DEFAULT FALSE,
          version INTEGER DEFAULT 1,
          current_version BOOLEAN DEFAULT TRUE,
          lineage TEXT,
          schema_hash TEXT
        )
      ")
      
      # Schema history table for tracking schema evolution
      DBI::dbExecute(self$conn, "
        CREATE TABLE IF NOT EXISTS crosslink_schema_history (
          id TEXT,
          version INTEGER,
          schema TEXT,
          schema_hash TEXT,
          changed_at TIMESTAMP,
          change_type TEXT,
          changes TEXT,
          PRIMARY KEY (id, version)
        )
      ")
      
      # Lineage table for tracking data provenance
      DBI::dbExecute(self$conn, "
        CREATE TABLE IF NOT EXISTS crosslink_lineage (
          dataset_id TEXT,
          source_dataset_id TEXT,
          source_dataset_version INTEGER,
          transformation TEXT,
          created_at TIMESTAMP,
          PRIMARY KEY (dataset_id, source_dataset_id)
        )
      ")
      
      # Add missing columns to existing databases for backward compatibility
      private$ensure_columns_exist()
    },
    
    #' @description Ensure all required columns exist in metadata table for backwards compatibility
    ensure_columns_exist = function() {
      columns_to_check <- list(
        c("arrow_data", "BOOLEAN DEFAULT FALSE"),
        c("version", "INTEGER DEFAULT 1"),
        c("current_version", "BOOLEAN DEFAULT TRUE"),
        c("lineage", "TEXT"),
        c("schema_hash", "TEXT")
      )
      
      for (col in columns_to_check) {
        result <- DBI::dbGetQuery(self$conn, paste0("
          SELECT count(*) as col_exists FROM pragma_table_info('crosslink_metadata') 
          WHERE name = '", col[1], "'
        "))
        
        if (result$col_exists == 0) {
          DBI::dbExecute(self$conn, paste0("
            ALTER TABLE crosslink_metadata ADD COLUMN ", col[1], " ", col[2], "
          "))
        }
      }
    },
    
    #' @description Compute a hash of the dataframe schema for detecting changes
    compute_schema_hash = function(df) {
      schema_dict <- list(
        columns = colnames(df),
        dtypes = sapply(df, class)
      )
      schema_str <- jsonlite::toJSON(schema_dict, auto_unbox = TRUE)
      return(digest::digest(schema_str, algo = "md5"))
    },
    
    #' @description Detect schema changes between old schema and new dataframe
    detect_schema_changes = function(old_schema, new_df) {
      if (is.null(old_schema)) {
        return(list(change_type = "initial_schema"))
      }
      
      old_schema_dict <- jsonlite::fromJSON(old_schema)
      old_columns <- old_schema_dict$columns
      old_dtypes <- old_schema_dict$dtypes
      
      new_columns <- colnames(new_df)
      new_dtypes <- sapply(new_df, class)
      
      added_columns <- setdiff(new_columns, old_columns)
      removed_columns <- setdiff(old_columns, new_columns)
      common_columns <- intersect(old_columns, new_columns)
      
      changed_dtypes <- list()
      for (col in common_columns) {
        old_type <- old_dtypes[[col]]
        new_type <- new_dtypes[[col]]
        if (!identical(old_type, new_type)) {
          changed_dtypes[[col]] <- list(old_type, new_type)
        }
      }
      
      changes <- list(
        added_columns = added_columns,
        removed_columns = removed_columns,
        changed_dtypes = changed_dtypes
      )
      
      if (length(added_columns) > 0 || length(removed_columns) > 0 || length(changed_dtypes) > 0) {
        if (length(removed_columns) > 0) {
          change_type <- "breaking"
        } else if (length(added_columns) > 0 && length(changed_dtypes) == 0) {
          change_type <- "non_breaking_addition"
        } else {
          change_type <- "potentially_breaking"
        }
      } else {
        change_type <- "no_change"
      }
      
      return(list(
        change_type = change_type,
        changes = changes
      ))
    }
  )
) 