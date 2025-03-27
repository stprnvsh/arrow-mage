#' PipeLink node utilities for R
#'
#' Helper functions for working with PipeLink in R node scripts

#' Load node metadata from command-line argument
#'
#' @return A list containing node metadata
#' @export
load_node_metadata <- function() {
  # Get metadata file path from command line
  args <- commandArgs(trailingOnly = TRUE)
  
  if (length(args) == 0) {
    stop("No metadata file provided. This script should be run by PipeLink.")
  }
  
  meta_path <- args[1]
  
  if (!file.exists(meta_path)) {
    stop("Metadata file not found: ", meta_path)
  }
  
  # Load metadata from YAML
  yaml::read_yaml(meta_path)
}

#' Null-coalescing operator
#'
#' @param x Left-hand side value
#' @param y Right-hand side value (default)
#' @return x if not NULL, otherwise y
#' @keywords internal
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}

#' Safe version of ifelse that properly handles NULL values
#'
#' @param test Logical test
#' @param yes Value if test is TRUE
#' @param no Value if test is FALSE
#' @return Result of conditional
#' @keywords internal
safe_ifelse <- function(test, yes, no) {
  if (is.null(test)) {
    return(no)
  }
  if (length(test) == 0) {
    return(if(length(yes) > 0) yes else no)
  }
  # Handle vectorized case safely
  result <- rep(NA, length(test))
  if (any(test, na.rm = TRUE)) {
    result[test] <- yes
  }
  if (any(!test, na.rm = TRUE)) {
    result[!test] <- no
  }
  return(result)
}

#' NodeContext class for working with PipeLink nodes
#'
#' @description A context manager for working with PipeLink nodes, providing easy access to input/output datasets and parameters
#' @export
NodeContext <- R6::R6Class(
  "NodeContext",
  
  public = list(
    #' @field meta Metadata from PipeLink
    meta = NULL,
    
    #' @field cl CrossLink instance
    cl = NULL,
    
    #' @field input_cache Cache of input datasets
    input_cache = NULL,
    
    #' @description Initialize the context with metadata and CrossLink
    #' @param meta_path Path to the metadata file, if NULL reads from command line
    initialize = function(meta_path = NULL) {
      # Load metadata
      if (is.null(meta_path)) {
        self$meta <- tryCatch({
          load_node_metadata()
        }, error = function(e) {
          warning("Failed to load metadata: ", e$message)
          list()
        })
      } else {
        if (!file.exists(meta_path)) {
          stop("Metadata file not found: ", meta_path)
        }
        self$meta <- yaml::read_yaml(meta_path)
      }
      
      # Initialize CrossLink
      self$cl <- CrossLink$new(db_path = self$meta$db_path %||% "crosslink.duckdb")
      self$input_cache <- list()
    },
    
    #' @description Get an input dataset by name
    #' @param name Name of the input dataset
    #' @param required Whether this input is required
    #' @param use_arrow Whether to use Arrow for data retrieval (default: TRUE)
    #' @param version Specific version to retrieve (default: latest version)
    #' @return A data frame with the requested data
    get_input = function(name, required = TRUE, use_arrow = TRUE, version = NULL) {
      # Check if this input is defined for this node
      if (!name %in% (self$meta$inputs %||% character(0))) {
        if (required) {
          stop("Input '", name, "' is not defined for this node")
        }
        return(NULL)
      }
      
      # Check cache
      if (!is.null(self$input_cache[[name]])) {
        return(self$input_cache[[name]])
      }
      
      # Pull from CrossLink
      result <- tryCatch({
        self$cl$pull(
          identifier = name, 
          to_pandas = TRUE, 
          use_arrow = use_arrow, 
          version = version
        )
      }, error = function(e) {
        if (required) {
          stop("Failed to get input '", name, "': ", e$message)
        }
        warning("Failed to get input '", name, "': ", e$message)
        return(NULL)
      })
      
      self$input_cache[[name]] <- result
      return(result)
    },
    
    #' @description Save an output dataset
    #' @param name Name of the output dataset
    #' @param data Data frame to save
    #' @param description Optional description for the dataset
    #' @param use_arrow Whether to use Arrow for data storage (default: TRUE)
    #' @param sources List of source dataset IDs or names that were used to create this dataset
    #' @param transformation Description of the transformation applied to create this dataset
    #' @param force_new_version Whether to create a new version even if schema hasn't changed
    #' @return Dataset ID
    save_output = function(name, data, description = NULL, use_arrow = TRUE, 
                          sources = NULL, transformation = NULL, force_new_version = FALSE) {
      # Check if this output is defined for this node
      if (!name %in% (self$meta$outputs %||% character(0))) {
        stop("Output '", name, "' is not defined for this node")
      }
      
      # Generate a description if not provided
      if (is.null(description)) {
        node_id <- self$meta$node_id %||% "unknown"
        description <- paste0("Output '", name, "' from node '", node_id, "'")
      }
      
      # Add node info to lineage if not provided
      if (is.null(transformation)) {
        node_id <- self$meta$node_id %||% "unknown"
        pipeline_name <- self$meta$pipeline_name %||% "unknown"
        transformation <- paste0("Processed by node '", node_id, "' in pipeline '", pipeline_name, "'")
      }
      
      # Collect input datasets as sources if not provided
      if (is.null(sources) && !is.null(self$meta$inputs)) {
        sources <- self$meta$inputs
      }
      
      # Push to CrossLink
      tryCatch({
        self$cl$push(
          df = data, 
          name = name, 
          description = description, 
          use_arrow = use_arrow,
          sources = sources,
          transformation = transformation,
          force_new_version = force_new_version
        )
      }, error = function(e) {
        stop("Failed to save output '", name, "': ", e$message)
      })
    },
    
    #' @description Get a parameter value
    #' @param name Parameter name
    #' @param default Default value if parameter is not found
    #' @return Parameter value or default
    get_param = function(name, default = NULL) {
      params <- self$meta$params %||% list()
      params[[name]] %||% default
    },
    
    #' @description Get the node ID
    #' @return Node ID string
    get_node_id = function() {
      self$meta$node_id %||% "unknown"
    },
    
    #' @description Get the pipeline name
    #' @return Pipeline name string
    get_pipeline_name = function() {
      self$meta$pipeline_name %||% "unknown"
    },
    
    #' @description List available versions of a dataset
    #' @param identifier Dataset ID or name
    #' @return A data frame with version information 
    list_versions = function(identifier) {
      tryCatch({
        self$cl$list_dataset_versions(identifier)
      }, error = function(e) {
        warning("Failed to list versions for '", identifier, "': ", e$message)
        return(NULL)
      })
    },
    
    #' @description Get schema history for a dataset
    #' @param identifier Dataset ID or name
    #' @return A data frame with schema history
    get_schema_history = function(identifier) {
      tryCatch({
        self$cl$get_schema_history(identifier)
      }, error = function(e) {
        warning("Failed to get schema history for '", identifier, "': ", e$message)
        return(NULL)
      })
    },
    
    #' @description Get lineage information for a dataset
    #' @param identifier Dataset ID or name
    #' @return A data frame with lineage information
    get_lineage = function(identifier) {
      tryCatch({
        self$cl$get_lineage(identifier)
      }, error = function(e) {
        warning("Failed to get lineage for '", identifier, "': ", e$message)
        return(NULL)
      })
    },
    
    #' @description Check compatibility between two datasets
    #' @param source_id Source dataset ID or name
    #' @param target_id Target dataset ID or name
    #' @return Compatibility information
    check_compatibility = function(source_id, target_id) {
      tryCatch({
        self$cl$check_compatibility(source_id, target_id)
      }, error = function(e) {
        warning("Failed to check compatibility: ", e$message)
        return(NULL)
      })
    },
    
    #' @description Close the CrossLink connection
    finalize = function() {
      if (!is.null(self$cl)) {
        tryCatch({
          self$cl$close()
        }, error = function(e) {
          warning("Error closing CrossLink connection: ", e$message)
        })
      }
    }
  )
)

#' Run a node function with NodeContext
#'
#' @param func Function to execute with context
#' @return Result of the function
#' @export
with_node_context <- function(func) {
  ctx <- NULL
  result <- NULL
  
  tryCatch({
    # Create context
    ctx <- NodeContext$new()
    
    # Execute function
    result <- func(ctx)
    
    # Return result
    return(result)
  }, error = function(e) {
    message("Error in node execution: ", e$message)
    stop(e)
  }, warning = function(w) {
    message("Warning in node execution: ", w$message)
    warning(w)
  }, finally = {
    # Always clean up
    if (!is.null(ctx)) {
      tryCatch({
        ctx$finalize()
      }, error = function(e) {
        message("Error while finalizing context: ", e$message)
      })
    }
  })
} 