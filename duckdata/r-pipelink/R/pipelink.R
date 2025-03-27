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

#' Load pipeline configuration from YAML file
#'
#' @param pipeline_file Path to pipeline YAML file
#' @return Pipeline configuration as a list
#' @export
load_pipeline <- function(pipeline_file) {
  if (!file.exists(pipeline_file)) {
    stop("Pipeline file not found: ", pipeline_file)
  }
  
  yaml::read_yaml(pipeline_file)
}

#' Resolve a path relative to a base directory
#'
#' @param base_dir Base directory
#' @param path Path to resolve
#' @return Absolute path
#' @keywords internal
resolve_path <- function(base_dir, path) {
  if (file.path.is.absolute(path)) {
    return(path)
  } else {
    normalizePath(file.path(base_dir, path), mustWork = FALSE)
  }
}

#' Check if a file path is absolute
#'
#' @param path Path to check
#' @return TRUE if path is absolute, FALSE otherwise
#' @keywords internal
file.path.is.absolute <- function(path) {
  if (.Platform$OS.type == "windows") {
    # Windows: check for drive letter or UNC path
    grepl("^[A-Za-z]:|^\\\\\\\\", path)
  } else {
    # Unix-like: check for leading slash
    grepl("^/", path)
  }
}

#' Validate pipeline configuration
#'
#' @param pipeline Pipeline configuration list
#' @return NULL (invisibly)
#' @keywords internal
validate_pipeline <- function(pipeline) {
  # Check required fields
  if (is.null(pipeline$name)) {
    stop("Pipeline configuration missing required field: name")
  }
  
  if (is.null(pipeline$nodes) || length(pipeline$nodes) == 0) {
    stop("Pipeline configuration missing required field: nodes")
  }
  
  # Validate each node
  node_ids <- character(0)
  
  for (node in pipeline$nodes) {
    # Check required fields
    if (is.null(node$id)) {
      stop("Node missing required field: id")
    }
    
    # Check for duplicate IDs
    if (node$id %in% node_ids) {
      stop("Duplicate node ID: ", node$id)
    }
    node_ids <- c(node_ids, node$id)
    
    if (is.null(node$language)) {
      stop("Node '", node$id, "' missing required field: language")
    }
    
    # Validate script field for code nodes
    if (node$language %in% c("python", "r", "julia")) {
      if (is.null(node$script)) {
        stop("Node '", node$id, "' missing required field: script")
      }
    }
    
    # Validate data nodes
    if (node$language == "data") {
      if (is.null(node$connection)) {
        stop("Data node '", node$id, "' missing required field: connection")
      }
      if (is.null(node$connection$type)) {
        stop("Data node '", node$id, "' connection missing required field: type")
      }
    }
    
    # Validate depends_on if present
    if (!is.null(node$depends_on)) {
      depends_on <- node$depends_on
      if (is.character(depends_on) && length(depends_on) == 1) {
        # Single dependency as string - this is valid
      } else if (is.character(depends_on) && length(depends_on) > 1) {
        # List of dependencies - this is valid
      } else {
        stop("Node '", node$id, "' has invalid depends_on format. Must be string or character vector")
      }
    }
  }
  
  invisible(NULL)
}

#' Build dependency graph from pipeline configuration
#'
#' @param pipeline Pipeline configuration list
#' @return igraph object representing the DAG
#' @importFrom igraph graph_from_data_frame V E is_dag all_simple_paths
#' @keywords internal
build_dependency_graph <- function(pipeline) {
  # Create empty graph data
  edges <- data.frame(
    from = character(0),
    to = character(0),
    stringsAsFactors = FALSE
  )
  
  # Add all nodes to a character vector
  nodes <- sapply(pipeline$nodes, function(node) node$id)
  
  # Add edges for dependencies
  for (node in pipeline$nodes) {
    # Check for explicit dependencies first
    if (!is.null(node$depends_on)) {
      # Handle both single string and vector formats
      depends_on <- node$depends_on
      if (length(depends_on) == 1 && is.character(depends_on)) {
        depends_on <- c(depends_on)
      }
      
      for (dependency in depends_on) {
        if (!dependency %in% nodes) {
          stop("Node '", node$id, "' depends on non-existent node '", dependency, "'")
        }
        edges <- rbind(edges, data.frame(from = dependency, to = node$id, stringsAsFactors = FALSE))
      }
    }
    
    # Get inputs (for backward compatibility and implicit dependencies)
    inputs <- node$inputs %||% character(0)
    
    # Find nodes that produce these inputs
    for (input_name in inputs) {
      # Find the node that produces this output
      producers <- character(0)
      for (producer in pipeline$nodes) {
        outputs <- producer$outputs %||% character(0)
        if (input_name %in% outputs) {
          producers <- c(producers, producer$id)
          # Add edge from producer to consumer
          edges <- rbind(edges, data.frame(from = producer$id, to = node$id, stringsAsFactors = FALSE))
        }
      }
      
      if (length(producers) == 0 && length(inputs) > 0) {
        warning("Input '", input_name, "' for node '", node$id, "' has no producer.")
      }
    }
  }
  
  # Create igraph object
  g <- igraph::graph_from_data_frame(edges, vertices = nodes, directed = TRUE)
  
  # Check for cycles
  if (!igraph::is_dag(g)) {
    # Find cycles
    cycles <- find_cycles(g)
    stop("Pipeline has circular dependencies. Cycles: ", paste(sapply(cycles, paste, collapse = " -> "), collapse = "; "))
  }
  
  # Add node attributes
  for (node in pipeline$nodes) {
    for (attr_name in names(node)) {
      if (attr_name != "id") {
        igraph::V(g)[node$id][[attr_name]] <- node[[attr_name]]
      }
    }
  }
  
  return(g)
}

#' Find cycles in a directed graph
#'
#' @param g igraph object
#' @return List of cycles
#' @importFrom igraph girth
#' @keywords internal
find_cycles <- function(g) {
  # Implementation of a DFS-based cycle detection
  # This is a simplistic implementation and may not be the most efficient
  visited <- rep(FALSE, igraph::vcount(g))
  rec_stack <- rep(FALSE, igraph::vcount(g))
  
  cycles <- list()
  
  # Helper function for DFS
  dfs_cycle <- function(v, path = c(), g, visited, rec_stack, cycles) {
    if (!visited[v]) {
      visited[v] <- TRUE
      rec_stack[v] <- TRUE
      
      path <- c(path, igraph::V(g)[v]$name)
      
      # Visit neighbors
      neighbors <- as.numeric(igraph::neighbors(g, v, mode = "out"))
      for (n in neighbors) {
        if (!visited[n]) {
          result <- dfs_cycle(n, path, g, visited, rec_stack, cycles)
          visited <- result$visited
          rec_stack <- result$rec_stack
          cycles <- result$cycles
        } else if (rec_stack[n]) {
          # Found a cycle
          cycle_start <- match(igraph::V(g)[n]$name, path)
          cycles <- c(cycles, list(path[cycle_start:length(path)]))
        }
      }
    }
    
    rec_stack[v] <- FALSE
    return(list(visited = visited, rec_stack = rec_stack, cycles = cycles))
  }
  
  # Run DFS for each vertex
  for (i in 1:igraph::vcount(g)) {
    if (!visited[i]) {
      result <- dfs_cycle(i, c(), g, visited, rec_stack, cycles)
      visited <- result$visited
      rec_stack <- result$rec_stack
      cycles <- result$cycles
    }
  }
  
  return(cycles)
}

#' Get execution order for nodes
#'
#' @param g igraph object representing the DAG
#' @param only_nodes Only execute these nodes (and their dependencies)
#' @param start_from Start execution from this node
#' @return Character vector of node IDs in execution order
#' @importFrom igraph topo_sort subcomponent
#' @keywords internal
get_execution_order <- function(g, only_nodes = NULL, start_from = NULL) {
  # Get topological sort (execution order)
  execution_order <- igraph::topo_sort(g, mode = "out")
  execution_order <- igraph::V(g)[execution_order]$name
  
  # Filter nodes if specified
  if (!is.null(only_nodes)) {
    # Find all dependencies for the specified nodes
    required_nodes <- only_nodes
    for (node in only_nodes) {
      if (!node %in% igraph::V(g)$name) {
        stop("Node not found: ", node)
      }
      
      # Get all ancestors (dependencies)
      ancestors <- igraph::subcomponent(g, node, mode = "in")
      ancestor_names <- igraph::V(g)[ancestors]$name
      required_nodes <- unique(c(required_nodes, ancestor_names))
    }
    
    # Filter execution order
    execution_order <- execution_order[execution_order %in% required_nodes]
  }
  
  # Start from a specific node if specified
  if (!is.null(start_from)) {
    if (!start_from %in% igraph::V(g)$name) {
      stop("Start node not found: ", start_from)
    }
    
    # Get index of start node
    start_idx <- match(start_from, execution_order)
    
    # Skip nodes before start node
    execution_order <- execution_order[start_idx:length(execution_order)]
  }
  
  return(execution_order)
}

#' Run a pipeline from a YAML file
#'
#' @param pipeline_file Path to pipeline YAML file
#' @param only_nodes Only execute these nodes (and their dependencies)
#' @param start_from Start execution from this node
#' @param verbose Enable verbose output
#' @return Invisible NULL
#' @export
run_pipeline <- function(pipeline_file, only_nodes = NULL, start_from = NULL, verbose = FALSE) {
  if (verbose) message("Loading pipeline from ", pipeline_file)
  
  # Load pipeline configuration
  pipeline <- load_pipeline(pipeline_file)
  
  # Set up working directory
  working_dir <- dirname(normalizePath(pipeline_file))
  if (!is.null(pipeline$working_dir)) {
    if (file.path.is.absolute(pipeline$working_dir)) {
      working_dir <- pipeline$working_dir
    } else {
      working_dir <- normalizePath(file.path(working_dir, pipeline$working_dir), mustWork = FALSE)
    }
  }
  
  # Validate pipeline configuration
  validate_pipeline(pipeline)
  
  # Build dependency graph
  g <- build_dependency_graph(pipeline)
  
  # Get execution order
  execution_order <- get_execution_order(g, only_nodes, start_from)
  
  if (verbose) message("Execution order: ", paste(execution_order, collapse = ", "))
  
  # Determine pipeline database path
  db_path <- pipeline$db_path %||% "pipeline.duckdb"
  db_path <- resolve_path(working_dir, db_path)
  
  if (verbose) message("Using database at ", db_path)
  
  # Generate a unique pipeline ID
  pipeline_id <- uuid::UUIDgenerate()
  
  # Run nodes in order
  for (node_id in execution_order) {
    # Find node config
    node_config <- NULL
    for (node in pipeline$nodes) {
      if (node$id == node_id) {
        node_config <- node
        break
      }
    }
    
    if (is.null(node_config)) {
      stop("Node not found in config: ", node_id)
    }
    
    # Run the node
    success <- run_node(node_config, pipeline, working_dir, pipeline_id, verbose)
    
    if (!success) {
      stop("Pipeline execution failed at node: ", node_id)
    }
  }
  
  if (verbose) message("Pipeline execution completed successfully.")
  
  invisible(NULL)
}

#' Run a single pipeline node
#'
#' @param node_config Node configuration
#' @param pipeline_config Full pipeline configuration
#' @param working_dir Working directory
#' @param pipeline_id Pipeline monitoring ID
#' @param verbose Enable verbose output
#' @return TRUE if successful, FALSE if failed
#' @keywords internal
run_node <- function(node_config, pipeline_config, working_dir, pipeline_id, verbose = FALSE) {
  node_id <- node_config$id
  language <- node_config$language
  
  if (verbose) message("Running node '", node_id, "' (", language, ")")
  
  # Create metadata file
  meta_file <- tempfile(fileext = ".yml")
  
  # Prepare metadata
  metadata <- list(
    node_id = node_id,
    pipeline_name = pipeline_config$name,
    inputs = node_config$inputs %||% character(0),
    outputs = node_config$outputs %||% character(0),
    params = node_config$params %||% list(),
    db_path = pipeline_config$db_path %||% "crosslink.duckdb",
    language = language
  )
  
  # Add connection details for data nodes
  if (language == "data") {
    metadata$connection <- node_config$connection %||% list()
  }
  
  # Write metadata
  yaml::write_yaml(metadata, meta_file)
  
  success <- TRUE
  tryCatch({
    # Run script based on language
    if (language == "python") {
      script_path <- resolve_path(working_dir, node_config$script)
      cmd <- c("python", script_path, meta_file)
    } else if (language == "r") {
      script_path <- resolve_path(working_dir, node_config$script)
      cmd <- c("Rscript", script_path, meta_file)
    } else if (language == "julia") {
      script_path <- resolve_path(working_dir, node_config$script)
      cmd <- c("julia", script_path, meta_file)
    } else if (language == "data") {
      # For data nodes, use a standard data connector script
      # In R we need to implement this - for now, just show a message
      message("Data nodes are not yet implemented in R version")
      return(FALSE)
    } else {
      stop("Unsupported language: ", language)
    }
    
    # Run command
    if (verbose) {
      result <- system(paste(cmd, collapse = " "), wait = TRUE)
    } else {
      # Redirect output if not verbose
      result <- suppressWarnings(system(paste(cmd, collapse = " "), wait = TRUE, ignore.stderr = !verbose, ignore.stdout = !verbose))
    }
    
    # Check result
    if (result != 0) {
      success <- FALSE
      message("Node '", node_id, "' failed with exit code ", result)
    }
  }, error = function(e) {
    success <- FALSE
    message("Error running node '", node_id, "': ", e$message)
  }, finally = {
    # Clean up metadata file
    if (file.exists(meta_file)) {
      file.remove(meta_file)
    }
  })
  
  return(success)
}

#' Create a new file and write YAML content to it
#'
#' @param path Path to write the file
#' @param content Content to write to the file
#' @return Invisible NULL
#' @keywords internal
write_yaml <- function(path, content) {
  yaml::write_yaml(content, path)
  invisible(NULL)
} 