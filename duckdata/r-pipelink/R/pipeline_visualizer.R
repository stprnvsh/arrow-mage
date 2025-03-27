#' Pipeline visualizer for PipeLink DAGs
#'
#' This module provides functionality to visualize pipeline DAGs.

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

#' Build a graph from pipeline configuration
#'
#' @param pipeline Pipeline configuration list
#' @return igraph object representing the DAG
#' @importFrom igraph graph_from_data_frame V E is_dag
#' @export
build_graph <- function(pipeline) {
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
    }
  }
  
  # Create igraph object with all node attributes
  g <- igraph::graph_from_data_frame(edges, vertices = nodes, directed = TRUE)
  
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

#' Visualize a pipeline as a directed graph
#'
#' @param pipeline_file Path to pipeline YAML file
#' @param output_file Path to save visualization image (optional)
#' @param show Whether to display the visualization
#' @return Invisible NULL
#' @importFrom igraph V E layout_with_fr layout_with_kk plot.igraph
#' @export
visualize_pipeline <- function(pipeline_file, output_file = NULL, show = TRUE) {
  # Load pipeline and build graph
  pipeline <- load_pipeline(pipeline_file)
  g <- build_graph(pipeline)
  
  # Define language colors
  language_colors <- list(
    python = "skyblue",
    r = "lightgreen",
    julia = "orange",
    data = "lightgray"
  )
  
  # Extract node languages
  languages <- igraph::V(g)$language
  
  # Create color vector based on node languages
  node_colors <- sapply(languages, function(lang) {
    if (!is.null(lang) && lang %in% names(language_colors)) {
      language_colors[[lang]]
    } else {
      "white"  # Default color
    }
  })
  
  # Create plot
  if (show || !is.null(output_file)) {
    # Set up the graphics device
    if (!is.null(output_file)) {
      grDevices::png(filename = output_file, width = 1200, height = 800, res = 100)
    }
    
    # Attempt to use optimal layout
    if (requireNamespace("igraph", quietly = TRUE)) {
      # Try to use a hierarchical layout for DAGs
      if (requireNamespace("Rgraphviz", quietly = TRUE)) {
        layout <- igraph::layout_with_sugiyama(g)
      } else {
        # Fall back to standard layouts
        layout <- igraph::layout_with_fr(g)
      }
    } else {
      stop("Package 'igraph' is required for visualization")
    }
    
    # Plot the graph
    plot(g, 
         layout = layout,
         vertex.color = node_colors,
         vertex.size = 30,
         vertex.label.dist = 0,
         vertex.label.color = "black",
         vertex.label.family = "sans",
         edge.arrow.size = 0.5,
         main = paste0("PipeLink DAG: ", pipeline$name %||% "Unnamed Pipeline"))
    
    # Add legend
    if (length(unique(languages)) > 0) {
      legend_colors <- language_colors[unique(languages)]
      legend("topright", 
             legend = names(legend_colors),
             fill = unlist(legend_colors),
             title = "Languages",
             cex = 0.8)
    }
    
    # Close device if output file specified
    if (!is.null(output_file)) {
      grDevices::dev.off()
    }
  }
  
  invisible(NULL)
}

#' Visualize a pipeline directly from configuration list
#'
#' @param pipeline Pipeline configuration list
#' @param output_file Path to save visualization image (optional)
#' @param show Whether to display the visualization
#' @return Invisible NULL
#' @export
visualize_from_dict <- function(pipeline, output_file = NULL, show = TRUE) {
  # Build graph
  g <- build_graph(pipeline)
  
  # Define language colors
  language_colors <- list(
    python = "skyblue",
    r = "lightgreen",
    julia = "orange",
    data = "lightgray"
  )
  
  # Extract node languages
  languages <- igraph::V(g)$language
  
  # Create color vector based on node languages
  node_colors <- sapply(languages, function(lang) {
    if (!is.null(lang) && lang %in% names(language_colors)) {
      language_colors[[lang]]
    } else {
      "white"  # Default color
    }
  })
  
  # Create plot
  if (show || !is.null(output_file)) {
    # Set up the graphics device
    if (!is.null(output_file)) {
      grDevices::png(filename = output_file, width = 1200, height = 800, res = 100)
    }
    
    # Attempt to use optimal layout
    if (requireNamespace("igraph", quietly = TRUE)) {
      # Try to use a hierarchical layout for DAGs
      if (requireNamespace("Rgraphviz", quietly = TRUE)) {
        layout <- igraph::layout_with_sugiyama(g)
      } else {
        # Fall back to standard layouts
        layout <- igraph::layout_with_fr(g)
      }
    } else {
      stop("Package 'igraph' is required for visualization")
    }
    
    # Plot the graph
    plot(g, 
         layout = layout,
         vertex.color = node_colors,
         vertex.size = 30,
         vertex.label.dist = 0,
         vertex.label.color = "black",
         vertex.label.family = "sans",
         edge.arrow.size = 0.5,
         main = paste0("PipeLink DAG: ", pipeline$name %||% "Unnamed Pipeline"))
    
    # Add legend
    if (length(unique(languages)) > 0) {
      legend_colors <- language_colors[unique(languages)]
      legend("topright", 
             legend = names(legend_colors),
             fill = unlist(legend_colors),
             title = "Languages",
             cex = 0.8)
    }
    
    # Close device if output file specified
    if (!is.null(output_file)) {
      grDevices::dev.off()
    }
  }
  
  invisible(NULL)
}

# Null-coalescing operator (duplicated from pipelink.R for independent use)
#' @keywords internal
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
} 