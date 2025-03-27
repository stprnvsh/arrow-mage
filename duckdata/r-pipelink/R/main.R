#' Command-line interface for PipeLink
#'
#' This script provides a command-line interface for running PipeLink pipelines.

#' Parse command line arguments
#'
#' @return List of parsed arguments
#' @keywords internal
parse_args <- function() {
  # Default values
  args <- list(
    pipeline_file = NULL,
    only_nodes = NULL,
    start_from = NULL,
    verbose = FALSE,
    visualize = FALSE,
    output_image = NULL
  )
  
  # Get command line arguments
  cmd_args <- commandArgs(trailingOnly = TRUE)
  
  # Process arguments
  i <- 1
  while (i <= length(cmd_args)) {
    arg <- cmd_args[i]
    
    if (arg == "--help" || arg == "-h") {
      cat("PipeLink: Cross-Language Pipeline Orchestration\n\n")
      cat("Usage: Rscript -e 'pipelink::main()' PIPELINE_FILE [OPTIONS]\n\n")
      cat("Options:\n")
      cat("  -h, --help             Show this help message and exit\n")
      cat("  -v, --verbose          Enable verbose output\n")
      cat("  -n NODES, --only NODES Only execute these nodes (comma-separated list)\n")
      cat("  -s NODE, --start NODE  Start execution from this node\n")
      cat("  --viz                  Visualize the pipeline DAG\n")
      cat("  -o FILE, --output FILE Save visualization to file\n\n")
      cat("Examples:\n")
      cat("  Rscript -e 'pipelink::main()' pipeline.yml\n")
      cat("  Rscript -e 'pipelink::main()' pipeline.yml -v -n node1,node2\n")
      cat("  Rscript -e 'pipelink::main()' pipeline.yml --viz -o pipeline.png\n")
      quit(status = 0)
    } else if (arg == "--verbose" || arg == "-v") {
      args$verbose <- TRUE
    } else if (arg == "--only" || arg == "-n") {
      if (i + 1 <= length(cmd_args)) {
        i <- i + 1
        args$only_nodes <- strsplit(cmd_args[i], ",")[[1]]
      }
    } else if (arg == "--start" || arg == "-s") {
      if (i + 1 <= length(cmd_args)) {
        i <- i + 1
        args$start_from <- cmd_args[i]
      }
    } else if (arg == "--viz") {
      args$visualize <- TRUE
    } else if (arg == "--output" || arg == "-o") {
      if (i + 1 <= length(cmd_args)) {
        i <- i + 1
        args$output_image <- cmd_args[i]
      }
    } else if (is.null(args$pipeline_file)) {
      args$pipeline_file <- arg
    }
    
    i <- i + 1
  }
  
  # Check required arguments
  if (is.null(args$pipeline_file)) {
    cat("Error: No pipeline file specified\n")
    cat("Run with --help for usage information\n")
    quit(status = 1)
  }
  
  return(args)
}

#' Main entry point for command-line usage
#'
#' @export
main <- function() {
  # Parse command line arguments
  args <- parse_args()
  
  # Check if pipeline file exists
  if (!file.exists(args$pipeline_file)) {
    cat("Error: Pipeline file not found:", args$pipeline_file, "\n")
    quit(status = 1)
  }
  
  # If visualize is requested, visualize the pipeline
  if (args$visualize) {
    tryCatch({
      visualize_pipeline(args$pipeline_file, args$output_image)
      if (!is.null(args$output_image)) {
        cat("Pipeline visualization saved to:", args$output_image, "\n")
      }
    }, error = function(e) {
      cat("Error visualizing pipeline:", e$message, "\n")
      quit(status = 1)
    })
    
    # If only visualization was requested, exit
    if (is.null(args$only_nodes) && is.null(args$start_from) && !args$verbose) {
      quit(status = 0)
    }
  }
  
  # Run the pipeline
  tryCatch({
    run_pipeline(
      pipeline_file = args$pipeline_file,
      only_nodes = args$only_nodes,
      start_from = args$start_from,
      verbose = args$verbose
    )
    
    cat("Pipeline execution completed successfully.\n")
  }, error = function(e) {
    cat("Error running pipeline:", e$message, "\n")
    quit(status = 1)
  })
} 