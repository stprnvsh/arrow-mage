"""
Command-line interface for PipeLink

This module provides a command-line interface for running PipeLink pipelines.
"""
module CLI

using ArgParse
using ..PipelineDAG: run_pipeline
using ..PipelineVisualizer: visualize_pipeline

export main

"""
    parse_args()

Parse command line arguments for PipeLink.

Returns:
    Dict containing parsed arguments
"""
function parse_args()
    s = ArgParseSettings(
        description = "PipeLink: Cross-Language Pipeline Orchestration",
        usage = "julia --project=. -e 'using PipeLink; PipeLink.CLI.main()' -- [options] PIPELINE_FILE",
        epilog = """
        Examples:
            julia --project=. -e 'using PipeLink; PipeLink.CLI.main()' -- pipeline.yml
            julia --project=. -e 'using PipeLink; PipeLink.CLI.main()' -- --verbose --only node1,node2 pipeline.yml
            julia --project=. -e 'using PipeLink; PipeLink.CLI.main()' -- --viz --output pipeline.png pipeline.yml
        """
    )
    
    @add_arg_table! s begin
        "pipeline_file"
            help = "Path to pipeline YAML file"
            required = true
        "--verbose", "-v"
            help = "Enable verbose output"
            action = :store_true
        "--only", "-n"
            help = "Only execute these nodes (comma-separated list)"
            arg_type = String
        "--start", "-s"
            help = "Start execution from this node"
            arg_type = String
        "--viz"
            help = "Visualize the pipeline DAG"
            action = :store_true
        "--output", "-o"
            help = "Save visualization to file"
            arg_type = String
    end
    
    return parse_args(s)
end

"""
    main()

Main entry point for command-line usage.
"""
function main()
    # Parse command line arguments
    args = parse_args()
    
    # Check if pipeline file exists
    if !isfile(args["pipeline_file"])
        println("Error: Pipeline file not found: $(args["pipeline_file"])")
        exit(1)
    end
    
    # If visualization is requested, visualize the pipeline
    if args["viz"]
        try
            visualize_pipeline(args["pipeline_file"], output_file=args["output"], show=true)
            if args["output"] !== nothing
                println("Pipeline visualization saved to: $(args["output"])")
            end
        catch e
            println("Error visualizing pipeline: $(e)")
            exit(1)
        end
        
        # If only visualization was requested, exit
        if args["only"] === nothing && args["start"] === nothing && !args["verbose"]
            exit(0)
        end
    end
    
    # Run the pipeline
    try
        # Parse only_nodes from comma-separated string
        only_nodes = nothing
        if args["only"] !== nothing
            only_nodes = split(args["only"], ",")
        end
        
        success = run_pipeline(
            args["pipeline_file"],
            only_nodes=only_nodes,
            start_from=args["start"],
            verbose=args["verbose"]
        )
        
        if success
            println("Pipeline execution completed successfully.")
        else
            println("Pipeline execution failed.")
            exit(1)
        end
    catch e
        println("Error running pipeline: $(e)")
        exit(1)
    end
end

end # module 