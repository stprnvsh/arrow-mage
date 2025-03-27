"""
PipelineVisualizer: DAG visualization for PipeLink

This module provides functionality to visualize pipeline DAGs.
"""
module PipelineVisualizer

using YAML
using Dates
using DataFrames
using LightGraphs
using MetaGraphs
using GraphRecipes
using Plots

# Import functions from PipelineDAG
using ..PipelineDAG: load_pipeline, build_dependency_graph

export visualize_pipeline, visualize_from_dict

"""
    visualize_pipeline(pipeline_file::String; output_file=nothing, show=true)

Visualize a pipeline as a directed graph.

Arguments:
- `pipeline_file`: Path to pipeline YAML file
- `output_file`: Path to save visualization image (optional)
- `show`: Whether to display the visualization

Returns:
    Plot object
"""
function visualize_pipeline(pipeline_file::String; output_file=nothing, show=true)
    # Load pipeline and build graph
    pipeline = load_pipeline(pipeline_file)
    return visualize_from_dict(pipeline; output_file=output_file, show=show)
end

"""
    visualize_from_dict(pipeline::Dict; output_file=nothing, show=true)

Visualize a pipeline directly from configuration dict.

Arguments:
- `pipeline`: Pipeline configuration dict
- `output_file`: Path to save visualization image (optional)
- `show`: Whether to display the visualization

Returns:
    Plot object
"""
function visualize_from_dict(pipeline::Dict; output_file=nothing, show=true)
    # Build graph
    g = build_dependency_graph(pipeline)
    
    # Define language colors
    language_colors = Dict(
        "python" => :skyblue,
        "r" => :lightgreen,
        "julia" => :orange,
        "data" => :lightgray
    )
    
    # Extract node languages and names
    node_languages = String[]
    node_labels = String[]
    
    for i in vertices(g)
        push!(node_labels, get_prop(g, i, :id))
        
        language = haskey(props(g, i), :language) ? get_prop(g, i, :language) : "unknown"
        push!(node_languages, language)
    end
    
    # Create color vector based on node languages
    node_colors = map(node_languages) do lang
        haskey(language_colors, lang) ? language_colors[lang] : :white
    end
    
    # Create the plot
    p = graphplot(g,
                 names=node_labels,
                 nodeshape=:circle,
                 nodecolor=node_colors,
                 nodesize=0.15,
                 fontsize=10,
                 curves=false,
                 arrow=true,
                 linewidth=2,
                 title="PipeLink DAG: $(get(pipeline, "name", "Unnamed Pipeline"))",
                 layout=:spring)
    
    # Add legend for languages
    unique_languages = unique(node_languages)
    legend_colors = [language_colors[lang] for lang in unique_languages if haskey(language_colors, lang)]
    
    if !isempty(legend_colors)
        scatter!([], [],
                marker=:circle,
                color=legend_colors,
                label=unique_languages,
                legend=:topright)
    end
    
    # Save if output file specified
    if output_file !== nothing
        savefig(p, output_file)
    end
    
    # Show if requested
    if show
        display(p)
    end
    
    return p
end

end # module 