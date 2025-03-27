"""
PipeLink: Cross-Language Pipeline Orchestration

A Julia package for building and executing data processing pipelines across Python, R, and Julia
with seamless data sharing between steps.
"""
module PipeLink

using YAML
using Dates
using DataFrames
using UUIDs

# Include submodules
include("CrossLink.jl")
include("PipeLinkNode.jl")
include("PipelineDAG.jl")
include("PipelineVisualizer.jl")
include("CLI.jl")

# Re-export CrossLink module
import .CrossLink: CrossLinkManager, push_data, pull_data, list_datasets, delete_dataset
import .CrossLink: list_dataset_versions, get_schema_history, get_lineage, check_compatibility

# Re-export PipeLinkNode module
import .PipeLinkNode: NodeContext, get_input, save_output, get_param, get_node_id, get_pipeline_name
import .PipeLinkNode: get_versions, get_dataset_schema_history, get_dataset_lineage, check_dataset_compatibility

# Re-export PipelineDAG module
import .PipelineDAG: load_pipeline, validate_pipeline, build_dependency_graph, get_execution_order
import .PipelineDAG: run_pipeline, run_node, resolve_path

# Re-export PipelineVisualizer module
import .PipelineVisualizer: visualize_pipeline, visualize_from_dict

# Re-export CLI module
import .CLI: main

# Export all necessary symbols
export CrossLinkManager, push_data, pull_data, list_datasets, delete_dataset
export list_dataset_versions, get_schema_history, get_lineage, check_compatibility
export NodeContext, get_input, save_output, get_param, get_node_id, get_pipeline_name
export get_versions, get_dataset_schema_history, get_dataset_lineage, check_dataset_compatibility
export load_pipeline, validate_pipeline, build_dependency_graph, get_execution_order
export run_pipeline, run_node, resolve_path
export visualize_pipeline, visualize_from_dict
export main

end # module 