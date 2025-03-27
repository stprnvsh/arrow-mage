"""
PipeLink: Cross-Language Pipeline Orchestration

A Julia package for building and executing data processing pipelines across Python, R, and Julia
with seamless data sharing between steps.
"""
module PipeLink

using YAML
using Dates
using DataFrames

# Include submodules
include("CrossLink.jl")
include("PipeLinkNode.jl")

# Re-export CrossLink module
import .CrossLink: CrossLinkManager, push_data, pull_data, list_datasets, delete_dataset
import .CrossLink: list_dataset_versions, get_schema_history, get_lineage, check_compatibility

# Re-export PipeLinkNode module
import .PipeLinkNode: NodeContext, get_input, save_output, get_param, get_node_id, get_pipeline_name
import .PipeLinkNode: get_versions, get_dataset_schema_history, get_dataset_lineage, check_dataset_compatibility

# Export all necessary symbols
export CrossLinkManager, push_data, pull_data, list_datasets, delete_dataset
export list_dataset_versions, get_schema_history, get_lineage, check_compatibility
export NodeContext, get_input, save_output, get_param, get_node_id, get_pipeline_name
export get_versions, get_dataset_schema_history, get_dataset_lineage, check_dataset_compatibility

end # module 