module CrossLink

using DuckDB
using DataFrames
using Arrow
using JSON
using Dates
using UUIDs
using CSV  # Add CSV for file-based import/export
import SHA: sha1

# Import submodules
include(joinpath(@__DIR__, "core", "core.jl"))
include(joinpath(@__DIR__, "metadata", "metadata.jl"))
include(joinpath(@__DIR__, "data_operations", "data_operations.jl"))
include(joinpath(@__DIR__, "arrow_integration", "arrow_integration.jl"))
include(joinpath(@__DIR__, "shared_memory", "shared_memory.jl"))
include(joinpath(@__DIR__, "utilities", "utilities.jl"))

# Export public functions
export CrossLinkManager, push_data, pull_data, get_table_reference, register_external_table
export list_datasets, query, close, cleanup_all_arrow_files
export share_arrow_table, get_arrow_table, create_duckdb_view_from_arrow
export cleanup_all_shared_memory

end # module 