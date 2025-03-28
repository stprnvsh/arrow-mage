"""
DuckConnect.jl - Optimized cross-language data sharing with DuckDB

This package provides an optimized way to share data between Python, R, and Julia
by leveraging DuckDB as a shared memory database with metadata to avoid
expensive push/pull operations between languages.
"""
module DuckConnect

# Core modules
include("core.jl")
include("metadata.jl")
include("transactions.jl")
include("facade.jl")

# Re-export main functionality
using .Facade: DuckConnect, register_dataset, get_dataset, update_dataset, delete_dataset,
               list_datasets, execute_query, register_transformation, DuckContext

# Export all main functionality
export DuckConnect, register_dataset, get_dataset, update_dataset, delete_dataset,
       list_datasets, execute_query, register_transformation, DuckContext

end # module 