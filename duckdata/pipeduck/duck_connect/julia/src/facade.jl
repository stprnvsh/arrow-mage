"""
Main API facade for DuckConnect in Julia.

This module provides the primary user-facing API for DuckConnect 
and coordinates operations between all the sub-modules.
"""

module Facade

using DuckDB
using DataFrames
using Dates
using UUIDs
using JSON
using Statistics

using ..Core
using ..Metadata
using ..Transactions

export DuckConnect, register_dataset, get_dataset, update_dataset, delete_dataset,
       list_datasets, execute_query, register_transformation, DuckContext

"""
    DuckConnect

Main facade class for interacting with DuckConnect from Julia.
"""
mutable struct DuckConnect
    conn_manager::Core.ConnectionManager
    db_path::String
    debug::Bool
    default_connection_id::Union{String, Nothing}
    
    """
        DuckConnect(db_path::String; debug::Bool=false)
        
    Initialize DuckConnect with a database file.
    """
    function DuckConnect(db_path::String; debug::Bool=false)
        conn_manager = Core.ConnectionManager(db_path)
        
        # Set up metadata tables
        Metadata.setup_metadata_tables(conn_manager)
        
        # Create default connection
        default_conn_id = string(uuid4())
        Core.get_connection(conn_manager, default_conn_id)
        
        new(conn_manager, db_path, debug, default_conn_id)
    end
end

"""
    register_dataset(dc::DuckConnect, data; 
                    name::String,
                    description::Union{String, Nothing}=nothing,
                    available_to_languages::Vector{String}=["python", "r", "julia"],
                    overwrite::Bool=false)

Register a dataset with DuckConnect.

Arguments:
- `dc`: DuckConnect instance
- `data`: DataFrame to register, or a SQL query string
- `name`: Name for the dataset
- `description`: Optional description
- `available_to_languages`: List of languages that can access this dataset
- `overwrite`: Whether to overwrite an existing dataset with the same name

Returns the ID of the registered dataset.
"""
function register_dataset(dc::DuckConnect, data; 
                         name::String,
                         description::Union{String, Nothing}=nothing,
                         available_to_languages::Vector{String}=["python", "r", "julia"],
                         overwrite::Bool=false)
    
    start_time = now()
    
    # Check if dataset exists and handle overwrite
    if overwrite
        existing = Core.execute_query(dc.conn_manager, 
            "SELECT id FROM duck_connect_metadata WHERE name = ?", [name])
        
        if size(existing, 1) > 0
            delete_dataset(dc, existing[1, :id])
        end
    end
    
    # Generate dataset ID and table name
    dataset_id = string(uuid4())
    table_name = "duck_connect_$(replace(dataset_id, "-" => "_"))"
    
    # Process based on input type
    row_count = 0
    column_count = 0
    schema = Dict()
    
    if data isa AbstractString && occursin(r"^SELECT ", uppercase(data))
        # SQL query
        try
            # Create table from query
            Core.execute_query(dc.conn_manager, "CREATE TABLE $table_name AS $data")
            
            # Get schema information
            result = Core.execute_query(dc.conn_manager, "SELECT * FROM $table_name LIMIT 0")
            schema = Dict(
                "columns" => names(result),
                "dtypes" => [string(eltype(col)) for col in eachcol(result)],
                "source_type" => "sql_query"
            )
            
            # Get row count
            count_result = Core.execute_query(dc.conn_manager, "SELECT COUNT(*) as row_count FROM $table_name")
            row_count = count_result[1, :row_count]
            column_count = length(schema["columns"])
            
        catch e
            @error "Failed to create dataset from SQL query: $e"
            rethrow(e)
        end
    elseif data isa DataFrame
        # DataFrame
        try
            # Create table from DataFrame
            df_stmt = DuckDB.Stmt(Core.get_connection(dc.conn_manager, dc.default_connection_id),
                "CREATE TABLE $table_name AS SELECT * FROM df_data")
            DuckDB.bind!(df_stmt, 1, data)
            DuckDB.execute(df_stmt)
            
            # Get schema information
            schema = Dict(
                "columns" => names(data),
                "dtypes" => [string(eltype(col)) for col in eachcol(data)],
                "source_type" => "julia_dataframe",
                "shape" => size(data)
            )
            
            row_count = size(data, 1)
            column_count = size(data, 2)
            
        catch e
            @error "Failed to create dataset from DataFrame: $e"
            rethrow(e)
        end
    else
        throw(ArgumentError("Data must be a DataFrame or a SQL query string"))
    end
    
    # Register metadata
    try
        Metadata.register_dataset_metadata(dc.conn_manager;
            id=dataset_id,
            name=name,
            table_name=table_name,
            schema=schema,
            source_language="julia",
            description=description,
            available_to_languages=available_to_languages
        )
    catch e
        # Clean up table if metadata registration fails
        Core.execute_query(dc.conn_manager, "DROP TABLE IF EXISTS $table_name")
        rethrow(e)
    end
    
    # Record transaction
    Transactions.record_transaction(dc.conn_manager,
        dataset_id,
        "create",
        "julia",
        Dict("rows" => row_count, "columns" => column_count)
    )
    
    # Record stats
    duration_ms = Dates.value(now() - start_time) / 1_000_000
    
    Transactions.record_stats(dc.conn_manager,
        dataset_id,
        "create",
        duration_ms,
        row_count,
        column_count
    )
    
    if dc.debug
        @info "Registered dataset '$name' with ID $dataset_id ($row_count rows, $column_count columns)"
    end
    
    return dataset_id
end

"""
    get_dataset(dc::DuckConnect, id_or_name::String)

Get a dataset by ID or name.

Returns the dataset as a DataFrame.
"""
function get_dataset(dc::DuckConnect, id_or_name::String)
    start_time = now()
    
    # Get metadata
    metadata = Metadata.get_dataset_metadata(dc.conn_manager, id_or_name)
    
    if isnothing(metadata)
        throw(ArgumentError("Dataset '$id_or_name' not found"))
    end
    
    # Query data
    result = Core.execute_query(dc.conn_manager, 
        "SELECT * FROM $(metadata.table_name)")
    
    # Record transaction
    Transactions.record_transaction(dc.conn_manager,
        metadata.id,
        "read",
        "julia",
        Dict("rows" => size(result, 1), "columns" => size(result, 2))
    )
    
    # Record stats
    duration_ms = Dates.value(now() - start_time) / 1_000_000
    
    Transactions.record_stats(dc.conn_manager,
        metadata.id,
        "read",
        duration_ms,
        size(result, 1),
        size(result, 2)
    )
    
    if dc.debug
        @info "Retrieved dataset '$(metadata.name)' ($(size(result, 1)) rows, $(size(result, 2)) columns)"
    end
    
    return result
end

"""
    update_dataset(dc::DuckConnect, id_or_name::String, data;
                  name::Union{String, Nothing}=nothing,
                  description::Union{String, Nothing}=nothing,
                  available_to_languages::Union{Vector{String}, Nothing}=nothing)

Update an existing dataset.

Returns the ID of the updated dataset.
"""
function update_dataset(dc::DuckConnect, id_or_name::String, data;
                       name::Union{String, Nothing}=nothing,
                       description::Union{String, Nothing}=nothing,
                       available_to_languages::Union{Vector{String}, Nothing}=nothing)
    
    start_time = now()
    
    # Get metadata
    metadata = Metadata.get_dataset_metadata(dc.conn_manager, id_or_name)
    
    if isnothing(metadata)
        throw(ArgumentError("Dataset '$id_or_name' not found"))
    end
    
    # Update data
    row_count = 0
    column_count = 0
    schema = Dict()
    
    if data isa AbstractString && occursin(r"^SELECT ", uppercase(data))
        # SQL query
        try
            # Create temporary table
            temp_table = "temp_$(replace(string(uuid4()), "-" => "_"))"
            Core.execute_query(dc.conn_manager, "CREATE TEMPORARY TABLE $temp_table AS $data")
            
            # Replace original table with temporary table
            Core.execute_query(dc.conn_manager, "DROP TABLE $(metadata.table_name)")
            Core.execute_query(dc.conn_manager, 
                "CREATE TABLE $(metadata.table_name) AS SELECT * FROM $temp_table")
            Core.execute_query(dc.conn_manager, "DROP TABLE $temp_table")
            
            # Get schema information
            result = Core.execute_query(dc.conn_manager, "SELECT * FROM $(metadata.table_name) LIMIT 0")
            schema = Dict(
                "columns" => names(result),
                "dtypes" => [string(eltype(col)) for col in eachcol(result)],
                "source_type" => "sql_query"
            )
            
            # Get row count
            count_result = Core.execute_query(dc.conn_manager, 
                "SELECT COUNT(*) as row_count FROM $(metadata.table_name)")
            row_count = count_result[1, :row_count]
            column_count = length(schema["columns"])
            
        catch e
            @error "Failed to update dataset from SQL query: $e"
            rethrow(e)
        end
    elseif data isa DataFrame
        # DataFrame
        try
            # Create temporary table
            temp_table = "temp_$(replace(string(uuid4()), "-" => "_"))"
            
            df_stmt = DuckDB.Stmt(Core.get_connection(dc.conn_manager, dc.default_connection_id),
                "CREATE TEMPORARY TABLE $temp_table AS SELECT * FROM df_data")
            DuckDB.bind!(df_stmt, 1, data)
            DuckDB.execute(df_stmt)
            
            # Replace original table with temporary table
            Core.execute_query(dc.conn_manager, "DROP TABLE $(metadata.table_name)")
            Core.execute_query(dc.conn_manager, 
                "CREATE TABLE $(metadata.table_name) AS SELECT * FROM $temp_table")
            Core.execute_query(dc.conn_manager, "DROP TABLE $temp_table")
            
            # Get schema information
            schema = Dict(
                "columns" => names(data),
                "dtypes" => [string(eltype(col)) for col in eachcol(data)],
                "source_type" => "julia_dataframe",
                "shape" => size(data)
            )
            
            row_count = size(data, 1)
            column_count = size(data, 2)
            
        catch e
            @error "Failed to update dataset from DataFrame: $e"
            rethrow(e)
        end
    else
        throw(ArgumentError("Data must be a DataFrame or a SQL query string"))
    end
    
    # Update metadata
    Metadata.update_dataset_metadata(dc.conn_manager, metadata.id;
        name=name,
        description=description,
        schema=schema,
        available_to_languages=available_to_languages
    )
    
    # Record transaction
    Transactions.record_transaction(dc.conn_manager,
        metadata.id,
        "update",
        "julia",
        Dict("rows" => row_count, "columns" => column_count)
    )
    
    # Record stats
    duration_ms = Dates.value(now() - start_time) / 1_000_000
    
    Transactions.record_stats(dc.conn_manager,
        metadata.id,
        "update",
        duration_ms,
        row_count,
        column_count
    )
    
    if dc.debug
        @info "Updated dataset '$(metadata.name)' ($row_count rows, $column_count columns)"
    end
    
    return metadata.id
end

"""
    delete_dataset(dc::DuckConnect, id_or_name::String)

Delete a dataset.
"""
function delete_dataset(dc::DuckConnect, id_or_name::String)
    start_time = now()
    
    # Get metadata
    metadata = Metadata.get_dataset_metadata(dc.conn_manager, id_or_name)
    
    if isnothing(metadata)
        throw(ArgumentError("Dataset '$id_or_name' not found"))
    end
    
    # Delete the table
    Core.execute_query(dc.conn_manager, "DROP TABLE IF EXISTS $(metadata.table_name)")
    
    # Delete metadata
    Metadata.delete_dataset_metadata(dc.conn_manager, metadata.id)
    
    # Record transaction
    Transactions.record_transaction(dc.conn_manager,
        metadata.id,
        "delete",
        "julia",
        Dict()
    )
    
    # Record stats
    duration_ms = Dates.value(now() - start_time) / 1_000_000
    
    Transactions.record_stats(dc.conn_manager,
        metadata.id,
        "delete",
        duration_ms,
        0,
        0
    )
    
    if dc.debug
        @info "Deleted dataset '$(metadata.name)'"
    end
    
    return nothing
end

"""
    list_datasets(dc::DuckConnect; language::Union{String, Nothing}=nothing)

List all available datasets.
"""
function list_datasets(dc::DuckConnect; language::Union{String, Nothing}=nothing)
    return Metadata.list_datasets(dc.conn_manager; language=language)
end

"""
    execute_query(dc::DuckConnect, query::String, params::Vector=[])

Execute a SQL query directly.
"""
function execute_query(dc::DuckConnect, query::String, params::Vector=[])
    return Core.execute_query(dc.conn_manager, query, params)
end

"""
    register_transformation(dc::DuckConnect,
                           target_dataset_id::String,
                           source_dataset_ids::Vector{String},
                           transformation::String)

Register a transformation between datasets for lineage tracking.
"""
function register_transformation(dc::DuckConnect,
                                target_dataset_id::String,
                                source_dataset_ids::Vector{String},
                                transformation::String)
    
    for source_id in source_dataset_ids
        Transactions.record_lineage(dc.conn_manager,
            target_dataset_id,
            source_id,
            transformation,
            "julia"
        )
    end
    
    return nothing
end

"""
    DuckContext

Context manager for PipeDuck integration.
"""
mutable struct DuckContext
    duck_connect::DuckConnect
    datasets::Dict{String, String}  # name -> id mapping
    
    function DuckContext(db_path::String)
        new(DuckConnect(db_path), Dict{String, String}())
    end
end

end # module 