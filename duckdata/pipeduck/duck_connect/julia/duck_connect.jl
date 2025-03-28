"""
DuckConnect: Optimized cross-language data sharing with DuckDB

This module provides an optimized way to share data between Python, R, and Julia
by leveraging DuckDB as a shared memory database with metadata to avoid
expensive push/pull operations between languages.
"""

# Load the modular implementation
include("src/DuckConnect.jl")

# Import all exports from the modular implementation
using .DuckConnect

# Keep everything exported for backwards compatibility
export DuckConnect, register_dataset, get_dataset, update_dataset, delete_dataset,
       list_datasets, execute_query, register_transformation, DuckContext

using DuckDB
using DataFrames
using JSON
using UUIDs
using Dates
using Markdown
using YAML

# Import Arrow if available
try
    @eval using Arrow
catch e
    @warn "Arrow package not found. Some features will be disabled."
end

export DuckConnectManager, get_table_reference

"""
    DuckConnectManager

A manager class for cross-language data sharing using DuckDB.
"""
mutable struct DuckConnectManager
    conn::DuckDB.DB
    db_path::String
    arrow_available::Bool
    debug::Bool
    
    """
        DuckConnectManager(db_path::String)

    Create a DuckConnectManager with a DuckDB database at the specified path.
    """
    function DuckConnectManager(db_path::String; debug=false)
        # Create or connect to DuckDB database
        if isfile(db_path)
            conn = DuckDB.DB(db_path)
        else
            # Create directory if it doesn't exist
            db_dir = dirname(db_path)
            if !isdir(db_dir) && !isempty(db_dir)
                mkpath(db_dir)
            end
            conn = DuckDB.DB(db_path)
        end
        
        # Try to detect Arrow availability
        arrow_available = try
            arrow_loaded = isdefined(Main, :Arrow) || @isdefined Arrow
            if !arrow_loaded
                @warn "Arrow package not detected. Some features will be disabled."
            end
            arrow_loaded
        catch
            false
        end
        
        manager = new(conn, db_path, arrow_available, debug)
        setup_metadata_tables(manager)
        return manager
    end
end

"""
    setup_metadata_tables(manager::DuckConnectManager)

Set up the metadata tables if they don't exist.
"""
function setup_metadata_tables(manager::DuckConnectManager)
    # Configure DuckDB for handling large datasets
    try
        # Set memory limit (80% of available RAM)
        DBInterface.execute(manager.conn, "PRAGMA memory_limit='80%'")
        
        # Set threads to use all available cores except one
        num_cores = max(1, Sys.CPU_THREADS - 1)
        DBInterface.execute(manager.conn, "PRAGMA threads=$num_cores")
        
        # Enable parallelism
        DBInterface.execute(manager.conn, "PRAGMA force_parallelism")
        
        # Enable progress bar for long-running queries
        DBInterface.execute(manager.conn, "PRAGMA enable_progress_bar")
        
        # Enable object cache for better performance
        DBInterface.execute(manager.conn, "PRAGMA enable_object_cache")
    catch e
        @warn "Failed to configure DuckDB performance settings: $e"
    end
    
    # Load Arrow extension
    if manager.arrow_available
        try
            DBInterface.execute(manager.conn, "INSTALL arrow")
            DBInterface.execute(manager.conn, "LOAD arrow")
        catch e
            @warn "Arrow extension could not be loaded: $e"
        end
    end
    
    # Create metadata tables for tracking data
    DBInterface.execute(manager.conn, """
        CREATE TABLE IF NOT EXISTS duck_connect_metadata (
            id TEXT PRIMARY KEY,
            name TEXT UNIQUE,
            source_language TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            description TEXT,
            schema TEXT,  -- JSON schema information
            table_name TEXT,  -- Actual DuckDB table name
            available_to_languages TEXT  -- JSON array of languages
        )
    """)
    
    # Create transactions table to track language operations
    DBInterface.execute(manager.conn, """
        CREATE TABLE IF NOT EXISTS duck_connect_transactions (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            operation TEXT,  -- 'create', 'read', 'update', 'delete'
            language TEXT,
            timestamp TIMESTAMP,
            details TEXT  -- JSON additional details
        )
    """)
    
    # Create lineage table for tracking data transformations
    DBInterface.execute(manager.conn, """
        CREATE TABLE IF NOT EXISTS duck_connect_lineage (
            id TEXT PRIMARY KEY,
            target_dataset_id TEXT,
            source_dataset_id TEXT,
            transformation TEXT,
            language TEXT,
            timestamp TIMESTAMP
        )
    """)
    
    # Create statistics table for performance tracking
    DBInterface.execute(manager.conn, """
        CREATE TABLE IF NOT EXISTS duck_connect_stats (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            operation TEXT,
            language TEXT,
            timestamp TIMESTAMP,
            duration_ms FLOAT,
            memory_usage_mb FLOAT,
            row_count INTEGER,
            column_count INTEGER
        )
    """)
end

"""
    register_dataset(manager::DuckConnectManager, data; 
                   name, description=nothing, 
                   source_language="julia",
                   available_to_languages=nothing,
                   overwrite=false)

Register a dataset with DuckConnect.

Instead of copying data, this creates or references a table in DuckDB and creates metadata.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `data`: DataFrame to register, or a SQL query string
- `name`: Name for the dataset
- `description`: Optional description
- `source_language`: Source language (default: "julia")
- `available_to_languages`: List of languages that can access this dataset
- `overwrite`: Whether to overwrite an existing dataset with the same name

# Returns
- `String`: ID of the registered dataset
"""
function register_dataset(manager::DuckConnectManager, data; 
                        name::String, 
                        description::Union{String, Nothing}=nothing, 
                        source_language::String="julia",
                        available_to_languages::Union{Vector{String}, Nothing}=nothing,
                        overwrite::Bool=false)
    # Record start time for stats
    start_time = now()
    
    # Check if dataset with this name already exists
    existing = DataFrame(DBInterface.execute(manager.conn, 
        "SELECT id, table_name FROM duck_connect_metadata WHERE name = ?", [name]))
    
    if size(existing, 1) > 0 && !overwrite
        error("Dataset with name '$name' already exists. Use overwrite=true to replace it.")
    end
    
    # Generate a unique ID for this dataset
    dataset_id = string(uuid4())
    
    # Set default available languages if not specified
    if isnothing(available_to_languages)
        available_to_languages = ["python", "r", "julia"]
    end
    
    # Generate unique table name
    table_name = "duck_connect_$(replace(dataset_id, "-" => "_"))"
    
    # Process based on input type
    if data isa AbstractString && occursin(r"^SELECT ", uppercase(data))
        # It's a SQL query
        try
            # Create table from query
            DBInterface.execute(manager.conn, "CREATE OR REPLACE TABLE $table_name AS $data")
            
            # Get schema from created table
            schema_df = DataFrame(DBInterface.execute(manager.conn, "SELECT * FROM $table_name LIMIT 0"))
            schema = Dict(
                "columns" => names(schema_df),
                "dtypes" => [string(eltype(col)) for col in eachcol(schema_df)],
                "source_type" => "sql_query"
            )
        catch e
            error("Failed to create table from SQL query: $e")
        end
    elseif data isa DataFrame
        # It's a DataFrame
        try
            # Create table in DuckDB
            df_stmt = DuckDB.Stmt(manager.conn, 
                "CREATE OR REPLACE TABLE $table_name AS SELECT * FROM df_data")
            DuckDB.bind!(df_stmt, 1, data)
            DuckDB.execute(df_stmt)
            
            # Create schema info
            schema = Dict(
                "columns" => names(data),
                "dtypes" => [string(eltype(col)) for col in eachcol(data)],
                "source_type" => "julia_dataframe",
                "shape" => size(data)
            )
        catch e
            error("Failed to store DataFrame in DuckDB: $e")
        end
    elseif data isa AbstractString && startswith(data, "duck_table:")
        # It's a reference to an existing DuckDB table
        ref_table = replace(data, "duck_table:" => "")
        
        # Verify the table exists
        table_exists = DataFrame(DBInterface.execute(manager.conn, 
            "SELECT COUNT(*) as count FROM information_schema.tables WHERE table_name = ?", 
            [ref_table]))[1, :count]
        
        if table_exists == 0
            error("Referenced table '$ref_table' does not exist in DuckDB")
        end
        
        # For references, we don't create a new table but use the existing one
        table_name = ref_table
        
        # Get schema info
        schema_df = DataFrame(DBInterface.execute(manager.conn, "SELECT * FROM $table_name LIMIT 0"))
        schema = Dict(
            "columns" => names(schema_df),
            "dtypes" => [string(eltype(col)) for col in eachcol(schema_df)],
            "source_type" => "table_reference"
        )
    else
        error("Unsupported data type for registration: $(typeof(data))")
    end
    
    # Serialize schema and languages to JSON
    schema_json = JSON.json(schema)
    languages_json = JSON.json(available_to_languages)
    
    # Delete existing metadata if overwriting
    if size(existing, 1) > 0 && overwrite
        old_id = existing[1, :id]
        old_table = existing[1, :table_name]
        
        # Delete old metadata
        DBInterface.execute(manager.conn, "DELETE FROM duck_connect_metadata WHERE id = ?", [old_id])
        
        # Delete old table if it's different from the new one and not a reference
        if old_table != table_name && !(data isa AbstractString && startswith(data, "duck_table:"))
            DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $old_table")
        end
    end
    
    # Insert metadata
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_metadata 
        (id, name, source_language, created_at, updated_at, description, schema, table_name, available_to_languages)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?)
    """, [dataset_id, name, source_language, description, schema_json, table_name, languages_json])
    
    # Record transaction
    transaction_id = string(uuid4())
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'create', ?, CURRENT_TIMESTAMP, ?)
    """, [
        transaction_id, 
        dataset_id, 
        source_language, 
        JSON.json(Dict("overwrite" => overwrite))
    ])
    
    # Record statistics
    duration_ms = Float64(Dates.value(now() - start_time)) / 1_000_000.0  # nanoseconds to milliseconds
    row_count = Int64(DataFrame(DBInterface.execute(manager.conn, 
        "SELECT COUNT(*) as count FROM $table_name"))[1, :count])
    column_count = length(schema["columns"])
    
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'create', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
    """, [
        string(uuid4()),
        dataset_id,
        source_language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
    ])
    
    println("Registered dataset '$name' with ID $dataset_id")
    return dataset_id
end

"""
    get_dataset(manager::DuckConnectManager, identifier; 
              language="julia", as_arrow=false)

Get a dataset by name or ID.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `identifier`: Dataset name or ID
- `language`: Language requesting the data (for access control and stats)
- `as_arrow`: Whether to return as Arrow Table instead of DataFrame

# Returns
- `DataFrame`: DataFrame with the dataset
"""
function get_dataset(manager::DuckConnectManager, identifier::String; 
                   language::String="julia", as_arrow::Bool=false)
    start_time = now()
    
    # Look up metadata
    metadata = DataFrame(DBInterface.execute(manager.conn, """
        SELECT id, table_name, schema, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
    """, [identifier, identifier]))
    
    if size(metadata, 1) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    dataset_id = metadata[1, :id]
    table_name = metadata[1, :table_name]
    schema_json = metadata[1, :schema]
    available_languages_json = metadata[1, :available_to_languages]
    
    # Check language access
    available_languages = JSON.parse(available_languages_json)
    if !(language in available_languages)
        error("Dataset '$identifier' is not available to language '$language'")
    end
    
    # Record transaction
    transaction_id = string(uuid4())
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'read', ?, CURRENT_TIMESTAMP, ?)
    """, [
        transaction_id, 
        dataset_id, 
        language, 
        JSON.json(Dict("as_arrow" => as_arrow))
    ])
    
    # Retrieve data
    result = try
        if as_arrow && manager.arrow_available
            # This requires Arrow integration, which we'll approximate here
            # In a full implementation, you would return an Arrow.Table
            # For now, let's return a DataFrame for simplicity
            DataFrame(DBInterface.execute(manager.conn, "SELECT * FROM $table_name"))
        else
            # Get as DataFrame
            DataFrame(DBInterface.execute(manager.conn, "SELECT * FROM $table_name"))
        end
    catch e
        error("Error retrieving dataset '$identifier': $e")
    end
    
    # Record statistics
    duration_ms = Float64(Dates.value(now() - start_time)) / 1_000_000.0  # nanoseconds to milliseconds
    schema = JSON.parse(schema_json)
    row_count = size(result, 1)
    column_count = length(schema["columns"])
    
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'read', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
    """, [
        string(uuid4()),
        dataset_id,
        language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
    ])
    
    return result
end

"""
    update_dataset(manager::DuckConnectManager, identifier, data; 
                 language="julia", description=nothing)

Update an existing dataset.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `identifier`: Dataset name or ID
- `data`: New data as DataFrame or SQL query
- `language`: Language performing the update
- `description`: Optional new description

# Returns
- `String`: ID of the updated dataset
"""
function update_dataset(manager::DuckConnectManager, identifier::String, data; 
                      language::String="julia", description::Union{String, Nothing}=nothing)
    start_time = now()
    
    # Look up metadata
    metadata = DataFrame(DBInterface.execute(manager.conn, """
        SELECT id, table_name, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
    """, [identifier, identifier]))
    
    if size(metadata, 1) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    dataset_id = metadata[1, :id]
    table_name = metadata[1, :table_name]
    available_languages_json = metadata[1, :available_to_languages]
    
    # Check language access
    available_languages = JSON.parse(available_languages_json)
    if !(language in available_languages)
        error("Dataset '$identifier' is not available to language '$language'")
    end
    
    # Process based on input type
    if data isa AbstractString && occursin(r"^SELECT ", uppercase(data))
        # It's a SQL query - replace table contents
        try
            DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $table_name")
            DBInterface.execute(manager.conn, "CREATE TABLE $table_name AS $data")
            
            # Get updated schema
            schema_df = DataFrame(DBInterface.execute(manager.conn, "SELECT * FROM $table_name LIMIT 0"))
            schema = Dict(
                "columns" => names(schema_df),
                "dtypes" => [string(eltype(col)) for col in eachcol(schema_df)],
                "source_type" => "sql_query"
            )
        catch e
            error("Failed to update table from SQL query: $e")
        end
    elseif data isa DataFrame
        # It's a DataFrame
        try
            # Replace table contents
            DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $table_name")
            df_stmt = DuckDB.Stmt(manager.conn, 
                "CREATE TABLE $table_name AS SELECT * FROM df_data")
            DuckDB.bind!(df_stmt, 1, data)
            DuckDB.execute(df_stmt)
            
            # Update schema info
            schema = Dict(
                "columns" => names(data),
                "dtypes" => [string(eltype(col)) for col in eachcol(data)],
                "source_type" => "julia_dataframe",
                "shape" => size(data)
            )
        catch e
            error("Failed to update DataFrame in DuckDB: $e")
        end
    else
        error("Unsupported data type for update: $(typeof(data))")
    end
    
    # Serialize schema to JSON
    schema_json = JSON.json(schema)
    
    # Update metadata
    if !isnothing(description)
        DBInterface.execute(manager.conn, """
            UPDATE duck_connect_metadata 
            SET updated_at = CURRENT_TIMESTAMP, description = ?, schema = ? 
            WHERE id = ?
        """, [description, schema_json, dataset_id])
    else
        DBInterface.execute(manager.conn, """
            UPDATE duck_connect_metadata 
            SET updated_at = CURRENT_TIMESTAMP, schema = ? 
            WHERE id = ?
        """, [schema_json, dataset_id])
    end
    
    # Record transaction
    transaction_id = string(uuid4())
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'update', ?, CURRENT_TIMESTAMP, ?)
    """, [
        transaction_id, 
        dataset_id, 
        language, 
        JSON.json(Dict("description_updated" => !isnothing(description)))
    ])
    
    # Record statistics
    duration_ms = Float64(Dates.value(now() - start_time)) / 1_000_000.0  # nanoseconds to milliseconds
    row_count = Int64(DataFrame(DBInterface.execute(manager.conn, 
        "SELECT COUNT(*) as count FROM $table_name"))[1, :count])
    column_count = length(schema["columns"])
    
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'update', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
    """, [
        string(uuid4()),
        dataset_id,
        language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
    ])
    
    println("Updated dataset '$identifier' (ID: $dataset_id)")
    return dataset_id
end

"""
    delete_dataset(manager::DuckConnectManager, identifier; language="julia")

Delete a dataset.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `identifier`: Dataset name or ID
- `language`: Language performing the deletion

# Returns
- `Bool`: True if successful
"""
function delete_dataset(manager::DuckConnectManager, identifier::String; language::String="julia")
    # Look up metadata
    metadata = DataFrame(DBInterface.execute(manager.conn, """
        SELECT id, table_name, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
    """, [identifier, identifier]))
    
    if size(metadata, 1) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    dataset_id = metadata[1, :id]
    table_name = metadata[1, :table_name]
    available_languages_json = metadata[1, :available_to_languages]
    
    # Check language access
    available_languages = JSON.parse(available_languages_json)
    if !(language in available_languages)
        error("Dataset '$identifier' is not available to language '$language'")
    end
    
    # Delete the table
    DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $table_name")
    
    # Delete metadata
    DBInterface.execute(manager.conn, "DELETE FROM duck_connect_metadata WHERE id = ?", [dataset_id])
    
    # Record transaction
    transaction_id = string(uuid4())
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'delete', ?, CURRENT_TIMESTAMP, ?)
    """, [
        transaction_id, 
        dataset_id, 
        language, 
        "{}"
    ])
    
    println("Deleted dataset '$identifier' (ID: $dataset_id)")
    return true
end

"""
    list_datasets(manager::DuckConnectManager; language="julia")

List all available datasets.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `language`: Language requesting the list (for access control)

# Returns
- `DataFrame`: DataFrame containing dataset information
"""
function list_datasets(manager::DuckConnectManager; language::String="julia")
    # Get all datasets
    datasets = DataFrame(DBInterface.execute(manager.conn, """
        SELECT id, name, source_language, created_at, updated_at, description, 
               table_name, available_to_languages
        FROM duck_connect_metadata
        ORDER BY updated_at DESC
    """))
    
    if size(datasets, 1) == 0
        return datasets
    end
    
    # Filter for language access
    filtered_datasets = DataFrame(
        id = String[],
        name = String[],
        source_language = String[],
        created_at = Union{DateTime, Missing}[],
        updated_at = Union{DateTime, Missing}[],
        description = Union{String, Missing}[],
        table_name = String[],
        available_to_languages = String[]
    )
    
    for row in eachrow(datasets)
        available_languages = JSON.parse(row.available_to_languages)
        if language in available_languages
            push!(filtered_datasets, row)
        end
    end
    
    return filtered_datasets
end

"""
    execute_query(manager::DuckConnectManager, query; 
                language="julia", as_arrow=false)

Execute a SQL query on the DuckDB database.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `query`: SQL query to execute
- `language`: Language executing the query
- `as_arrow`: Whether to return as Arrow Table

# Returns
- `DataFrame`: DataFrame with query results
"""
function execute_query(manager::DuckConnectManager, query::String; 
                     language::String="julia", as_arrow::Bool=false)
    start_time = now()
    
    # Execute query
    result = try
        if as_arrow && manager.arrow_available
            # In a full implementation, return an Arrow table
            # For now, return a DataFrame
            DataFrame(DBInterface.execute(manager.conn, query))
        else
            DataFrame(DBInterface.execute(manager.conn, query))
        end
    catch e
        error("Error executing query: $e")
    end
    
    # Record transaction
    transaction_id = string(uuid4())
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'query', ?, CURRENT_TIMESTAMP, ?)
    """, [
        transaction_id, 
        "N/A", 
        language, 
        JSON.json(Dict(
            "query" => length(query) > 1000 ? query[1:1000] : query,
            "as_arrow" => as_arrow
        ))
    ])
    
    # Record statistics
    duration_ms = Float64(Dates.value(now() - start_time)) / 1_000_000.0  # nanoseconds to milliseconds
    row_count = size(result, 1)
    column_count = size(result, 2)
    
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, 'query', ?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
    """, [
        string(uuid4()),
        "N/A",
        language,
        duration_ms,
        0.0,  # Memory usage not available here
        row_count,
        column_count
    ])
    
    return result
end

"""
    get_table_reference(manager::DuckConnectManager, identifier; language="julia")

Get a reference to a DuckDB table for efficient cross-language access.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `identifier`: Dataset name or ID
- `language`: Language requesting the reference

# Returns
- `String`: Table reference string that can be used directly in SQL
"""
function get_table_reference(manager::DuckConnectManager, identifier::String; language::String="julia")
    # Look up metadata
    metadata = DataFrame(DBInterface.execute(manager.conn, """
        SELECT id, table_name, available_to_languages 
        FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
    """, [identifier, identifier]))
    
    if size(metadata, 1) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    dataset_id = metadata[1, :id]
    table_name = metadata[1, :table_name]
    available_languages_json = metadata[1, :available_to_languages]
    
    # Check language access
    available_languages = JSON.parse(available_languages_json)
    if !(language in available_languages)
        error("Dataset '$identifier' is not available to language '$language'")
    end
    
    # Record transaction for the reference access
    transaction_id = string(uuid4())
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, 'reference', ?, CURRENT_TIMESTAMP, ?)
    """, [
        transaction_id, 
        dataset_id, 
        language, 
        "{}"
    ])
    
    # Return the reference as a special string
    return "duck_table:$table_name"
end

"""
    register_transformation(manager::DuckConnectManager, target_dataset, source_dataset, 
                          transformation; language="julia")

Register a transformation between datasets for lineage tracking.

# Arguments
- `manager::DuckConnectManager`: DuckConnect manager
- `target_dataset`: Target dataset name or ID
- `source_dataset`: Source dataset name or ID
- `transformation`: Description of the transformation
- `language`: Language that performed the transformation

# Returns
- `String`: ID of the lineage record
"""
function register_transformation(manager::DuckConnectManager, target_dataset::String, 
                               source_dataset::String, transformation::String; 
                               language::String="julia")
    # Look up target dataset
    target_metadata = DataFrame(DBInterface.execute(manager.conn, """
        SELECT id FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
    """, [target_dataset, target_dataset]))
    
    if size(target_metadata, 1) == 0
        error("Target dataset '$target_dataset' not found")
    end
    
    target_id = target_metadata[1, :id]
    
    # Look up source dataset
    source_metadata = DataFrame(DBInterface.execute(manager.conn, """
        SELECT id FROM duck_connect_metadata 
        WHERE id = ? OR name = ?
    """, [source_dataset, source_dataset]))
    
    if size(source_metadata, 1) == 0
        error("Source dataset '$source_dataset' not found")
    end
    
    source_id = source_metadata[1, :id]
    
    # Generate unique ID for lineage record
    lineage_id = string(uuid4())
    
    # Insert lineage record
    DBInterface.execute(manager.conn, """
        INSERT INTO duck_connect_lineage
        (id, target_dataset_id, source_dataset_id, transformation, language, timestamp)
        VALUES (?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
    """, [lineage_id, target_id, source_id, transformation, language])
    
    println("Registered transformation from '$source_dataset' to '$target_dataset'")
    return lineage_id
end

"""
    DuckContext

A context manager for working with DuckConnect in Julia node scripts.
"""
mutable struct DuckContext
    dc::DuckConnectManager
    meta::Dict{String, Any}
    language::String
    input_cache::Dict{String, Any}
    
    """
        DuckContext(;db_path=nothing, meta_path=nothing)

    Initialize context with a connection to DuckConnect.
    
    # Arguments
    - `db_path`: Optional path to DuckDB database
    - `meta_path`: Optional path to metadata file
    """
    function DuckContext(;db_path::Union{String, Nothing}=nothing, 
                            meta_path::Union{String, Nothing}=nothing)
        # Load metadata
        meta = load_metadata(meta_path)
        
        # Connect to DuckConnect
        if isnothing(db_path) && haskey(meta, "db_path")
            db_path = meta["db_path"]
        end
        
        dc = DuckConnectManager(isnothing(db_path) ? "duck_connect.duckdb" : db_path)
        
        new(dc, meta, "julia", Dict{String, Any}())
    end
end

"""
    load_metadata(meta_path)

Load metadata from file.

# Arguments
- `meta_path`: Path to metadata file

# Returns
- `Dict`: Dictionary containing metadata
"""
function load_metadata(meta_path::Union{String, Nothing})
    if isnothing(meta_path)
        # Try to get from command line arguments
        if length(ARGS) > 0 && isfile(ARGS[1])
            meta_path = ARGS[1]
        else
            # No metadata file
            return Dict{String, Any}()
        end
    end
    
    # Load metadata
    if isfile(meta_path)
        try
            return YAML.load_file(meta_path)
        catch e
            @warn "Failed to load metadata file: $e"
            return Dict{String, Any}()
        end
    end
    
    return Dict{String, Any}()
end

"""
    get_input(ctx::DuckContext, name; required=true)

Get input dataset by name.

# Arguments
- `ctx`: DuckContext instance
- `name`: Name of the input
- `required`: Whether this input is required

# Returns
- `DataFrame`: DataFrame with the data
"""
function get_input(ctx::DuckContext, name::String; required::Bool=true)
    # Check if input is defined for this node
    inputs = get(ctx.meta, "inputs", String[])
    if !(name in inputs)
        if required
            error("Input '$name' is not defined for this node")
        end
        return nothing
    end
    
    # Check cache
    if haskey(ctx.input_cache, name)
        return ctx.input_cache[name]
    end
    
    # Get from DuckConnect
    try
        data = get_dataset(ctx.dc, name, language=ctx.language)
        ctx.input_cache[name] = data
        return data
    catch e
        if required
            error("Error retrieving input '$name': $e")
        end
        return nothing
    end
end

"""
    save_output(ctx::DuckContext, name, data; description=nothing)

Save output dataset.

# Arguments
- `ctx`: DuckContext instance
- `name`: Name of the output
- `data`: DataFrame to save
- `description`: Optional description

# Returns
- `String`: Dataset ID
"""
function save_output(ctx::DuckContext, name::String, data; description::Union{String, Nothing}=nothing)
    # Check if output is defined for this node
    outputs = get(ctx.meta, "outputs", String[])
    if !(name in outputs)
        error("Output '$name' is not defined for this node")
    end
    
    # Generate description if not provided
    if isnothing(description)
        node_id = get(ctx.meta, "node_id", "unknown")
        description = "Output from $node_id"
    end
    
    # First check if dataset already exists
    try
        existing = list_datasets(ctx.dc, language=ctx.language)
        if !isempty(existing) && name in existing.name
            # Update existing dataset
            return update_dataset(ctx.dc, name, data, language=ctx.language, description=description)
        else
            # Register new dataset
            return register_dataset(ctx.dc, data, name=name, description=description, 
                                  source_language=ctx.language)
        end
    catch e
        error("Error saving output '$name': $e")
    end
end

"""
    get_param(ctx::DuckContext, name, default=nothing)

Get parameter value.

# Arguments
- `ctx`: DuckContext instance
- `name`: Parameter name
- `default`: Default value if not found

# Returns
- Parameter value
"""
function get_param(ctx::DuckContext, name::String, default=nothing)
    params = get(ctx.meta, "params", Dict{String, Any}())
    return get(params, name, default)
end

"""
    execute_query(ctx::DuckContext, query; as_arrow=false)

Execute a SQL query.

# Arguments
- `ctx`: DuckContext instance
- `query`: SQL query to execute
- `as_arrow`: Whether to return as Arrow Table

# Returns
- `DataFrame`: DataFrame with query results
"""
function execute_query(ctx::DuckContext, query::String; as_arrow::Bool=false)
    return execute_query(ctx.dc, query, language=ctx.language, as_arrow=as_arrow)
end

"""
    get_node_id(ctx::DuckContext)

Get node ID from metadata.
"""
function get_node_id(ctx::DuckContext)
    return get(ctx.meta, "node_id", "unknown")
end

"""
    get_pipeline_name(ctx::DuckContext)

Get pipeline name from metadata.
"""
function get_pipeline_name(ctx::DuckContext)
    return get(ctx.meta, "pipeline_name", "unknown")
end

"""
    close(ctx::DuckContext)

Close connection to DuckConnect.
"""
function close(ctx::DuckContext)
    # No explicit close method for DuckDB in Julia
    # Resources will be garbage collected
    ctx.dc.conn = nothing
end

end # module 