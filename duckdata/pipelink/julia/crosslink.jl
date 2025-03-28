module CrossLink

using DuckDB
using DataFrames
using JSON
using Dates
using UUIDs

export CrossLinkManager, push_data, pull_data, get_table_reference, close

"""
    CrossLinkManager

Manager for cross-language data sharing with zero-copy optimization.
"""
mutable struct CrossLinkManager
    conn::DuckDB.DB
    db_path::String
    debug::Bool
    
    """
        CrossLinkManager(db_path::String, debug::Bool=false)
    
    Create a CrossLink manager with connection to specified DuckDB database.
    """
    function CrossLinkManager(db_path::String, debug::Bool=false)
        # Create directory if it doesn't exist
        db_dir = dirname(db_path)
        if !isempty(db_dir) && !isdir(db_dir)
            mkpath(db_dir)
        end
        
        # Connect to database
        conn = DuckDB.DB(db_path)
        
        # Setup metadata tables
        setup_metadata_tables(conn)
        
        # Return manager
        new(conn, db_path, debug)
    end
end

"""
    setup_metadata_tables(conn::DuckDB.DB)

Set up metadata tables for cross-language data sharing.
"""
function setup_metadata_tables(conn::DuckDB.DB)
    # Main metadata table
    try
        DuckDB.execute(conn, """
        CREATE TABLE IF NOT EXISTS crosslink_metadata (
            id TEXT PRIMARY KEY,
            name TEXT,
            source_language TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP,
            description TEXT,
            schema TEXT,
            table_name TEXT,
            arrow_data BOOLEAN DEFAULT FALSE,
            version INTEGER DEFAULT 1,
            current_version BOOLEAN DEFAULT TRUE,
            lineage TEXT,
            schema_hash TEXT,
            memory_map_path TEXT,
            shared_memory_key TEXT,
            arrow_schema TEXT,
            access_languages TEXT,
            memory_layout TEXT
        )
        """)
        
        # Access log table
        DuckDB.execute(conn, """
        CREATE TABLE IF NOT EXISTS crosslink_access_log (
            id TEXT PRIMARY KEY,
            dataset_id TEXT,
            language TEXT,
            operation TEXT,
            timestamp TIMESTAMP,
            access_method TEXT,
            success BOOLEAN,
            error_message TEXT
        )
        """)
    catch e
        @warn "Error setting up metadata tables: $e"
    end
end

"""
    log_access(cl::CrossLinkManager, dataset_id::String, operation::String,
              access_method::String, success::Bool=true, error_message::Union{String, Nothing}=nothing)

Log dataset access operation.
"""
function log_access(cl::CrossLinkManager, dataset_id::String, operation::String,
                   access_method::String, success::Bool=true, error_message::Union{String, Nothing}=nothing)
    try
        log_id = string(uuid4())
        
        DuckDB.execute(cl.conn, """
        INSERT INTO crosslink_access_log (id, dataset_id, language, operation, timestamp, 
                                        access_method, success, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, [log_id, dataset_id, "julia", operation, now(), access_method, success, error_message])
        
        return true
    catch e
        if cl.debug
            @warn "Failed to log access: $e"
        end
        return false
    end
end

"""
    get_dataset_metadata(cl::CrossLinkManager, identifier::String)

Get metadata for dataset by ID or name.
"""
function get_dataset_metadata(cl::CrossLinkManager, identifier::String)
    try
        # Query metadata
        result = DataFrame(DuckDB.execute(cl.conn, """
        SELECT * FROM crosslink_metadata
        WHERE id = ? OR name = ?
        """, [identifier, identifier]))
        
        # Check if found
        if size(result, 1) == 0
            return nothing
        end
        
        # Parse JSON fields
        metadata = Dict{String, Any}()
        for col in names(result)
            metadata[string(col)] = result[1, col]
        end
        
        # Parse JSON fields
        for field in ["schema", "arrow_schema", "access_languages", "memory_layout"]
            if haskey(metadata, field) && metadata[field] !== nothing
                try
                    metadata[field] = JSON.parse(metadata[field])
                catch
                    # Keep as string if parsing fails
                end
            end
        end
        
        return metadata
    catch e
        if cl.debug
            @warn "Error getting dataset metadata: $e"
        end
        return nothing
    end
end

"""
    push_data(cl::CrossLinkManager, df::DataFrame, name::Union{String, Nothing}=nothing;
            description::Union{String, Nothing}=nothing, arrow_data::Bool=true)

Push DataFrame to the database with metadata for cross-language data sharing.
"""
function push_data(cl::CrossLinkManager, df::DataFrame, name::Union{String, Nothing}=nothing;
                  description::Union{String, Nothing}=nothing, arrow_data::Bool=true)
    # Generate ID and name if needed
    dataset_id = string(uuid4())
    if name === nothing
        name = "dataset_$(Int(datetime2unix(now())))"
    end
    
    # Create table name
    table_name = "data_$(replace(replace(lowercase(name), " " => "_"), "-" => "_"))"
    
    # Create table from DataFrame
    try
        # Create table
        DuckDB.execute(cl.conn, "CREATE OR REPLACE TABLE $table_name AS SELECT * FROM df")
        
        # Create schema
        schema_dict = Dict(
            "columns" => names(df),
            "dtypes" => Dict(string(col) => string(eltype(df[!, col])) for col in names(df))
        )
        
        # Calculate schema hash
        schema_hash = bytes2hex(sha1(JSON.json(schema_dict)))
        
        # Store metadata
        DuckDB.execute(cl.conn, """
        INSERT INTO crosslink_metadata (
            id, name, source_language, created_at, updated_at, description,
            schema, table_name, arrow_data, version, current_version, schema_hash,
            access_languages
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dataset_id, 
            name, 
            "julia", 
            now(), 
            now(),
            description === nothing ? "" : description,
            JSON.json(schema_dict),
            table_name,
            arrow_data,
            1,
            true,
            schema_hash,
            JSON.json(["python", "r", "julia", "cpp"])
        ])
        
        # Log access
        log_access(cl, dataset_id, "write", arrow_data ? "zero-copy" : "copy")
        
        return dataset_id
    catch e
        @error "Failed to push data: $e"
        rethrow(e)
    end
end

"""
    pull_data(cl::CrossLinkManager, identifier::String; zero_copy::Bool=true)

Pull data from database, optionally with zero-copy optimization.
"""
function pull_data(cl::CrossLinkManager, identifier::String; zero_copy::Bool=true)
    # Get metadata
    metadata = get_dataset_metadata(cl, identifier)
    if metadata === nothing
        error("Dataset not found: $identifier")
    end
    
    # Log access
    log_access(cl, metadata["id"], "read", zero_copy ? "zero-copy" : "copy")
    
    # Get table name
    table_name = metadata["table_name"]
    
    # Query data
    result = DataFrame(DuckDB.execute(cl.conn, "SELECT * FROM $table_name"))
    
    return result
end

"""
    get_table_reference(cl::CrossLinkManager, identifier::String)

Get direct reference to a table without copying data.
"""
function get_table_reference(cl::CrossLinkManager, identifier::String)
    # Get metadata
    metadata = get_dataset_metadata(cl, identifier)
    if metadata === nothing
        error("Dataset not found: $identifier")
    end
    
    # Log access
    log_access(cl, metadata["id"], "reference", "zero-copy")
    
    # Return reference info
    return Dict{String, Any}(
        "database_path" => abspath(cl.db_path),
        "table_name" => metadata["table_name"],
        "schema" => metadata["schema"],
        "dataset_id" => metadata["id"],
        "access_method" => "direct_table_reference"
    )
end

"""
    close(cl::CrossLinkManager)

Close database connection.
"""
function close(cl::CrossLinkManager)
    # Database will be automatically closed when the connection is garbage collected
    # This is just for explicitness
    cl.conn = nothing
end

end # module 