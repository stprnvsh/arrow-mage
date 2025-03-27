"""
CrossLink: Simple cross-language data sharing

A module to easily share data between Julia, Python, and R
"""
module CrossLink

using DuckDB
using DataFrames
using JSON
using UUIDs
using Dates
using Markdown
using SHA

# Import Arrow if available
try
    @eval using Arrow
catch e
    @warn "Arrow package not found. Some features will be disabled."
end

export CrossLinkManager, push_data, pull_data, list_datasets, delete_dataset
export list_dataset_versions, get_schema_history, get_lineage, check_compatibility

# Define a local function for Arrow table registration instead of extending DuckDB
"""
    register_arrow_table(connection, arrow_table, view_name)

Register an Arrow table as a DuckDB view.
"""
function register_arrow_table(connection::DuckDB.DB, arrow_table::Arrow.Table, view_name::String)
    # Convert Arrow table to a temporary file in memory
    io = IOBuffer()
    Arrow.write(io, arrow_table)
    arrow_bytes = take!(io)
    
    # Use DuckDB's arrow_scan functionality to create a view from the Arrow data
    stmt = DuckDB.Stmt(connection, """
    CREATE OR REPLACE VIEW $view_name AS 
    SELECT * FROM arrow_scan(?)
    """)
    DuckDB.bind!(stmt, 1, arrow_bytes)
    DuckDB.execute(stmt)
end

"""
    CrossLinkManager

A manager class for cross-language data sharing.
"""
mutable struct CrossLinkManager
    conn::DuckDB.DB
    arrow_available::Bool
    debug::Bool
    metadata_cache::Dict{String, Any}
    storage_configs::Dict{String, Dict}
    s3_configured::Bool
    last_schema_update::DateTime
    min_schema_update_interval_ms::Int
    
    """
        CrossLinkManager(db_path::String)

    Create a CrossLinkManager with a DuckDB database at the specified path.
    """
    function CrossLinkManager(db_path::String)
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
        
        manager = new(
            conn, 
            arrow_available, 
            false,                # debug
            Dict{String, Any}(),  # metadata_cache
            Dict{String, Dict}(), # storage_configs
            false,                # s3_configured
            Dates.now(),          # last_schema_update
            1000                  # min_schema_update_interval_ms
        )
        setup_metadata_tables(manager)
        return manager
    end
    
    """
        CrossLinkManager(db_connection::DuckDB.DB)

    Create a CrossLinkManager with an existing DuckDB connection.
    """
    function CrossLinkManager(db_connection::DuckDB.DB; 
                             debug=false, 
                             create_tables=true, 
                             min_schema_update_interval_ms=1000)
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
        
        mgr = new(
            db_connection,
            arrow_available,
            debug,
            Dict{String, Any}(),       # Metadata cache
            Dict{String, Dict}(),      # Storage configurations
            false,                     # S3 configured flag
            Dates.now(),               # Last schema update
            min_schema_update_interval_ms
        )
        
        # Setup database schema if requested
        if create_tables
            setup_metadata_tables(mgr)
        end
        
        return mgr
    end
end

"""
    setup_metadata_tables(manager::CrossLinkManager)

Set up the metadata tables if they don't exist.
"""
function setup_metadata_tables(manager::CrossLinkManager)
    # Configure DuckDB for handling large datasets
    try
        # Set memory limit with absolute value instead of percentage
        # Use a simpler, more robust approach - default to a conservative value
        memory_limit = "4GB"
        
        # Only attempt to detect memory if supported on this platform
        if Sys.islinux()
            try
                # Use the simpler command to avoid parsing issues
                mem_total_kb = parse(Int, read(`head -n1 /proc/meminfo`, String)[13:end-3])
                if mem_total_kb > 0
                    memory_limit = "$(round(Int, mem_total_kb * 0.8 / 1024))MB"
                end
            catch
                # Silently fall back to default if system command fails
            end
        elseif Sys.isapple()
            try
                # On macOS, use sysctl directly with simplified parsing
                mem_bytes = parse(Int, strip(read(`sysctl -n hw.memsize`, String)))
                if mem_bytes > 0
                    memory_limit = "$(round(Int, mem_bytes * 0.8 / 1024^3))GB"
                end
            catch
                # Silently fall back to default if system command fails
            end
        end
        
        DBInterface.execute(manager.conn, "PRAGMA memory_limit='$memory_limit'")
        
        # Set threads to a reasonable number (adjust based on system capabilities)
        num_cores = max(1, Sys.CPU_THREADS - 1)  # Leave 1 core for other processes
        DBInterface.execute(manager.conn, "PRAGMA threads=$num_cores")
        
        # Enable parallelism
        DBInterface.execute(manager.conn, "PRAGMA verify_parallelism")
        
        # Enable progress bar for long-running queries (useful for debugging)
        DBInterface.execute(manager.conn, "PRAGMA enable_progress_bar")
        
        # Enable object cache for better performance with repeated queries
        DBInterface.execute(manager.conn, "PRAGMA enable_object_cache")
    catch e
        @warn "Failed to configure DuckDB performance settings: $e"
    end
    
    # Enable external IO if possible
    try
        DBInterface.execute(manager.conn, "INSTALL httpfs")
        DBInterface.execute(manager.conn, "LOAD httpfs")
    catch e
        @warn "httpfs extension not available. Remote file access will be limited."
    end
    
    # Main metadata table for current versions
    DBInterface.execute(manager.conn, """
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
        schema_hash TEXT
    )
    """)
    
    # Schema history table for tracking schema evolution
    DBInterface.execute(manager.conn, """
    CREATE TABLE IF NOT EXISTS crosslink_schema_history (
        id TEXT,
        version INTEGER,
        schema TEXT,
        schema_hash TEXT,
        changed_at TIMESTAMP,
        change_type TEXT,
        changes TEXT,
        PRIMARY KEY (id, version)
    )
    """)
    
    # Lineage table for tracking data provenance
    DBInterface.execute(manager.conn, """
    CREATE TABLE IF NOT EXISTS crosslink_lineage (
        dataset_id TEXT,
        source_dataset_id TEXT,
        source_dataset_version INTEGER,
        transformation TEXT,
        created_at TIMESTAMP,
        PRIMARY KEY (dataset_id, source_dataset_id)
    )
    """)
    
    # Add missing columns to existing databases for backward compatibility
    ensure_columns_exist(manager)
end

"""
    ensure_columns_exist(manager::CrossLinkManager)

Ensure all required columns exist in metadata table for backwards compatibility.
"""
function ensure_columns_exist(manager::CrossLinkManager)
    columns_to_check = [
        ("arrow_data", "BOOLEAN DEFAULT FALSE"),
        ("version", "INTEGER DEFAULT 1"),
        ("current_version", "BOOLEAN DEFAULT TRUE"),
        ("lineage", "TEXT"),
        ("schema_hash", "TEXT")
    ]
    
    for (col_name, col_type) in columns_to_check
        result = DBInterface.execute(manager.conn, """
        SELECT count(*) as col_exists FROM pragma_table_info('crosslink_metadata') 
        WHERE name = '$col_name'
        """) |> DataFrame
        
        if result[1, :col_exists] == 0
            DBInterface.execute(manager.conn, """
            ALTER TABLE crosslink_metadata ADD COLUMN $col_name $col_type
            """)
        end
    end
end

"""
    compute_schema_hash(df::DataFrame)

Compute a hash of the DataFrame schema for detecting changes.
"""
function compute_schema_hash(df::DataFrame)
    schema_dict = Dict(
        "columns" => sort(names(df)),
        "dtypes" => Dict(name => string(eltype(df[!, name])) for name in sort(names(df)))
    )
    schema_str = JSON.json(schema_dict)
    return bytes2hex(sha1(schema_str))
end

"""
    detect_schema_changes(old_schema, new_df::DataFrame)

Detect schema changes between old schema and new DataFrame.

Returns:
    Dict: Description of changes
"""
function detect_schema_changes(old_schema, new_df::DataFrame)
    if old_schema === nothing
        return Dict("change_type" => "initial_schema")
    end
    
    old_schema_dict = JSON.parse(old_schema)
    old_columns = Set(old_schema_dict["columns"])
    old_dtypes = old_schema_dict["dtypes"]
    
    new_columns = Set(names(new_df))
    new_dtypes = Dict(name => string(eltype(new_df[!, name])) for name in names(new_df))
    
    added_columns = setdiff(new_columns, old_columns)
    removed_columns = setdiff(old_columns, new_columns)
    common_columns = intersect(old_columns, new_columns)
    
    changed_dtypes = Dict{String, Tuple{String, String}}()
    for col in common_columns
        old_type = get(old_dtypes, col, "")
        new_type = get(new_dtypes, col, "")
        if old_type != new_type
            changed_dtypes[col] = (old_type, new_type)
        end
    end
    
    changes = Dict(
        "added_columns" => collect(added_columns),
        "removed_columns" => collect(removed_columns),
        "changed_dtypes" => changed_dtypes
    )
    
    if !isempty(added_columns) || !isempty(removed_columns) || !isempty(changed_dtypes)
        if !isempty(removed_columns)
            change_type = "breaking"
        elseif !isempty(added_columns) && isempty(changed_dtypes)
            change_type = "non_breaking_addition"
        else
            change_type = "potentially_breaking"
        end
    else
        change_type = "no_change"
    end
    
    return Dict(
        "change_type" => change_type,
        "changes" => changes
    )
end

"""
    list_datasets(manager::CrossLinkManager)

List all available datasets in the CrossLink registry.
"""
function list_datasets(manager::CrossLinkManager)
    query = """
    SELECT id, name, source_language, created_at, table_name, description, version
    FROM crosslink_metadata
    WHERE current_version = TRUE
    ORDER BY updated_at DESC
    """
    
    return DBInterface.execute(manager.conn, query) |> DataFrame
end

"""
    list_dataset_versions(manager::CrossLinkManager, identifier::String)
    
List all versions of a dataset.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `identifier::String`: Dataset ID or name

# Returns
- `DataFrame`: DataFrame containing version information
"""
function list_dataset_versions(manager::CrossLinkManager, identifier::String)
    query = """
    SELECT id, name, version, created_at, updated_at, schema_hash, current_version
    FROM crosslink_metadata 
    WHERE id = ? OR name = ?
    ORDER BY version DESC
    """
    
    result = DBInterface.execute(manager.conn, query, [identifier, identifier]) |> DataFrame
    
    if nrow(result) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    return result
end

"""
    get_schema_history(manager::CrossLinkManager, identifier::String)
    
Get schema evolution history for a dataset.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `identifier::String`: Dataset ID or name

# Returns
- `DataFrame`: DataFrame containing schema history
"""
function get_schema_history(manager::CrossLinkManager, identifier::String)
    # First get the ID if name was provided
    id_query = """
    SELECT id FROM crosslink_metadata
    WHERE id = ? OR name = ?
    LIMIT 1
    """
    
    id_result = DBInterface.execute(manager.conn, id_query, [identifier, identifier]) |> DataFrame
    
    if nrow(id_result) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    dataset_id = id_result[1, :id]
    
    query = """
    SELECT id, version, schema, schema_hash, changed_at, change_type, changes
    FROM crosslink_schema_history
    WHERE id = ?
    ORDER BY version
    """
    
    return DBInterface.execute(manager.conn, query, [dataset_id]) |> DataFrame
end

"""
    get_lineage(manager::CrossLinkManager, identifier::String)
    
Get data lineage information for a dataset.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `identifier::String`: Dataset ID or name

# Returns
- `DataFrame`: DataFrame containing lineage information
"""
function get_lineage(manager::CrossLinkManager, identifier::String)
    # First get the ID if name was provided
    id_query = """
    SELECT id FROM crosslink_metadata
    WHERE id = ? OR name = ?
    LIMIT 1
    """
    
    id_result = DBInterface.execute(manager.conn, id_query, [identifier, identifier]) |> DataFrame
    
    if nrow(id_result) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    dataset_id = id_result[1, :id]
    
    query = """
    SELECT dataset_id, source_dataset_id, source_dataset_version, transformation, created_at
    FROM crosslink_lineage
    WHERE dataset_id = ?
    """
    
    return DBInterface.execute(manager.conn, query, [dataset_id]) |> DataFrame
end

"""
    push_data(manager::CrossLinkManager, df::DataFrame; name=nothing, description=nothing, use_arrow=true, 
              sources=nothing, transformation=nothing, force_new_version=false,
              chunk_size=nothing, use_disk_spilling=true)

Push a DataFrame to the CrossLink registry.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `df::DataFrame`: DataFrame to share
- `name`: Optional name for the dataset
- `description`: Optional description of the dataset
- `use_arrow`: Whether to use Arrow for storage (default: true)
- `sources`: List of source dataset IDs or names that were used to create this dataset
- `transformation`: Description of the transformation applied to create this dataset
- `force_new_version`: Whether to create a new version even if schema hasn't changed
- `chunk_size`: Size of chunks when handling large datasets (nothing = auto-detect based on data size)
- `use_disk_spilling`: Whether to allow DuckDB to spill to disk for large datasets

# Returns
- `String`: The ID of the registered dataset
"""
function push_data(manager::CrossLinkManager, df::DataFrame; name=nothing, description=nothing, use_arrow=true, 
                  sources=nothing, transformation=nothing, force_new_version=false,
                  chunk_size=nothing, use_disk_spilling=true)
    # Enable disk spilling if requested
    if use_disk_spilling
        try
            # Configure DuckDB for large datasets
            # Set memory limit with a simpler, more robust approach
            memory_limit = "4GB"  # Default to conservative value
            
            # Only attempt to detect memory if supported on this platform
            if Sys.islinux()
                try
                    # Use the simpler command to avoid parsing issues
                    mem_total_kb = parse(Int, read(`head -n1 /proc/meminfo`, String)[13:end-3])
                    if mem_total_kb > 0
                        memory_limit = "$(round(Int, mem_total_kb * 0.8 / 1024))MB"
                    end
                catch
                    # Silently fall back to default if system command fails
                end
            elseif Sys.isapple()
                try
                    # On macOS, use sysctl directly with simplified parsing
                    mem_bytes = parse(Int, strip(read(`sysctl -n hw.memsize`, String)))
                    if mem_bytes > 0
                        memory_limit = "$(round(Int, mem_bytes * 0.8 / 1024^3))GB"
                    end
                catch
                    # Silently fall back to default if system command fails
                end
            end
            
            DBInterface.execute(manager.conn, "PRAGMA memory_limit='$memory_limit'")
            
            # Set threads to a reasonable number based on CPU count
            num_cores = max(1, Sys.CPU_THREADS - 1)  # Leave 1 core for other processes
            DBInterface.execute(manager.conn, "PRAGMA threads=$num_cores")
            
            # Enable parallelism for better performance
            DBInterface.execute(manager.conn, "PRAGMA verify_parallelism")
        catch e
            @warn "Failed to configure DuckDB memory settings: $e"
        end
    end
    
    # Use the provided name or generate one
    if isnothing(name)
        name = "julia_data_$(round(Int, datetime2unix(now())))"
    end
    
    # Check if dataset with this name already exists
    existing_query = """
    SELECT id, schema, version FROM crosslink_metadata 
    WHERE name = ? AND current_version = TRUE
    """
    
    existing = DBInterface.execute(manager.conn, existing_query, [name]) |> DataFrame
    
    dataset_id = nothing
    create_new_version = true
    old_version = 1
    
    if nrow(existing) > 0
        dataset_id = existing[1, :id]
        old_schema = existing[1, :schema]
        old_version = existing[1, :version]
        
        # Detect schema changes
        schema_hash = compute_schema_hash(df)
        schema_changes = detect_schema_changes(old_schema, df)
        
        # Only create new version if schema changed or forced
        if schema_changes["change_type"] == "no_change" && !force_new_version
            create_new_version = false
        end
    else
        # New dataset
        dataset_id = string(uuid4())
        old_schema = nothing
        schema_changes = Dict("change_type" => "initial_schema")
    end
    
    # Generate unique table name for this version
    new_version = create_new_version ? old_version + 1 : old_version
    table_name = "crosslink_data_$(replace(dataset_id, "-" => "_"))_$new_version"
    
    # Calculate schema hash and JSON
    schema_dict = Dict(
        "columns" => names(df),
        "dtypes" => Dict(name => string(eltype(df[!, name])) for name in names(df)),
        "is_arrow" => use_arrow && manager.arrow_available
    )
    schema_json = JSON.json(schema_dict)
    schema_hash = compute_schema_hash(df)
    
    # Determine if we should use chunking
    use_chunking = false
    
    # Estimate memory usage of the DataFrame (rough estimate)
    function estimate_memory_size(df::DataFrame)
        size_bytes = 0
        # Calculate size for each column
        for col in names(df)
            col_data = df[!, col]
            # Basic size estimate based on element type
            element_type = eltype(col_data)
            if element_type <: Number
                # For numeric types, get the size in bytes
                bytes_per_element = sizeof(element_type)
                size_bytes += length(col_data) * bytes_per_element
            elseif element_type <: AbstractString
                # For strings, estimate based on actual string lengths
                for str in col_data
                    size_bytes += sizeof(str)
                end
            elseif element_type <: AbstractArray
                # For array columns, estimate recursively
                for arr in col_data
                    for elem in arr
                        size_bytes += sizeof(elem)
                    end
                end
            else
                # For other types, use a conservative estimate
                size_bytes += length(col_data) * 64  # 64 bytes per element as fallback
            end
        end
        return size_bytes
    end
    
    estimated_size_bytes = estimate_memory_size(df)
    
    # Determine chunking strategy
    if isnothing(chunk_size)
        # Auto-detect: if dataset is larger than 1GB, use chunking (align with Python and R)
        use_chunking = estimated_size_bytes > 1_000_000_000  # 1GB
        chunk_size = use_chunking ? 500_000 : nothing  # Default chunk size of 500k rows
    else
        use_chunking = chunk_size > 0
    end
    
    # Store data using Arrow if possible, otherwise fall back to direct table creation
    arrow_stored = false
    
    if use_arrow && manager.arrow_available
        try
            if use_chunking
                # Stream data in chunks to avoid memory issues
                # First create empty table with correct schema
                sample_df = first(df, 1)
                arrow_table_sample = Arrow.Table(sample_df)
                
                if create_new_version
                    # Create a new table with the schema
                    temp_view_name = "arrow_schema_$(rand(1000:9999))"
                    register_arrow_table(manager.conn, arrow_table_sample, temp_view_name)
                    DBInterface.execute(manager.conn, "CREATE TABLE $table_name AS SELECT * FROM $temp_view_name WHERE 1=0")
                    DBInterface.execute(manager.conn, "DROP VIEW IF EXISTS $temp_view_name")
                    
                    # Insert data in chunks
                    total_rows = nrow(df)
                    for i in 1:chunk_size:total_rows
                        end_idx = min(i + chunk_size - 1, total_rows)
                        chunk = df[i:end_idx, :]
                        
                        arrow_chunk = Arrow.Table(chunk)
                        temp_chunk_view = "arrow_chunk_$(rand(1000:9999))"
                        register_arrow_table(manager.conn, arrow_chunk, temp_chunk_view)
                        DBInterface.execute(manager.conn, "INSERT INTO $table_name SELECT * FROM $temp_chunk_view")
                        DBInterface.execute(manager.conn, "DROP VIEW IF EXISTS $temp_chunk_view")
                        
                        # Manually trigger garbage collection to free memory
                        chunk = nothing
                        arrow_chunk = nothing
                        GC.gc()
                    end
                else
                    # Update existing table
                    old_table_name = "crosslink_data_$(replace(dataset_id, "-" => "_"))_$old_version"
                    DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $old_table_name")
                    
                    # Create empty table with schema
                    temp_view_name = "arrow_schema_$(rand(1000:9999))"
                    register_arrow_table(manager.conn, arrow_table_sample, temp_view_name)
                    DBInterface.execute(manager.conn, "CREATE TABLE $old_table_name AS SELECT * FROM $temp_view_name WHERE 1=0")
                    DBInterface.execute(manager.conn, "DROP VIEW IF EXISTS $temp_view_name")
                    
                    # Insert data in chunks
                    total_rows = nrow(df)
                    for i in 1:chunk_size:total_rows
                        end_idx = min(i + chunk_size - 1, total_rows)
                        chunk = df[i:end_idx, :]
                        
                        arrow_chunk = Arrow.Table(chunk)
                        temp_chunk_view = "arrow_chunk_$(rand(1000:9999))"
                        register_arrow_table(manager.conn, arrow_chunk, temp_chunk_view)
                        DBInterface.execute(manager.conn, "INSERT INTO $old_table_name SELECT * FROM $temp_chunk_view")
                        DBInterface.execute(manager.conn, "DROP VIEW IF EXISTS $temp_chunk_view")
                        
                        # Manually trigger garbage collection to free memory
                        chunk = nothing
                        arrow_chunk = nothing
                        GC.gc()
                    end
                    
                    table_name = old_table_name
                end
                
                # Mark as successfully stored using Arrow
                arrow_stored = true
            else
                # Non-chunked approach for smaller datasets
                # Convert DataFrame to Arrow Table
                arrow_table = Arrow.Table(df)
                
                if create_new_version
                    # Create a new table for the new version
                    temp_view_name = "temp_arrow_view_$(replace(dataset_id, "-" => "_"))"
                    register_arrow_table(manager.conn, arrow_table, temp_view_name)
                    DBInterface.execute(manager.conn, "CREATE TABLE $table_name AS SELECT * FROM $temp_view_name")
                    DBInterface.execute(manager.conn, "DROP VIEW IF EXISTS $temp_view_name")
                else
                    # Update existing table
                    old_table_name = "crosslink_data_$(replace(dataset_id, "-" => "_"))_$old_version"
                    DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $old_table_name")
                    temp_view_name = "temp_arrow_view_$(replace(dataset_id, "-" => "_"))"
                    register_arrow_table(manager.conn, arrow_table, temp_view_name)
                    DBInterface.execute(manager.conn, "CREATE TABLE $old_table_name AS SELECT * FROM $temp_view_name")
                    DBInterface.execute(manager.conn, "DROP VIEW IF EXISTS $temp_view_name")
                    table_name = old_table_name
                end
                
                # Mark as successfully stored using Arrow
                arrow_stored = true
            end
        catch e
            @warn "Failed to use Arrow for data storage: $e"
            @warn "Falling back to direct DuckDB table creation"
            arrow_stored = false
        end
    end
    
    if !arrow_stored
        # Direct table creation without Arrow
        try
            if use_chunking
                # Create empty table with schema first
                if create_new_version
                    # Create table with schema using the first row
                    sample_df = first(df, 1)
                    DuckDB.register_data_frame(manager.conn, sample_df, "temp_schema")
                    DBInterface.execute(manager.conn, "CREATE TABLE $table_name AS SELECT * FROM temp_schema WHERE 1=0")
                    DBInterface.execute(manager.conn, "DROP VIEW temp_schema")
                    
                    # Insert data in chunks
                    total_rows = nrow(df)
                    for i in 1:chunk_size:total_rows
                        end_idx = min(i + chunk_size - 1, total_rows)
                        chunk = df[i:end_idx, :]
                        
                        DuckDB.register_data_frame(manager.conn, chunk, "temp_chunk")
                        DBInterface.execute(manager.conn, "INSERT INTO $table_name SELECT * FROM temp_chunk")
                        DBInterface.execute(manager.conn, "DROP VIEW temp_chunk")
                        
                        # Manually trigger garbage collection to free memory
                        chunk = nothing
                        GC.gc()
                    end
                else
                    # Update existing table
                    old_table_name = "crosslink_data_$(replace(dataset_id, "-" => "_"))_$old_version"
                    DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $old_table_name")
                    
                    # Create table with schema using the first row
                    sample_df = first(df, 1)
                    DuckDB.register_data_frame(manager.conn, sample_df, "temp_schema")
                    DBInterface.execute(manager.conn, "CREATE TABLE $old_table_name AS SELECT * FROM temp_schema WHERE 1=0")
                    DBInterface.execute(manager.conn, "DROP VIEW temp_schema")
                    
                    # Insert data in chunks
                    total_rows = nrow(df)
                    for i in 1:chunk_size:total_rows
                        end_idx = min(i + chunk_size - 1, total_rows)
                        chunk = df[i:end_idx, :]
                        
                        DuckDB.register_data_frame(manager.conn, chunk, "temp_chunk")
                        DBInterface.execute(manager.conn, "INSERT INTO $old_table_name SELECT * FROM temp_chunk")
                        DBInterface.execute(manager.conn, "DROP VIEW temp_chunk")
                        
                        # Manually trigger garbage collection to free memory
                        chunk = nothing
                        GC.gc()
                    end
                    
                    table_name = old_table_name
                end
            else
                # Non-chunked approach for smaller datasets
                if create_new_version
                    # Register the DataFrame with DuckDB and create the table directly
                    DuckDB.register_data_frame(manager.conn, df, "temp_df")
                    DBInterface.execute(manager.conn, "CREATE TABLE $table_name AS SELECT * FROM temp_df")
                    DBInterface.execute(manager.conn, "DROP VIEW temp_df")
                else
                    # Update existing table
                    old_table_name = "crosslink_data_$(replace(dataset_id, "-" => "_"))_$old_version"
                    DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $old_table_name")
                    DuckDB.register_data_frame(manager.conn, df, "temp_df")
                    DBInterface.execute(manager.conn, "CREATE TABLE $old_table_name AS SELECT * FROM temp_df")
                    DBInterface.execute(manager.conn, "DROP VIEW temp_df")
                    table_name = old_table_name
                end
            end
        catch e
            # If we still have memory issues, try with a smaller chunk size (similar to Python/R)
            if !use_chunking || chunk_size > 10000
                @warn "Memory issues encountered. Retrying with smaller chunk size."
                smaller_chunk = use_chunking ? div(chunk_size, 2) : 10000
                return push_data(manager, df; 
                              name=name, 
                              description=description, 
                              use_arrow=use_arrow, 
                              sources=sources, 
                              transformation=transformation, 
                              force_new_version=force_new_version, 
                              chunk_size=smaller_chunk, 
                              use_disk_spilling=use_disk_spilling)
            else
                # If we're already using a small chunk size and still having issues, rethrow the error
                rethrow(e)
            end
        end
    end
    
    # If creating a new version, mark old versions as not current
    if create_new_version && nrow(existing) > 0
        DBInterface.execute(manager.conn, """
        UPDATE crosslink_metadata SET current_version = FALSE
        WHERE id = ?
        """, [dataset_id])
        
        # Record schema history
        DBInterface.execute(manager.conn, """
        INSERT INTO crosslink_schema_history 
        (id, version, schema, schema_hash, changed_at, change_type, changes)
        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, ?, ?)
        """, [
            dataset_id, 
            new_version, 
            schema_json, 
            schema_hash, 
            schema_changes["change_type"], 
            JSON.json(schema_changes["changes"])
        ])
    end
    
    # Record lineage information
    if sources !== nothing && create_new_version
        if !isa(sources, Array)
            sources = [sources]
        end
        
        for source in sources
            # Get source dataset ID and version
            source_query = """
            SELECT id, version FROM crosslink_metadata 
            WHERE (id = ? OR name = ?) AND current_version = TRUE
            """
            
            source_metadata = DBInterface.execute(manager.conn, source_query, [source, source]) |> DataFrame
            
            if nrow(source_metadata) > 0
                source_id = source_metadata[1, :id]
                source_version = source_metadata[1, :version]
                
                # Record lineage
                lineage_query = """
                INSERT OR REPLACE INTO crosslink_lineage
                (dataset_id, source_dataset_id, source_dataset_version, transformation, created_at)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                """
                
                transform_desc = transformation === nothing ? "unknown" : transformation
                DBInterface.execute(manager.conn, lineage_query, [dataset_id, source_id, source_version, transform_desc])
            end
        end
    end
    
    # Insert or update metadata
    if create_new_version
        # Insert new metadata record
        insert_query = """
        INSERT INTO crosslink_metadata 
        (id, name, source_language, created_at, updated_at, description, schema, table_name, arrow_data, version, current_version, schema_hash)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, ?, ?, ?, ?, ?, TRUE, ?)
        """
        
        DBInterface.execute(manager.conn, insert_query, [
            dataset_id, name, "julia", description, schema_json, table_name, arrow_stored, new_version, schema_hash
        ])
    else
        # Update existing metadata record
        update_query = """
        UPDATE crosslink_metadata SET
        updated_at = CURRENT_TIMESTAMP,
        description = COALESCE(?, description),
        arrow_data = ?
        WHERE id = ? AND version = ?
        """
        
        DBInterface.execute(manager.conn, update_query, [description, arrow_stored, dataset_id, old_version])
    end
    
    return dataset_id
end

"""
    pull_data(manager::CrossLinkManager, id; 
              version=nothing, 
              as_arrow=nothing, 
              chunk_size=nothing, 
              use_disk_spilling=true,
              max_memory_usage="80%")

Pull a dataset from the CrossLink registry.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `id`: Dataset ID or name
- `version`: Optional specific version to retrieve (default: latest)
- `as_arrow`: Whether to return as Arrow table (default: auto-detect based on schema)
- `chunk_size`: Size of chunks when handling large datasets (nothing = auto-detect)
- `use_disk_spilling`: Whether to allow DuckDB to spill to disk for large queries
- `max_memory_usage`: Maximum memory usage as percentage or absolute value (e.g. "80%" or "8GB")

# Returns
- `Union{DataFrame, Arrow.Table}`: The data as a DataFrame or Arrow Table (based on schema and as_arrow)
"""
function pull_data(manager::CrossLinkManager, id; 
                  version=nothing, 
                  as_arrow=nothing, 
                  chunk_size=nothing, 
                  use_disk_spilling=true,
                  max_memory_usage="80%")
    # Configure DuckDB for large dataset handling
    if use_disk_spilling
        try
            # Set memory limit based on the provided max_memory_usage
            if isa(max_memory_usage, AbstractString)
                # If it's a percentage, calculate it based on system memory
                if endswith(max_memory_usage, "%")
                    percentage = parse(Float64, max_memory_usage[1:end-1]) / 100.0
                    memory_limit = "4GB"  # Default fallback
                    
                    # Try to detect system memory if possible
                    if Sys.islinux()
                        try
                            mem_total_kb = parse(Int, read(`head -n1 /proc/meminfo`, String)[13:end-3])
                            if mem_total_kb > 0
                                memory_limit = "$(round(Int, mem_total_kb * percentage / 1024))MB"
                            end
                        catch
                            # Silently fall back to default
                        end
                    elseif Sys.isapple()
                        try
                            mem_bytes = parse(Int, strip(read(`sysctl -n hw.memsize`, String)))
                            if mem_bytes > 0
                                memory_limit = "$(round(Int, mem_bytes * percentage / 1024^3))GB"
                            end
                        catch
                            # Silently fall back to default
                        end
                    end
                else
                    # It's an absolute value (e.g., "8GB", "1000MB")
                    memory_limit = max_memory_usage
                end
                
                DBInterface.execute(manager.conn, "PRAGMA memory_limit='$memory_limit'")
            end
            
            # Set threads to a reasonable number based on CPU count
            num_cores = max(1, Sys.CPU_THREADS - 1)  # Leave 1 core for other processes
            DBInterface.execute(manager.conn, "PRAGMA threads=$num_cores")
            
            # Enable parallelism for better performance with large datasets
            DBInterface.execute(manager.conn, "PRAGMA verify_parallelism")
        catch e
            @warn "Failed to configure DuckDB memory settings: $e"
        end
    end
    
    # Determine table name and version
    metadata_query = """
    SELECT table_name, schema, arrow_data, version
    FROM crosslink_metadata
    WHERE (id = ? OR name = ?)
    """
    
    if version === nothing
        metadata_query *= " AND current_version = TRUE"
    else
        metadata_query *= " AND version = ?"
    end
    
    # Execute query based on parameters
    metadata = version === nothing ?
        DBInterface.execute(manager.conn, metadata_query, [id, id]) |> DataFrame :
        DBInterface.execute(manager.conn, metadata_query, [id, id, version]) |> DataFrame
        
    if nrow(metadata) == 0
        error("Dataset $(id) (version: $(version === nothing ? "latest" : version)) not found.")
    end
    
    table_name = metadata[1, :table_name]
    schema_json = metadata[1, :schema]
    is_arrow = metadata[1, :arrow_data]
    version_num = metadata[1, :version]
    
    # Parse schema
    schema_dict = JSON.parse(schema_json)
    
    # Debug output for schema information
    if manager.debug
        println("Schema: $schema_dict")
        
        if manager.arrow_available
            println("Arrow data types:")
            for (col, type_str) in schema_dict["dtypes"]
                arrow_type = if type_str == "String"
                    Arrow.VarString
                elseif type_str == "Int64"
                    Int64
                elseif type_str == "Float64"
                    Float64
                else
                    nothing
                end
                println("  $col: $type_str => $arrow_type")
            end
        end
    end
    
    # Determine if we should use chunking based on data size
    use_chunking = false
    if !isnothing(chunk_size)
        use_chunking = chunk_size > 0
    else
        # Auto-detect based on table size
        try
            size_query = "SELECT COUNT(*) AS row_count, SUM(* IS NOT NULL) AS cell_count FROM $table_name"
            size_result = DBInterface.execute(manager.conn, size_query) |> DataFrame
            
            if nrow(size_result) > 0
                row_count = size_result[1, :row_count]
                cell_count = size_result[1, :cell_count]
                
                # Estimate size: if dataset has more than 1 million rows or 10 million cells, use chunking
                use_chunking = row_count > 1_000_000 || cell_count > 10_000_000
                chunk_size = use_chunking ? 500_000 : nothing  # Default to 500k rows if chunking
            end
        catch e
            @warn "Failed to determine dataset size, defaulting to non-chunked: $e"
        end
    end
    
    # Determine return type (Arrow or DataFrame)
    return_as_arrow = if as_arrow === nothing
        # Auto-detect based on schema
        get(schema_dict, "is_arrow", false) && manager.arrow_available
    else
        # Use explicit user preference
        as_arrow && manager.arrow_available
    end
    
    if use_chunking
        if return_as_arrow && manager.arrow_available
            # Chunked reading with Arrow output
            batches = []
            total_rows = 0
            
            try
                # Get total row count for chunking
                count_query = "SELECT COUNT(*) AS count FROM $table_name"
                count_result = DBInterface.execute(manager.conn, count_query) |> DataFrame
                total_rows = count_result[1, :count]
                
                # Process in chunks
                for offset in 0:chunk_size:(total_rows-1)
                    limit = min(chunk_size, total_rows - offset)
                    
                    # Fetch chunk with pagination
                    chunk_query = "SELECT * FROM $table_name LIMIT $limit OFFSET $offset"
                    chunk_df = DBInterface.execute(manager.conn, chunk_query) |> DataFrame
                    
                    # Convert to Arrow batch and store
                    push!(batches, Arrow.Table(chunk_df))
                    
                    # Clean up to free memory
                    chunk_df = nothing
                    GC.gc()
                end
                
                # Combine batches into a single Arrow table
                # This is a simplified approach - for very large datasets, you might want to
                # stream the batches or process them individually
                if length(batches) == 1
                    return batches[1]
                else
                    # Combine Arrow tables - in practice you might want a more efficient approach
                    # for extremely large datasets
                    combined_df = vcat([DataFrame(batch) for batch in batches]...)
                    combined_arrow = Arrow.Table(combined_df)
                    
                    # Clean up intermediate objects
                    batches = nothing
                    combined_df = nothing
                    GC.gc()
                    
                    return combined_arrow
                end
            catch e
                @warn "Error in chunked Arrow reading: $e"
                @warn "Falling back to standard DuckDB query"
                
                # Clear out any partial results to free memory
                batches = nothing
                GC.gc()
                
                # Fall back to standard query
                use_chunking = false
            end
        else
            # Chunked reading with DataFrame output
            result_df = nothing
            total_rows = 0
            
            try
                # Get total row count for chunking
                count_query = "SELECT COUNT(*) AS count FROM $table_name"
                count_result = DBInterface.execute(manager.conn, count_query) |> DataFrame
                total_rows = count_result[1, :count]
                
                # Process in chunks
                for offset in 0:chunk_size:(total_rows-1)
                    limit = min(chunk_size, total_rows - offset)
                    
                    # Fetch chunk with pagination
                    chunk_query = "SELECT * FROM $table_name LIMIT $limit OFFSET $offset"
                    chunk_df = DBInterface.execute(manager.conn, chunk_query) |> DataFrame
                    
                    # Append to result
                    if isnothing(result_df)
                        result_df = chunk_df
                    else
                        append!(result_df, chunk_df)
                    end
                    
                    # Clean up chunk to free memory
                    chunk_df = nothing
                    GC.gc()
                end
                
                return result_df
            catch e
                @warn "Error in chunked DataFrame reading: $e"
                @warn "Falling back to standard DuckDB query"
                
                # Clear any partial results to free memory
                result_df = nothing
                GC.gc()
                
                # Fall back to standard query
                use_chunking = false
            end
        end
    end
    
    # If not chunking or chunking failed, use standard query
    if !use_chunking
        # Regular single-query approach
        query = "SELECT * FROM $table_name"
        
        # Execute query
        result = DBInterface.execute(manager.conn, query)
        
        # Convert to appropriate format
        if return_as_arrow && manager.arrow_available
            # Convert to Arrow Table
            df = result |> DataFrame
            
            # Use the correct method to convert DataFrame to Arrow.Table
            # Arrow.Table constructor directly supports DataFrames
            arrow_table = Arrow.Table(df)
            
            # Free the intermediate DataFrame
            df = nothing
            GC.gc()
            
            return arrow_table
        else
            # Return as DataFrame
            return result |> DataFrame
        end
    end
end

"""
    delete_dataset(manager::CrossLinkManager, identifier::String; version=nothing)

Delete a dataset from the CrossLink registry.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `identifier::String`: Dataset ID or name
- `version=nothing`: Specific version to delete (default: all versions)
"""
function delete_dataset(manager::CrossLinkManager, identifier::String; version=nothing)
    # Get metadata for the dataset
    version_condition = ""
    params = [identifier, identifier]
    
    if version !== nothing
        version_condition = "AND version = ?"
        push!(params, version)
    end
    
    query = """
    SELECT id, table_name, version 
    FROM crosslink_metadata 
    WHERE (id = ? OR name = ?) $version_condition
    """
    
    metadata_results = DBInterface.execute(manager.conn, query, params) |> DataFrame
    
    if nrow(metadata_results) == 0
        error("Dataset with identifier '$identifier' not found")
    end
    
    dataset_id = metadata_results[1, :id]
    
    for i in 1:nrow(metadata_results)
        table_name = metadata_results[i, :table_name]
        ver = metadata_results[i, :version]
        
        # Delete the data table
        DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $table_name")
        
        # Delete schema history
        schema_query = """
        DELETE FROM crosslink_schema_history 
        WHERE id = ? AND version = ?
        """
        
        DBInterface.execute(manager.conn, schema_query, [dataset_id, ver])
        
        # Delete the metadata
        metadata_query = """
        DELETE FROM crosslink_metadata 
        WHERE id = ? AND version = ?
        """
        
        DBInterface.execute(manager.conn, metadata_query, [dataset_id, ver])
    end
    
    # If deleting all versions, also clean up lineage
    if version === nothing
        lineage_query = """
        DELETE FROM crosslink_lineage 
        WHERE dataset_id = ? OR source_dataset_id = ?
        """
        
        DBInterface.execute(manager.conn, lineage_query, [dataset_id, dataset_id])
    end
end

"""
    check_compatibility(manager::CrossLinkManager, source_id::String, target_id::String)

Check if two datasets have compatible schemas.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `source_id::String`: Source dataset ID or name
- `target_id::String`: Target dataset ID or name

# Returns
- `Dict`: Compatibility information
"""
function check_compatibility(manager::CrossLinkManager, source_id::String, target_id::String)
    source_query = """
    SELECT schema 
    FROM crosslink_metadata 
    WHERE (id = ? OR name = ?) AND current_version = TRUE
    """
    
    target_query = """
    SELECT schema 
    FROM crosslink_metadata 
    WHERE (id = ? OR name = ?) AND current_version = TRUE
    """
    
    source_metadata = DBInterface.execute(manager.conn, source_query, [source_id, source_id]) |> DataFrame
    target_metadata = DBInterface.execute(manager.conn, target_query, [target_id, target_id]) |> DataFrame
    
    if nrow(source_metadata) == 0 || nrow(target_metadata) == 0
        missing = String[]
        if nrow(source_metadata) == 0
            push!(missing, "Source dataset '$source_id' not found")
        end
        if nrow(target_metadata) == 0
            push!(missing, "Target dataset '$target_id' not found")
        end
        return Dict("compatible" => false, "reason" => join(missing, "; "))
    end
    
    source_schema = JSON.parse(source_metadata[1, :schema])
    target_schema = JSON.parse(target_metadata[1, :schema])
    
    source_cols = Set(source_schema["columns"])
    target_cols = Set(target_schema["columns"])
    
    missing_cols = setdiff(source_cols, target_cols)
    if !isempty(missing_cols)
        return Dict(
            "compatible" => false,
            "reason" => "Target schema missing columns: $(join(missing_cols, ", "))"
        )
    end
    
    common_cols = intersect(source_cols, target_cols)
    source_dtypes = source_schema["dtypes"]
    target_dtypes = target_schema["dtypes"]
    
    dtype_mismatches = String[]
    for col in common_cols
        source_type = get(source_dtypes, col, "unknown")
        target_type = get(target_dtypes, col, "unknown")
        if source_type != target_type
            push!(dtype_mismatches, "$col: $source_type vs $target_type")
        end
    end
    
    if !isempty(dtype_mismatches)
        return Dict(
            "compatible" => false,
            "reason" => "Type mismatches: $(join(dtype_mismatches, "; "))"
        )
    end
    
    return Dict("compatible" => true)
end

"""
    close(manager::CrossLinkManager)

Close the database connection.
"""
function Base.close(manager::CrossLinkManager)
    DBInterface.close!(manager.conn)
end

"""
    configure_s3(manager::CrossLinkManager; 
                 access_key_id=nothing, 
                 secret_access_key=nothing,
                 session_token=nothing,
                 region=nothing,
                 endpoint=nothing,
                 use_ssl=true,
                 verify=true)

Configure S3 settings for remote storage access.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `access_key_id`: AWS access key ID (can be set via AWS_ACCESS_KEY_ID environment variable)
- `secret_access_key`: AWS secret access key (can be set via AWS_SECRET_ACCESS_KEY environment variable)
- `session_token`: AWS session token for temporary credentials (can be set via AWS_SESSION_TOKEN environment variable)
- `region`: AWS region (can be set via AWS_REGION environment variable)
- `endpoint`: Custom endpoint URL for S3-compatible services
- `use_ssl`: Whether to use SSL for S3 connections
- `verify`: Whether to verify SSL certificates
"""
function configure_s3(manager::CrossLinkManager; 
                     access_key_id=nothing, 
                     secret_access_key=nothing,
                     session_token=nothing,
                     region=nothing,
                     endpoint=nothing,
                     use_ssl=true,
                     verify=true)
    try
        # First, try to load credentials from environment variables if not provided
        if isnothing(access_key_id)
            access_key_id = get(ENV, "AWS_ACCESS_KEY_ID", nothing)
        end
        
        if isnothing(secret_access_key)
            secret_access_key = get(ENV, "AWS_SECRET_ACCESS_KEY", nothing)
        end
        
        if isnothing(session_token)
            session_token = get(ENV, "AWS_SESSION_TOKEN", nothing)
        end
        
        if isnothing(region)
            region = get(ENV, "AWS_REGION", nothing)
        end
        
        # Build the configuration SQL command for DuckDB
        config_commands = String[]
        
        # Handle credentials
        if !isnothing(access_key_id) && !isnothing(secret_access_key)
            push!(config_commands, "SET s3_access_key_id='$(access_key_id)'")
            push!(config_commands, "SET s3_secret_access_key='$(secret_access_key)'")
        end
        
        # Add session token if provided
        if !isnothing(session_token)
            push!(config_commands, "SET s3_session_token='$(session_token)'")
        end
        
        # Add region if provided
        if !isnothing(region)
            push!(config_commands, "SET s3_region='$(region)'")
        end
        
        # Add custom endpoint if provided
        if !isnothing(endpoint)
            push!(config_commands, "SET s3_endpoint='$(endpoint)'")
        end
        
        # Set SSL settings
        push!(config_commands, "SET s3_use_ssl=$(use_ssl ? true : false)")
        push!(config_commands, "SET s3_verify_ssl=$(verify ? true : false)")
        
        # Execute all configuration commands
        for cmd in config_commands
            DBInterface.execute(manager.conn, cmd)
        end
        
        # Store configuration in manager for reference
        manager.s3_configured = true
        manager.storage_configs["s3"] = Dict(
            "access_key_id" => access_key_id,
            "secret_access_key" => secret_access_key !== nothing ? "***" : nothing,  # Don't store actual secret
            "session_token" => session_token !== nothing ? "***" : nothing,  # Don't store actual token
            "region" => region,
            "endpoint" => endpoint,
            "use_ssl" => use_ssl,
            "verify" => verify
        )
        
        return true
    catch e
        @warn "Failed to configure S3: $e"
        return false
    end
end

"""
    configure_storage(manager::CrossLinkManager, storage_type::String; kwargs...)

Configure remote storage options.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `storage_type`: Type of storage ("s3", "azure", "gcs", etc.)
- `kwargs...`: Configuration options specific to the storage type

# Returns
- Boolean indicating success or failure
"""
function configure_storage(manager::CrossLinkManager, storage_type::String; kwargs...)
    if lowercase(storage_type) == "s3"
        return configure_s3(manager; kwargs...)
    elseif lowercase(storage_type) == "azure"
        @warn "Azure storage configuration not yet implemented"
        return false
    elseif lowercase(storage_type) == "gcs"
        @warn "Google Cloud Storage configuration not yet implemented"
        return false
    else
        @warn "Unsupported storage type: $storage_type"
        return false
    end
end

"""
    read_remote(manager::CrossLinkManager, url::String; 
                format="auto", 
                as_arrow=nothing,
                chunk_size=nothing)

Read data directly from a remote location.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `url`: Remote file URL (e.g., "s3://bucket/file.parquet")
- `format`: File format ("auto", "parquet", "csv", "arrow", etc.)
- `as_arrow`: Whether to return as Arrow Table (nothing = auto-detect)
- `chunk_size`: Size of chunks for large files (nothing = auto-detect)

# Returns
- Dataset as DataFrame or Arrow.Table
"""
function read_remote(manager::CrossLinkManager, url::String; 
                    format="auto", 
                    as_arrow=nothing,
                    chunk_size=nothing)
    # Validate URL
    if !startswith(url, "s3://") && !startswith(url, "http://") && !startswith(url, "https://")
        error("Unsupported URL scheme: $url. Must be s3:// or http(s)://")
    end
    
    # Detect format if auto
    detected_format = format
    if format == "auto"
        if endswith(url, ".parquet")
            detected_format = "parquet"
        elseif endswith(url, ".csv") || endswith(url, ".csv.gz")
            detected_format = "csv"
        elseif endswith(url, ".arrow") || endswith(url, ".feather")
            detected_format = "arrow"
        else
            @warn "Could not detect format from URL, defaulting to parquet"
            detected_format = "parquet"
        end
    end
    
    # Configure query based on format
    query = ""
    if detected_format == "parquet"
        query = "SELECT * FROM read_parquet('$url')"
    elseif detected_format == "csv"
        query = "SELECT * FROM read_csv_auto('$url')"
    elseif detected_format == "arrow" || detected_format == "feather"
        query = "SELECT * FROM read_arrow_auto('$url')"
    else
        error("Unsupported format: $detected_format")
    end
    
    # Determine if we should enable chunking
    use_chunking = chunk_size !== nothing && chunk_size > 0
    
    # Determine output format (Arrow or DataFrame)
    return_as_arrow = if as_arrow === nothing
        # Auto-detect based on input format
        detected_format == "arrow" && manager.arrow_available
    else
        # Use explicit user preference
        as_arrow && manager.arrow_available
    end
    
    if use_chunking
        @warn "Chunked reading is currently simplified for remote files"
        
        # For large files, we'll use LIMIT and OFFSET in the query
        # This is a simpler approach than what's in pull_data but still helps with memory usage
        
        try
            # First get the row count (this might be expensive for remote files, but helps with chunking)
            count_query = "SELECT COUNT(*) AS count FROM ($query) t"
            count_result = DBInterface.execute(manager.conn, count_query) |> DataFrame
            total_rows = count_result[1, :count]
            
            if return_as_arrow && manager.arrow_available
                # Chunked reading with Arrow output
                batches = []
                
                # Process in chunks
                for offset in 0:chunk_size:(total_rows-1)
                    limit = min(chunk_size, total_rows - offset)
                    chunk_query = "$query LIMIT $limit OFFSET $offset"
                    
                    chunk_df = DBInterface.execute(manager.conn, chunk_query) |> DataFrame
                    push!(batches, Arrow.Table(chunk_df))
                    
                    # Clean up to free memory
                    chunk_df = nothing
                    GC.gc()
                end
                
                # Return a single Arrow table
                if length(batches) == 1
                    return batches[1]
                else
                    # Combine batches (this could be memory-intensive for very large datasets)
                    combined_df = vcat([DataFrame(batch) for batch in batches]...)
                    arrow_result = Arrow.Table(combined_df)
                    
                    # Clean up intermediate data
                    batches = nothing
                    combined_df = nothing
                    GC.gc()
                    
                    return arrow_result
                end
            else
                # Chunked reading with DataFrame output
                result_df = nothing
                
                # Process in chunks
                for offset in 0:chunk_size:(total_rows-1)
                    limit = min(chunk_size, total_rows - offset)
                    chunk_query = "$query LIMIT $limit OFFSET $offset"
                    
                    chunk_df = DBInterface.execute(manager.conn, chunk_query) |> DataFrame
                    
                    # Append to result
                    if isnothing(result_df)
                        result_df = chunk_df
                    else
                        append!(result_df, chunk_df)
                    end
                    
                    # Clean up to free memory
                    chunk_df = nothing
                    GC.gc()
                end
                
                return result_df
            end
        catch e
            @warn "Error in chunked reading from remote file: $e"
            @warn "Falling back to standard query"
            
            # Fall back to non-chunked approach
            use_chunking = false
        end
    end
    
    # Standard non-chunked approach
    if !use_chunking
        result = DBInterface.execute(manager.conn, query)
        
        if return_as_arrow && manager.arrow_available
            df = result |> DataFrame
            
            # Use the correct method to convert DataFrame to Arrow.Table
            # Arrow.Table constructor directly supports DataFrames
            arrow_table = Arrow.Table(df)
            
            # Free the intermediate DataFrame
            df = nothing
            GC.gc()
            
            return arrow_table
        else
            # Return as DataFrame
            return result |> DataFrame
        end
    end
end

"""
    export_to_remote(manager::CrossLinkManager, df::DataFrame, url::String;
                    format="parquet",
                    compression="snappy",
                    chunk_size=nothing)

Export a DataFrame directly to a remote location.

# Arguments
- `manager::CrossLinkManager`: The CrossLink manager
- `df`: DataFrame to export
- `url`: Remote file URL (e.g., "s3://bucket/file.parquet")
- `format`: File format ("parquet", "csv", "arrow")
- `compression`: Compression format ("snappy", "gzip", "zstd")
- `chunk_size`: Size of chunks for large files (nothing = auto-detect)

# Returns
- Boolean indicating success or failure
"""
function export_to_remote(manager::CrossLinkManager, df::DataFrame, url::String;
                         format="parquet",
                         compression="snappy",
                         chunk_size=nothing)
    # Validate URL
    if !startswith(url, "s3://")
        error("Unsupported URL scheme: $url. Only s3:// is currently supported for export")
    end
    
    # Determine if we should use chunking based on data size
    use_chunking = false
    if isnothing(chunk_size)
        # Auto-detect: similar logic to push_data
        estimated_size_bytes = 0
        
        # Simple size estimation
        for col in names(df)
            element_type = eltype(df[!, col])
            if element_type <: Number
                estimated_size_bytes += length(df[!, col]) * sizeof(element_type)
            elseif element_type <: AbstractString
                for str in df[!, col]
                    estimated_size_bytes += sizeof(str)
                end
            else
                # Conservative estimate for other types
                estimated_size_bytes += length(df[!, col]) * 64
            end
        end
        
        # Use chunking if dataset is larger than 1GB
        use_chunking = estimated_size_bytes > 1_000_000_000
        chunk_size = use_chunking ? 500_000 : nothing
    else
        use_chunking = chunk_size > 0
    end
    
    # Register the DataFrame with DuckDB
    temp_table_name = "temp_export_$(rand(1000:9999))"
    
    try
        if use_chunking
            # Create a temporary table
            sample_df = first(df, 1)
            DuckDB.register_data_frame(manager.conn, sample_df, "temp_schema")
            DBInterface.execute(manager.conn, "CREATE TABLE $temp_table_name AS SELECT * FROM temp_schema WHERE 1=0")
            DBInterface.execute(manager.conn, "DROP VIEW temp_schema")
            
            # Insert data in chunks
            total_rows = nrow(df)
            for i in 1:chunk_size:total_rows
                end_idx = min(i + chunk_size - 1, total_rows)
                chunk = df[i:end_idx, :]
                
                DuckDB.register_data_frame(manager.conn, chunk, "temp_chunk")
                DBInterface.execute(manager.conn, "INSERT INTO $temp_table_name SELECT * FROM temp_chunk")
                DBInterface.execute(manager.conn, "DROP VIEW temp_chunk")
                
                # Clean up to free memory
                chunk = nothing
                GC.gc()
            end
        else
            # For smaller datasets, register the entire DataFrame
            DuckDB.register_data_frame(manager.conn, df, temp_table_name)
        end
        
        # Configure export SQL based on format
        copy_sql = ""
        if format == "parquet"
            copy_sql = "COPY $temp_table_name TO '$url' (FORMAT PARQUET, COMPRESSION '$compression')"
        elseif format == "csv"
            copy_sql = "COPY $temp_table_name TO '$url' (FORMAT CSV, HEADER, COMPRESSION '$compression')"
        elseif format == "arrow" || format == "feather"
            copy_sql = "COPY $temp_table_name TO '$url' (FORMAT ARROW, COMPRESSION '$compression')"
        else
            error("Unsupported format: $format")
        end
        
        # Execute the COPY command
        DBInterface.execute(manager.conn, copy_sql)
        
        # Clean up
        if use_chunking
            DBInterface.execute(manager.conn, "DROP TABLE $temp_table_name")
        else
            DBInterface.execute(manager.conn, "DROP VIEW $temp_table_name")
        end
        
        return true
    catch e
        @warn "Failed to export to remote: $e"
        
        # Clean up on error
        try
            if use_chunking
                DBInterface.execute(manager.conn, "DROP TABLE IF EXISTS $temp_table_name")
            else
                DBInterface.execute(manager.conn, "DROP VIEW IF EXISTS $temp_table_name")
            end
        catch
            # Ignore cleanup errors
        end
        
        return false
    end
end

end # module 