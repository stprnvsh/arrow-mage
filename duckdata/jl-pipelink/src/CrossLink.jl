module CrossLink

using DuckDB
using DataFrames
using JSON
using Dates
using UUIDs
import SHA: sha1
# Add Arrow.jl for zero-copy optimization
using Arrow

export CrossLinkManager, push_data, pull_data, get_table_reference, register_external_table, list_datasets, query, close

# Store instances for connection pooling
const _INSTANCES = Dict{String, Any}()
# Cache for memory-mapped Arrow tables
const _MMAPPED_TABLES = Dict{String, Any}()

"""
    CrossLinkManager

Manager for cross-language data sharing with zero-copy optimization.
"""
mutable struct CrossLinkManager
    conn::DuckDB.DB
    db_path::String
    debug::Bool
    metadata_cache::Dict{String, Any}
    tables_initialized::Bool
    arrow_available::Bool
    
    """
        CrossLinkManager(db_path::String, debug::Bool=false)
    
    Create a CrossLink manager with connection to specified DuckDB database.
    """
    function CrossLinkManager(db_path::String, debug::Bool=false)
        # Check for cached instance
        abs_path = abspath(db_path)
        if haskey(_INSTANCES, abs_path)
            return _INSTANCES[abs_path]
        end
        
        # Create directory if it doesn't exist
        db_dir = dirname(db_path)
        if !isempty(db_dir) && !isdir(db_dir)
            mkpath(db_dir)
        end
        
        # Connect to database
        conn = DuckDB.DB(db_path)
        
        # Check if Arrow is available
        arrow_available = true # Arrow.jl is now a dependency
        
        # Create instance 
        instance = new(conn, db_path, debug, Dict{String, Any}(), false, arrow_available)
        
        # Cache for reuse
        _INSTANCES[abs_path] = instance
        
        return instance
    end
end

"""
    setup_metadata_tables(conn::DuckDB.DB)

Set up metadata tables for cross-language data sharing.
"""
function setup_metadata_tables(cl::CrossLinkManager)
    # Skip if already initialized
    if cl.tables_initialized
        return
    end
    
    # Main metadata table
    try
        DuckDB.execute(cl.conn, """
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
        DuckDB.execute(cl.conn, """
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
        
        # Configure DuckDB for performance
        try
            # Set a reasonable memory limit
            DuckDB.execute(cl.conn, "PRAGMA memory_limit='4GB'")
            
            # Set threads to a reasonable number
            num_cores = max(1, Sys.CPU_THREADS - 1)  # Leave 1 core for other processes
            DuckDB.execute(cl.conn, "PRAGMA threads=$num_cores")
            
            # Enable object cache for better performance with repeated queries
            DuckDB.execute(cl.conn, "PRAGMA enable_object_cache")
            
            # Additional performance optimizations
            DuckDB.execute(cl.conn, "PRAGMA preserve_insertion_order=false")  # Allows reordering for memory efficiency
            DuckDB.execute(cl.conn, "PRAGMA temp_directory=':memory:'")       # Use memory for temp files when possible
            DuckDB.execute(cl.conn, "PRAGMA checkpoint_threshold='4GB'")      # Less frequent checkpoints
            DuckDB.execute(cl.conn, "PRAGMA force_parallelism")               # Enable parallelism
            DuckDB.execute(cl.conn, "PRAGMA cache_size=2048")                 # Larger cache (2GB)
        catch e
            cl.debug && @warn "Failed to configure DuckDB performance settings: $e"
        end
        
        cl.tables_initialized = true
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
    # Skip logging in fast mode
    if !cl.debug
        return true
    end
    
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
    # Ensure tables are initialized
    setup_metadata_tables(cl)
    
    # Check cache first
    if haskey(cl.metadata_cache, identifier)
        return cl.metadata_cache[identifier]
    end
    
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
        
        # Store in cache
        cl.metadata_cache[identifier] = metadata
        if identifier != metadata["id"]
            cl.metadata_cache[metadata["id"]] = metadata
        end
        if identifier != metadata["name"]
            cl.metadata_cache[metadata["name"]] = metadata
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
            description::Union{String, Nothing}=nothing, enable_zero_copy::Bool=true,
            memory_mapped::Bool=true)

Push DataFrame to the database with metadata for cross-language data sharing.
"""
function push_data(cl::CrossLinkManager, df::DataFrame, name::Union{String, Nothing}=nothing;
                  description::Union{String, Nothing}=nothing, enable_zero_copy::Bool=true,
                  memory_mapped::Bool=true)
    # Ensure tables are initialized
    setup_metadata_tables(cl)
    
    # Generate ID and name if needed
    dataset_id = string(uuid4())
    if name === nothing
        name = "dataset_$(Int(datetime2unix(now())))"
    end
    
    # Create table name
    table_name = "data_$(replace(replace(lowercase(name), " " => "_"), "-" => "_"))"
    
    # Setup memory-mapped file if requested
    memory_map_path = nothing
    arrow_schema = nothing
    arrow_table = nothing
    
    if enable_zero_copy && cl.arrow_available && memory_mapped
        try
            # Convert DataFrame to Arrow Table
            arrow_table = Arrow.Table(df)
            
            # Create directory for memory-mapped files if it doesn't exist
            mmaps_dir = joinpath(dirname(cl.db_path), "crosslink_mmaps")
            isdir(mmaps_dir) || mkdir(mmaps_dir)
            
            # Create a unique file path for this dataset
            memory_map_path = joinpath(mmaps_dir, "$(dataset_id).arrow")
            
            # Write the Arrow table to the memory-mapped file
            Arrow.write(memory_map_path, arrow_table)
            
            # Serialize Arrow schema
            arrow_schema = Dict(
                "schema" => string(arrow_table.schema),
                "metadata" => Dict{String, String}() # Arrow.jl schema metadata handling would go here
            )
            
            if cl.debug
                @info "Wrote Arrow table to memory-mapped file: $memory_map_path"
            end
        catch e
            cl.debug && @warn "Failed to create memory-mapped file: $e"
            memory_map_path = nothing
        end
    end
    
    # Create table from DataFrame
    try
        # Try multiple creation methods with fallbacks
        table_created = false
        
        # Method 1: Arrow Table method if available
        if !table_created && cl.arrow_available && arrow_table !== nothing
            try
                # Use optimized Arrow method
                # Note: This requires DuckDB Arrow integration in Julia which might not be fully supported yet
                # Register arrow table with DuckDB
                # This is a placeholder for when DuckDB Julia has better Arrow integration
                DuckDB.execute(cl.conn, """
                CREATE OR REPLACE TABLE $(table_name) AS SELECT * FROM df
                """, Dict("df" => df))
                table_created = true
                cl.debug && @info "Created table using Arrow method"
            catch e
                cl.debug && @warn "Arrow method failed: $e"
            end
        end
        
        # Method 2: Use optimized direct method
        if !table_created
            try
                # Use optimized direct method with DataFrame
                DuckDB.execute(cl.conn, "CREATE OR REPLACE TABLE $(table_name) AS SELECT * FROM df", Dict("df" => df))
                table_created = true
                cl.debug && @info "Created table using direct DataFrame method"
            catch e
                cl.debug && @warn "Direct DataFrame method failed: $e"
            end
        end
        
        # Method 3: Manual batch insert if other methods fail
        if !table_created
            # First create an empty table with the right schema
            cols_with_types = []
            for col_name in names(df)
                # Map Julia types to SQL types
                col_type = eltype(df[!, col_name])
                sql_type = if col_type <: Integer
                    "BIGINT"
                elseif col_type <: AbstractFloat
                    "DOUBLE"
                elseif col_type <: AbstractString
                    "VARCHAR"
                elseif col_type <: Bool
                    "BOOLEAN"
                elseif col_type <: TimeType
                    "TIMESTAMP"
                else
                    "VARCHAR" # Default to VARCHAR for other types
                end
                push!(cols_with_types, "\"$(col_name)\" $(sql_type)")
            end
            
            # Create the table
            schema_sql = join(cols_with_types, ", ")
            DuckDB.execute(cl.conn, "CREATE OR REPLACE TABLE $(table_name) ($(schema_sql))")
            
            # Insert data in batches with optimized batch size
            batch_size = min(100000, size(df, 1))  # Larger batch size for better performance
            num_rows = size(df, 1)
            
            if num_rows > 0
                # Prepare a single SQL statement with multiple values for bulk insert
                placeholders = join(fill("?", length(names(df))), ", ")
                
                # Process in batches
                for start_idx in 1:batch_size:num_rows
                    end_idx = min(start_idx + batch_size - 1, num_rows)
                    batch_rows = start_idx:end_idx
                    
                    # For small dataframes, do a single batch insert
                    if length(batch_rows) == num_rows
                        # Create multiple value groups (val1, val2, val3), (val4, val5, val6), etc.
                        values_groups = []
                        for row in eachrow(df)
                            values = [row[col] for col in names(df)]
                            push!(values_groups, values)
                        end
                        
                        # Flatten the array of arrays for the query
                        flattened_values = vcat(values_groups...)
                        
                        # Create value placeholders for each row
                        value_groups = []
                        for _ in 1:length(batch_rows)
                            push!(value_groups, "($placeholders)")
                        end
                        all_placeholders = join(value_groups, ", ")
                        
                        # Execute the batch insert
                        DuckDB.execute(cl.conn, 
                            "INSERT INTO $(table_name) VALUES $all_placeholders", 
                            flattened_values)
                    else
                        # For large dataframes, use batched execution
                        # Create multiple value groups for this batch
                        value_groups = []
                        for _ in 1:length(batch_rows)
                            push!(value_groups, "($placeholders)")
                        end
                        batch_placeholders = join(value_groups, ", ")
                        
                        # Collect batch values
                        values_batch = []
                        for i in batch_rows
                            row = df[i, :]
                            for col in names(df)
                                push!(values_batch, row[col])
                            end
                        end
                        
                        # Execute the batch insert
                        DuckDB.execute(cl.conn, 
                            "INSERT INTO $(table_name) VALUES $batch_placeholders", 
                            values_batch)
                    end
                end
            end
            
            table_created = true
            cl.debug && @info "Created table using batch insert method"
        end
        
        # Create schema
        schema_dict = Dict(
            "columns" => names(df),
            "dtypes" => Dict(string(col) => string(eltype(df[!, col])) for col in names(df))
        )
        
        # Calculate schema hash
        schema_hash = bytes2hex(sha1(JSON.json(schema_dict)))
        
        # Store metadata with memory map path
        DuckDB.execute(cl.conn, """
        INSERT INTO crosslink_metadata (
            id, name, source_language, created_at, updated_at, description,
            schema, table_name, arrow_data, version, current_version, schema_hash,
            access_languages, memory_map_path, arrow_schema
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            dataset_id, 
            name, 
            "julia", 
            now(), 
            now(),
            description === nothing ? "" : description,
            JSON.json(schema_dict),
            table_name,
            cl.arrow_available,
            1,
            true,
            schema_hash,
            JSON.json(["python", "r", "julia", "cpp"]),
            memory_map_path,
            arrow_schema === nothing ? nothing : JSON.json(arrow_schema)
        ])
        
        # Store in cache
        metadata = Dict{String, Any}(
            "id" => dataset_id,
            "name" => name,
            "table_name" => table_name,
            "schema" => schema_dict,
            "source_language" => "julia",
            "arrow_data" => cl.arrow_available,
            "memory_map_path" => memory_map_path,
            "arrow_schema" => arrow_schema
        )
        cl.metadata_cache[dataset_id] = metadata
        cl.metadata_cache[name] = metadata
        
        # Log access
        cl.debug && log_access(cl, dataset_id, "write", enable_zero_copy ? "zero-copy" : "copy")
        
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
    # Ensure tables are initialized
    setup_metadata_tables(cl)
    
    # Get metadata from cache first
    metadata = get(cl.metadata_cache, identifier, nothing)
    
    # If not in cache, get from database
    if metadata === nothing
        metadata = get_dataset_metadata(cl, identifier)
    end
    
    if metadata === nothing
        error("Dataset not found: $identifier")
    end
    
    # First try memory-mapped Arrow path if available
    memory_map_path = get(metadata, "memory_map_path", nothing)
    
    if zero_copy && cl.arrow_available && memory_map_path !== nothing && isfile(memory_map_path)
        try
            # Check if we already have this table in our cache
            if haskey(_MMAPPED_TABLES, memory_map_path)
                arrow_table = _MMAPPED_TABLES[memory_map_path]
            else
                # Load directly from memory-mapped file
                arrow_table = Arrow.Table(memory_map_path)
                
                # Cache the table for future use
                _MMAPPED_TABLES[memory_map_path] = arrow_table
            end
            
            # Convert to DataFrame - Arrow.jl's Table to DataFrame conversion 
            # is optimized for minimal copying
            result = DataFrame(arrow_table)
            
            # Log the zero-copy access
            cl.debug && log_access(cl, metadata["id"], "read", "zero-copy-mmap")
            
            return result
        catch e
            cl.debug && @warn "Memory-mapped file read failed, falling back: $e"
            # Fall through to standard methods
        end
    end
    
    # If memory-mapped approach failed, try database approach
    table_name = metadata["table_name"]
    
    # Try to use Arrow for zero-copy from DuckDB
    if zero_copy && cl.arrow_available
        try
            # Query directly to Arrow Table if possible
            # This is a placeholder for when DuckDB Julia has better Arrow integration
            # For now, we fall back to standard DataFrame query
            result = DataFrame(DuckDB.execute(cl.conn, "SELECT * FROM $table_name"))
            
            # Log access
            cl.debug && log_access(cl, metadata["id"], "read", "copy")
            
            return result
        catch e
            cl.debug && @warn "Arrow query failed, falling back: $e"
        end
    end
    
    # Standard SQL approach
    result = DataFrame(DuckDB.execute(cl.conn, "SELECT * FROM $table_name"))
    
    # Log access
    cl.debug && log_access(cl, metadata["id"], "read", "copy")
    
    return result
end

"""
    get_table_reference(cl::CrossLinkManager, identifier::String)

Get direct reference to a table without copying data.
"""
function get_table_reference(cl::CrossLinkManager, identifier::String)
    # Ensure tables are initialized
    setup_metadata_tables(cl)
    
    # Get metadata
    metadata = get_dataset_metadata(cl, identifier)
    if metadata === nothing
        error("Dataset not found: $identifier")
    end
    
    # Log access
    cl.debug && log_access(cl, metadata["id"], "reference", "zero-copy")
    
    # Return reference info
    return Dict{String, Any}(
        "database_path" => abspath(cl.db_path),
        "table_name" => metadata["table_name"],
        "schema" => metadata["schema"],
        "dataset_id" => metadata["id"],
        "memory_map_path" => get(metadata, "memory_map_path", nothing),
        "arrow_schema" => get(metadata, "arrow_schema", nothing),
        "access_method" => "direct_table_reference"
    )
end

"""
    register_external_table(cl::CrossLinkManager, external_db_path::String, external_table_name::String;
                           name::Union{String, Nothing}=nothing, description::Union{String, Nothing}=nothing)

Register an external table from another DuckDB database without copying data.

This enables true zero-copy access across different database files.
"""
function register_external_table(cl::CrossLinkManager, external_db_path::String, external_table_name::String;
                                name::Union{String, Nothing}=nothing, description::Union{String, Nothing}=nothing)
    # Generate dataset ID and name if not provided
    dataset_id = string(uuid4())
    if name === nothing
        name = "ext_$(basename(external_db_path))_$(external_table_name)"
    end
    
    # Use absolute path for external database
    external_db_path = abspath(external_db_path)
    attach_name = "ext_$(replace(dataset_id, "-" => "_"))"
    
    try
        # Attach the external database
        DuckDB.execute(cl.conn, "ATTACH DATABASE '$(external_db_path)' AS $(attach_name)")
        
        # Get schema information from the external table
        schema_df = DataFrame(DuckDB.execute(cl.conn, "DESCRIBE $(attach_name).$(external_table_name)"))
        
        # Create a schema dictionary
        schema_dict = Dict(
            "columns" => schema_df.column_name,
            "dtypes" => Dict(row.column_name => row.column_type for row in eachrow(schema_df))
        )
        
        # Create a view to the external table
        view_name = "ext_view_$(replace(dataset_id, "-" => "_"))"
        DuckDB.execute(cl.conn, "CREATE VIEW $(view_name) AS SELECT * FROM $(attach_name).$(external_table_name)")
        
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
            "external", 
            now(), 
            now(),
            description === nothing ? "External table reference to $(external_db_path):$(external_table_name)" : description,
            JSON.json(schema_dict),
            view_name,
            false,
            1,
            true,
            schema_hash,
            JSON.json(["python", "r", "julia", "cpp"])
        ])
        
        # Log access
        log_access(cl, dataset_id, "reference", "external_table")
        
        return dataset_id
    catch e
        @error "Failed to register external table: $e"
        # Try to detach the database if it was attached
        try
            DuckDB.execute(cl.conn, "DETACH DATABASE $(attach_name)")
        catch
            # Ignore errors during cleanup
        end
        rethrow(e)
    end
end

"""
    list_datasets(cl::CrossLinkManager)

List all available datasets in the CrossLink registry.
"""
function list_datasets(cl::CrossLinkManager)
    # Ensure tables are initialized
    setup_metadata_tables(cl)
    
    try
        result = DataFrame(DuckDB.execute(cl.conn, """
        SELECT id, name, source_language, created_at, table_name, description, version 
        FROM crosslink_metadata
        WHERE current_version = TRUE
        ORDER BY updated_at DESC
        """))
        
        return result
    catch e
        cl.debug && @error "Failed to list datasets: $e"
        return DataFrame()
    end
end

"""
    query(cl::CrossLinkManager, sql::String; use_arrow::Bool=true)

Execute a SQL query on the CrossLink database.
"""
function query(cl::CrossLinkManager, sql::String; use_arrow::Bool=true)
    try
        # Check if prepare method exists, otherwise use standard execute
        result = if hasproperty(DuckDB, :prepare)
            # Use prepared statement for better performance
            stmt = DuckDB.prepare(cl.conn, sql)
            DataFrame(DuckDB.execute(stmt))
        else
            # Fallback to standard execute if prepare not available
            DataFrame(DuckDB.execute(cl.conn, sql))
        end
        return result
    catch e
        cl.debug && @error "Failed to execute query: $e"
        rethrow(e)
    end
end

"""
    close(cl::CrossLinkManager)

Close database connection.
"""
function close(cl::CrossLinkManager)
    # Remove from instance cache
    db_path = cl.db_path
    abs_path = abspath(db_path)
    if haskey(_INSTANCES, abs_path)
        delete!(_INSTANCES, abs_path)
    end
    
    # Clear mmapped table cache
    empty!(_MMAPPED_TABLES)
    
    # Clear metadata cache
    empty!(cl.metadata_cache)
    
    # Close connection
    cl.conn = nothing
end

end # module 