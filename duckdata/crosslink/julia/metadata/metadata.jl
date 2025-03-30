"""
    setup_metadata_tables(cl::CrossLinkManager)

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
    create_dataset_metadata(cl::CrossLinkManager, dataset_id::String, name::String, 
                          source_language::String, table_name::String, schema::Dict, 
                          description::String="", arrow_data::Bool=false,
                          memory_map_path::Union{String, Nothing}=nothing,
                          shared_memory_key::Union{String, Nothing}=nothing,
                          arrow_schema::Union{Dict, Nothing}=nothing,
                          access_languages::Vector{String}=["python", "r", "julia", "cpp"])

Create metadata for a dataset in the CrossLink database.
"""
function create_dataset_metadata(cl::CrossLinkManager, dataset_id::String, name::String, 
                               source_language::String, table_name::String, schema::Dict, 
                               description::String="", arrow_data::Bool=false,
                               memory_map_path::Union{String, Nothing}=nothing,
                               shared_memory_key::Union{String, Nothing}=nothing,
                               arrow_schema::Union{Dict, Nothing}=nothing,
                               access_languages::Vector{String}=["python", "r", "julia", "cpp"])
    # Ensure tables exist                          
    setup_metadata_tables(cl)
    
    # Calculate schema hash
    schema_json = JSON.json(schema)
    schema_hash = bytes2hex(sha1(schema_json))
    
    # Prepare access languages as JSON
    access_languages_json = JSON.json(access_languages)
    
    # Format Arrow schema as JSON if present
    arrow_schema_json = arrow_schema !== nothing ? JSON.json(arrow_schema) : nothing
    
    # Current timestamp
    now_time = Dates.now()
    
    # Create metadata entry
    DuckDB.execute(cl.conn, """
    INSERT INTO crosslink_metadata (
        id, name, source_language, created_at, updated_at, description,
        schema, table_name, arrow_data, version, current_version, schema_hash,
        memory_map_path, shared_memory_key, arrow_schema, access_languages
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        dataset_id, name, source_language, now_time, now_time, description,
        schema_json, table_name, arrow_data, 1, true, schema_hash,
        memory_map_path, shared_memory_key, arrow_schema_json, access_languages_json
    ])
    
    return dataset_id
end 