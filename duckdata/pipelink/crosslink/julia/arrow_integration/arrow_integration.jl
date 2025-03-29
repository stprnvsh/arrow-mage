# Cache for memory-mapped Arrow tables
const _MMAPPED_TABLES = Dict{String, Any}()

"""
    create_duckdb_view_from_arrow(cl::CrossLinkManager, table, view_name::Union{String, Nothing}=nothing)

Create a DuckDB view from an Arrow table.
"""
function create_duckdb_view_from_arrow(cl::CrossLinkManager, table, view_name::Union{String, Nothing}=nothing)
    # Generate view name if not provided
    if view_name === nothing
        view_id = replace(string(uuid4()), "-" => "_")
        view_name = "arrow_view_$(view_id)"
    end
    
    # Try to create a view to query this arrow table
    
    # Method 1: Try direct registration with DuckDB arrow scanner
    success = false
    try
        # Create a temporary arrow file
        temp_file = tempname() * ".arrow"
        Arrow.write(temp_file, table)
        
        # Add to cleanup list
        push!(cl.arrow_files, temp_file)
        
        # Try using parquet reader (most compatible)
        try
            DuckDB.execute(cl.conn, """
            CREATE OR REPLACE VIEW $(view_name) AS 
            SELECT * FROM read_parquet('$(temp_file)')
            """)
            success = true
            cl.debug && @info "Created view from arrow file using parquet reader: $view_name"
        catch e1
            # Try arrow reader
            try
                DuckDB.execute(cl.conn, """
                CREATE OR REPLACE VIEW $(view_name) AS 
                SELECT * FROM read_arrow('$(temp_file)')
                """)
                success = true
                cl.debug && @info "Created view from arrow file using arrow reader: $view_name"
            catch e2
                cl.debug && @warn "Both parquet and arrow reader failed: $e1, $e2"
                # Fall through to next method
            end
        end
    catch e
        cl.debug && @warn "Failed to create view via arrow file: $e"
        # Fall through to next method
    end
    
    # Method 2: Convert to DataFrame and create view
    if !success
        try
            # Convert Arrow table to DataFrame
            df = DataFrame(table)
            
            # Try different methods to create the table
            try
                # Method 2.1: Direct DataFrame parameter
                DuckDB.execute(cl.conn, """
                CREATE OR REPLACE TABLE $(view_name) AS
                SELECT * FROM ?
                """, [df])
                success = true
                cl.debug && @info "Created table from DataFrame (direct parameter): $view_name"
            catch e1
                try
                    # Method 2.2: Named parameter
                    DuckDB.execute(cl.conn, """
                    CREATE OR REPLACE TABLE $(view_name) AS
                    SELECT * FROM df
                    """, Dict("df" => df))
                    success = true
                    cl.debug && @info "Created table from DataFrame (named parameter): $view_name"
                catch e2
                    try
                        # Method 2.3: CSV intermediary
                        temp_csv = tempname() * ".csv"
                        CSV.write(temp_csv, df)
                        DuckDB.execute(cl.conn, """
                        CREATE OR REPLACE TABLE $(view_name) AS
                        SELECT * FROM read_csv_auto('$(temp_csv)')
                        """)
                        isfile(temp_csv) && rm(temp_csv)
                        success = true
                        cl.debug && @info "Created table from DataFrame via CSV: $view_name"
                    catch e3
                        cl.debug && @warn "All DataFrame methods failed: $e1, $e2, $e3"
                    end
                end
            end
        catch e
            cl.debug && @warn "Failed to create view via DataFrame: $e"
            # No more methods to try
        end
    end
    
    # Method 3: Manual table creation with schema inference
    if !success
        try
            # Convert to DataFrame
            df = DataFrame(table)
            
            # Get column info
            cols = names(df)
            
            # Create table schema manually
            col_defs = []
            for col in cols
                col_type = eltype(df[!, col])
                sql_type = if col_type <: Integer
                    "INTEGER"
                elseif col_type <: AbstractFloat
                    "DOUBLE"
                elseif col_type <: AbstractString
                    "VARCHAR"
                elseif col_type <: Bool
                    "BOOLEAN"
                elseif col_type <: TimeType
                    "TIMESTAMP"
                else
                    "VARCHAR"  # Default to string for other types
                end
                push!(col_defs, "\"$(col)\" $(sql_type)")
            end
            
            # Create table
            create_sql = "CREATE OR REPLACE TABLE $(view_name) ($(join(col_defs, ", ")))"
            DuckDB.execute(cl.conn, create_sql)
            
            # Insert data in batches
            batch_size = 100
            for i in 1:batch_size:nrow(df)
                end_idx = min(i + batch_size - 1, nrow(df))
                # Create CSV for this batch
                batch_csv = tempname() * ".csv"
                CSV.write(batch_csv, df[i:end_idx, :])
                
                # Insert from CSV
                DuckDB.execute(cl.conn, """
                INSERT INTO $(view_name)
                SELECT * FROM read_csv_auto('$(batch_csv)')
                """)
                
                # Clean up
                isfile(batch_csv) && rm(batch_csv)
            end
            
            success = true
            cl.debug && @info "Created table using manual schema definition: $view_name"
        catch e
            cl.debug && @warn "Manual table creation failed: $e"
        end
    end
    
    if !success
        error("Failed to create DuckDB view from Arrow table")
    end
    
    return view_name
end

"""
    share_arrow_table(cl::CrossLinkManager, table; 
                     name::Union{String, Nothing}=nothing,
                     description::Union{String, Nothing}=nothing,
                     shared_memory::Bool=true, 
                     memory_mapped::Bool=true,
                     register_in_duckdb::Bool=true,
                     access_languages::Vector{String}=["python", "r", "julia", "cpp"])

Share an Arrow table directly without converting to a DataFrame.

This is the most efficient way to share data between languages as it avoids unnecessary conversions.
"""
function share_arrow_table(cl::CrossLinkManager, table;
                          name::Union{String, Nothing}=nothing,
                          description::Union{String, Nothing}=nothing,
                          shared_memory::Bool=true, 
                          memory_mapped::Bool=true,
                          register_in_duckdb::Bool=true,
                          access_languages::Vector{String}=["python", "r", "julia", "cpp"])
    # If C++ implementation is available, try using it first
    if cl.cpp_available && cl.cpp_instance !== nothing
        try
            # Use C++ implementation via the wrapper
            dataset_id = CrossLinkCppWrapper.push(
                cl.cpp_instance,
                table,
                name === nothing ? "" : name,
                description === nothing ? "" : description
            )
            
            cl.debug && println("Shared Arrow table using C++ implementation")
            
            return dataset_id
        catch e
            cl.debug && println("C++ share_arrow_table failed, falling back to Julia implementation: $e")
            # Continue with regular Julia implementation
        end
    end
    
    # Ensure the metadata tables are initialized
    setup_metadata_tables(cl)
    
    # Generate ID and name
    dataset_id = string(uuid4())
    if name === nothing
        name = "arrow_direct_$(dataset_id[1:8])"
    end
    
    # Initialize storage methods
    memory_map_path = nothing
    shared_memory_info = nothing
    table_name = nothing
    
    # Set up memory-mapped file if requested
    if memory_mapped
        try
            # Create directory for memory-mapped files
            mmaps_dir = joinpath(dirname(cl.db_path), "crosslink_mmaps")
            if !isdir(mmaps_dir)
                mkdir(mmaps_dir)
            end
            
            # Create file path
            memory_map_path = joinpath(mmaps_dir, "$(dataset_id).arrow")
            
            # Write Arrow table to file - use compression to reduce file size
            Arrow.write(memory_map_path, table, compress=:zstd)
            
            # Track for cleanup
            push!(cl.arrow_files, memory_map_path)
            
            cl.debug && @info "Wrote Arrow table to $(memory_map_path)"
        catch e
            cl.debug && @warn "Failed to create memory-mapped file: $e"
            memory_map_path = nothing
        end
    end
    
    # Set up shared memory if requested
    if shared_memory
        shared_memory_info = setup_shared_memory(cl, table, dataset_id)
    end
    
    # Register in DuckDB if requested
    if register_in_duckdb
        try
            # Create a table name
            table_name = "arrow_$(replace(dataset_id, "-" => "_"))"
            
            # Try to create a view
            if memory_map_path !== nothing
                try
                    # First try with parquet reader (more compatible)
                    DuckDB.execute(cl.conn, """
                    CREATE OR REPLACE VIEW $(table_name) AS 
                    SELECT * FROM read_parquet('$(memory_map_path)')
                    """)
                    cl.debug && @info "Created DuckDB view using parquet reader: $(table_name)"
                catch e1
                    try
                        # Try with arrow reader
                        DuckDB.execute(cl.conn, """
                        CREATE OR REPLACE VIEW $(table_name) AS 
                        SELECT * FROM read_arrow('$(memory_map_path)')
                        """)
                        cl.debug && @info "Created DuckDB view using arrow reader: $(table_name)"
                    catch e2
                        # Fall back to create_duckdb_view_from_arrow
                        cl.debug && @warn "Direct view creation failed: $e1, $e2. Falling back to helper function."
                        table_name = create_duckdb_view_from_arrow(cl, table, table_name)
                    end
                end
            else
                # Use helper function
                table_name = create_duckdb_view_from_arrow(cl, table, table_name)
            end
        catch e
            cl.debug && @warn "Failed to register in DuckDB: $e"
            table_name = nothing
        end
    end
    
    # Create schema dictionary
    schema_dict = Dict(
        "columns" => [field.name for field in table.schema],
        "dtypes" => Dict(field.name => string(field.type) for field in table.schema)
    )
    
    # Serialize Arrow schema information
    arrow_schema = Dict(
        "schema" => string(table.schema),
        "serialized" => string(table.schema),  # Not ideal but will use string representation
        "metadata" => Dict{String, String}()  # Add metadata if available
    )
    
    # Calculate schema hash
    schema_hash = bytes2hex(sha1(JSON.json(schema_dict)))
    
    # Store metadata
    DuckDB.execute(cl.conn, """
    INSERT INTO crosslink_metadata (
        id, name, source_language, created_at, updated_at, description,
        schema, table_name, arrow_data, version, current_version, schema_hash,
        access_languages, memory_map_path, shared_memory_key, arrow_schema
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        dataset_id, 
        name, 
        "julia", 
        now(), 
        now(),
        description === nothing ? "Direct Arrow table" : description,
        JSON.json(schema_dict),
        table_name,
        true,
        1,
        true,
        schema_hash,
        JSON.json(access_languages),
        memory_map_path,
        shared_memory_info === nothing ? nothing : shared_memory_info["shared_memory_key"],
        JSON.json(arrow_schema)
    ])
    
    # Store in cache
    metadata = Dict{String, Any}(
        "id" => dataset_id,
        "name" => name,
        "table_name" => table_name,
        "schema" => schema_dict,
        "source_language" => "julia",
        "arrow_data" => true,
        "memory_map_path" => memory_map_path,
        "shared_memory_key" => shared_memory_info === nothing ? nothing : shared_memory_info["shared_memory_key"],
        "arrow_schema" => arrow_schema
    )
    cl.metadata_cache[dataset_id] = metadata
    cl.metadata_cache[name] = metadata
    
    # Log access
    log_access(cl, dataset_id, "write", "direct_arrow")
    
    return dataset_id
end

"""
    get_arrow_table(cl::CrossLinkManager, identifier::String)

Get an Arrow table directly using the most efficient method available.

This method prioritizes:
1. Shared memory (fastest if available)
2. Memory mapped files (fast file-based)
3. DuckDB query with Arrow results (fallback)
"""
function get_arrow_table(cl::CrossLinkManager, identifier::String)
    # Get metadata
    metadata = get(cl.metadata_cache, identifier, nothing)
    if metadata === nothing
        metadata = get_dataset_metadata(cl, identifier)
        if metadata === nothing
            error("Dataset with identifier '$(identifier)' not found")
        end
    end
    
    # Try shared memory first (fastest)
    shared_memory_key = get(metadata, "shared_memory_key", nothing)
    if shared_memory_key !== nothing && _SHARED_MEMORY_AVAILABLE
        arrow_table = get_from_shared_memory(cl, shared_memory_key)
        if arrow_table !== nothing
            # Log access
            log_access(cl, metadata["id"], "read", "shared_memory")
            return arrow_table
        end
    end
    
    # Try memory-mapped file
    memory_map_path = get(metadata, "memory_map_path", nothing)
    if memory_map_path !== nothing && isfile(memory_map_path)
        # Check if we already have this table in our cache
        arrow_table = get(_MMAPPED_TABLES, memory_map_path, nothing)
        if arrow_table === nothing
            # Read from file
            arrow_table = Arrow.Table(memory_map_path)
            # Cache for future use
            _MMAPPED_TABLES[memory_map_path] = arrow_table
        end
        
        # Log access
        log_access(cl, metadata["id"], "read", "memory_mapped")
        return arrow_table
    end
    
    # Try through DuckDB with Arrow results
    table_name = get(metadata, "table_name", nothing)
    if table_name !== nothing
        try
            # Prepare and execute query
            df = DataFrame(DuckDB.execute(cl.conn, "SELECT * FROM $(table_name)"))
            
            # Convert to Arrow table
            arrow_table = Arrow.Table(df)
            
            # Log access
            log_access(cl, metadata["id"], "read", "duckdb_convert")
            return arrow_table
        catch e
            cl.debug && @warn "Failed to get data via DuckDB: $e"
            error("Failed to retrieve Arrow table: $e")
        end
    end
    
    error("Could not retrieve Arrow data for $(identifier) using any available method")
end 