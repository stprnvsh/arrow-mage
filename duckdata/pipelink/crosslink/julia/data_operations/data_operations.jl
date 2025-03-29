"""
    push_data(cl::CrossLinkManager, df::DataFrame, name::Union{String, Nothing}=nothing;
             description::Union{String, Nothing}=nothing, arrow_data::Union{Bool, Nothing}=nothing,
             enable_zero_copy::Bool=true, memory_mapped::Bool=true, shared_memory::Bool=false,
             access_languages::Vector{String}=["python", "r", "julia", "cpp"])

Push a DataFrame to a CrossLink database with zero-copy optimization.
"""
function push_data(cl::CrossLinkManager, df::Union{DataFrame, Arrow.Table}, name::Union{String, Nothing}=nothing;
                  description::Union{String, Nothing}=nothing, arrow_data::Union{Bool, Nothing}=nothing,
                  enable_zero_copy::Bool=true, memory_mapped::Bool=true, shared_memory::Bool=false,
                  access_languages::Vector{String}=["python", "r", "julia", "cpp"])
    # For backward compatibility: if arrow_data is provided, use it for enable_zero_copy
    if arrow_data !== nothing
        enable_zero_copy = arrow_data
    end
    
    # If C++ implementation is available, use it
    if cl.cpp_available && cl.cpp_instance !== nothing
        try
            # Convert to Arrow Table if needed
            is_arrow_table = df isa Arrow.Table
            arrow_table = is_arrow_table ? df : Arrow.Table(df)
            
            # Use C++ implementation via the wrapper
            dataset_id = CrossLinkCppWrapper.push(
                cl.cpp_instance,
                arrow_table,
                name === nothing ? "" : name,
                description === nothing ? "" : description
            )
            
            cl.debug && println("Pushed data using C++ implementation")
            
            return dataset_id
        catch e
            cl.debug && println("C++ push failed, falling back to Julia implementation: $e")
            # Continue with regular Julia implementation
        end
    end
    
    # Check if input is already an Arrow Table
    is_arrow_table = df isa Arrow.Table
    
    # If it's an Arrow table and we want zero-copy, use direct Arrow path
    if is_arrow_table && enable_zero_copy
        return share_arrow_table(
            cl,
            df;
            name=name,
            description=description,
            shared_memory=shared_memory,
            memory_mapped=memory_mapped,
            access_languages=access_languages
        )
    end
    
    # Ensure tables are initialized
    setup_metadata_tables(cl)
    
    # Generate dataset ID and name if needed
    dataset_id = string(uuid4())
    if name === nothing
        name = "dataset_$(Int(datetime2unix(now())))"
    end
    
    # Create table name
    table_name = "data_$(replace(replace(lowercase(name), " " => "_"), "-" => "_"))"
    
    # Convert to Arrow Table if needed for zero-copy operations
    memory_map_path = nothing
    arrow_schema = nothing
    arrow_table = nothing
    shared_memory_info = nothing
    
    if enable_zero_copy
        try
            # Convert DataFrame to Arrow Table if it isn't already
            if is_arrow_table
                arrow_table = df
            else
                arrow_table = Arrow.Table(df)
            end
            
            # Set up memory-mapped file if requested
            if memory_mapped
                # Create directory for memory-mapped files if it doesn't exist
                mmaps_dir = joinpath(dirname(cl.db_path), "crosslink_mmaps")
                isdir(mmaps_dir) || mkdir(mmaps_dir)
                
                # Create a unique file path for this dataset
                memory_map_path = joinpath(mmaps_dir, "$(dataset_id).arrow")
                
                # Write the Arrow table to the memory-mapped file
                Arrow.write(memory_map_path, arrow_table)
                
                # Track this file for cleanup
                push!(cl.arrow_files, memory_map_path)
                
                cl.debug && @info "Wrote Arrow table to memory-mapped file: $memory_map_path"
            end
            
            # Set up shared memory if requested
            if shared_memory
                shared_memory_info = setup_shared_memory(cl, arrow_table, dataset_id)
            end
            
            # Serialize Arrow schema
            arrow_schema = Dict(
                "schema" => string(arrow_table.schema),
                "serialized" => string(arrow_table.schema),
                "metadata" => Dict{String, String}()  # Add any schema metadata here
            )
        catch e
            cl.debug && @warn "Failed to create Arrow representation: $e"
            memory_map_path = nothing
            arrow_table = nothing
        end
    end
    
    # Try different methods to create the table/view
    table_created = false
    
    # Method 1: Arrow-based view to memory mapped file (most efficient)
    if !table_created && memory_map_path !== nothing
        try
            # Create a view that reads directly from the Arrow file
            DuckDB.execute(cl.conn, """
            CREATE OR REPLACE VIEW $(table_name) AS 
            SELECT * FROM read_parquet('$(memory_map_path)')
            """)
            
            table_created = true
            cl.debug && @info "Created view from memory-mapped Arrow file"
        catch e
            cl.debug && @warn "View creation from memory-mapped file failed: $e"
        end
    end
    
    # Method 2: Direct Arrow table registration if available
    if !table_created && arrow_table !== nothing
        try
            # Create a temporary file for Arrow data
            temp_file = tempname() * ".arrow"
            Arrow.write(temp_file, arrow_table)
            
            # Track for cleanup
            push!(cl.arrow_files, temp_file)
            
            # Create view from arrow file
            DuckDB.execute(cl.conn, """
            CREATE OR REPLACE VIEW $(table_name) AS 
            SELECT * FROM read_parquet('$(temp_file)')
            """)
            
            table_created = true
            cl.debug && @info "Created view from Arrow file"
        catch e
            cl.debug && @warn "Arrow file registration failed: $e"
        end
    end
    
    # Method 3: Direct DataFrame to table conversion
    if !table_created
        try
            # Convert to DataFrame if not already
            dataframe = is_arrow_table ? DataFrame(df) : df
            
            # Create a temporary view first
            temp_name = "temp_$(replace(string(uuid4()), "-" => ""))"
            
            # Register the DataFrame with DuckDB (this varies by DuckDB.jl version)
            try
                # Try first method - using execute with parameters
                DuckDB.execute(cl.conn, """
                CREATE OR REPLACE TABLE $(table_name) AS
                SELECT * FROM ?
                """, [dataframe])
                
                table_created = true
                cl.debug && @info "Created table using DataFrame parameter (method 1)"
            catch e1
                try
                    # Try second method - using a named parameter
                    DuckDB.execute(cl.conn, """
                    CREATE OR REPLACE TABLE $(table_name) AS
                    SELECT * FROM df
                    """, Dict("df" => dataframe))
                    
                    table_created = true
                    cl.debug && @info "Created table using DataFrame named parameter (method 2)"
                catch e2
                    try
                        # Try third method - using a temporary CSV file
                        temp_csv = joinpath(tempdir(), "$(replace(string(uuid4()), "-" => "")).csv")
                        CSV.write(temp_csv, dataframe)
                        
                        DuckDB.execute(cl.conn, """
                        CREATE OR REPLACE TABLE $(table_name) AS
                        SELECT * FROM read_csv_auto('$(temp_csv)')
                        """)
                        
                        # Clean up the temporary file
                        isfile(temp_csv) && rm(temp_csv)
                        
                        table_created = true
                        cl.debug && @info "Created table using CSV method (method 3)"
                    catch e3
                        # Try fourth method - iterate through rows manually
                        try
                            # Get column names and types
                            cols = names(dataframe)
                            types = [string(eltype(dataframe[!, col])) for col in cols]
                            
                            # Create SQL types
                            sql_types = []
                            for t in types
                                if occursin("Int", t)
                                    push!(sql_types, "INTEGER")
                                elseif occursin("Float", t)
                                    push!(sql_types, "DOUBLE")
                                elseif occursin("String", t)
                                    push!(sql_types, "VARCHAR")
                                elseif occursin("Bool", t)
                                    push!(sql_types, "BOOLEAN")
                                elseif occursin("Date", t) || occursin("DateTime", t)
                                    push!(sql_types, "TIMESTAMP")
                                else
                                    push!(sql_types, "VARCHAR")  # Default to string for unknown types
                                end
                            end
                            
                            # Create table schema
                            schema_sql = join(["$(col) $(sql_types[i])" for (i, col) in enumerate(cols)], ", ")
                            create_sql = "CREATE OR REPLACE TABLE $(table_name) ($(schema_sql))"
                            DuckDB.execute(cl.conn, create_sql)
                            
                            # Insert data in batches
                            batch_size = min(1000, nrow(dataframe))
                            for i in 1:batch_size:nrow(dataframe)
                                end_idx = min(i + batch_size - 1, nrow(dataframe))
                                batch = dataframe[i:end_idx, :]
                                
                                # Create temporary CSV for batch
                                batch_csv = joinpath(tempdir(), "batch_$(i).csv")
                                CSV.write(batch_csv, batch)
                                
                                # Insert from CSV
                                DuckDB.execute(cl.conn, """
                                INSERT INTO $(table_name)
                                SELECT * FROM read_csv_auto('$(batch_csv)')
                                """)
                                
                                # Clean up
                                isfile(batch_csv) && rm(batch_csv)
                            end
                            
                            table_created = true
                            cl.debug && @info "Created table using batched CSV method (method 4)"
                        catch e4
                            # Log all errors for debugging
                            cl.debug && @warn "DataFrame parameter method 1 failed: $e1"
                            cl.debug && @warn "DataFrame parameter method 2 failed: $e2"
                            cl.debug && @warn "CSV method failed: $e3"
                            cl.debug && @warn "Batched CSV method failed: $e4"
                        end
                    end
                end
            end
        catch e
            cl.debug && @warn "All DataFrame table creation methods failed: $e"
        end
    end
    
    # If all methods failed, raise exception
    if !table_created
        # Try one last method - create empty table with proper schema and insert rows
        try
            # Convert to DataFrame if not already
            dataframe = is_arrow_table ? DataFrame(df) : df
            
            # Get column names and types
            cols = names(dataframe)
            
            # Create an empty table with the right schema
            col_defs = []
            for col in cols
                col_type = eltype(dataframe[!, col])
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
            
            create_sql = "CREATE OR REPLACE TABLE $(table_name) ($(join(col_defs, ", ")))"
            DuckDB.execute(cl.conn, create_sql)
            
            # Create placeholders for parametrized INSERT
            placeholders = join(fill("?", length(cols)), ", ")
            insert_sql = "INSERT INTO $(table_name) VALUES ($(placeholders))"
            
            # Insert rows in batches
            batch_size = 100
            for i in 1:batch_size:nrow(dataframe)
                end_idx = min(i + batch_size - 1, nrow(dataframe))
                for row_idx in i:end_idx
                    row_values = [dataframe[row_idx, col] for col in cols]
                    DuckDB.execute(cl.conn, insert_sql, row_values)
                end
            end
            
            table_created = true
            cl.debug && @info "Created table using direct row insertion (last resort method)"
        catch e
            cl.debug && @warn "Direct row insertion method failed: $e"
            error("Failed to create table using any available method")
        end
    end
    
    # Create schema dictionary
    if is_arrow_table
        # Get schema from Arrow table
        schema_dict = Dict(
            "columns" => [field.name for field in df.schema],
            "dtypes" => Dict(field.name => string(field.type) for field in df.schema)
        )
    else
        # Extract schema from DataFrame
        schema_dict = Dict(
            "columns" => names(df),
            "dtypes" => Dict(string(col) => string(eltype(df[!, col])) for col in names(df))
        )
    end
    
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
        description === nothing ? "" : description,
        JSON.json(schema_dict),
        table_name,
        true,  # arrow_data is always true since Julia uses Arrow.jl
        1,
        true,
        schema_hash,
        JSON.json(access_languages),
        memory_map_path,
        shared_memory_info === nothing ? nothing : shared_memory_info["shared_memory_key"],
        arrow_schema === nothing ? nothing : JSON.json(arrow_schema)
    ])
    
    # Store in cache
    metadata = Dict(
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
    
    # Log the operation
    access_method = is_arrow_table ? "direct_arrow" : enable_zero_copy ? "zero-copy" : "copy"
    log_access(cl, dataset_id, "write", access_method)
    
    return dataset_id
end

"""
    pull_data(cl::CrossLinkManager, identifier::String;
             zero_copy::Bool=true, to_arrow::Bool=false)

Pull a dataset from CrossLink with zero-copy optimization when possible.
"""
function pull_data(cl::CrossLinkManager, identifier::String;
                  zero_copy::Bool=true, to_arrow::Bool=false)
    # If C++ implementation is available, use it
    if cl.cpp_available && cl.cpp_instance !== nothing
        try
            # Use C++ implementation via the wrapper
            arrow_table = CrossLinkCppWrapper.pull(cl.cpp_instance, identifier)
            
            # Convert to DataFrame if requested
            if !to_arrow
                df = DataFrame(arrow_table)
                cl.debug && println("Pulled data using C++ implementation, converted to DataFrame")
                return df
            else
                cl.debug && println("Pulled data using C++ implementation as Arrow table")
                return arrow_table
            end
        catch e
            cl.debug && println("C++ pull failed, falling back to Julia implementation: $e")
            # Continue with regular Julia implementation
        end
    end
    
    # For direct Arrow output, use get_arrow_table
    if to_arrow
        try
            return get_arrow_table(cl, identifier)
        catch e
            cl.debug && @warn "Direct Arrow access failed, falling back: $e"
            # Fall through to standard methods
        end
    end
    
    # Ensure tables are initialized
    setup_metadata_tables(cl)
    
    # Get metadata
    metadata = get(cl.metadata_cache, identifier, nothing)
    if metadata === nothing
        metadata = get_dataset_metadata(cl, identifier)
        if metadata === nothing
            error("Dataset not found: $identifier")
        end
    end
    
    # Priority order for access:
    # 1. Shared memory (fastest)
    # 2. Memory-mapped Arrow file
    # 3. DuckDB query
    
    # 1. Try shared memory first (fastest)
    shared_memory_key = get(metadata, "shared_memory_key", nothing)
    if zero_copy && shared_memory_key !== nothing && shared_memory_key !== missing && _SHARED_MEMORY_AVAILABLE
        arrow_table = get_from_shared_memory(cl, shared_memory_key)
        if arrow_table !== nothing
            # Convert to DataFrame
            df = DataFrame(arrow_table)
            
            # Log access
            log_access(cl, metadata["id"], "read", "shared_memory")
            return df
        end
    else
        cl.debug && @info "Shared memory not available or not configured for this dataset"
    end
    
    # 2. Check for memory-mapped file
    memory_map_path = get(metadata, "memory_map_path", nothing)
    if zero_copy && memory_map_path !== nothing && memory_map_path !== missing && isfile(memory_map_path)
        try
            # Check if we already have this table in our cache
            arrow_table = get(_MMAPPED_TABLES, memory_map_path, nothing)
            if arrow_table === nothing
                # Read from file
                arrow_table = Arrow.Table(memory_map_path)
                # Cache for future use
                _MMAPPED_TABLES[memory_map_path] = arrow_table
            end
            
            # Convert to DataFrame
            df = DataFrame(arrow_table)
            
            # Log access
            log_access(cl, metadata["id"], "read", "memory_mapped")
            return df
        catch e
            cl.debug && @warn "Memory-mapped file read failed, falling back: $e"
            # Fall through to next method
        end
    else
        cl.debug && @info "Memory-mapped file not available or not configured for this dataset"
    end
    
    # 3. Standard query approach
    table_name = metadata["table_name"]
    
    # Query as DataFrame
    try
        cl.debug && @info "Using DuckDB query fallback method"
        df = DataFrame(DuckDB.execute(cl.conn, "SELECT * FROM $table_name"))
        
        # Log access
        log_access(cl, metadata["id"], "read", "copy")
        
        return df
    catch e
        cl.debug && @error "DuckDB query failed: $e"
        error("Failed to retrieve data using any available method")
    end
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
    # If C++ implementation is available, use it
    if cl.cpp_available && cl.cpp_instance !== nothing
        try
            # Use C++ implementation via the wrapper
            datasets = CrossLinkCppWrapper.list_datasets(cl.cpp_instance)
            cl.debug && println("Listed datasets using C++ implementation")
            return datasets
        catch e
            cl.debug && println("C++ list_datasets failed, falling back to Julia implementation: $e")
            # Continue with regular Julia implementation
        end
    end
    
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
    query_data(cl::CrossLinkManager, sql::String; to_arrow::Bool=false)
    
Execute a SQL query and return the results.
"""
function query_data(cl::CrossLinkManager, sql::String; to_arrow::Bool=false)
    # If C++ implementation is available, use it
    if cl.cpp_available && cl.cpp_instance !== nothing
        try
            # Use C++ implementation via the wrapper
            arrow_table = CrossLinkCppWrapper.query(cl.cpp_instance, sql)
            
            # Convert to DataFrame if requested
            if !to_arrow
                df = DataFrame(arrow_table)
                cl.debug && println("Executed query using C++ implementation, converted to DataFrame")
                return df
            else
                cl.debug && println("Executed query using C++ implementation as Arrow table")
                return arrow_table
            end
        catch e
            cl.debug && println("C++ query failed, falling back to Julia implementation: $e")
            # Continue with regular Julia implementation
        end
    end
    
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