# Store instances for connection pooling
const _INSTANCES = Dict{String, Any}()

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
    arrow_files::Vector{String}  # Track created arrow files for cleanup
    shared_memory_files::Vector{String}  # Track created shared memory files for cleanup
    cpp_available::Bool  # Whether C++ bindings are available
    cpp_instance::Any    # C++ instance if available
    
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
        
        # Try to use C++ implementation if available
        cpp_available = false
        cpp_instance = nothing
        
        # Load the C++ wrapper module
        try
            # Check if the CPP wrapper module already exists in this namespace
            if !@isdefined(CrossLinkCppWrapper)
                # Include the cpp_wrapper.jl file
                include(joinpath(dirname(@__DIR__), "shared_memory", "cpp_wrapper.jl"))
            end
            
            # Check if C++ is available
            cpp_available = CrossLinkCppWrapper.is_cpp_available()
            
            if cpp_available
                # Create C++ instance
                cpp_instance = CrossLinkCppWrapper.CrossLinkCpp(db_path, debug)
                if debug
                    println("Using C++ implementation for improved performance")
                end
            else
                if debug
                    println("C++ implementation not available, using pure Julia implementation")
                end
            end
        catch e
            if debug
                println("Failed to load C++ bindings: $e")
            end
            cpp_available = false
        end
        
        # Create directory if it doesn't exist
        db_dir = dirname(db_path)
        if !isempty(db_dir) && !isdir(db_dir)
            mkpath(db_dir)
        end
        
        # Connect to database
        conn = DuckDB.DB(db_path)
        
        # Try to optimize DuckDB settings for performance
        try
            # Enable parallel query execution
            DuckDB.execute(conn, "PRAGMA force_parallelism")
            
            # Enable SIMD optimization
            DuckDB.execute(conn, "PRAGMA enable_optimizer")
            
            # Set memory limit if needed
            # DuckDB.execute(conn, "PRAGMA memory_limit='8GB'")
            
            if debug
                println("Configured DuckDB with optimized settings")
            end
        catch e
            if debug
                @warn "Failed to configure DuckDB performance settings: $e"
            end
        end
        
        # Check if Arrow is available - now a dependency so should always be true
        arrow_available = true
        
        # Create instance
        instance = new(
            conn, 
            db_path, 
            debug, 
            Dict{String, Any}(), 
            false, 
            arrow_available, 
            String[], 
            String[],
            cpp_available,
            cpp_instance
        )
        
        # Cache for reuse
        _INSTANCES[abs_path] = instance
        
        # Initialize metadata tables
        setup_metadata_tables(instance)
        
        return instance
    end
end

# Simple dummy implementation of setup_metadata_tables to enable initialization
# This will be replaced by the actual implementation in metadata.jl
function setup_metadata_tables(cl::CrossLinkManager)
    if cl.tables_initialized
        return
    end
    
    # This will be properly implemented in metadata.jl
    cl.tables_initialized = true
end 