"""
Core connection management for DuckConnect.

This module provides the core connection functionality for DuckConnect
and handles connection pooling and initialization.
"""

module Core

using DuckDB
using DataFrames
using Dates
using UUIDs
using Logging

export ConnectionManager, get_connection, close_connection, execute_query

"""
    ConnectionManager

Manages DuckDB connections and provides connection pooling.
"""
mutable struct ConnectionManager
    db_path::String
    connections::Dict{String, DuckDB.DB}
    pool_limit::Int
    active_connections::Int
    last_used::Dict{String, DateTime}
    
    function ConnectionManager(db_path::String; pool_limit::Int=4)
        new(db_path, Dict{String, DuckDB.DB}(), pool_limit, 0, Dict{String, DateTime}())
    end
end

"""
    get_connection(manager::ConnectionManager, id::Union{String, Nothing}=nothing)

Get a connection from the pool or create a new one.
"""
function get_connection(manager::ConnectionManager, id::Union{String, Nothing}=nothing)
    if isnothing(id)
        id = string(uuid4())
    end
    
    if haskey(manager.connections, id)
        # Connection exists, update last_used
        manager.last_used[id] = now()
        return manager.connections[id]
    else
        # Check if we need to close some connections
        if manager.active_connections >= manager.pool_limit
            # Close least recently used connection
            lru_id = sort(collect(pairs(manager.last_used)), by=x->x[2])[1][1]
            close_connection(manager, lru_id)
        end
        
        # Create new connection
        try
            conn = DuckDB.DB(manager.db_path)
            
            # Configure connection
            DBInterface.execute(conn, "PRAGMA memory_limit='80%'")
            cores = max(1, Sys.CPU_THREADS - 1)
            DBInterface.execute(conn, "PRAGMA threads=$cores")
            DBInterface.execute(conn, "PRAGMA force_parallelism")
            
            # Try to load Arrow extension
            try
                DBInterface.execute(conn, "INSTALL arrow")
                DBInterface.execute(conn, "LOAD arrow")
            catch e
                @warn "Arrow extension could not be loaded: $e"
            end
            
            # Store connection
            manager.connections[id] = conn
            manager.last_used[id] = now()
            manager.active_connections += 1
            
            return conn
        catch e
            @error "Failed to create database connection: $e"
            rethrow(e)
        end
    end
end

"""
    close_connection(manager::ConnectionManager, id::String)

Close a specific connection in the pool.
"""
function close_connection(manager::ConnectionManager, id::String)
    if haskey(manager.connections, id)
        try
            close(manager.connections[id])
        catch e
            @warn "Error closing connection: $e"
        end
        
        delete!(manager.connections, id)
        delete!(manager.last_used, id)
        manager.active_connections -= 1
    end
end

"""
    execute_query(manager::ConnectionManager, query::String, params::Vector=[];
                connection_id::Union{String, Nothing}=nothing)

Execute a SQL query with the given parameters.
"""
function execute_query(manager::ConnectionManager, query::String, params::Vector=[]; 
                      connection_id::Union{String, Nothing}=nothing)
    conn = get_connection(manager, connection_id)
    
    start_time = now()
    result = nothing
    
    try
        if isempty(params)
            result = DataFrame(DBInterface.execute(conn, query))
        else
            result = DataFrame(DBInterface.execute(conn, query, params))
        end
    catch e
        @error "Query execution failed: $e\nQuery: $query\nParams: $params"
        rethrow(e)
    end
    
    duration_ms = Dates.value(now() - start_time) / 1_000_000
    if duration_ms > 1000
        @info "Long-running query ($(round(duration_ms/1000, digits=2))s): $query"
    end
    
    return result
end

end # module 