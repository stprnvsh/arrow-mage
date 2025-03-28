"""
Transaction management for DuckConnect.

This module provides functionality for tracking operations on datasets
and maintaining lineage information.
"""

module Transactions

using DuckDB
using DataFrames
using Dates
using UUIDs
using JSON
using ..Core

export record_transaction, record_lineage, get_dataset_history,
       get_dataset_lineage, record_stats

"""
    record_transaction(conn_manager::Core.ConnectionManager,
                      dataset_id::String,
                      operation::String,
                      language::String="julia",
                      details::Dict=Dict())

Record a transaction for a dataset operation.
"""
function record_transaction(conn_manager::Core.ConnectionManager,
                           dataset_id::String,
                           operation::String,
                           language::String="julia",
                           details::Dict=Dict())
    
    # Validate operation type
    valid_operations = ["create", "read", "update", "delete"]
    if !(operation in valid_operations)
        throw(ArgumentError("Invalid operation: $operation. Must be one of $valid_operations"))
    end
    
    # Generate transaction ID
    transaction_id = string(uuid4())
    
    # Record transaction
    Core.execute_query(conn_manager, """
        INSERT INTO duck_connect_transactions
        (id, dataset_id, operation, language, timestamp, details)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [
        transaction_id,
        dataset_id,
        operation,
        language,
        now(),
        JSON.json(details)
    ])
    
    return transaction_id
end

"""
    record_lineage(conn_manager::Core.ConnectionManager,
                  target_dataset_id::String,
                  source_dataset_id::String,
                  transformation::String,
                  language::String="julia")

Record lineage information between datasets.
"""
function record_lineage(conn_manager::Core.ConnectionManager,
                       target_dataset_id::String,
                       source_dataset_id::String,
                       transformation::String,
                       language::String="julia")
    
    # Generate lineage ID
    lineage_id = string(uuid4())
    
    # Record lineage
    Core.execute_query(conn_manager, """
        INSERT INTO duck_connect_lineage
        (id, target_dataset_id, source_dataset_id, transformation, language, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [
        lineage_id,
        target_dataset_id,
        source_dataset_id,
        transformation,
        language,
        now()
    ])
    
    return lineage_id
end

"""
    get_dataset_history(conn_manager::Core.ConnectionManager,
                       dataset_id::String)

Get the history of operations for a dataset.
"""
function get_dataset_history(conn_manager::Core.ConnectionManager,
                            dataset_id::String)
    
    return Core.execute_query(conn_manager, """
        SELECT id, operation, language, timestamp, details
        FROM duck_connect_transactions
        WHERE dataset_id = ?
        ORDER BY timestamp DESC
    """, [dataset_id])
end

"""
    get_dataset_lineage(conn_manager::Core.ConnectionManager,
                       dataset_id::String;
                       direction::String="upstream")

Get the lineage information for a dataset.
"""
function get_dataset_lineage(conn_manager::Core.ConnectionManager,
                            dataset_id::String;
                            direction::String="upstream")
    
    if direction == "upstream"
        # Get source datasets that contributed to this dataset
        return Core.execute_query(conn_manager, """
            SELECT l.id, l.source_dataset_id as dataset_id, l.transformation, 
                   l.language, l.timestamp, m.name as dataset_name
            FROM duck_connect_lineage l
            JOIN duck_connect_metadata m ON l.source_dataset_id = m.id
            WHERE l.target_dataset_id = ?
            ORDER BY l.timestamp DESC
        """, [dataset_id])
    elseif direction == "downstream"
        # Get target datasets derived from this dataset
        return Core.execute_query(conn_manager, """
            SELECT l.id, l.target_dataset_id as dataset_id, l.transformation, 
                   l.language, l.timestamp, m.name as dataset_name
            FROM duck_connect_lineage l
            JOIN duck_connect_metadata m ON l.target_dataset_id = m.id
            WHERE l.source_dataset_id = ?
            ORDER BY l.timestamp DESC
        """, [dataset_id])
    else
        throw(ArgumentError("Invalid direction: $direction. Must be 'upstream' or 'downstream'"))
    end
end

"""
    record_stats(conn_manager::Core.ConnectionManager,
                dataset_id::String,
                operation::String,
                duration_ms::Float64,
                row_count::Int,
                column_count::Int,
                memory_usage_mb::Float64=0.0,
                language::String="julia")

Record performance statistics for a dataset operation.
"""
function record_stats(conn_manager::Core.ConnectionManager,
                     dataset_id::String,
                     operation::String,
                     duration_ms::Float64,
                     row_count::Int,
                     column_count::Int,
                     memory_usage_mb::Float64=0.0,
                     language::String="julia")
    
    # Generate stats ID
    stats_id = string(uuid4())
    
    # Record stats
    Core.execute_query(conn_manager, """
        INSERT INTO duck_connect_stats
        (id, dataset_id, operation, language, timestamp, 
         duration_ms, memory_usage_mb, row_count, column_count)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, [
        stats_id,
        dataset_id,
        operation,
        language,
        now(),
        duration_ms,
        memory_usage_mb,
        row_count,
        column_count
    ])
    
    return stats_id
end

end # module 