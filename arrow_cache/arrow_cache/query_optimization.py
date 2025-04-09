"""
Query optimization module for ArrowCache

Provides functionality for SQL query optimization including:
- Query plan caching
- Optimization hints
- Statistics-based optimization
"""
import hashlib
import logging
import re
import threading
import time
from typing import Dict, List, Set, Optional, Any, Tuple, Union, Callable
from dataclasses import dataclass
import functools
import json

import duckdb
import pyarrow as pa

logger = logging.getLogger(__name__)


@dataclass
class QueryPlan:
    """Represents a DuckDB query plan with metadata"""
    sql: str
    plan: str
    optimization_time: float
    execution_time: float
    created_at: float
    hit_count: int = 0
    last_used_at: float = 0


class QueryOptimizer:
    """
    Optimizer for DuckDB queries providing plan caching and hints
    """
    def __init__(self, config: Any):
        """
        Initialize the query optimizer
        
        Args:
            config: ArrowCache configuration
        """
        self.config = config
        self.query_plan_cache_size = config["query_plan_cache_size"]
        self.enable_cache = config["cache_query_plans"]
        self.use_statistics = config["use_statistics"]
        
        self.query_plans = {}  # sql hash -> QueryPlan
        self.lock = threading.RLock()
        
        # Initialize hint patterns
        self._hint_patterns = self._init_hint_patterns()
    
    def _init_hint_patterns(self) -> Dict[str, Dict[str, Any]]:
        """
        Initialize regex patterns for identifying query optimization opportunities
        
        Returns:
            Dictionary of patterns and their associated optimization hints
        """
        return {
            # Large aggregations
            "large_group_by": {
                "pattern": re.compile(r"GROUP\s+BY", re.IGNORECASE),
                "hint": "Add a LIMIT clause or use more selective filters before GROUP BY for large datasets"
            },
            # Cross joins (often unintentional)
            "cross_join": {
                "pattern": re.compile(r"CROSS\s+JOIN|FROM\s+[^\s,]+(,)[^\s,]+", re.IGNORECASE),
                "hint": "Consider using an explicit JOIN with a join condition"
            },
            # Multiple nested subqueries
            "nested_subqueries": {
                "pattern": re.compile(r"SELECT.*?\(SELECT.*?\(SELECT", re.IGNORECASE | re.DOTALL),
                "hint": "Multiple nested subqueries can be inefficient, consider rewriting with CTEs"
            },
            # IN clause with large lists
            "in_clause": {
                "pattern": re.compile(r"IN\s*\([^)]{100,}\)", re.IGNORECASE),
                "hint": "Large IN clauses can be inefficient, consider using a JOIN against a temporary table"
            },
            # Functions in WHERE clause
            "function_in_where": {
                "pattern": re.compile(r"WHERE.*?\([^\)]*?col[^\)]*?\)", re.IGNORECASE),
                "hint": "Functions in WHERE clause prevent index usage, move to a derived column if possible"
            },
            # LIKE with leading wildcard
            "like_leading_wildcard": {
                "pattern": re.compile(r"LIKE\s+['\"]%", re.IGNORECASE),
                "hint": "LIKE with leading wildcard can't use indexes efficiently"
            },
            # ORDER BY on large result set
            "order_by_without_limit": {
                "pattern": re.compile(r"ORDER\s+BY[^;]*?(?!LIMIT)", re.IGNORECASE),
                "hint": "Consider adding a LIMIT clause when using ORDER BY"
            },
            # UNION instead of UNION ALL
            "union_vs_union_all": {
                "pattern": re.compile(r"UNION(?!\s+ALL)", re.IGNORECASE),
                "hint": "UNION removes duplicates which is expensive, use UNION ALL if duplicates are acceptable"
            }
        }
    
    def _hash_sql(self, sql: str) -> str:
        """
        Generate a hash for SQL query to use as cache key
        
        Args:
            sql: SQL query string
            
        Returns:
            Hash string
        """
        # Normalize whitespace and remove comments to improve cache hits
        normalized = re.sub(r'\s+', ' ', sql).strip()
        normalized = re.sub(r'--.*?$', '', normalized, flags=re.MULTILINE)
        normalized = re.sub(r'/\*.*?\*/', '', normalized, flags=re.DOTALL)
        
        # Remove literals to allow caching queries that differ only in literal values
        # This is simplistic - a real implementation would use SQL parsing
        normalized = re.sub(r"'[^']*'", "'?'", normalized)
        normalized = re.sub(r"\d+\.\d+", "?.?", normalized)
        normalized = re.sub(r"\d+", "?", normalized)
        
        return hashlib.md5(normalized.encode('utf-8')).hexdigest()
    
    def get_query_plan(self, con: duckdb.DuckDBPyConnection, sql: str) -> str:
        """
        Get the query plan for a SQL query
        
        Args:
            con: DuckDB connection
            sql: SQL query
            
        Returns:
            Query plan string
        """
        try:
            # Use DuckDB's EXPLAIN statement to get the query plan
            explain_sql = f"EXPLAIN {sql}"
            result = con.execute(explain_sql).fetchall()
            return "\n".join(str(row[0]) for row in result)
        except Exception as e:
            logger.warning(f"Failed to get query plan: {e}")
            return ""
    
    def optimize_query(self, sql: str) -> Tuple[str, List[str]]:
        """
        Optimize a SQL query and return hints
        
        Args:
            sql: SQL query
            
        Returns:
            Tuple of (optimized SQL, list of optimization hints)
        """
        if not sql:
            return sql, []
            
        optimization_hints = []
        optimized_sql = sql
        
        # Fix date/time function compatibility issues
        optimized_sql = self._fix_date_functions(optimized_sql)
        
        # Fix table references that look like function calls
        optimized_sql = self._fix_table_references(optimized_sql)
        
        # Check for optimization opportunities using regex patterns
        for name, pattern_info in self._hint_patterns.items():
            pattern = pattern_info["pattern"]
            if pattern.search(sql):
                optimization_hints.append(pattern_info["hint"])
        
        # Apply query rewriting optimizations
        # This is just a simple example - a real implementation would use SQL parsing
        
        # Add PRAGMA to optimize memory usage for large aggregations
        if re.search(r"GROUP\s+BY", sql, re.IGNORECASE) and "memory_limit" not in sql.lower():
            if not sql.lstrip().startswith("PRAGMA"):
                optimized_sql = "PRAGMA memory_limit='8GB';\n" + optimized_sql
                optimization_hints.append("Added memory limit pragma for aggregation query")
        
        # Add PRAGMA for thread count control
        if self.config["thread_count"] > 0 and "threads" not in sql.lower():
            complex_ops = ["JOIN", "GROUP BY", "ORDER BY", "WINDOW"]
            if any(op in sql.upper() for op in complex_ops):
                thread_count = min(self.config["thread_count"], 8)  # Limit to reasonable number
                if not sql.lstrip().startswith("PRAGMA"):
                    optimized_sql = f"PRAGMA threads={thread_count};\n" + optimized_sql
                    optimization_hints.append(f"Set thread count to {thread_count} for parallel execution")
        
        return optimized_sql, optimization_hints
    
    def _fix_date_functions(self, sql: str) -> str:
        """
        Fix compatibility issues with date/time functions
        
        Args:
            sql: Original SQL query
            
        Returns:
            SQL query with fixed date/time functions
        """
        original_sql = sql
        
        # Fix EXTRACT(DAYOFWEEK FROM ...) -> EXTRACT(dow FROM ...)
        sql = re.sub(
            r'EXTRACT\s*\(\s*DAYOFWEEK\s+FROM\s+([^)]+)\)',
            r'EXTRACT(dow FROM \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        # Fix date_part('DAYOFWEEK', ...) -> date_part('dow', ...)
        sql = re.sub(
            r"date_part\s*\(\s*'DAYOFWEEK'\s*,\s*([^)]+)\)",
            r"date_part('dow', \1)",
            sql,
            flags=re.IGNORECASE
        )
        sql = re.sub(
            r'date_part\s*\(\s*"DAYOFWEEK"\s*,\s*([^)]+)\)',
            r'date_part("dow", \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        # Fix other common date format inconsistencies
        # DAYOFMONTH -> day
        sql = re.sub(
            r'EXTRACT\s*\(\s*DAYOFMONTH\s+FROM\s+([^)]+)\)',
            r'EXTRACT(day FROM \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        # DAYOFYEAR -> doy
        sql = re.sub(
            r'EXTRACT\s*\(\s*DAYOFYEAR\s+FROM\s+([^)]+)\)',
            r'EXTRACT(doy FROM \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        # WEEKDAY -> dow
        sql = re.sub(
            r'EXTRACT\s*\(\s*WEEKDAY\s+FROM\s+([^)]+)\)',
            r'EXTRACT(dow FROM \1)',
            sql,
            flags=re.IGNORECASE
        )
        
        # Handle type casting for common timestamp columns
        # This helps with VARCHAR columns that need to be interpreted as timestamps
        # Look for date functions on pickup_datetime, dropoff_datetime, etc.
        for date_col in ['pickup_datetime', 'dropoff_datetime', 'created_at', 'timestamp', 'date', 'time']:
            # Add explicit CAST for EXTRACT function
            sql = re.sub(
                rf'EXTRACT\s*\(\s*(\w+)\s+FROM\s+{date_col}\b([^)]*)?\)',
                rf'EXTRACT(\1 FROM CAST({date_col} AS TIMESTAMP)\2)',
                sql,
                flags=re.IGNORECASE
            )
            
            # Add explicit CAST for date_part function
            sql = re.sub(
                rf"date_part\s*\(\s*'(\w+)'\s*,\s*{date_col}\b([^)]*)?\)",
                rf"date_part('\1', CAST({date_col} AS TIMESTAMP)\2)",
                sql,
                flags=re.IGNORECASE
            )
            sql = re.sub(
                rf'date_part\s*\(\s*"(\w+)"\s*,\s*{date_col}\b([^)]*)?\)',
                rf'date_part("\1", CAST({date_col} AS TIMESTAMP)\2)',
                sql,
                flags=re.IGNORECASE
            )
            
        # Add explicit TIMESTAMP type for date literals in comparisons
        sql = re.sub(
            r"(\b\w+_datetime\b|\bdatetime\b|\btimestamp\b|\bdate\b|\btime\b)\s*([=><]+)\s*'(\d{4}-\d{2}-\d{2}[^']*)'",
            r"\1 \2 TIMESTAMP '\3'",
            sql,
            flags=re.IGNORECASE
        )
        
        # Log if any changes were made
        if sql != original_sql:
            logger.info("Fixed date/time function compatibility issues in SQL query")
            logger.debug(f"Original SQL: {original_sql}")
            logger.debug(f"Fixed SQL: {sql}")
        
        return sql
    
    def _fix_table_references(self, sql: str) -> str:
        """
        Fix table references that DuckDB might misinterpret as function calls
        
        Args:
            sql: Original SQL query
            
        Returns:
            SQL query with fixed table references
        """
        original_sql = sql
        
        # Pattern to match table references that look like function calls
        # like "_cache_nyc_yellow_taxi_(jan_2023)" or "_cache_nyc_yellow_taxi_ (jan_2023)"
        # Updated regex to better handle spaces before parentheses
        pattern = re.compile(r'(FROM|JOIN)\s+(_cache_[a-zA-Z0-9_]+)(\s*\(\s*[^)]*\s*\))', re.IGNORECASE)
        
        # Function to replace matches
        def replace_func(match):
            clause = match.group(1)  # FROM or JOIN
            table_name = match.group(2)  # _cache_table
            params = match.group(3)  # (params)
            
            # Extract the parameter content without parentheses
            # Improved to handle extra whitespace
            param_content = re.sub(r'^\s*\(\s*|\s*\)\s*$', '', params).strip()
            
            # Convert to proper table syntax: 
            # "_cache_table(param)" becomes "_cache_table_param"
            if param_content:
                # Clean param_content to be part of an identifier
                clean_param = re.sub(r'[^a-zA-Z0-9_]', '_', param_content)
                new_table = f'{table_name}_{clean_param}'
                return f'{clause} {new_table}'
            else:
                return f'{clause} {table_name}'
        
        # Replace in the SQL
        modified_sql = pattern.sub(replace_func, sql)
        
        # Also check for table names in subqueries
        subquery_pattern = re.compile(r'(\(\s*SELECT[^)]*?FROM\s+)(_cache_[a-zA-Z0-9_]+)(\s*\(\s*[^)]*\s*\))', re.IGNORECASE | re.DOTALL)
        modified_sql = subquery_pattern.sub(lambda m: m.group(1) + replace_func(m).split(' ', 1)[1], modified_sql)
        
        # Log if any changes were made
        if modified_sql != original_sql:
            logger.info("Fixed table references that looked like function calls")
            logger.debug(f"Original SQL: {original_sql}")
            logger.debug(f"Fixed SQL: {modified_sql}")
        
        return modified_sql
    
    def get_cached_plan(self, sql: str) -> Optional[QueryPlan]:
        """
        Get a cached query plan if available
        
        Args:
            sql: SQL query
            
        Returns:
            Cached query plan or None if not found
        """
        if not self.enable_cache:
            return None
            
        with self.lock:
            sql_hash = self._hash_sql(sql)
            plan = self.query_plans.get(sql_hash)
            
            if plan:
                plan.hit_count += 1
                plan.last_used_at = time.time()
                
            return plan
    
    def add_to_cache(
        self,
        sql: str,
        plan: str,
        optimization_time: float,
        execution_time: float
    ) -> None:
        """
        Add a query plan to the cache
        
        Args:
            sql: SQL query
            plan: Query plan string
            optimization_time: Time spent optimizing (seconds)
            execution_time: Time spent executing (seconds)
        """
        if not self.enable_cache:
            return
            
        with self.lock:
            # Enforce cache size limit
            if len(self.query_plans) >= self.query_plan_cache_size:
                # Remove least recently used plan
                lru_hash = min(
                    self.query_plans.keys(),
                    key=lambda h: self.query_plans[h].last_used_at
                )
                del self.query_plans[lru_hash]
            
            # Add new plan to cache
            sql_hash = self._hash_sql(sql)
            self.query_plans[sql_hash] = QueryPlan(
                sql=sql,
                plan=plan,
                optimization_time=optimization_time,
                execution_time=execution_time,
                created_at=time.time(),
                hit_count=1,
                last_used_at=time.time()
            )
    
    def optimize_and_execute(
        self,
        con: duckdb.DuckDBPyConnection,
        sql: str,
        ensure_tables_callback: Optional[Callable[[List[str]], None]] = None,
        deregister_tables_callback: Optional[Callable[[List[str]], None]] = None
    ) -> Tuple[pa.Table, List[str], Dict[str, Any]]:
        """
        Optimize and execute a SQL query
        
        Args:
            con: DuckDB connection
            sql: SQL query
            ensure_tables_callback: Callback to ensure tables are registered
            deregister_tables_callback: Callback to deregister tables after query execution
            
        Returns:
            Tuple of (result table, optimization hints, execution info)
        """
        start_time = time.time()
        table_refs = []
        
        # First, extract table references and ensure they're registered
        table_refs = self._extract_table_references(sql)
        if table_refs and ensure_tables_callback:
            # Call back to the cache to ensure these tables are registered
            ensure_tables_callback(table_refs)
            
        cached_plan = self.get_cached_plan(sql)
        
        if cached_plan:
            # We have a cached plan, use it
            optimization_hints = []
            optimized_sql = sql
            plan = cached_plan.plan
            optimization_time = 0
        else:
            # No cached plan, optimize the query
            # Acquire lock for optimization and plan caching to ensure thread safety
            with self.lock:
                # Check again after acquiring the lock to prevent race conditions
                cached_plan = self.get_cached_plan(sql)
                if cached_plan:
                    # Another thread cached the plan while we were waiting for the lock
                    optimization_hints = []
                    optimized_sql = sql
                    plan = cached_plan.plan
                    optimization_time = 0
                else:
                    # Still no cached plan, optimize the query and cache it
                    optimization_start = time.time()
                    optimized_sql, optimization_hints = self.optimize_query(sql)
                    plan = self.get_query_plan(con, optimized_sql)
                    optimization_time = time.time() - optimization_start
        
        # Execute the optimized query
        execution_start = time.time()
        result = None
        try:
            result = con.execute(optimized_sql).arrow()
        except Exception as e:
            # Check if the error is about missing tables
            if "Table with name" in str(e) and "does not exist" in str(e) and ensure_tables_callback:
                logger.warning(f"Table not found error, trying to register tables and retry: {e}")
                # Extract table references again - the first attempt might have missed some
                table_refs = self._extract_table_references(optimized_sql)
                if table_refs:
                    # Register the tables and retry
                    ensure_tables_callback(table_refs)
                    # Try again with the registered tables
                    result = con.execute(optimized_sql).arrow()
                else:
                    # If we can't extract table references, re-raise the original error
                    raise
            else:
                # Not a missing table error or no callback to register tables, re-raise
                raise
                
        execution_time = time.time() - execution_start
        
        # Deregister tables after query completion if callback provided
        if deregister_tables_callback and table_refs:
            try:
                deregister_tables_callback(table_refs)
                logger.debug(f"Deregistered {len(table_refs)} tables after query execution")
            except Exception as e:
                logger.warning(f"Error deregistering tables: {e}")
        
        # Cache the plan if it's not already cached
        if not cached_plan:
            self.add_to_cache(sql, plan, optimization_time, execution_time)
        
        # Prepare execution info
        execution_info = {
            "optimization_time": optimization_time,
            "execution_time": execution_time,
            "total_time": time.time() - start_time,
            "row_count": result.num_rows,
            "from_cache": cached_plan is not None,
            "plan": plan,
            "tables_referenced": table_refs
        }
        
        return result, optimization_hints, execution_info
    
    def _extract_table_references(self, sql: str) -> List[str]:
        """
        Extract table references from a SQL query
        
        Args:
            sql: SQL query
            
        Returns:
            List of table references
        """
        # First, fix any table references that might be function calls
        fixed_sql = self._fix_table_references(sql)
        
        # Simple regex-based extraction - handles common patterns
        # Find tables in FROM clauses - also handle table names with parentheses
        # This pattern accounts for cases like "_cache_nyc_yellow_taxi_(jan_2023)"
        # which should be treated as a table name, not a function call
        
        # First, search for potential function-like patterns that are actually table references
        func_table_pattern = re.compile(r'FROM\s+(_cache_)?([a-zA-Z0-9_]+)(\s*\([^)]*\))', re.IGNORECASE)
        func_matches = func_table_pattern.findall(sql)
        
        # Regular table reference pattern
        from_tables = re.findall(r'FROM\s+(_cache_)?([a-zA-Z0-9_]+)', fixed_sql, re.IGNORECASE)
        
        # Find tables in JOIN clauses
        join_tables = re.findall(r'JOIN\s+(_cache_)?([a-zA-Z0-9_]+)', fixed_sql, re.IGNORECASE)
        
        # Also check for function-like tables in JOIN clauses
        join_func_matches = re.findall(r'JOIN\s+(_cache_)?([a-zA-Z0-9_]+)(\s*\([^)]*\))', sql, re.IGNORECASE)
        
        # Process function-like matches to get the full table name with parentheses
        tables = []
        for prefix, table, params in func_matches:
            # Skip common SQL keywords that might be mistaken for tables
            if table.lower() in ('select', 'where', 'group', 'order', 'having', 'limit', 'offset'):
                continue
            
            # Extract parameter content
            param_content = re.sub(r'^\s*\(\s*|\s*\)\s*$', '', params).strip()
            if param_content:
                # Create both versions of the table name
                full_table_name = table + params.strip()
                tables.append(table)
                
                # Add the transformed name that will be used in the query
                clean_param = re.sub(r'[^a-zA-Z0-9_]', '_', param_content)
                transformed_name = f"{table}_{clean_param}"
                tables.append(transformed_name)
            else:
                tables.append(table)
                
        # Process join function-like matches similarly
        for prefix, table, params in join_func_matches:
            # Skip common SQL keywords
            if table.lower() in ('select', 'where', 'group', 'order', 'having', 'limit', 'offset'):
                continue
            
            # Extract parameter content
            param_content = re.sub(r'^\s*\(\s*|\s*\)\s*$', '', params).strip()
            if param_content:
                # Create both versions of the table name
                full_table_name = table + params.strip()
                tables.append(table)
                
                # Add the transformed name that will be used in the query
                clean_param = re.sub(r'[^a-zA-Z0-9_]', '_', param_content)
                transformed_name = f"{table}_{clean_param}"
                tables.append(transformed_name)
            else:
                tables.append(table)
        
        # Combine and process regular table references
        for prefix, table in from_tables + join_tables:
            # Skip common SQL keywords that might be mistaken for tables
            if table.lower() in ('select', 'where', 'group', 'order', 'having', 'limit', 'offset'):
                continue
            # Add the table name
            tables.append(table)
        
        # Remove duplicates while preserving order
        seen = set()
        return [t for t in tables if not (t in seen or seen.add(t))]
    
    def clear_cache(self) -> None:
        """Clear the query plan cache"""
        with self.lock:
            self.query_plans.clear()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Get statistics about the query plan cache
        
        Returns:
            Dictionary with cache statistics
        """
        with self.lock:
            if not self.query_plans:
                return {
                    "size": 0,
                    "hit_count": 0,
                    "avg_execution_time": 0,
                    "enabled": self.enable_cache
                }
                
            total_hits = sum(plan.hit_count for plan in self.query_plans.values())
            avg_execution = sum(plan.execution_time for plan in self.query_plans.values()) / len(self.query_plans)
            
            return {
                "size": len(self.query_plans),
                "max_size": self.query_plan_cache_size,
                "hit_count": total_hits,
                "avg_execution_time": avg_execution,
                "enabled": self.enable_cache
            }


def optimize_duckdb_connection(con: duckdb.DuckDBPyConnection, config: Any) -> None:
    """
    Apply optimal configuration to a DuckDB connection
    
    Args:
        con: DuckDB connection
        config: ArrowCache configuration
    """
    # Set memory limit
    if config["memory_limit"]:
        # Use 80% of the configured memory limit for DuckDB
        memory_limit = int(config["memory_limit"] * 0.8)
        memory_limit_str = f"{memory_limit // (1024*1024)}MB"
        con.execute(f"PRAGMA memory_limit='{memory_limit_str}'")
    
    # Set thread count
    if config["thread_count"] > 0:
        con.execute(f"PRAGMA threads={config['thread_count']}")
    
    # Enable progress bar for long-running queries
    con.execute("PRAGMA enable_progress_bar")
    
    # Enable object cache for better query performance
    con.execute("PRAGMA enable_object_cache")
    
    # Configure temp directory for spilling
    if config["spill_to_disk"] and config["spill_directory"]:
        con.execute(f"PRAGMA temp_directory='{config['spill_directory']}'")


def optimize_for_timeseries(con: duckdb.DuckDBPyConnection) -> None:
    """
    Apply optimizations specifically for time series data
    
    Args:
        con: DuckDB connection
    """
    # These are experimental settings specific to time series workloads
    con.execute("PRAGMA enable_object_cache")
    con.execute("PRAGMA memory_limit='75%'")  # Use more memory for time series operations


def optimize_for_joins(con: duckdb.DuckDBPyConnection) -> None:
    """
    Apply optimizations specifically for join-heavy queries
    
    Args:
        con: DuckDB connection
    """
    # These are experimental settings for join-heavy workloads
    con.execute("PRAGMA memory_limit='80%'")
    con.execute("PRAGMA window_mode='Parallel'")
    
    # Increase thread count for joins, but not too much to avoid thrashing
    import multiprocessing
    threads = min(multiprocessing.cpu_count(), 8)
    con.execute(f"PRAGMA threads={threads}")


def add_query_hints(sql: str, hints: Dict[str, Any]) -> str:
    """
    Add query hints to SQL statement using comments or DuckDB PRAGMA
    
    Args:
        sql: Original SQL query
        hints: Dictionary of hints
        
    Returns:
        SQL query with hints
    """
    if not hints:
        return sql
        
    pragma_statements = []
    
    # Convert hints to PRAGMA statements
    if hints.get("memory_limit"):
        pragma_statements.append(f"PRAGMA memory_limit='{hints['memory_limit']}'")
        
    if hints.get("threads"):
        pragma_statements.append(f"PRAGMA threads={hints['threads']}")
        
    if hints.get("force_index"):
        # This is a hypothetical pragma, not actually in DuckDB
        pragma_statements.append(f"PRAGMA force_index='{hints['force_index']}'")
    
    # Combine pragma statements with original SQL
    if pragma_statements:
        return ";\n".join(pragma_statements) + ";\n" + sql
    
    return sql


def explain_query(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    """
    Get a human-readable explanation of a query plan
    
    Args:
        con: DuckDB connection
        sql: SQL query
        
    Returns:
        Human-readable explanation
    """
    try:
        explain_sql = f"EXPLAIN {sql}"
        result = con.execute(explain_sql).fetchall()
        plan = "\n".join(str(row[0]) for row in result)
        
        # Extract key information from the plan
        scan_count = plan.count("SCAN")
        filter_count = plan.count("FILTER")
        join_count = plan.count("JOIN")
        hash_count = plan.count("HASH")
        sort_count = plan.count("SORT")
        
        explanation = [
            f"Query will perform approximately:",
            f"- {scan_count} table scans",
            f"- {filter_count} filters",
            f"- {join_count} joins",
            f"- {hash_count} hash operations",
            f"- {sort_count} sorting operations",
        ]
        
        # Add warnings
        if "CROSS PRODUCT" in plan:
            explanation.append("WARNING: Query uses a cross product which can be very expensive")
            
        if sort_count > 0 and "TOP" not in plan:
            explanation.append("WARNING: Query performs sorting without a LIMIT")
            
        if "FULL SCAN" in plan and scan_count > 2:
            explanation.append("WARNING: Multiple full table scans may indicate missing indexes")
        
        return "\n".join(explanation)
    except Exception as e:
        logger.warning(f"Failed to explain query: {e}")
        return "Unable to explain query" 