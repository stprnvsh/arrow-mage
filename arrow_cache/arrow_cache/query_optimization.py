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
        
        # We no longer transform table references to avoid breaking valid table names
        # optimized_sql = self._fix_table_references(optimized_sql)
        
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
        # Don't modify the query at all - keep exact table names
        # This is a temporary fix to prevent incorrect transformations
        # that break queries with complex table names
        return sql
    
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
        
        # Extract table references and ensure they're registered
        table_refs = self._extract_table_references(sql)
        if table_refs and ensure_tables_callback:
            # Call back to the cache to ensure these tables are registered
            try:
                ensure_tables_callback(table_refs)
                logger.debug(f"Ensured tables are registered: {table_refs}")
            except Exception as e:
                logger.warning(f"Error ensuring tables are registered: {e}")
            
        cached_plan = self.get_cached_plan(sql)
        
        if cached_plan:
            # We have a cached plan, use it
            optimization_hints = []
            optimized_sql = sql
            plan = cached_plan.plan
            optimization_time = 0
        else:
            # No cached plan, optimize the query
            with self.lock:
                cached_plan = self.get_cached_plan(sql)
                if cached_plan:
                    # Another thread cached the plan while we were waiting
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
        
        # Update SQL to properly quote table names with special characters
        modified_sql = optimized_sql
        
        # First try to handle direct table references with parentheses
        # Look for unquoted tables with the cache_ prefix and parentheses pattern
        for table_ref in table_refs:
            # If the table reference contains parentheses and isn't already quoted
            if '(' in table_ref and ')' in table_ref and f'"{table_ref}"' not in modified_sql:
                # Replace unquoted reference with quoted reference
                # Use regex to match the table reference precisely
                escaped_ref = re.escape(table_ref)
                modified_sql = re.sub(
                    fr'(FROM|JOIN)\s+{escaped_ref}(?!\s*AS|\s+[a-zA-Z]|\s*")',
                    fr'\1 "{table_ref}"',
                    modified_sql,
                    flags=re.IGNORECASE
                )
        
        # Also do a general pass to catch any table with parentheses
        # This regex looks for any table_name(param) pattern that's not quoted
        table_with_paren_pattern = re.compile(r'(FROM|JOIN)\s+([a-zA-Z0-9_]+\s*\([^)]*\))(?!\s*AS|\s+[a-zA-Z]|\s*")', re.IGNORECASE)
        for match in table_with_paren_pattern.finditer(optimized_sql):
            table_with_params = match.group(2).strip()
            # Add quotes if not already quoted
            if f'"{table_with_params}"' not in modified_sql:
                escaped_ref = re.escape(table_with_params)
                modified_sql = re.sub(
                    fr'(FROM|JOIN)\s+{escaped_ref}(?!\s*AS|\s+[a-zA-Z]|\s*")',
                    fr'\1 "{table_with_params}"',
                    modified_sql,
                    flags=re.IGNORECASE
                )
        
        # Execute the optimized query
        execution_start = time.time()
        result = None
        try:
            # Try to execute the query with quoted table names
            logger.debug(f"Executing SQL with quoted table names: {modified_sql}")
            result = con.execute(modified_sql).arrow()
        except Exception as first_error:
            error_msg = str(first_error)
            logger.warning(f"Error executing query: {error_msg}")
            
            # Check if the error is about missing tables
            if "Table with name" in error_msg and "does not exist" in error_msg and ensure_tables_callback:
                # Extract the table name from the error message
                table_name_match = re.search(r"Table with name ([^\s]+) does not exist", error_msg)
                if table_name_match:
                    missing_table = table_name_match.group(1).strip('"')
                    logger.info(f"Detected missing table: {missing_table}")
                    
                    # Try to find a matching table with original parentheses syntax
                    original_table_refs = self._extract_table_references(sql)
                    for ref in original_table_refs:
                        # Check if this is a cache table with parentheses
                        if '(' in ref and ')' in ref:
                            logger.info(f"Original table reference with parentheses: {ref}")
                            try:
                                # Try to register this specific table
                                ensure_tables_callback([ref])
                                logger.info(f"Registered table with original format: {ref}")
                            except Exception as e:
                                logger.warning(f"Failed to register table {ref}: {e}")
                
                # Try to register all tables again
                try:
                    table_refs = self._extract_table_references(optimized_sql)
                    if table_refs:
                        ensure_tables_callback(table_refs)
                        # Try again with the registered tables and quoted table names
                        result = con.execute(modified_sql).arrow()
                    else:
                        # If we can't extract table references, re-raise the original error
                        raise first_error
                except Exception as retry_error:
                    # If we still get an error, try with the original SQL
                    logger.warning(f"Retry failed, attempting with original SQL: {retry_error}")
                    try:
                        # Try with the original SQL but with quoted table names
                        original_with_quotes = sql
                        for table_ref in table_refs:
                            if '(' in table_ref and ')' in table_ref and f'"{table_ref}"' not in original_with_quotes:
                                escaped_ref = re.escape(table_ref)
                                original_with_quotes = re.sub(
                                    fr'(FROM|JOIN)\s+{escaped_ref}(?!\s*AS|\s+[a-zA-Z]|\s*")',
                                    fr'\1 "{table_ref}"',
                                    original_with_quotes,
                                    flags=re.IGNORECASE
                                )
                        
                        # Also apply the general pattern to catch any missed tables with parentheses
                        table_with_paren_pattern = re.compile(r'(FROM|JOIN)\s+([a-zA-Z0-9_]+\s*\([^)]*\))(?!\s*AS|\s+[a-zA-Z]|\s*")', re.IGNORECASE)
                        for match in table_with_paren_pattern.finditer(sql):
                            table_with_params = match.group(2).strip()
                            # Add quotes if not already quoted
                            if f'"{table_with_params}"' not in original_with_quotes:
                                escaped_ref = re.escape(table_with_params)
                                original_with_quotes = re.sub(
                                    fr'(FROM|JOIN)\s+{escaped_ref}(?!\s*AS|\s+[a-zA-Z]|\s*")',
                                    fr'\1 "{table_with_params}"',
                                    original_with_quotes,
                                    flags=re.IGNORECASE
                                )
                                
                        logger.debug(f"Trying original SQL with quotes: {original_with_quotes}")
                        result = con.execute(original_with_quotes).arrow()
                    except Exception:
                        # Last resort: try directly replacing the problematic pattern
                        try:
                            # Handle specific pattern cache_name(param) -> "cache_name(param)"
                            direct_fix = re.sub(
                                r'(FROM|JOIN)\s+((?:_)?cache_[a-zA-Z0-9_]+\s*\([^)]*\))',
                                r'\1 "\2"',
                                sql,
                                flags=re.IGNORECASE
                            )
                            logger.debug(f"Trying direct pattern replacement: {direct_fix}")
                            result = con.execute(direct_fix).arrow()
                        except Exception:
                            # Very last resort: try the original unmodified query
                            result = con.execute(sql).arrow()
            else:
                # Not a missing table error or no callback to register tables, re-raise
                raise first_error
                
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
        tables = []
        
        # First look for both forms of cache table patterns with parentheses
        # Format: _cache_name(param) or cache_name(param)
        cache_pattern = re.compile(r'(FROM|JOIN)\s+((?:_)?cache_[a-zA-Z0-9_]+\s*\([^)]*\))', re.IGNORECASE)
        for match in cache_pattern.finditer(sql):
            table_with_params = match.group(2).strip()
            tables.append(table_with_params)
            
        # Also look for quoted table references that may have special characters
        # Format: "table_name" or "_cache_name" or "cache_name"
        quoted_pattern = re.compile(r'(FROM|JOIN)\s+"([^"]+)"', re.IGNORECASE)
        for match in quoted_pattern.finditer(sql):
            table_name = match.group(2).strip()
            if (table_name.startswith('_cache_') or table_name.startswith('cache_')) and table_name not in tables:
                tables.append(table_name)
        
        # Standard table references without parentheses
        std_pattern = re.compile(r'(FROM|JOIN)\s+([a-zA-Z0-9_\.]+)(?!\s*\(|\s*")', re.IGNORECASE)
        for match in std_pattern.finditer(sql):
            table_name = match.group(2).strip()
            # Skip if it's a SQL keyword
            if table_name.lower() in ('select', 'where', 'group', 'order', 'having', 'limit', 'offset'):
                continue
            # For cache tables, include them
            if (table_name.startswith('_cache_') or table_name.startswith('cache_')) and table_name not in tables:
                tables.append(table_name)
        
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


def extract_table_references(sql):
    """
    Extract table references from a SQL query
    
    Args:
        sql: SQL query
        
    Returns:
        List of table references
    """
    # Extract tables using a combination of regex patterns
    tables = []
    
    # Special pattern for function-like syntax: table_(params)
    function_pattern = re.compile(r'(FROM|JOIN)\s+(_?cache_[a-zA-Z0-9_]+)_?\s*\(([^)]*)\)', re.IGNORECASE)
    for match in function_pattern.finditer(sql):
        # Reconstruct the full table name with parentheses
        table_base = match.group(2)
        params = match.group(3)
        
        # Handle trailing underscore
        if table_base.endswith('_'):
            full_table = f"{table_base[:-1]}({params})"
        else:
            full_table = f"{table_base}({params})"
            
        # Add to our tables list
        if full_table not in tables:
            tables.append(full_table)
    
    # Regular expression patterns to match table names in different SQL contexts
    patterns = [
        # FROM clause
        r'FROM\s+(?:"|`)?(_?cache_[a-zA-Z0-9_]+(?:\s*\([^)]*\))?)(?:"|`)?',
        # JOIN clause 
        r'JOIN\s+(?:"|`)?(_?cache_[a-zA-Z0-9_]+(?:\s*\([^)]*\))?)(?:"|`)?',
        # UPDATE clause
        r'UPDATE\s+(?:"|`)?(_?cache_[a-zA-Z0-9_]+(?:\s*\([^)]*\))?)(?:"|`)?',
        # INSERT INTO clause
        r'INSERT\s+INTO\s+(?:"|`)?(_?cache_[a-zA-Z0-9_]+(?:\s*\([^)]*\))?)(?:"|`)?',
    ]
    
    for pattern in patterns:
        matches = re.finditer(pattern, sql, re.IGNORECASE)
        for match in matches:
            table_ref = match.group(1).strip()
            # Skip if already added
            if table_ref not in tables:
                tables.append(table_ref)
    
    # Filter out SQL keywords that might be mistaken for tables
    sql_keywords = {'select', 'from', 'where', 'group', 'order', 'by', 'having', 
                   'join', 'inner', 'outer', 'left', 'right', 'cross', 'limit'}
    tables = [t for t in tables if t.lower() not in sql_keywords]
    
    return tables


def prepare_query_for_execution(conn, sql, table_registry_callback=None):
    """
    Prepare a SQL query for execution by ensuring tables are registered
    and properly quoted.
    
    Args:
        conn: DuckDB connection
        sql: SQL query
        table_registry_callback: Callback to register tables
        
    Returns:
        Prepared SQL query
    """
    # Extract table references
    table_refs = extract_table_references(sql)
    
    # Register tables if needed
    if table_registry_callback and table_refs:
        table_registry_callback(table_refs)
    
    # Special case: Handle function-like syntax where table_(params) is used
    # This pattern captures: FROM table_(params) or JOIN table_(params)
    function_pattern = re.compile(r'(FROM|JOIN)\s+(_?cache_[a-zA-Z0-9_]+)_?\s*\(([^)]*)\)', re.IGNORECASE)
    
    # Replace function-like references with properly quoted table references
    modified_sql = sql
    for match in function_pattern.finditer(sql):
        clause = match.group(1)      # FROM or JOIN
        table_base = match.group(2)  # table name without parameters
        params = match.group(3)      # parameters inside parentheses
        
        # Build the proper quoted reference
        if table_base.endswith('_'):
            # Handle trailing underscore case: FROM table_(params) -> FROM "table(params)"
            quoted_ref = f'"{table_base[:-1]}({params})"'
        else:
            # Standard case: FROM table(params) -> FROM "table(params)"
            quoted_ref = f'"{table_base}({params})"'
        
        # Replace in the SQL (match the whole pattern)
        original_text = match.group(0)
        replacement = f"{clause} {quoted_ref}"
        modified_sql = modified_sql.replace(original_text, replacement, 1)
    
    # Now handle other cases where table names need quoting
    for table_ref in table_refs:
        if '(' in table_ref and ')' in table_ref and not (table_ref.startswith('"') and table_ref.endswith('"')):
            # Need to quote this table reference, but only if not already handled by the function pattern
            quoted_ref = f'"{table_ref}"'
            
            # Replace unquoted references with quoted ones, only for whole words
            pattern = r'(FROM|JOIN|UPDATE|INSERT\s+INTO)\s+' + re.escape(table_ref) + r'\b'
            replacement = r'\1 ' + quoted_ref
            
            # Only replace if not already replaced by the function pattern
            if re.search(pattern, modified_sql, re.IGNORECASE):
                modified_sql = re.sub(pattern, replacement, modified_sql, flags=re.IGNORECASE)
    
    return modified_sql


def optimize_and_execute(conn, sql, ensure_tables_callback=None, deregister_tables_callback=None):
    """
    Optimize and execute a SQL query.
    
    Args:
        conn: DuckDB connection
        sql: SQL query
        ensure_tables_callback: Callback to ensure tables are registered
        deregister_tables_callback: Callback to deregister tables after execution
        
    Returns:
        Tuple of (result table, optimization hints, execution info)
    """
    # First, prepare the query by ensuring tables are registered and properly quoted
    prepared_sql = prepare_query_for_execution(conn, sql, ensure_tables_callback)
    
    # Execute the prepared query
    try:
        start_time = time.time()
        result = conn.execute(prepared_sql).arrow()
        execution_time = time.time() - start_time
        execution_info = {"execution_time": execution_time}
        
        # Deregister tables if requested
        if deregister_tables_callback:
            table_refs = extract_table_references(sql)
            deregister_tables_callback(table_refs)
        
        return result, [], execution_info
    except Exception as e:
        logger.warning(f"Error executing query: {e}")
        raise 