"""Arrow Cache MCP - Data Science Sandbox for MCP Agents

A production-grade data science sandbox for MCP agents that supports:
- Multiple datasets (arrow, pandas, geopandas, parquet, geoparquet)
- Memory-managed caching with disk spillover
- SQL querying via DuckDB
- Dataset management (add/remove/list)
- Data visualization and exploration capabilities
"""

from . import server
from .server import DataScienceSandbox
import asyncio

def main():
    """Main entry point for the package."""
    asyncio.run(server.main())

# Expose important items at package level for easy importing
__all__ = ['main', 'server', 'DataScienceSandbox']