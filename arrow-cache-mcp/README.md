# Arrow Cache MCP

A powerful data science toolkit and MCP server leveraging Apache Arrow for efficient memory management and data processing.

## Features

- **High Performance Data Handling**: Efficient memory management using Apache Arrow
- **Intelligent Memory Management**: Automatic partitioning and compression of large datasets
- **Multiple File Format Support**: Load datasets from CSV, Parquet, Arrow, Feather, JSON, Excel, and more
- **SQL Query Capabilities**: Advanced SQL analytics with DuckDB integration
- **Visualization Support**: Create plots and charts from your datasets
- **AI-Powered Data Analysis**: Optional Claude integration for natural language queries
- **Memory Overflow Protection**: Automatic spilling to disk when memory limits are reached

## Components

### Resources

The server exposes cached datasets as resources:
- Custom `arrowcache://` URI scheme for accessing datasets
- Each dataset resource has detailed metadata about size, shape, and structure

### Tools

The server implements the following tools:

- **run_sql_query**: Execute SQL queries against cached datasets
  - Use `_cache_<dataset_name>` syntax in FROM clause
  
- **load_dataset**: Load a dataset from a file or URL into the cache
  - Support for many common file formats
  
- **get_dataset_sample**: Get a sample of rows from a dataset
  - Useful for previewing large datasets
  
- **get_dataset_info**: Get detailed information about a dataset
  - Schema, row counts, column statistics, etc.
  
- **remove_dataset**: Remove a dataset from the cache

- **create_plot**: Create visualizations from datasets
  - Support for various plot types (line, bar, scatter, etc.)
  
- **get_memory_usage**: Get detailed memory usage statistics

## Installation

```bash
pip install arrow-cache-mcp
```

With optional dependencies:

```bash
# For geospatial data support
pip install "arrow-cache-mcp[geospatial]"

# For enhanced visualization
pip install "arrow-cache-mcp[viz]"

# For all features
pip install "arrow-cache-mcp[geospatial,viz]"
```

## Configuration

### Environment Variables

- `ARROW_CACHE_MEMORY_LIMIT`: Maximum memory usage in bytes (default: 4GB)
- `ARROW_CACHE_SPILL_DIRECTORY`: Directory for spilling data to disk when memory limit is reached
- `ARROW_CACHE_SPILL_TO_DISK`: Whether to allow spilling to disk (true/false)
- `ANTHROPIC_API_KEY`: API key for Claude integration (optional)

## Quickstart

### Configure in Claude Desktop

On MacOS: `~/Library/Application\ Support/Claude/claude_desktop_config.json`
On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

<details>
  <summary>Development/Unpublished Servers Configuration</summary>
  
  ```json
  "mcpServers": {
    "arrow-cache-mcp": {
      "command": "uv",
      "args": [
        "--directory",
        "/PATH/TO/arrow-cache-mcp",
        "run",
        "arrow-cache-mcp"
      ]
    }
  }
  ```
</details>

<details>
  <summary>Published Servers Configuration</summary>
  
  ```json
  "mcpServers": {
    "arrow-cache-mcp": {
      "command": "uvx",
      "args": [
        "arrow-cache-mcp"
      ]
    }
  }
  ```
</details>

## Example Usage

1. Load a dataset:
```
I'd like to load the NYC Yellow Taxi dataset from January 2023
```

2. Query the data:
```
How many taxi trips were there per day of the week?
```

3. Create a visualization:
```
Create a bar chart showing average fare amount by day of week
```

## Development

### Building and Publishing

To prepare the package for distribution:

1. Sync dependencies and update lockfile:
```bash
uv sync
```

2. Build package distributions:
```bash
uv build
```

This will create source and wheel distributions in the `dist/` directory.

3. Publish to PyPI:
```bash
uv publish
```

Note: You'll need to set PyPI credentials via environment variables or command flags.

### Debugging

Since MCP servers run over stdio, debugging can be challenging. For the best debugging
experience, we recommend using the [MCP Inspector](https://github.com/modelcontextprotocol/inspector).

You can launch the MCP Inspector via [`npm`](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) with this command:

```bash
npx @modelcontextprotocol/inspector uv --directory /PATH/TO/arrow-cache-mcp run arrow-cache-mcp
```

Upon launching, the Inspector will display a URL that you can access in your browser to begin debugging.

## Architecture

Arrow Cache MCP builds on these key components:

1. **Arrow Cache**: A memory-managed caching system for Arrow tables
2. **DuckDB**: A high-performance analytical database
3. **PyArrow**: Python bindings for Apache Arrow
4. **MCP Protocol**: Model Context Protocol for AI agent integration

The system features smart memory management with automatic partitioning, spilling, and compression to efficiently handle datasets of any size while preventing out-of-memory errors.