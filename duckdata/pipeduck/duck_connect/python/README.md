# DuckConnect - Python Implementation

This directory contains the Python implementation of DuckConnect, providing high-performance cross-language data sharing capabilities.

## Installation

The Python version of DuckConnect is installed automatically when you install the DuckData package:

```bash
# Install from the repository root
pip install -e .

# Or use the installation script for all languages
./install_all.sh
```

## Usage

### Basic Usage

```python
from pipeduck.duck_connect import DuckConnect

# Create a DuckConnect instance
dc = DuckConnect("my_database.duckdb")

# Register a pandas DataFrame
import pandas as pd
data = pd.DataFrame({
    'id': [1, 2, 3, 4, 5],
    'value': [10, 20, 30, 40, 50]
})
dc.register_dataset(data, name="my_dataset")

# Get a dataset
result = dc.get_dataset("my_dataset")
print(result)

# Execute a SQL query
query_result = dc.execute_query("SELECT * FROM my_dataset WHERE value > 25")
print(query_result)
```

### DuckContext for Pipeline Integration

```python
from pipeduck.duck_connect import DuckContext

# In a pipeline node
def process_node():
    # Initialize context
    ctx = DuckContext(db_path="pipeline.duckdb")
    
    # Get input data
    input_data = ctx.get_input("input_dataset")
    
    # Process data
    processed = input_data.copy()
    processed["doubled"] = processed["value"] * 2
    
    # Set output data
    ctx.set_output(processed, "output_dataset")
    
    return "Success"
```

### Working with Arrow Data

```python
from pipeduck.duck_connect import DuckConnect
import pyarrow as pa

# Create a DuckConnect instance
dc = DuckConnect("my_database.duckdb")

# Using Arrow Tables
import pyarrow as pa
array1 = pa.array([1, 2, 3, 4])
array2 = pa.array(['a', 'b', 'c', 'd'])
table = pa.Table.from_arrays([array1, array2], names=['numbers', 'letters'])

# Register an Arrow Table
dc.register_dataset(table, name="arrow_data")
```

### Tracking Data Lineage

```python
from pipeduck.duck_connect import DuckConnect

# Create a DuckConnect instance
dc = DuckConnect("my_database.duckdb")

# Register source datasets
dc.register_dataset(source1_df, name="source1")
dc.register_dataset(source2_df, name="source2")

# Process and create a new dataset
merged_df = process_and_merge(source1_df, source2_df)
target_id = dc.register_dataset(merged_df, name="merged_result")

# Track the lineage
dc.register_transformation(
    target_dataset_id=target_id,
    source_dataset_ids=["source1", "source2"],
    transformation="merged and aggregated"
)
```

## API Reference

### DuckConnect Class

```python
class DuckConnect:
    def __init__(self, db_path, debug=False):
        """Initialize DuckConnect with a database file."""
        
    def register_dataset(self, data, name, description=None, 
                        available_to_languages=None, overwrite=False):
        """Register a dataset in the database."""
        
    def get_dataset(self, id_or_name):
        """Retrieve a dataset by ID or name."""
        
    def update_dataset(self, data, id_or_name):
        """Update an existing dataset."""
        
    def delete_dataset(self, id_or_name):
        """Delete a dataset."""
        
    def list_datasets(self, language=None):
        """List all available datasets."""
        
    def execute_query(self, query, params=None):
        """Execute a SQL query against the database."""
        
    def register_transformation(self, target_dataset_id, source_dataset_ids, 
                               transformation):
        """Register a transformation for lineage tracking."""
```

### DuckContext Class

```python
class DuckContext:
    def __init__(self, db_path=None, meta_path=None):
        """Initialize a DuckContext for pipeline nodes."""
        
    def get_input(self, name, required=True):
        """Get an input dataset."""
        
    def set_output(self, data, name):
        """Set an output dataset."""
```

## Python-Specific Features

- **Pandas Integration**: Seamless conversion between DuckDB tables and pandas DataFrames
- **PyArrow Support**: Efficient handling of Arrow Tables and Arrays
- **NumPy Compatibility**: Direct conversion to/from NumPy arrays
- **Context Manager**: Use DuckConnect as a context manager for automatic cleanup

## Dependencies

- duckdb
- pandas
- pyarrow
- numpy
- uuid

## Performance Considerations

- For large datasets, consider using PyArrow tables instead of pandas DataFrames
- Use SQL queries for filtering/aggregation when possible (pushdown to DuckDB)
- For complex transformations, consider registering intermediate datasets

## Troubleshooting

### Common Issues

1. **Connection Issues**: If you encounter connection problems, ensure the database path is accessible and has the correct permissions.

2. **Missing Dependencies**: Make sure all required packages are installed with `pip install "duckdata[all]"`.

3. **Schema Mismatches**: When updating datasets, ensure the schema is compatible with the existing data.

## Contributing

Contributions to the Python implementation are welcome! Please see the main project README for guidelines. 