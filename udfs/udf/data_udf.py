import fused
import pandas as pd

@fused.udf(
    metadata={
        "fused:mcp": {
            "description": "Sample Data UDF - Returns a sample dataset with numeric data",
            "parameters": [
                {
                    "name": "rows",
                    "type": "integer",
                    "description": "Number of rows to generate",
                    "required": True
                }
            ]
        }
    }
)
def sample_data(rows=10):
    """Returns a sample dataset with the specified number of rows."""
    import numpy as np
    
    # Generate random data
    data = {
        'id': range(1, rows + 1),
        'value_a': np.random.rand(rows) * 100,
        'value_b': np.random.rand(rows) * 100,
        'category': np.random.choice(['A', 'B', 'C'], size=rows)
    }
    
    # Create DataFrame
    df = pd.DataFrame(data)
    
    return df 