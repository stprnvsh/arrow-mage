import fused
import pandas as pd

@fused.udf(
    metadata={
        "fused:mcp": {
            "description": "Hello World UDF - A simple test UDF to return a greeting message",
            "parameters": [
                {
                    "name": "name",
                    "type": "string",
                    "description": "Name to greet",
                    "required": True
                }
            ]
        }
    }
)
def hello_world(name="World"):
    """Returns a greeting message."""
    return f"Hello, {name}!" 