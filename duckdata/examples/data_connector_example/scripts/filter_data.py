#!/usr/bin/env python3
"""
PipeLink Example: Filter Data Node

This script filters the input data based on a minimum quantity value.
"""

import sys
from pipelink.python.node_utils import NodeContext

def main():
    # Create node context from metadata file passed as argument
    with NodeContext() as ctx:
        # Get the input data
        df = ctx.get_input("raw_data")
        
        # Get the filter parameter
        min_value = ctx.get_param("min_value", 0)
        
        # Filter the data
        filtered_df = df[df['quantity'] >= min_value]
        
        # Save the filtered data
        ctx.save_output(
            "filtered_data", 
            filtered_df, 
            f"Data filtered by quantity >= {min_value}"
        )
        
        print(f"Filtered data from {len(df)} to {len(filtered_df)} rows")

if __name__ == "__main__":
    main() 