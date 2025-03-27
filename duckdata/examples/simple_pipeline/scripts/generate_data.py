"""
Generate data for the PipeLink example pipeline.

This is the first node in the pipeline that generates random data.
"""
from pipelink import NodeContext
import pandas as pd
import numpy as np

def main():
    with NodeContext() as ctx:
        # Get parameters
        rows = ctx.get_param('rows', 100)
        
        # Generate random data
        df = pd.DataFrame({
            'id': range(rows),
            'value': np.random.normal(0, 1, rows),
            'category': np.random.choice(['A', 'B', 'C'], size=rows)
        })
        
        # Save output
        ctx.save_output('raw_data', df, 'Random data for demonstration')
        
        print(f"Generated {rows} rows of data")

if __name__ == "__main__":
    main() 