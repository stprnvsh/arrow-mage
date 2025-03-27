"""
Generate data for the DAG pipeline example.

This is the initial node in the DAG pipeline that generates random data.
"""
from pipelink import NodeContext
import pandas as pd
import numpy as np

def main():
    with NodeContext() as ctx:
        # Get parameters
        rows = ctx.get_param('rows', 1000)
        
        # Generate random data
        df = pd.DataFrame({
            'id': range(rows),
            'value': np.random.normal(0, 1, rows),
            'category': np.random.choice(['A', 'B', 'C'], size=rows),
            'timestamp': pd.date_range(start='2023-01-01', periods=rows, freq='H')
        })
        
        # Add some missing values for the cleaning step
        mask = np.random.choice([True, False], size=rows, p=[0.05, 0.95])
        df.loc[mask, 'value'] = np.nan
        
        # Save output
        ctx.save_output('raw_data', df, 'Random data for DAG pipeline demonstration')
        
        print(f"Generated {rows} rows of data with timestamps and some missing values")

if __name__ == "__main__":
    main() 