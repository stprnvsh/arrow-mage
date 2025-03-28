#!/usr/bin/env python3

"""
PipeDuck Node: Generate Sales Data
This node generates synthetic sales data and makes it available to the pipeline.
"""

import numpy as np
import pandas as pd
from datetime import datetime
import os
import sys

# Import DuckContext from pipeduck
from pipeduck.python.duck_context import DuckContext

def generate_sales_data(n_records=1000):
    """Generate synthetic sales data"""
    np.random.seed(42)  # For reproducibility
    
    # Generate dates for the last 90 days
    dates = pd.date_range(end=datetime.now(), periods=90)
    
    # Create random sales data
    data = {
        'date': np.random.choice(dates, n_records),
        'product_id': np.random.randint(1, 101, n_records),
        'quantity': np.random.randint(1, 11, n_records),
        'price': np.random.uniform(10.0, 1000.0, n_records).round(2),
        'customer_id': np.random.randint(1, 1001, n_records),
        'store_id': np.random.randint(1, 51, n_records)
    }
    
    # Calculate the revenue
    df = pd.DataFrame(data)
    df['revenue'] = df['quantity'] * df['price']
    
    return df

def main():
    """Main function to execute when the node is run"""
    print(f"Executing node: Generate Sales Data")
    
    # Get metadata path from command-line arguments (used by PipeDuck)
    meta_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    # Use DuckContext to manage the connection to DuckDB
    with DuckContext(meta_path=meta_path) as ctx:
        # Generate the sales data
        print("Generating synthetic sales data...")
        sales_data = generate_sales_data()
        print(f"Generated {len(sales_data)} records")
        
        # Set the output for the next node in the pipeline
        ctx.set_output(sales_data, "raw_sales")
        
        print(f"Node execution complete: {len(sales_data)} records created")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 