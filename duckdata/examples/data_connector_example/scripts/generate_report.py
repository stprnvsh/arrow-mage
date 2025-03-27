#!/usr/bin/env python3
"""
PipeLink Example: Generate Customer Report

This script combines high-value orders and customer data to generate a customer report.
"""

import sys
from pipelink.python.node_utils import NodeContext

def main():
    # Create node context from metadata file passed as argument
    with NodeContext() as ctx:
        # Get the input data
        orders_df = ctx.get_input("filtered_data")
       
        
        # Save the report
        ctx.save_output(
            "customer_report", 
            orders_df, 
            "Customer spending report with product purchases and spending tiers"
        )
        

if __name__ == "__main__":
    main() 