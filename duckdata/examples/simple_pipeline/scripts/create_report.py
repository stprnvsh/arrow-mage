"""
Create final report for the PipeLink example pipeline.

This is the last node in the pipeline that creates a report using data from previous nodes.
"""
from pipelink import NodeContext
import pandas as pd
import numpy as np

def main():
    with NodeContext() as ctx:
        # Get input data
        transformed_data = ctx.get_input('transformed_data')
        analysis_results = ctx.get_input('analysis_results')
       
        
        # Create report
        report = pd.DataFrame({
            'value': [
                transformed_data['value'].mean(),
                transformed_data['value'].std(),
            ]
        })
        
        # Save output
        ctx.save_output('final_report', report, 'Final report with summary metrics')
        
        print("Report created successfully")

if __name__ == "__main__":
    main() 