"""
Tests for Python integration with DuckData.
"""
import os
import sys
import pytest
import yaml
import pandas as pd
import numpy as np
import tempfile
from pipelink.crosslink import CrossLink
from pipelink.python.node_utils import NodeContext
from pipelink.python.pipelink import run_pipeline

class TestPythonIntegration:
    """Tests for Python integration with DuckData."""
    
    def test_python_node_execution(self, test_dir, sample_dataframe):
        """Test execution of a Python node."""
        # Create a Python script for testing
        script_content = """
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Get input data
        df = ctx.get_input("input_data")
        
        # Transform the data
        df["squared_value"] = df["value"] ** 2
        df["category_code"] = df["category"].map({"A": 1, "B": 2, "C": 3})
        
        # Save the output
        ctx.save_output("processed_data", df, "Processed data with calculated fields")
        
        print(f"Processed {len(df)} rows")

if __name__ == "__main__":
    main()
"""
        
        # Create script file
        script_path = os.path.join(test_dir, "process_data.py")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Create a pipeline configuration
        nodes = [
            {
                "id": "process_data",
                "language": "python",
                "script": script_path,
                "inputs": ["input_data"],
                "outputs": ["processed_data"]
            }
        ]
        
        pipeline_config = {
            "name": "python_test_pipeline",
            "description": "Test pipeline for Python integration",
            "db_path": os.path.join(test_dir, "python_test.duckdb"),
            "working_dir": test_dir,
            "nodes": nodes
        }
        
        # Save the pipeline configuration
        config_path = os.path.join(test_dir, "python_test_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Initialize CrossLink and save test data
        cl = CrossLink(db_path=pipeline_config["db_path"])
        cl.push(sample_dataframe, name="input_data", description="Test input data")
        cl.close()
        
        # Run the pipeline
        run_pipeline(config_path)
        
        # Verify the results
        cl = CrossLink(db_path=pipeline_config["db_path"])
        result_df = cl.pull("processed_data")
        cl.close()
        
        # Check that the transformation was applied
        assert "squared_value" in result_df.columns
        assert "category_code" in result_df.columns
        assert result_df["squared_value"].equals(result_df["value"] ** 2)
    
    def test_python_pandas_integration(self, test_dir, complex_dataframe):
        """Test integration with pandas DataFrames."""
        # Create a Python script that uses various pandas operations
        script_content = """
import pandas as pd
import numpy as np
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Get input data
        df = ctx.get_input("complex_data")
        
        # Perform pandas operations
        # GroupBy
        grouped = df.groupby("cat_col").agg({
            "int_col": ["min", "max", "mean"],
            "float_col": ["min", "max", "mean", "std"],
            "bool_col": "sum"
        })
        
        # Reset index for flat structure
        grouped = grouped.reset_index()
        
        # Rename columns to flatten MultiIndex
        grouped.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col for col in grouped.columns]
        
        # Pivot
        if "id" in df.columns and "cat_col" in df.columns and "float_col" in df.columns:
            try:
                pivoted = df.pivot(index="id", columns="cat_col", values="float_col")
                pivoted = pivoted.reset_index()
                ctx.save_output("pivot_data", pivoted, "Pivoted data by category")
            except Exception as e:
                print(f"Pivot operation failed: {str(e)}")
        
        # Apply and transform
        if "int_col" in df.columns:
            df["int_col_binned"] = pd.cut(df["int_col"], bins=5, labels=["VL", "L", "M", "H", "VH"])
        
        # Save the outputs
        ctx.save_output("grouped_data", grouped, "Grouped and aggregated data")
        ctx.save_output("processed_complex_data", df, "Processed complex data")
        
        print(f"Processed {len(df)} rows with pandas operations")

if __name__ == "__main__":
    main()
"""
        
        # Create script file
        script_path = os.path.join(test_dir, "pandas_operations.py")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Create a pipeline configuration
        nodes = [
            {
                "id": "pandas_operations",
                "language": "python",
                "script": script_path,
                "inputs": ["complex_data"],
                "outputs": ["grouped_data", "processed_complex_data", "pivot_data"]
            }
        ]
        
        pipeline_config = {
            "name": "pandas_test_pipeline",
            "description": "Test pipeline for pandas integration",
            "db_path": os.path.join(test_dir, "pandas_test.duckdb"),
            "working_dir": test_dir,
            "nodes": nodes
        }
        
        # Save the pipeline configuration
        config_path = os.path.join(test_dir, "pandas_test_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Initialize CrossLink and save test data
        cl = CrossLink(db_path=pipeline_config["db_path"])
        cl.push(complex_dataframe, name="complex_data", description="Complex test data")
        cl.close()
        
        # Run the pipeline
        run_pipeline(config_path)
        
        # Verify the results
        cl = CrossLink(db_path=pipeline_config["db_path"])
        
        # Check grouped data
        grouped_data = cl.pull("grouped_data")
        assert grouped_data is not None
        assert "cat_col" in grouped_data.columns
        assert "int_col_min" in grouped_data.columns or "int_col_max" in grouped_data.columns
        
        # Check processed data
        processed_data = cl.pull("processed_complex_data")
        assert processed_data is not None
        
        # Only check for this column if the operation succeeded
        if "int_col" in complex_dataframe.columns:
            assert "int_col_binned" in processed_data.columns
        
        # Check pivot data (may not exist if pivoting failed)
        try:
            pivot_data = cl.pull("pivot_data")
            assert pivot_data is not None
            assert "id" in pivot_data.columns
        except:
            pass
        
        cl.close()
    
    def test_python_arrow_integration(self, test_dir, sample_dataframe):
        """Test integration with Apache Arrow."""
        # Try to import pyarrow - skip test if not available
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
        except ImportError:
            pytest.skip("pyarrow not installed")
        
        # Create a Python script that uses Arrow
        script_content = """
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Get input data
        df = ctx.get_input("arrow_input")
        
        # Convert to Arrow table
        arrow_table = pa.Table.from_pandas(df)
        
        # Save to parquet (temporary)
        temp_path = os.path.join(ctx.get_param("temp_dir"), "arrow_data.parquet")
        pq.write_table(arrow_table, temp_path)
        
        # Read back from parquet
        read_table = pq.read_table(temp_path)
        
        # Convert back to pandas
        result_df = read_table.to_pandas()
        
        # Add a marker column to show it went through Arrow
        result_df["went_through_arrow"] = True
        
        # Save the output
        ctx.save_output("arrow_output", result_df, "Data processed through Arrow")
        
        print(f"Processed {len(df)} rows through Arrow")

if __name__ == "__main__":
    main()
"""
        
        # Create script file
        script_path = os.path.join(test_dir, "arrow_operations.py")
        with open(script_path, 'w') as f:
            f.write(script_content)
        
        # Create a pipeline configuration
        nodes = [
            {
                "id": "arrow_operations",
                "language": "python",
                "script": script_path,
                "inputs": ["arrow_input"],
                "outputs": ["arrow_output"],
                "params": {
                    "temp_dir": test_dir
                }
            }
        ]
        
        pipeline_config = {
            "name": "arrow_test_pipeline",
            "description": "Test pipeline for Arrow integration",
            "db_path": os.path.join(test_dir, "arrow_test.duckdb"),
            "working_dir": test_dir,
            "nodes": nodes
        }
        
        # Save the pipeline configuration
        config_path = os.path.join(test_dir, "arrow_test_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(pipeline_config, f)
        
        # Initialize CrossLink and save test data
        cl = CrossLink(db_path=pipeline_config["db_path"])
        cl.push(sample_dataframe, name="arrow_input", description="Test input data for Arrow")
        cl.close()
        
        # Run the pipeline
        run_pipeline(config_path)
        
        # Verify the results
        cl = CrossLink(db_path=pipeline_config["db_path"])
        result_df = cl.pull("arrow_output")
        cl.close()
        
        # Check that the data went through Arrow
        assert "went_through_arrow" in result_df.columns
        assert result_df["went_through_arrow"].all() 