"""
Integration tests for cross-component interactions in PipeLink.
"""
import os
import sys
import pytest
import yaml
import pandas as pd
import numpy as np
import time
from pathlib import Path
from pipelink.python.pipelink import run_pipeline
from pipelink.crosslink import CrossLink

class TestCrossComponentIntegration:
    """Tests for cross-component interactions in PipeLink."""
    
    def test_data_flow_between_nodes(self, test_dir, create_pipeline_config, sample_dataframe):
        """Test data flow between multiple nodes in a pipeline."""
        # Create scripts for a multi-node pipeline
        # Node 1: Generate data
        generator_script = os.path.join(test_dir, "data_generator.py")
        with open(generator_script, 'w') as f:
            f.write("""
import pandas as pd
import numpy as np
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Generate data
        df = pd.DataFrame({
            'id': range(100),
            'value': np.random.normal(0, 1, 100),
            'category': np.random.choice(['A', 'B', 'C'], size=100)
        })
        # Save output
        ctx.save_output('raw_data', df, 'Raw data generated')
        print(f"Generated {len(df)} rows of data")

if __name__ == "__main__":
    main()
""")

        # Node 2: Transform data
        transform_script = os.path.join(test_dir, "data_transformer.py")
        with open(transform_script, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Get input data
        df = ctx.get_input('raw_data')
        
        # Transform data
        df['value_squared'] = df['value'] ** 2
        df['category'] = df['category'].astype('category')
        
        # Save transformed data
        ctx.save_output('transformed_data', df, 'Transformed data')
        print(f"Transformed {len(df)} rows of data")

if __name__ == "__main__":
    main()
""")

        # Node 3: Analyze data
        analyze_script = os.path.join(test_dir, "data_analyzer.py")
        with open(analyze_script, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Get transformed data
        df = ctx.get_input('transformed_data')
        
        # Analyze data - group by category
        result = df.groupby('category').agg({
            'id': 'count',
            'value': ['mean', 'std'],
            'value_squared': ['mean', 'std']
        }).reset_index()
        
        # Flatten the MultiIndex columns
        result.columns = ['_'.join(col).rstrip('_') for col in result.columns.values]
        
        # Save analysis results
        ctx.save_output('analysis_results', result, 'Analysis results')
        print(f"Generated analysis with {len(result)} rows")

if __name__ == "__main__":
    main()
""")

        # Create pipeline configuration
        nodes = [
            {
                "id": "generate",
                "language": "python",
                "script": generator_script,
                "outputs": ["raw_data"]
            },
            {
                "id": "transform",
                "language": "python",
                "script": transform_script,
                "inputs": ["raw_data"],
                "outputs": ["transformed_data"]
            },
            {
                "id": "analyze",
                "language": "python",
                "script": analyze_script,
                "inputs": ["transformed_data"],
                "outputs": ["analysis_results"]
            }
        ]
        
        pipeline_name = "data_flow_test"
        db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
        config = create_pipeline_config(pipeline_name, nodes, db_path)
        
        config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Run the pipeline
        result = run_pipeline(config_path)
        
        # Verify pipeline execution
        assert result['success'], "Pipeline execution should succeed"
        
        # Check data flow using CrossLink
        cl = CrossLink(db_path=db_path)
        
        # Check that all outputs were created
        raw_data = cl.pull("raw_data")
        transformed_data = cl.pull("transformed_data")
        analysis_results = cl.pull("analysis_results")
        
        assert raw_data is not None, "Raw data should be present"
        assert transformed_data is not None, "Transformed data should be present"
        assert analysis_results is not None, "Analysis results should be present"
        
        # Verify data transformations
        assert 'value_squared' in transformed_data.columns, "Transformed data should have value_squared column"
        assert len(analysis_results) <= 3, "Analysis results should have at most 3 rows (one per category)"
        
        # Clean up
        cl.close()
    
    def test_incremental_pipeline_execution(self, test_dir, create_pipeline_config):
        """Test executing a pipeline incrementally (one node at a time)."""
        # Create scripts for a multi-node pipeline
        # Node 1: Generate data
        generator_script = os.path.join(test_dir, "inc_generator.py")
        with open(generator_script, 'w') as f:
            f.write("""
import pandas as pd
import numpy as np
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Generate data
        df = pd.DataFrame({
            'id': range(50),
            'value': np.random.normal(0, 1, 50)
        })
        # Track execution with a marker file
        with open(f"{ctx.get_working_dir()}/generator_executed.txt", "w") as marker:
            marker.write("executed")
        # Save output
        ctx.save_output('inc_data', df, 'Incremental test data')

if __name__ == "__main__":
    main()
""")

        # Node 2: Process data
        processor_script = os.path.join(test_dir, "inc_processor.py")
        with open(processor_script, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Get input data
        df = ctx.get_input('inc_data')
        
        # Process data
        df['processed'] = True
        
        # Track execution with a marker file
        with open(f"{ctx.get_working_dir()}/processor_executed.txt", "w") as marker:
            marker.write("executed")
        
        # Save output
        ctx.save_output('processed_data', df, 'Processed data')

if __name__ == "__main__":
    main()
""")

        # Create pipeline configuration
        nodes = [
            {
                "id": "generate",
                "language": "python",
                "script": generator_script,
                "outputs": ["inc_data"]
            },
            {
                "id": "process",
                "language": "python",
                "script": processor_script,
                "inputs": ["inc_data"],
                "outputs": ["processed_data"]
            }
        ]
        
        pipeline_name = "incremental_test"
        db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
        config = create_pipeline_config(pipeline_name, nodes, db_path)
        
        config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Run only the generator node
        result = run_pipeline(config_path, only_nodes=["generate"])
        
        # Verify generator execution
        assert result['success'], "Generator execution should succeed"
        assert os.path.exists(os.path.join(test_dir, "generator_executed.txt")), "Generator should have executed"
        assert not os.path.exists(os.path.join(test_dir, "processor_executed.txt")), "Processor should not have executed yet"
        
        # Check that only generator output exists
        cl = CrossLink(db_path=db_path)
        assert cl.exists("inc_data"), "Generator output should exist"
        assert not cl.exists("processed_data"), "Processor output should not exist yet"
        
        # Run the processor node
        result = run_pipeline(config_path, only_nodes=["process"])
        
        # Verify processor execution
        assert result['success'], "Processor execution should succeed"
        assert os.path.exists(os.path.join(test_dir, "processor_executed.txt")), "Processor should have executed"
        
        # Check that processor output now exists
        assert cl.exists("processed_data"), "Processor output should now exist"
        processed_data = cl.pull("processed_data")
        assert 'processed' in processed_data.columns, "Processed data should have 'processed' column"
        
        # Clean up
        cl.close()
    
    def test_pipeline_dependency_resolution(self, test_dir, create_pipeline_config):
        """Test that pipeline dependencies are correctly resolved and executed in order."""
        # Create a pipeline with complex dependencies
        scripts_dir = os.path.join(test_dir, "dep_scripts")
        os.makedirs(scripts_dir, exist_ok=True)
        
        # Create scripts for each node and record execution order
        execution_log = os.path.join(test_dir, "execution_order.txt")
        
        # Helper function to create node scripts
        def create_node_script(node_id, inputs=None, outputs=None):
            script_path = os.path.join(scripts_dir, f"{node_id}.py")
            with open(script_path, 'w') as f:
                f.write(f"""
import pandas as pd
import time
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Record execution order
        with open("{execution_log}", "a") as log:
            log.write("{node_id}\\n")
        
        # Small delay to ensure we can distinguish execution times
        time.sleep(0.1)
        
        # Process inputs if any
        data = pd.DataFrame({{'node_id': ['{node_id}'], 'timestamp': [time.time()]}})
        
        # If we have inputs, merge them
        input_list = {inputs or []}
        for input_name in input_list:
            if ctx.has_input(input_name):
                input_data = ctx.get_input(input_name)
                # Add this input data to our dataframe
                data = pd.concat([data, input_data])
        
        # Save outputs if any
        output_list = {outputs or []}
        for output_name in output_list:
            ctx.save_output(output_name, data, f"Output from {node_id}")

if __name__ == "__main__":
    main()
""")
            return script_path
        
        # Create a diamond dependency pattern: A -> B,C -> D
        script_a = create_node_script("A", outputs=["output_a"])
        script_b = create_node_script("B", inputs=["output_a"], outputs=["output_b"])
        script_c = create_node_script("C", inputs=["output_a"], outputs=["output_c"])
        script_d = create_node_script("D", inputs=["output_b", "output_c"], outputs=["output_d"])
        
        # Add another branch: A -> E -> F
        script_e = create_node_script("E", inputs=["output_a"], outputs=["output_e"])
        script_f = create_node_script("F", inputs=["output_e"], outputs=["output_f"])
        
        # Create pipeline configuration
        nodes = [
            # Intentionally define in non-dependency order
            {
                "id": "D",
                "language": "python",
                "script": script_d,
                "inputs": ["output_b", "output_c"],
                "outputs": ["output_d"]
            },
            {
                "id": "A",
                "language": "python",
                "script": script_a,
                "outputs": ["output_a"]
            },
            {
                "id": "C",
                "language": "python",
                "script": script_c,
                "inputs": ["output_a"],
                "outputs": ["output_c"]
            },
            {
                "id": "F",
                "language": "python",
                "script": script_f,
                "inputs": ["output_e"],
                "outputs": ["output_f"]
            },
            {
                "id": "B",
                "language": "python",
                "script": script_b,
                "inputs": ["output_a"],
                "outputs": ["output_b"]
            },
            {
                "id": "E",
                "language": "python",
                "script": script_e,
                "inputs": ["output_a"],
                "outputs": ["output_e"]
            }
        ]
        
        pipeline_name = "dependency_test"
        db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
        config = create_pipeline_config(pipeline_name, nodes, db_path)
        
        config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Clear the execution log
        with open(execution_log, "w") as f:
            f.write("")
        
        # Run the pipeline
        result = run_pipeline(config_path)
        
        # Verify pipeline execution
        assert result['success'], "Pipeline execution should succeed"
        
        # Read the execution order
        with open(execution_log, "r") as f:
            execution_order = [line.strip() for line in f.readlines()]
        
        # Verify topological ordering constraints
        # A must come before B, C, D, E, F
        a_idx = execution_order.index("A")
        # B must come before D
        b_idx = execution_order.index("B")
        # C must come before D
        c_idx = execution_order.index("C") 
        # E must come before F
        e_idx = execution_order.index("E")
        # D and F can be in any order relative to each other
        d_idx = execution_order.index("D")
        f_idx = execution_order.index("F")
        
        assert a_idx < b_idx, "A should execute before B"
        assert a_idx < c_idx, "A should execute before C"
        assert a_idx < e_idx, "A should execute before E"
        assert b_idx < d_idx, "B should execute before D"
        assert c_idx < d_idx, "C should execute before D"
        assert e_idx < f_idx, "E should execute before F"
    
    def test_error_propagation(self, test_dir, create_pipeline_config):
        """Test that errors in one node prevent dependent nodes from executing."""
        # Create scripts for a pipeline with an error in the middle
        # Node 1: Generate data
        generator_script = os.path.join(test_dir, "error_gen.py")
        with open(generator_script, 'w') as f:
            f.write("""
import pandas as pd
import numpy as np
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Generate data
        df = pd.DataFrame({
            'id': range(10),
            'value': np.random.normal(0, 1, 10)
        })
        # Track execution
        with open(f"{ctx.get_working_dir()}/error_gen_executed.txt", "w") as marker:
            marker.write("executed")
        # Save output
        ctx.save_output('error_data', df, 'Error test data')

if __name__ == "__main__":
    main()
""")

        # Node 2: Process data with error
        error_script = os.path.join(test_dir, "error_proc.py")
        with open(error_script, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Get input data
        df = ctx.get_input('error_data')
        
        # Track execution
        with open(f"{ctx.get_working_dir()}/error_proc_executed.txt", "w") as marker:
            marker.write("executed")
        
        # Deliberate error
        raise RuntimeError("Deliberate error in processor node")
        
        # This should not execute
        ctx.save_output('error_processed', df, 'This should not be saved')

if __name__ == "__main__":
    main()
""")

        # Node 3: Dependent on the error node
        downstream_script = os.path.join(test_dir, "error_downstream.py")
        with open(downstream_script, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # This should not execute because of the upstream error
        with open(f"{ctx.get_working_dir()}/error_downstream_executed.txt", "w") as marker:
            marker.write("executed")
        
        # Try to get input that won't exist due to upstream error
        df = ctx.get_input('error_processed')
        ctx.save_output('final_output', df, 'This should not be saved')

if __name__ == "__main__":
    main()
""")

        # Create pipeline configuration
        nodes = [
            {
                "id": "generator",
                "language": "python",
                "script": generator_script,
                "outputs": ["error_data"]
            },
            {
                "id": "processor",
                "language": "python",
                "script": error_script,
                "inputs": ["error_data"],
                "outputs": ["error_processed"]
            },
            {
                "id": "downstream",
                "language": "python",
                "script": downstream_script,
                "inputs": ["error_processed"],
                "outputs": ["final_output"]
            }
        ]
        
        pipeline_name = "error_propagation_test"
        db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
        config = create_pipeline_config(pipeline_name, nodes, db_path)
        
        config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Run the pipeline - it should fail
        result = run_pipeline(config_path)
        
        # Verify pipeline execution
        assert not result['success'], "Pipeline execution should fail"
        
        # Check which nodes executed
        assert os.path.exists(os.path.join(test_dir, "error_gen_executed.txt")), "Generator should have executed"
        assert os.path.exists(os.path.join(test_dir, "error_proc_executed.txt")), "Processor should have executed"
        assert not os.path.exists(os.path.join(test_dir, "error_downstream_executed.txt")), "Downstream should not have executed"
        
        # Check data outputs
        cl = CrossLink(db_path=db_path)
        assert cl.exists("error_data"), "Generator output should exist"
        assert not cl.exists("error_processed"), "Processor output should not exist due to error"
        assert not cl.exists("final_output"), "Downstream output should not exist"
        
        # Clean up
        cl.close()
    
    def test_parallel_node_execution(self, test_dir, create_pipeline_config):
        """Test that independent nodes can execute in parallel."""
        # Create a pipeline with independent branches
        scripts_dir = os.path.join(test_dir, "parallel_scripts")
        os.makedirs(scripts_dir, exist_ok=True)
        
        # Create a common start time file
        start_time_file = os.path.join(test_dir, "parallel_start_time.txt")
        with open(start_time_file, 'w') as f:
            f.write(str(time.time()))
        
        # Helper function to create node scripts with sleep
        def create_parallel_script(node_id, sleep_time, inputs=None, outputs=None):
            script_path = os.path.join(scripts_dir, f"{node_id}.py")
            with open(script_path, 'w') as f:
                f.write(f"""
import pandas as pd
import time
import os
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Read the common start time
        with open("{start_time_file}", "r") as f:
            start_time = float(f.read().strip())
        
        # Sleep for the specified time
        time.sleep({sleep_time})
        
        # Record execution time relative to start
        execution_time = time.time() - start_time
        
        # Save execution time to a file
        with open(f"{{ctx.get_working_dir()}}/{node_id}_time.txt", "w") as f:
            f.write(str(execution_time))
        
        # Create a simple dataframe
        data = pd.DataFrame({{'node_id': ['{node_id}'], 'execution_time': [execution_time]}})
        
        # Save outputs if any
        output_list = {outputs or []}
        for output_name in output_list:
            ctx.save_output(output_name, data, f"Output from {node_id}")

if __name__ == "__main__":
    main()
""")
            return script_path
        
        # Create parallel branches: A -> B1, B2, B3 (parallel) -> C
        # A is quick, B nodes sleep for different times, C depends on all B nodes
        script_a = create_parallel_script("A", 0.1, outputs=["output_a"])
        script_b1 = create_parallel_script("B1", 2.0, inputs=["output_a"], outputs=["output_b1"])
        script_b2 = create_parallel_script("B2", 2.0, inputs=["output_a"], outputs=["output_b2"])
        script_b3 = create_parallel_script("B3", 2.0, inputs=["output_a"], outputs=["output_b3"])
        script_c = create_parallel_script("C", 0.1, 
                                         inputs=["output_b1", "output_b2", "output_b3"], 
                                         outputs=["output_c"])
        
        # Create pipeline configuration with parallel execution enabled
        nodes = [
            {
                "id": "A",
                "language": "python",
                "script": script_a,
                "outputs": ["output_a"]
            },
            {
                "id": "B1",
                "language": "python",
                "script": script_b1,
                "inputs": ["output_a"],
                "outputs": ["output_b1"]
            },
            {
                "id": "B2",
                "language": "python",
                "script": script_b2,
                "inputs": ["output_a"],
                "outputs": ["output_b2"]
            },
            {
                "id": "B3",
                "language": "python",
                "script": script_b3,
                "inputs": ["output_a"],
                "outputs": ["output_b3"]
            },
            {
                "id": "C",
                "language": "python",
                "script": script_c,
                "inputs": ["output_b1", "output_b2", "output_b3"],
                "outputs": ["output_c"]
            }
        ]
        
        pipeline_name = "parallel_test"
        db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
        config = create_pipeline_config(pipeline_name, nodes, db_path)
        
        # Enable parallel execution
        config["parallel"] = True
        config["max_workers"] = 3  # Allow 3 parallel workers
        
        config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Update start time right before running
        with open(start_time_file, 'w') as f:
            f.write(str(time.time()))
        
        # Run the pipeline
        result = run_pipeline(config_path)
        
        # Verify pipeline execution
        assert result['success'], "Pipeline execution should succeed"
        
        # Read execution times
        b1_time_file = os.path.join(test_dir, "B1_time.txt")
        b2_time_file = os.path.join(test_dir, "B2_time.txt")
        b3_time_file = os.path.join(test_dir, "B3_time.txt")
        c_time_file = os.path.join(test_dir, "C_time.txt")
        
        assert os.path.exists(b1_time_file), "B1 should have executed"
        assert os.path.exists(b2_time_file), "B2 should have executed"
        assert os.path.exists(b3_time_file), "B3 should have executed"
        assert os.path.exists(c_time_file), "C should have executed"
        
        with open(b1_time_file, 'r') as f:
            b1_time = float(f.read().strip())
        with open(b2_time_file, 'r') as f:
            b2_time = float(f.read().strip())
        with open(b3_time_file, 'r') as f:
            b3_time = float(f.read().strip())
        with open(c_time_file, 'r') as f:
            c_time = float(f.read().strip())
        
        # Verify that B nodes executed in parallel (times should be similar)
        # and C executed after all B nodes
        parallel_threshold = 0.5  # Maximum expected time difference for parallel execution
        
        assert abs(b1_time - b2_time) < parallel_threshold, "B1 and B2 should execute in parallel"
        assert abs(b1_time - b3_time) < parallel_threshold, "B1 and B3 should execute in parallel"
        assert abs(b2_time - b3_time) < parallel_threshold, "B2 and B3 should execute in parallel"
        
        assert c_time > b1_time, "C should execute after B1"
        assert c_time > b2_time, "C should execute after B2"
        assert c_time > b3_time, "C should execute after B3"
    
    def test_crosslink_integration(self, test_dir, create_pipeline_config, sample_dataframe):
        """Test that PipeLink and CrossLink are properly integrated."""
        # Create a pipeline that uses CrossLink directly
        crosslink_script = os.path.join(test_dir, "crosslink_test.py")
        with open(crosslink_script, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext
from pipelink.crosslink import CrossLink

def main():
    with NodeContext() as ctx:
        # Get the database path from the context
        db_path = ctx.get_db_path()
        
        # Create a direct CrossLink connection to do custom operations
        cl = CrossLink(db_path=db_path)
        
        # Create a custom table outside the normal output mechanism
        cl.register_table("custom_table")
        custom_df = pd.DataFrame({
            'custom_id': range(10),
            'custom_value': range(10, 20)
        })
        cl.push(custom_df, name="custom_table", description="Custom table created directly")
        
        # Also use the standard output mechanism
        ctx.save_output('standard_output', custom_df, 'Standard output')
        
        # Close the direct connection
        cl.close()

if __name__ == "__main__":
    main()
""")

        # Create pipeline configuration
        nodes = [
            {
                "id": "crosslink_node",
                "language": "python",
                "script": crosslink_script,
                "outputs": ["standard_output"]
            }
        ]
        
        pipeline_name = "crosslink_integration_test"
        db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
        config = create_pipeline_config(pipeline_name, nodes, db_path)
        
        config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Run the pipeline
        result = run_pipeline(config_path)
        
        # Verify pipeline execution
        assert result['success'], "Pipeline execution should succeed"
        
        # Check both outputs using CrossLink
        cl = CrossLink(db_path=db_path)
        
        # Both tables should exist
        assert cl.exists("standard_output"), "Standard output should exist"
        assert cl.exists("custom_table"), "Custom table should exist"
        
        # Verify the data
        standard_df = cl.pull("standard_output")
        custom_df = cl.pull("custom_table")
        
        assert len(standard_df) == 10, "Standard output should have 10 rows"
        assert len(custom_df) == 10, "Custom table should have 10 rows"
        assert 'custom_id' in standard_df.columns, "Standard output should have custom_id column"
        assert 'custom_id' in custom_df.columns, "Custom table should have custom_id column"
        
        # Clean up
        cl.close() 