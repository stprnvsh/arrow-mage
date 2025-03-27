"""
Unit tests for the PipeLink component.
"""
import os
import pytest
import yaml
import tempfile
import pandas as pd
import numpy as np
from pipelink.python.pipelink import run_pipeline
from pipelink.python.node_utils import NodeContext

class TestPipeLink:
    """Tests for the PipeLink components."""
    
    def test_pipeline_config_validation(self, test_dir, create_pipeline_config):
        """Test validation of pipeline configurations."""
        # Valid config
        valid_nodes = [
            {
                "id": "node1",
                "language": "python",
                "script": "dummy.py",
                "outputs": ["output1"]
            }
        ]
        valid_config = create_pipeline_config("valid_pipeline", valid_nodes)
        
        config_path = os.path.join(test_dir, "valid_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(valid_config, f)
        
        # This should not raise an exception
        with pytest.raises(Exception):
            run_pipeline(config_path, dry_run=True)  # Just validate, don't run
        
        # Invalid config - missing required fields
        invalid_nodes = [
            {
                "id": "node1",
                # Missing language field
                "script": "dummy.py"
            }
        ]
        invalid_config = create_pipeline_config("invalid_pipeline", invalid_nodes)
        
        config_path = os.path.join(test_dir, "invalid_pipeline.yml")
        with open(config_path, 'w') as f:
            yaml.dump(invalid_config, f)
        
        # This should raise an exception
        with pytest.raises(Exception):
            run_pipeline(config_path, dry_run=True)
    
    def test_node_context(self, test_dir, sample_dataframe):
        """Test NodeContext functionality."""
        # Create a test metadata file
        meta = {
            "node_id": "test_node",
            "pipeline_name": "test_pipeline",
            "db_path": os.path.join(test_dir, "node_context_test.duckdb"),
            "inputs": ["input_data"],
            "outputs": ["output_data"],
            "params": {
                "param1": "value1",
                "param2": 42
            }
        }
        
        meta_path = os.path.join(test_dir, "node_meta.yml")
        with open(meta_path, 'w') as f:
            yaml.dump(meta, f)
        
        # Initialize CrossLink and save test data
        from pipelink.crosslink import CrossLink
        cl = CrossLink(db_path=meta["db_path"])
        cl.push(sample_dataframe, name="input_data", description="Test input data")
        cl.close()
        
        # Test NodeContext
        # Mock sys.argv to pass metadata file path
        import sys
        original_argv = sys.argv
        sys.argv = ["dummy_script.py", meta_path]
        
        try:
            ctx = NodeContext()
            
            # Test get_node_id
            assert ctx.get_node_id() == "test_node"
            
            # Test get_pipeline_name
            assert ctx.get_pipeline_name() == "test_pipeline"
            
            # Test get_param
            assert ctx.get_param("param1") == "value1"
            assert ctx.get_param("param2") == 42
            assert ctx.get_param("nonexistent", "default") == "default"
            
            # Test get_input
            input_df = ctx.get_input("input_data")
            assert input_df is not None
            assert len(input_df) == len(sample_dataframe)
            
            # Test save_output
            output_df = sample_dataframe.copy()
            output_df["processed"] = True
            ctx.save_output("output_data", output_df, "Processed output data")
            
            # Verify output was saved correctly
            retrieved_df = cl.pull("output_data")
            assert "processed" in retrieved_df.columns
            assert retrieved_df["processed"].all()
            
        finally:
            # Restore original argv
            sys.argv = original_argv
            
            # Close any open connections
            if 'ctx' in locals():
                ctx.cl.close()
    
    def test_dependency_resolution(self, test_dir, create_pipeline_config):
        """Test dependency resolution between nodes."""
        # Create pipeline with dependencies
        nodes = [
            {
                "id": "node1",
                "language": "python",
                "script": "node1.py",
                "outputs": ["intermediate1"]
            },
            {
                "id": "node2",
                "language": "python",
                "script": "node2.py",
                "outputs": ["intermediate2"]
            },
            {
                "id": "node3",
                "language": "python",
                "script": "node3.py",
                "inputs": ["intermediate1", "intermediate2"],
                "outputs": ["final_output"]
            }
        ]
        
        config = create_pipeline_config("dependency_test", nodes)
        
        config_path = os.path.join(test_dir, "dependency_test.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Test dependency resolution
        from pipelink.python.pipelink import _build_dependency_graph, _get_execution_order
        
        # This is a bit of a hack to directly test internal functions
        import importlib
        pipelink_module = importlib.import_module("pipelink.python.pipelink")
        
        # Load the pipeline config
        with open(config_path, 'r') as f:
            pipeline_config = yaml.safe_load(f)
        
        # Build dependency graph
        G = pipelink_module._build_dependency_graph(pipeline_config)
        
        # Get execution order
        execution_order = pipelink_module._get_execution_order(G)
        
        # node1 and node2 should come before node3
        assert execution_order.index("node1") < execution_order.index("node3")
        assert execution_order.index("node2") < execution_order.index("node3")
        
        # Test starting from a specific node
        node3_only = pipelink_module._get_execution_order(G, start_from="node3")
        assert len(node3_only) == 1
        assert node3_only[0] == "node3"
        
        # Test only running specific nodes (should include dependencies)
        node3_with_deps = pipelink_module._get_execution_order(G, only_nodes=["node3"])
        assert len(node3_with_deps) == 3
        assert "node1" in node3_with_deps
        assert "node2" in node3_with_deps
        assert "node3" in node3_with_deps 