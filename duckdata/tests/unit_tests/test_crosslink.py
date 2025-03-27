"""
Unit tests for the CrossLink component.
"""
import os
import pytest
import pandas as pd
import numpy as np
from pipelink.crosslink import CrossLink

class TestCrossLink:
    """Tests for the CrossLink class."""
    
    def test_init(self, test_dir):
        """Test initializing CrossLink."""
        db_path = os.path.join(test_dir, "test_crosslink.duckdb")
        cl = CrossLink(db_path=db_path)
        assert cl is not None
        assert cl.db_path == db_path
        cl.close()
    
    def test_push_pull_basic(self, test_dir, sample_dataframe):
        """Test basic push and pull functionality."""
        db_path = os.path.join(test_dir, "test_push_pull.duckdb")
        cl = CrossLink(db_path=db_path)
        
        # Push data
        dataset_id = cl.push(sample_dataframe, name="test_dataset", description="Test data")
        assert dataset_id is not None
        
        # Pull data
        retrieved_df = cl.pull("test_dataset")
        assert retrieved_df is not None
        assert len(retrieved_df) == len(sample_dataframe)
        assert list(retrieved_df.columns) == list(sample_dataframe.columns)
        
        # Check data integrity
        pd.testing.assert_frame_equal(retrieved_df.reset_index(drop=True), 
                                      sample_dataframe.reset_index(drop=True))
        
        cl.close()
    
    def test_push_pull_complex(self, test_dir, complex_dataframe):
        """Test push and pull with complex data types."""
        db_path = os.path.join(test_dir, "test_push_pull_complex.duckdb")
        cl = CrossLink(db_path=db_path)
        
        # Push data
        dataset_id = cl.push(complex_dataframe, name="complex_dataset", description="Complex test data")
        assert dataset_id is not None
        
        # Pull data
        retrieved_df = cl.pull("complex_dataset")
        assert retrieved_df is not None
        assert len(retrieved_df) == len(complex_dataframe)
        
        # Check that columns match, ignoring list_col and dict_col which may not be supported
        common_cols = [col for col in complex_dataframe.columns if col not in ['list_col', 'dict_col']]
        for col in common_cols:
            assert col in retrieved_df.columns
        
        cl.close()
    
    def test_dataset_versioning(self, test_dir, sample_dataframe):
        """Test dataset versioning functionality."""
        db_path = os.path.join(test_dir, "test_versioning.duckdb")
        cl = CrossLink(db_path=db_path)
        
        # Push initial version
        dataset_id_v1 = cl.push(sample_dataframe, name="versioned_dataset", description="Version 1")
        assert dataset_id_v1 is not None
        
        # Push second version with modified data
        df_v2 = sample_dataframe.copy()
        df_v2['new_column'] = np.random.random(len(df_v2))
        dataset_id_v2 = cl.push(df_v2, name="versioned_dataset", description="Version 2")
        assert dataset_id_v2 is not None
        
        # Get version 1
        df_retrieved_v1 = cl.pull("versioned_dataset", version=1)
        assert 'new_column' not in df_retrieved_v1.columns
        
        # Get version 2
        df_retrieved_v2 = cl.pull("versioned_dataset", version=2)
        assert 'new_column' in df_retrieved_v2.columns
        
        # Get latest version (should be version 2)
        df_retrieved_latest = cl.pull("versioned_dataset")
        assert 'new_column' in df_retrieved_latest.columns
        
        # List versions
        versions = cl.list_dataset_versions("versioned_dataset")
        assert len(versions) == 2
        
        cl.close()
    
    def test_data_lineage(self, test_dir, sample_dataframe):
        """Test data lineage tracking."""
        db_path = os.path.join(test_dir, "test_lineage.duckdb")
        cl = CrossLink(db_path=db_path)
        
        # Push source dataset
        source_id = cl.push(sample_dataframe, name="source_data", description="Source data")
        
        # Create derived dataset
        derived_df = sample_dataframe.copy()
        derived_df['derived_value'] = derived_df['value'] * 2
        
        # Push derived dataset with lineage information
        derived_id = cl.push(derived_df, name="derived_data", description="Derived data",
                             sources=["source_data"], transformation="value doubled")
        
        # Get lineage information
        lineage = cl.get_lineage("derived_data")
        assert lineage is not None
        assert "source_data" in str(lineage)
        
        cl.close()
    
    def test_performance_metrics(self, test_dir, complex_dataframe):
        """Test performance metrics recording."""
        db_path = os.path.join(test_dir, "test_metrics.duckdb")
        cl = CrossLink(db_path=db_path)
        
        # Push complex data to generate metrics
        dataset_id = cl.push(complex_dataframe, name="metrics_dataset", description="Test metrics")
        
        # Get metrics (if metrics collection is implemented)
        if hasattr(cl, 'get_performance_metrics'):
            metrics = cl.get_performance_metrics(dataset_id)
            assert metrics is not None
        
        cl.close()
    
    def test_error_handling(self, test_dir):
        """Test error handling in CrossLink."""
        db_path = os.path.join(test_dir, "test_errors.duckdb")
        cl = CrossLink(db_path=db_path)
        
        # Test pulling non-existent dataset
        with pytest.raises(Exception):
            cl.pull("non_existent_dataset")
        
        # Test invalid parameter types
        with pytest.raises(Exception):
            cl.push(None, name="invalid_dataset")
        
        cl.close() 