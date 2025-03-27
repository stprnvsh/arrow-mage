"""
Integration tests for S3 functionality in DuckData.
These tests use moto to mock the S3 service.
"""
import os
import sys
import pytest
import yaml
import pandas as pd
import numpy as np
import boto3
import io
import tempfile
from unittest.mock import patch, MagicMock
from pipelink.python.pipelink import run_pipeline
from pipelink.crosslink import CrossLink

# Check if moto is available, and skip tests if not
try:
    from moto import mock_s3
    MOTO_AVAILABLE = True
except ImportError:
    MOTO_AVAILABLE = False

@pytest.mark.skipif(not MOTO_AVAILABLE, reason="moto package not available")
class TestS3Integration:
    """Tests for S3 integration in DuckData."""
    
    def setup_s3_mock(self, bucket_name="test-bucket"):
        """Setup a mock S3 environment."""
        # Create a mock S3 service
        self.s3_mock = mock_s3()
        self.s3_mock.start()
        
        # Create a bucket for testing
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.s3.create_bucket(Bucket=bucket_name)
        
        self.bucket_name = bucket_name
        
        # Upload some test data to the bucket
        sample_csv = "id,value,category\n1,100,A\n2,200,B\n3,300,C\n"
        self.s3.put_object(Bucket=bucket_name, Key="test_data.csv", Body=sample_csv)
        
        # Upload a Parquet file as well
        df = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'value': [10, 20, 30, 40, 50],
            'category': ['X', 'Y', 'Z', 'X', 'Y']
        })
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer)
        parquet_buffer.seek(0)
        self.s3.put_object(Bucket=bucket_name, Key="test_data.parquet", Body=parquet_buffer.getvalue())
    
    def teardown_s3_mock(self):
        """Tear down the mock S3 environment."""
        self.s3_mock.stop()
    
    def test_s3_credential_configuration(self, test_dir):
        """Test setting up S3 credentials in CrossLink."""
        self.setup_s3_mock()
        try:
            # Set up credentials
            access_key = "test-access-key"
            secret_key = "test-secret-key"
            region = "us-east-1"
            
            # Create CrossLink instance
            db_path = os.path.join(test_dir, "s3_credentials_test.duckdb")
            cl = CrossLink(db_path=db_path)
            
            # Configure S3 credentials
            cl.configure_storage(
                storage_type="s3",
                access_key_id=access_key,
                secret_access_key=secret_key,
                region=region
            )
            
            # Verify configuration (can't easily verify the credentials directly,
            # but we can check if the configuration was created)
            assert "s3" in cl.get_storage_configs(), "S3 should be configured"
            
            # Clean up
            cl.close()
        finally:
            self.teardown_s3_mock()
    
    def test_read_csv_from_s3(self, test_dir, create_pipeline_config):
        """Test reading a CSV file from S3."""
        self.setup_s3_mock()
        try:
            # Create a script to read from S3
            s3_reader_script = os.path.join(test_dir, "s3_csv_reader.py")
            with open(s3_reader_script, 'w') as f:
                f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Configure S3 credentials
        ctx.configure_storage(
            storage_type="s3",
            access_key_id="test-access-key",
            secret_access_key="test-secret-key",
            region="us-east-1"
        )
        
        # Read data from S3
        s3_path = ctx.get_param('s3_path')
        df = ctx.read_csv(s3_path)
        
        # Save the data
        ctx.save_output('s3_csv_data', df, 'Data from S3 CSV')

if __name__ == "__main__":
    main()
""")

            # Create pipeline configuration
            nodes = [
                {
                    "id": "s3_reader",
                    "language": "python",
                    "script": s3_reader_script,
                    "params": {
                        "s3_path": f"s3://{self.bucket_name}/test_data.csv"
                    },
                    "outputs": ["s3_csv_data"]
                }
            ]
            
            pipeline_name = "s3_csv_test"
            db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
            config = create_pipeline_config(pipeline_name, nodes, db_path)
            
            config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
            
            # Run the pipeline
            result = run_pipeline(config_path)
            
            # Verify pipeline execution
            assert result['success'], "Pipeline execution should succeed"
            
            # Check the data
            cl = CrossLink(db_path=db_path)
            csv_data = cl.pull("s3_csv_data")
            
            assert csv_data is not None, "Data should be loaded from S3"
            assert len(csv_data) == 3, "CSV data should have 3 rows"
            assert 'id' in csv_data.columns, "CSV data should have id column"
            assert 'value' in csv_data.columns, "CSV data should have value column"
            assert 'category' in csv_data.columns, "CSV data should have category column"
            
            # Clean up
            cl.close()
        finally:
            self.teardown_s3_mock()
    
    def test_read_parquet_from_s3(self, test_dir, create_pipeline_config):
        """Test reading a Parquet file from S3."""
        self.setup_s3_mock()
        try:
            # Create a script to read from S3
            s3_reader_script = os.path.join(test_dir, "s3_parquet_reader.py")
            with open(s3_reader_script, 'w') as f:
                f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Configure S3 credentials
        ctx.configure_storage(
            storage_type="s3",
            access_key_id="test-access-key",
            secret_access_key="test-secret-key",
            region="us-east-1"
        )
        
        # Read data from S3
        s3_path = ctx.get_param('s3_path')
        df = ctx.read_parquet(s3_path)
        
        # Save the data
        ctx.save_output('s3_parquet_data', df, 'Data from S3 Parquet')

if __name__ == "__main__":
    main()
""")

            # Create pipeline configuration
            nodes = [
                {
                    "id": "s3_reader",
                    "language": "python",
                    "script": s3_reader_script,
                    "params": {
                        "s3_path": f"s3://{self.bucket_name}/test_data.parquet"
                    },
                    "outputs": ["s3_parquet_data"]
                }
            ]
            
            pipeline_name = "s3_parquet_test"
            db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
            config = create_pipeline_config(pipeline_name, nodes, db_path)
            
            config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
            
            # Run the pipeline
            result = run_pipeline(config_path)
            
            # Verify pipeline execution
            assert result['success'], "Pipeline execution should succeed"
            
            # Check the data
            cl = CrossLink(db_path=db_path)
            parquet_data = cl.pull("s3_parquet_data")
            
            assert parquet_data is not None, "Data should be loaded from S3"
            assert len(parquet_data) == 5, "Parquet data should have 5 rows"
            assert 'id' in parquet_data.columns, "Parquet data should have id column"
            assert 'value' in parquet_data.columns, "Parquet data should have value column"
            assert 'category' in parquet_data.columns, "Parquet data should have category column"
            
            # Clean up
            cl.close()
        finally:
            self.teardown_s3_mock()
    
    def test_write_to_s3(self, test_dir, create_pipeline_config, sample_dataframe):
        """Test writing data to S3."""
        self.setup_s3_mock()
        try:
            # Create a script to write to S3
            s3_writer_script = os.path.join(test_dir, "s3_writer.py")
            with open(s3_writer_script, 'w') as f:
                f.write("""
import pandas as pd
import numpy as np
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Configure S3 credentials
        ctx.configure_storage(
            storage_type="s3",
            access_key_id="test-access-key",
            secret_access_key="test-secret-key",
            region="us-east-1"
        )
        
        # Generate data
        df = pd.DataFrame({
            'id': range(20),
            'value': np.random.normal(0, 1, 20),
            'group': np.random.choice(['A', 'B', 'C'], size=20)
        })
        
        # Save data to output for verification
        ctx.save_output('data_to_export', df, 'Data to be exported to S3')
        
        # Write data to S3
        s3_path = ctx.get_param('s3_path')
        s3_format = ctx.get_param('s3_format', 'parquet')
        
        if s3_format == 'parquet':
            ctx.write_parquet(df, s3_path)
        elif s3_format == 'csv':
            ctx.write_csv(df, s3_path)
        else:
            raise ValueError(f"Unsupported format: {s3_format}")
        
        # Record the S3 path for verification
        with open(f"{ctx.get_working_dir()}/s3_export_path.txt", "w") as f:
            f.write(s3_path)

if __name__ == "__main__":
    main()
""")

            # Create pipeline configurations for different formats
            for s3_format in ['parquet', 'csv']:
                nodes = [
                    {
                        "id": "s3_writer",
                        "language": "python",
                        "script": s3_writer_script,
                        "params": {
                            "s3_path": f"s3://{self.bucket_name}/exported_data.{s3_format}",
                            "s3_format": s3_format
                        },
                        "outputs": ["data_to_export"]
                    }
                ]
                
                pipeline_name = f"s3_write_{s3_format}_test"
                db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
                config = create_pipeline_config(pipeline_name, nodes, db_path)
                
                config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
                with open(config_path, 'w') as f:
                    yaml.dump(config, f)
                
                # Run the pipeline
                result = run_pipeline(config_path)
                
                # Verify pipeline execution
                assert result['success'], f"Pipeline execution should succeed for {s3_format} format"
                
                # Get the exported path
                export_path_file = os.path.join(test_dir, "s3_export_path.txt")
                with open(export_path_file, 'r') as f:
                    export_path = f.read().strip()
                
                # Verify the file exists in S3
                bucket_name, key = export_path[5:].split('/', 1)  # Remove 's3://' and split
                response = self.s3.list_objects_v2(Bucket=bucket_name, Prefix=key)
                assert 'Contents' in response, f"Exported {s3_format} file should exist in S3"
                
                # Check that the data can be read back (for verification)
                if s3_format == 'parquet':
                    response = self.s3.get_object(Bucket=bucket_name, Key=key)
                    parquet_data = pd.read_parquet(io.BytesIO(response['Body'].read()))
                    assert len(parquet_data) == 20, f"Parquet data should have 20 rows"
                elif s3_format == 'csv':
                    response = self.s3.get_object(Bucket=bucket_name, Key=key)
                    csv_data = pd.read_csv(io.BytesIO(response['Body'].read()))
                    assert len(csv_data) == 20, f"CSV data should have 20 rows"
        finally:
            self.teardown_s3_mock()
    
    def test_s3_error_handling(self, test_dir, create_pipeline_config):
        """Test error handling for S3 operations."""
        self.setup_s3_mock()
        try:
            # Create a script that attempts to read a non-existent file
            s3_error_script = os.path.join(test_dir, "s3_error.py")
            with open(s3_error_script, 'w') as f:
                f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Configure S3 credentials
        ctx.configure_storage(
            storage_type="s3",
            access_key_id="test-access-key",
            secret_access_key="test-secret-key",
            region="us-east-1"
        )
        
        try:
            # Try to read a non-existent file
            s3_path = ctx.get_param('s3_path')
            df = ctx.read_parquet(s3_path)
            
            # We shouldn't get here
            ctx.save_output('error_test_data', df, 'This should not be saved')
        except Exception as e:
            # Record the error
            with open(f"{ctx.get_working_dir()}/s3_error.txt", "w") as f:
                f.write(str(e))
            # Re-raise to fail the node
            raise

if __name__ == "__main__":
    main()
""")

            # Create pipeline configuration
            nodes = [
                {
                    "id": "s3_error_node",
                    "language": "python",
                    "script": s3_error_script,
                    "params": {
                        "s3_path": f"s3://{self.bucket_name}/non_existent_file.parquet"
                    },
                    "outputs": ["error_test_data"]
                }
            ]
            
            pipeline_name = "s3_error_test"
            db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
            config = create_pipeline_config(pipeline_name, nodes, db_path)
            
            config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
            with open(config_path, 'w') as f:
                yaml.dump(config, f)
            
            # Run the pipeline - it should fail
            result = run_pipeline(config_path)
            
            # Verify pipeline execution
            assert not result['success'], "Pipeline execution should fail for non-existent file"
            
            # Check that the error was recorded
            error_file = os.path.join(test_dir, "s3_error.txt")
            assert os.path.exists(error_file), "Error file should exist"
            
            with open(error_file, 'r') as f:
                error_text = f.read()
            
            # Check error message
            assert "not found" in error_text.lower() or "not exist" in error_text.lower() or "no such key" in error_text.lower(), \
                "Error should mention that the file doesn't exist"
            
            # Check that no output was created
            cl = CrossLink(db_path=db_path)
            assert not cl.exists("error_test_data"), "No output should be created on error"
            cl.close()
        finally:
            self.teardown_s3_mock()
    
    def test_invalid_s3_credentials(self, test_dir, create_pipeline_config):
        """Test handling of invalid S3 credentials."""
        # No need to set up mock here since we want the credentials to fail
        
        # Create a script with invalid credentials
        s3_creds_script = os.path.join(test_dir, "s3_invalid_creds.py")
        with open(s3_creds_script, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Configure invalid S3 credentials
        ctx.configure_storage(
            storage_type="s3",
            access_key_id="invalid-key",
            secret_access_key="invalid-secret",
            region="us-east-1"
        )
        
        try:
            # Try to read a file with invalid credentials
            s3_path = ctx.get_param('s3_path')
            df = ctx.read_csv(s3_path)
            
            # We shouldn't get here
            ctx.save_output('creds_test_data', df, 'This should not be saved')
        except Exception as e:
            # Record the error
            with open(f"{ctx.get_working_dir()}/s3_creds_error.txt", "w") as f:
                f.write(str(e))
            # Re-raise to fail the node
            raise

if __name__ == "__main__":
    main()
""")

        # Create pipeline configuration
        nodes = [
            {
                "id": "s3_creds_node",
                "language": "python",
                "script": s3_creds_script,
                "params": {
                    "s3_path": "s3://nonexistent-bucket/some-file.csv"
                },
                "outputs": ["creds_test_data"]
            }
        ]
        
        pipeline_name = "s3_creds_test"
        db_path = os.path.join(test_dir, f"{pipeline_name}.duckdb")
        config = create_pipeline_config(pipeline_name, nodes, db_path)
        
        config_path = os.path.join(test_dir, f"{pipeline_name}.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Run the pipeline - it should fail
        result = run_pipeline(config_path)
        
        # Verify pipeline execution
        assert not result['success'], "Pipeline execution should fail with invalid credentials"
        
        # Check that the error was recorded
        error_file = os.path.join(test_dir, "s3_creds_error.txt")
        assert os.path.exists(error_file), "Error file should exist"
        
        with open(error_file, 'r') as f:
            error_text = f.read()
        
        # Check error message 
        auth_fail_terms = ["credential", "auth", "access", "permission", "forbidden"]
        assert any(term in error_text.lower() for term in auth_fail_terms), \
            "Error should mention authentication/credential problems" 