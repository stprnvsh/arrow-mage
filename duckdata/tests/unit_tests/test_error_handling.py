"""
Tests for error handling in PipeLink.
This test suite verifies that PipeLink correctly handles various error scenarios.
"""
import os
import sys
import pytest
import yaml
import tempfile
import pandas as pd
import numpy as np
import socket
import shutil
from unittest.mock import patch, MagicMock
from pipelink.python.pipelink import run_pipeline
from pipelink.python.node_utils import NodeContext
from pipelink.crosslink import CrossLink

class TestErrorHandling:
    """Tests for error handling in PipeLink."""
    
    def test_invalid_pipeline_config(self, test_dir, create_pipeline_config):
        """Test handling of invalid pipeline configurations."""
        # Missing required field 'language'
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
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path, dry_run=True)
        
        assert "language" in str(exc_info.value).lower(), "Error should mention missing language field"
    
    def test_invalid_node_dependencies(self, test_dir, create_pipeline_config):
        """Test handling of invalid node dependencies."""
        # Create pipeline with circular dependencies
        nodes = [
            {
                "id": "node1",
                "language": "python",
                "script": "node1.py",
                "inputs": ["output_from_node3"],
                "outputs": ["output_from_node1"]
            },
            {
                "id": "node2",
                "language": "python",
                "script": "node2.py",
                "inputs": ["output_from_node1"],
                "outputs": ["output_from_node2"]
            },
            {
                "id": "node3",
                "language": "python",
                "script": "node3.py",
                "inputs": ["output_from_node2"],
                "outputs": ["output_from_node3"]
            }
        ]
        
        config = create_pipeline_config("circular_deps", nodes)
        
        config_path = os.path.join(test_dir, "circular_deps.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # This should raise an exception about circular dependencies
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path, dry_run=True)
        
        assert "cyclic" in str(exc_info.value).lower() or "circular" in str(exc_info.value).lower(), \
            "Error should mention circular dependencies"
    
    def test_missing_script_file(self, test_dir, create_pipeline_config):
        """Test handling of missing script files."""
        # Create pipeline with non-existent script
        nodes = [
            {
                "id": "node1",
                "language": "python",
                "script": "non_existent_script.py",
                "outputs": ["output1"]
            }
        ]
        
        config = create_pipeline_config("missing_script", nodes)
        
        config_path = os.path.join(test_dir, "missing_script.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # This should raise an exception about missing script
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path)
        
        assert "file" in str(exc_info.value).lower() and "not found" in str(exc_info.value).lower(), \
            "Error should mention missing file"
    
    @patch('socket.socket')
    def test_network_failure(self, mock_socket, test_dir, create_pipeline_config, sample_dataframe):
        """Test handling of network failures during data transfers."""
        # Mock socket to simulate network failure
        mock_socket_instance = MagicMock()
        mock_socket.return_value = mock_socket_instance
        mock_socket_instance.connect.side_effect = socket.error("Network failure")
        
        # Set up a test database and CrossLink instance
        db_path = os.path.join(test_dir, "network_test.duckdb")
        if os.path.exists(db_path):
            os.remove(db_path)
        
        # Create a node with remote data access (this will be mocked)
        nodes = [
            {
                "id": "remote_data_node",
                "language": "python",
                "script": "remote_data.py",
                "params": {
                    "remote_url": "http://example.com/data.csv"
                },
                "outputs": ["remote_data"]
            }
        ]
        
        config = create_pipeline_config("network_test", nodes, db_path)
        
        config_path = os.path.join(test_dir, "network_test.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Create the script that will try to access remote data
        script_path = os.path.join(test_dir, "remote_data.py")
        with open(script_path, 'w') as f:
            f.write("""
import socket
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        url = ctx.get_param('remote_url')
        try:
            # This will fail due to our mock
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect(('example.com', 80))
            # If we somehow get here, create some dummy data
            df = pd.DataFrame({'data': [1, 2, 3]})
            ctx.save_output('remote_data', df, 'Data from remote')
        except Exception as e:
            # Report the error
            error_msg = f"Network error: {str(e)}"
            ctx.log_error(error_msg)
            raise RuntimeError(error_msg)

if __name__ == "__main__":
    main()
""")
        
        # This should raise a network error
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path)
        
        assert "network" in str(exc_info.value).lower() or "socket" in str(exc_info.value).lower(), \
            "Error should mention network failure"
    
    def test_disk_space_limitation(self, test_dir, create_pipeline_config, sample_dataframe):
        """Test handling of disk space limitations."""
        # Mock os.statvfs to simulate low disk space
        original_statvfs = os.statvfs if hasattr(os, 'statvfs') else None
        
        # Skip this test on Windows which doesn't have statvfs
        if original_statvfs is None:
            pytest.skip("os.statvfs not available (Windows)")
            return
        
        # Set up a test database and CrossLink instance
        db_path = os.path.join(test_dir, "disk_space_test.duckdb")
        if os.path.exists(db_path):
            os.remove(db_path)
        
        # Create a node with a script that checks disk space
        nodes = [
            {
                "id": "disk_check_node",
                "language": "python",
                "script": "disk_check.py",
                "params": {
                    "min_required_space_mb": 1000000  # Very large to ensure failure
                },
                "outputs": ["disk_check_result"]
            }
        ]
        
        config = create_pipeline_config("disk_space_test", nodes, db_path)
        
        config_path = os.path.join(test_dir, "disk_space_test.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Create the script that will check disk space
        script_path = os.path.join(test_dir, "disk_check.py")
        with open(script_path, 'w') as f:
            f.write("""
import os
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        min_space_mb = ctx.get_param('min_required_space_mb')
        
        # Check available disk space
        try:
            if hasattr(os, 'statvfs'):  # Unix/Linux/MacOS
                stats = os.statvfs(ctx.get_working_dir())
                available_space_mb = (stats.f_bavail * stats.f_frsize) / (1024 * 1024)
            else:  # Windows or other
                import shutil
                available_space_mb = shutil.disk_usage(ctx.get_working_dir()).free / (1024 * 1024)
            
            if available_space_mb < min_space_mb:
                error_msg = f"Insufficient disk space: {available_space_mb}MB available, {min_space_mb}MB required"
                ctx.log_error(error_msg)
                raise RuntimeError(error_msg)
            
            # If we somehow have enough space, create some dummy data
            df = pd.DataFrame({'space_mb': [available_space_mb]})
            ctx.save_output('disk_check_result', df, 'Disk space check result')
            
        except Exception as e:
            error_msg = f"Disk space check error: {str(e)}"
            ctx.log_error(error_msg)
            raise RuntimeError(error_msg)

if __name__ == "__main__":
    main()
""")
        
        # This should raise a disk space error
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path)
        
        assert "disk space" in str(exc_info.value).lower() or "space" in str(exc_info.value).lower(), \
            "Error should mention disk space limitation"
    
    def test_invalid_data_format(self, test_dir, create_pipeline_config):
        """Test handling of invalid data formats."""
        # Set up a test database and CrossLink instance
        db_path = os.path.join(test_dir, "data_format_test.duckdb")
        if os.path.exists(db_path):
            os.remove(db_path)
        
        # Create a corrupted CSV file
        csv_path = os.path.join(test_dir, "corrupted_data.csv")
        with open(csv_path, 'w') as f:
            f.write("header1,header2,header3\n")
            f.write("value1,value2\n")  # Missing a value
            f.write("v1,v2,v3,v4\n")    # Too many values
            f.write("x,y,\n")           # Empty value
        
        # Create a node that tries to read the corrupted CSV
        nodes = [
            {
                "id": "csv_reader_node",
                "language": "python",
                "script": "csv_reader.py",
                "params": {
                    "csv_path": csv_path
                },
                "outputs": ["csv_data"]
            }
        ]
        
        config = create_pipeline_config("data_format_test", nodes, db_path)
        
        config_path = os.path.join(test_dir, "data_format_test.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Create the script that will try to read the corrupted CSV
        script_path = os.path.join(test_dir, "csv_reader.py")
        with open(script_path, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        csv_path = ctx.get_param('csv_path')
        try:
            # Read CSV with strict mode to catch errors
            df = pd.read_csv(csv_path, error_bad_lines=True)  # will raise on malformed lines
            ctx.save_output('csv_data', df, 'Data from CSV')
        except Exception as e:
            error_msg = f"CSV parsing error: {str(e)}"
            ctx.log_error(error_msg)
            raise RuntimeError(error_msg)

if __name__ == "__main__":
    main()
""")
        
        # This should raise a CSV parsing error
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path)
        
        assert "csv" in str(exc_info.value).lower() or "pars" in str(exc_info.value).lower(), \
            "Error should mention CSV parsing issue"
    
    def test_permission_issues(self, test_dir, create_pipeline_config):
        """Test handling of permission issues."""
        # Skip on Windows as permissions work differently
        if sys.platform.startswith('win'):
            pytest.skip("Permission test not applicable on Windows")
            return
        
        # Create a directory with restricted permissions
        restricted_dir = os.path.join(test_dir, "restricted")
        os.makedirs(restricted_dir, exist_ok=True)
        
        # Create a file inside that we'll remove permissions from
        restricted_file = os.path.join(restricted_dir, "data.txt")
        with open(restricted_file, 'w') as f:
            f.write("test data")
        
        # Remove read permissions
        os.chmod(restricted_file, 0o000)  # No permissions
        
        # Create a node that tries to read the restricted file
        nodes = [
            {
                "id": "permission_node",
                "language": "python",
                "script": "permission_test.py",
                "params": {
                    "file_path": restricted_file
                },
                "outputs": ["file_data"]
            }
        ]
        
        config = create_pipeline_config("permission_test", nodes)
        
        config_path = os.path.join(test_dir, "permission_test.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Create the script that will try to read the restricted file
        script_path = os.path.join(test_dir, "permission_test.py")
        with open(script_path, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        file_path = ctx.get_param('file_path')
        try:
            # Try to read the file
            with open(file_path, 'r') as f:
                content = f.read()
            
            # If we somehow get here, create some dummy data
            df = pd.DataFrame({'content': [content]})
            ctx.save_output('file_data', df, 'Data from file')
        except Exception as e:
            error_msg = f"Permission error: {str(e)}"
            ctx.log_error(error_msg)
            raise RuntimeError(error_msg)

if __name__ == "__main__":
    main()
""")
        
        # This should raise a permission error
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path)
        
        # Restore permissions so cleanup can happen
        os.chmod(restricted_file, 0o666)
        
        assert "permission" in str(exc_info.value).lower() or "denied" in str(exc_info.value).lower(), \
            "Error should mention permission issue"
    
    def test_language_runtime_error(self, test_dir, create_pipeline_config):
        """Test handling of language runtime errors."""
        # Create a node with a script that has a runtime error
        nodes = [
            {
                "id": "runtime_error_node",
                "language": "python",
                "script": "runtime_error.py",
                "outputs": ["result"]
            }
        ]
        
        config = create_pipeline_config("runtime_error_test", nodes)
        
        config_path = os.path.join(test_dir, "runtime_error_test.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Create the script with a Python runtime error
        script_path = os.path.join(test_dir, "runtime_error.py")
        with open(script_path, 'w') as f:
            f.write("""
import pandas as pd
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        # Deliberate ZeroDivisionError
        x = 1 / 0
        
        # We should never get here
        df = pd.DataFrame({'value': [x]})
        ctx.save_output('result', df, 'Result data')

if __name__ == "__main__":
    main()
""")
        
        # This should raise a zero division error
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path)
        
        assert "zero" in str(exc_info.value).lower() or "division" in str(exc_info.value).lower(), \
            "Error should mention division by zero"
    
    def test_memory_limitation(self, test_dir, create_pipeline_config):
        """Test handling of memory limitations."""
        # Create a node with a script that tries to allocate too much memory
        nodes = [
            {
                "id": "memory_hog_node",
                "language": "python",
                "script": "memory_hog.py",
                "outputs": ["result"]
            }
        ]
        
        config = create_pipeline_config("memory_test", nodes)
        
        config_path = os.path.join(test_dir, "memory_test.yml")
        with open(config_path, 'w') as f:
            yaml.dump(config, f)
        
        # Create the script that tries to allocate a large amount of memory
        script_path = os.path.join(test_dir, "memory_hog.py")
        with open(script_path, 'w') as f:
            f.write("""
import pandas as pd
import numpy as np
from pipelink import NodeContext

def main():
    with NodeContext() as ctx:
        try:
            # Try to create a very large DataFrame (may raise MemoryError)
            size = 10000000000  # Very large size that should fail on most systems
            
            # The following will fail on most systems
            df = pd.DataFrame({
                'big_array': np.random.rand(size)
            })
            
            ctx.save_output('result', df.head(), 'Memory test result')
        except Exception as e:
            error_msg = f"Memory allocation error: {str(e)}"
            ctx.log_error(error_msg)
            raise RuntimeError(error_msg)

if __name__ == "__main__":
    main()
""")
        
        # This should raise a memory error
        with pytest.raises(Exception) as exc_info:
            run_pipeline(config_path)
        
        assert "memory" in str(exc_info.value).lower() or "allocation" in str(exc_info.value).lower(), \
            "Error should mention memory allocation issue" 