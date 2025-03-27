"""
Configuration for DuckData test suite.
"""
import os
import sys
import pytest
import pandas as pd
import numpy as np
import tempfile
import shutil
from pathlib import Path

# Add the parent directory to the path so we can import duckdata
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Test fixtures

@pytest.fixture(scope="session")
def test_dir():
    """Provides a temporary directory for test files."""
    temp_dir = tempfile.mkdtemp(prefix="duckdata_test_")
    yield temp_dir
    shutil.rmtree(temp_dir)  # Clean up after tests

@pytest.fixture(scope="session")
def database_path(test_dir):
    """Provides a path to a test database."""
    return os.path.join(test_dir, "test_pipeline.duckdb")

@pytest.fixture
def sample_dataframe():
    """Creates a sample pandas DataFrame for testing."""
    return pd.DataFrame({
        'id': range(100),
        'value': np.random.normal(0, 1, 100),
        'category': np.random.choice(['A', 'B', 'C'], size=100)
    })

@pytest.fixture
def complex_dataframe():
    """Creates a more complex pandas DataFrame with various data types."""
    return pd.DataFrame({
        'id': range(50),
        'int_col': np.random.randint(-100, 100, 50),
        'float_col': np.random.normal(0, 1, 50),
        'str_col': [f"str_{i}" for i in range(50)],
        'bool_col': np.random.choice([True, False], 50),
        'date_col': pd.date_range('2022-01-01', periods=50),
        'cat_col': pd.Categorical(np.random.choice(['X', 'Y', 'Z'], 50)),
        'null_col': [None if i % 5 == 0 else i for i in range(50)]
    })

@pytest.fixture
def create_pipeline_config(test_dir):
    """Creates a function to generate pipeline configurations."""
    def _create_config(name, nodes, db_path=None):
        if db_path is None:
            db_path = os.path.join(test_dir, f"{name}.duckdb")
        
        config = {
            "name": name,
            "description": f"Test pipeline: {name}",
            "db_path": db_path,
            "working_dir": test_dir,
            "nodes": nodes
        }
        return config
    
    return _create_config

@pytest.fixture
def check_language_available():
    """Returns a function to check if a language is available in the system."""
    def _check(language):
        if language == "python":
            return True  # Python is always available since we're running tests with it
        
        if language == "r":
            # Check if R is available
            try:
                result = os.system("Rscript --version > /dev/null 2>&1")
                return result == 0
            except:
                return False
                
        if language == "julia":
            # Check if Julia is available
            try:
                result = os.system("julia --version > /dev/null 2>&1")
                return result == 0
            except:
                return False
                
        return False
    
    return _check 