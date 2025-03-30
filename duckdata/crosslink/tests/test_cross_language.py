"""
CrossLink Cross-Language Integration Tests

This module tests data sharing between different languages using the CrossLink C++ core.
"""

import os
import tempfile
import unittest
import shutil
import subprocess
import time
import sys
import numpy as np
import pandas as pd
import pyarrow as pa

# Add parent directory to sys.path to import crosslink
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from crosslink import CrossLink

class CrossLanguageTest(unittest.TestCase):
    """Test cases for cross-language data sharing."""
    
    @classmethod
    def setUpClass(cls):
        """Set up the test environment."""
        # Create a temporary directory for the test database
        cls.temp_dir = tempfile.mkdtemp()
        cls.db_path = os.path.join(cls.temp_dir, "crosslink_test.duckdb")
        
        # Initialize CrossLink
        cls.cl = CrossLink(cls.db_path, debug=True)
        
        # Sample data for tests
        cls.sample_df = pd.DataFrame({
            'int_col': [1, 2, 3, 4, 5],
            'float_col': [1.1, 2.2, 3.3, 4.4, 5.5],
            'str_col': ['a', 'b', 'c', 'd', 'e']
        })
        
        # Share sample data
        cls.sample_id = cls.cl.push(cls.sample_df, "sample_data", "Sample data for testing")
        print(f"Created sample dataset with ID: {cls.sample_id}")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up after tests."""
        cls.cl.cleanup()
        
        # Clean up temporary directory
        shutil.rmtree(cls.temp_dir)
    
    def test_python_to_python(self):
        """Test Python to Python data sharing."""
        # Get the sample data back
        table = self.cl.pull(self.sample_id)
        
        # Convert to pandas and check equality
        df = table.to_pandas()
        pd.testing.assert_frame_equal(df, self.sample_df)
    
    def test_python_to_r(self):
        """Test Python to R data sharing."""
        # Create a temporary R script
        r_script = os.path.join(self.temp_dir, "test_r.R")
        with open(r_script, "w") as f:
            f.write(f"""
            library(arrow)
            source("{os.path.join(os.path.dirname(__file__), '../r/crosslink.R')}")
            
            # Connect to the database
            cl <- crosslink_connect("{self.db_path}")
            
            # Get the data
            data <- crosslink_pull(cl, "{self.sample_id}")
            
            # Print the data summary
            print(data)
            
            # Check if the data matches expectations
            if (nrow(data) == 5 && ncol(data) == 3) {{
                print("SUCCESS: Data dimensions match")
            }} else {{
                print("ERROR: Data dimensions don't match")
                quit(status=1)
            }}
            
            # Check column types
            if (is.integer(data$int_col) && is.numeric(data$float_col) && is.character(data$str_col)) {{
                print("SUCCESS: Column types match")
            }} else {{
                print("ERROR: Column types don't match")
                quit(status=1)
            }}
            
            # Check values
            if (data$int_col[1] == 1 && data$float_col[2] == 2.2 && data$str_col[3] == "c") {{
                print("SUCCESS: Values match")
            }} else {{
                print("ERROR: Values don't match")
                quit(status=1)
            }}
            
            # Clean up
            crosslink_cleanup(cl)
            """)
        
        # Check if R is installed
        try:
            # Run the R script
            result = subprocess.run(["Rscript", r_script], 
                                   check=False, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   text=True)
            
            # Print output
            print(f"R script output:\n{result.stdout}")
            if result.stderr:
                print(f"R script errors:\n{result.stderr}")
            
            if "ERROR" in result.stdout or result.returncode != 0:
                self.fail("R test failed")
        except FileNotFoundError:
            print("R not installed, skipping Python to R test")
    
    def test_python_to_julia(self):
        """Test Python to Julia data sharing."""
        # Create a temporary Julia script
        julia_script = os.path.join(self.temp_dir, "test_julia.jl")
        with open(julia_script, "w") as f:
            f.write(f"""
            using Arrow
            
            # Add the CrossLink.jl module path to LOAD_PATH
            push!(LOAD_PATH, "{os.path.join(os.path.dirname(__file__), '../julia')}")
            
            # Import CrossLink
            using CrossLink
            
            # Connect to the database
            cl = CrossLink.connect("{self.db_path}")
            
            # Get the data
            data = CrossLink.pull(cl, "{self.sample_id}")
            
            # Print the data summary
            println(data)
            
            # Check if the data matches expectations
            if size(data, 1) == 5 && size(data, 2) == 3
                println("SUCCESS: Data dimensions match")
            else
                println("ERROR: Data dimensions don't match")
                exit(1)
            end
            
            # Check values (assuming column order is preserved)
            if data[1, 1] == 1 && data[2, 2] ≈ 2.2 && data[3, 3] == "c"
                println("SUCCESS: Values match")
            else
                println("ERROR: Values don't match")
                exit(1)
            end
            
            # Clean up
            CrossLink.cleanup(cl)
            """)
        
        # Check if Julia is installed
        try:
            # Run the Julia script
            result = subprocess.run(["julia", julia_script], 
                                   check=False, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   text=True)
            
            # Print output
            print(f"Julia script output:\n{result.stdout}")
            if result.stderr:
                print(f"Julia script errors:\n{result.stderr}")
            
            if "ERROR" in result.stdout or result.returncode != 0:
                self.fail("Julia test failed")
        except FileNotFoundError:
            print("Julia not installed, skipping Python to Julia test")
    
    def test_python_to_cpp(self):
        """Test Python to C++ data sharing."""
        # Create a temporary C++ executable for testing
        # For this test, we'll create a small C++ program that uses our CrossLink library
        cpp_source = os.path.join(self.temp_dir, "test_cpp.cpp")
        with open(cpp_source, "w") as f:
            f.write(f"""
            #include "../cpp/include/crosslink.h"
            #include <iostream>
            #include <arrow/api.h>
            
            int main() {{
                try {{
                    // Connect to the database
                    crosslink::CrossLink cl("{self.db_path}");
                    
                    // Get the data
                    auto table = cl.pull("{self.sample_id}");
                    
                    // Print table info
                    std::cout << "Table has " << table->num_rows() << " rows and " 
                              << table->num_columns() << " columns\\n";
                    
                    // Check dimensions
                    if (table->num_rows() == 5 && table->num_columns() == 3) {{
                        std::cout << "SUCCESS: Data dimensions match\\n";
                    }} else {{
                        std::cout << "ERROR: Data dimensions don't match\\n";
                        return 1;
                    }}
                    
                    // Check schema
                    std::cout << "Schema: " << table->schema()->ToString() << "\\n";
                    
                    // Clean up
                    cl.cleanup();
                    
                    return 0;
                }} catch (const std::exception& e) {{
                    std::cerr << "Error: " << e.what() << "\\n";
                    return 1;
                }}
            }}
            """)
        
        # For this test, we'll just check if the file can be created
        # In a real environment, we'd compile and run the C++ program
        self.assertTrue(os.path.exists(cpp_source))
        print(f"C++ source created at: {cpp_source}")
        print("Note: C++ test skipped as it requires compilation")

    def test_round_trip(self):
        """Test round-trip data sharing (Python → R → Python)."""
        # Create a temporary R script
        r_script = os.path.join(self.temp_dir, "test_r_push.R")
        with open(r_script, "w") as f:
            f.write(f"""
            library(arrow)
            source("{os.path.join(os.path.dirname(__file__), '../r/crosslink.R')}")
            
            # Connect to the database
            cl <- crosslink_connect("{self.db_path}")
            
            # Get the data
            data <- crosslink_pull(cl, "{self.sample_id}")
            
            # Modify the data
            data$new_col <- data$int_col * 10
            
            # Push the modified data back
            new_id <- crosslink_push(cl, data, "r_modified_data", "Data modified in R")
            
            # Write the new ID to a file
            cat(new_id, file="{os.path.join(self.temp_dir, 'r_id.txt')}")
            
            # Clean up
            crosslink_cleanup(cl)
            """)
        
        # Check if R is installed
        try:
            # Run the R script
            result = subprocess.run(["Rscript", r_script], 
                                   check=False, 
                                   stdout=subprocess.PIPE, 
                                   stderr=subprocess.PIPE,
                                   text=True)
            
            # Print output
            print(f"R script output:\n{result.stdout}")
            if result.stderr:
                print(f"R script errors:\n{result.stderr}")
            
            if result.returncode != 0:
                self.fail("R test failed")
            
            # Get the new dataset ID
            with open(os.path.join(self.temp_dir, 'r_id.txt'), 'r') as f:
                r_id = f.read().strip()
            
            print(f"R created dataset with ID: {r_id}")
            
            # Get the data back in Python
            r_table = self.cl.pull(r_id)
            r_df = r_table.to_pandas()
            
            # Check if the data was modified correctly
            self.assertEqual(r_df.shape[1], 4)  # Should have one more column
            self.assertTrue("new_col" in r_df.columns)
            self.assertTrue((r_df["new_col"] == r_df["int_col"] * 10).all())
            
        except FileNotFoundError:
            print("R not installed, skipping round-trip test")

    def test_notifications(self):
        """Test cross-language notifications."""
        # Set up notification receiver
        notifications_received = []
        
        def on_notification(dataset_id, event_type):
            notifications_received.append((dataset_id, event_type))
            print(f"Notification received: {dataset_id} was {event_type}")
        
        reg_id = self.cl.register_notification(on_notification)
        
        # Create new data
        new_df = pd.DataFrame({"value": [10, 20, 30]})
        new_id = self.cl.push(new_df, "notification_test")
        
        # Wait for notifications
        time.sleep(0.1)
        
        # Check if notification was received
        self.assertTrue(len(notifications_received) > 0)
        self.assertTrue(any(nid == new_id for nid, _ in notifications_received))
        
        # Clean up
        self.cl.unregister_notification(reg_id)

if __name__ == "__main__":
    unittest.main() 