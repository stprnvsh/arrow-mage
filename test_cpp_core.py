# test_cpp_core.py
import pyarrow as pa
import os
import warnings

# Attempt to import CrossLink and the C++ availability checker
try:
    # Assuming the main CrossLink class is the intended entry point
    from duckdata.crosslink import CrossLink 
    # Check function is likely in the wrapper module
    from duckdata.crosslink.python.shared_memory.cpp_wrapper import is_cpp_available 
except ImportError as e:
    print(f"Error importing CrossLink modules: {e}")
    print("Please ensure the duckdata.crosslink package is installed and")
    print("the C++ bindings are built correctly and accessible.")
    exit(1)

# Define a unique database path for this test
DB_PATH = "test_cpp_core.duckdb"

def main():
    print("--- Starting CrossLink C++ Core Test ---")

    # Clean up previous test database if it exists
    if os.path.exists(DB_PATH):
        print(f"Removing existing database: {DB_PATH}")
        os.remove(DB_PATH)

    # 1. Check C++ Backend Availability
    cpp_available = is_cpp_available()
    print(f"C++ backend available according to is_cpp_available(): {cpp_available}")
    if not cpp_available:
        warnings.warn("C++ bindings not found or failed to load. "
                      "CrossLink might fall back to Python implementation.",
                      RuntimeWarning)
    else:
         print("Attempting to use C++ backend.")

    # 2. Instantiate CrossLink
    cl_instance = None
    try:
        print(f"Instantiating CrossLink with db_path='{DB_PATH}'...")
        # Enable debug to get more verbose output, potentially showing which backend is used
        cl_instance = CrossLink(db_path=DB_PATH).set_debug(True) 
        print("CrossLink instance created.")
        # A more robust check after instantiation (if cpp backend was expected)
        if cpp_available and (not hasattr(cl_instance, '_cpp_instance') or not cl_instance._cpp_instance):
             warnings.warn("is_cpp_available() was True, but CrossLink instance "
                           "doesn't seem to have a _cpp_instance.", RuntimeWarning)
        elif not cpp_available and hasattr(cl_instance, '_cpp_instance') and cl_instance._cpp_instance:
             warnings.warn("is_cpp_available() was False, but CrossLink instance "
                           "seems to have created a _cpp_instance unexpectedly.", RuntimeWarning)


    except Exception as e:
        print(f"Error instantiating CrossLink: {e}")
        import traceback
        traceback.print_exc()
        return # Exit if instantiation fails

    # 3. Create Sample Data (PyArrow Table)
    try:
        print("Creating sample pyarrow Table...")
        data = {'col1': [10, 20, 30, 40], 'col2': ['apple', 'banana', 'cherry', 'date']}
        original_table = pa.Table.from_pydict(data)
        print("Sample Table:")
        print(original_table)
    except Exception as e:
        print(f"Error creating pyarrow Table: {e}")
        if cl_instance:
             cl_instance.close()
        return

    # 4. Push Data
    dataset_id = None
    try:
        print("Pushing table to CrossLink...")
        # Use a specific name for easier identification
        dataset_name = "my_cpp_test_table" 
        dataset_id = cl_instance.push(original_table, name=dataset_name)
        print(f"Table pushed successfully. Dataset ID/Name: {dataset_id}")
        assert dataset_id == dataset_name # push should return the name if provided
    except Exception as e:
        print(f"Error pushing data: {e}")
        import traceback
        traceback.print_exc()
        if cl_instance:
            cl_instance.close()
        return

    # 5. Pull Data
    pulled_table = None
    try:
        print(f"Pulling table with identifier: {dataset_id}...")
        # Request the table as pyarrow.Table, not pandas DataFrame
        pulled_table = cl_instance.pull(dataset_id, to_pandas=False) 
        print("Table pulled successfully.")
        print("Pulled Table:")
        # Print the Arrow table directly
        print(pulled_table)
    except Exception as e:
        print(f"Error pulling data: {e}")
        import traceback
        traceback.print_exc()
        if cl_instance:
            cl_instance.close()
        return

    # 6. Verify Data
    print("Verifying pulled data...")
    if pulled_table is not None and original_table.equals(pulled_table):
        print("SUCCESS: Pulled table matches the original table.")
    elif pulled_table is None:
        print("FAILURE: Pulled table is None.")
    else:
        print("FAILURE: Pulled table does NOT match the original table.")
        print("Original Schema:")
        print(original_table.schema)
        print("Pulled Schema:")
        print(pulled_table.schema)


    # 7. Cleanup
    print("Closing CrossLink instance...")
    if cl_instance:
        cl_instance.close()
    
    # Optional: remove the test database file after test
    # if os.path.exists(DB_PATH):
    #     print(f"Removing test database: {DB_PATH}")
    #     os.remove(DB_PATH)
        
    print("--- CrossLink C++ Core Test Finished ---")

if __name__ == "__main__":
    # Ensure pyarrow is installed
    if pa is None:
        print("Error: pyarrow is not installed. Please install it (`pip install pyarrow`)")
    else:
        main() 