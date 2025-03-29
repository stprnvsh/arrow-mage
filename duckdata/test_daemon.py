from pipelink.python.language_daemon import LanguageDaemon
import os
import time
import tempfile
import yaml

# Create a temporary metadata file
meta_file = tempfile.NamedTemporaryFile(suffix='.yaml', delete=False)
meta_path = meta_file.name

# Write metadata to the file
with open('test_r_daemon/meta.yaml', 'r') as f:
    meta_content = f.read()

with open(meta_path, 'w') as f:
    f.write(meta_content)

# Create the daemon
print("Creating R daemon...")
daemon = LanguageDaemon('r', os.getcwd(), '/tmp/pipelink_sockets')

try:
    # Start the daemon
    print("Starting R daemon...")
    daemon.start()
    print("R daemon started successfully!")
    
    # Execute the test script
    print("Executing test script...")
    script_path = os.path.join(os.getcwd(), 'test_r_daemon/test_script.R')
    success, stdout, stderr = daemon.execute(script_path, meta_path, os.getcwd())
    
    # Print results
    print("\nExecution result:", "Success" if success else "Failed")
    print("\nStandard output:")
    print(stdout)
    
    if stderr:
        print("\nStandard error:")
        print(stderr)
    
    print("\nTest completed!")
finally:
    # Shutdown the daemon
    print("Shutting down R daemon...")
    daemon.shutdown()
    
    # Clean up the temporary file
    if os.path.exists(meta_path):
        os.unlink(meta_path) 