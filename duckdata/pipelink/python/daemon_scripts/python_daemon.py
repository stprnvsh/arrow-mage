#!/usr/bin/env python3
import os
import sys
import socket
import json
import subprocess
import threading
import signal
import traceback

def handle_client(conn, addr):
    try:
        # Read message size (4 bytes)
        msg_size_bytes = conn.recv(4)
        if not msg_size_bytes:
            return
            
        msg_size = int.from_bytes(msg_size_bytes, byteorder='big')
        
        # Read message data
        data = b''
        while len(data) < msg_size:
            chunk = conn.recv(min(4096, msg_size - len(data)))
            if not chunk:
                return
            data += chunk
        
        # Parse message
        request = json.loads(data.decode('utf-8'))
        action = request.get('action')
        
        if action == 'shutdown':
            print(f"Daemon shutdown requested")
            conn.close()
            os._exit(0)
            
        elif action == 'execute':
            script_path = request.get('script_path')
            meta_path = request.get('meta_path')
            working_dir = request.get('working_dir')
            
            print(f"Executing script: {script_path}")
            
            # Set environment variable to encourage zero-copy usage
            env = os.environ.copy()
            env['PIPELINK_ENABLE_ZERO_COPY'] = '1'
            
            # Run the script
            proc = subprocess.run(
                [sys.executable, script_path, meta_path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=working_dir,
                env=env
            )
            
            # Prepare response
            response = {
                'success': proc.returncode == 0,
                'stdout': proc.stdout,
                'stderr': proc.stderr
            }
            
            # Send response
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big'))
            conn.sendall(response_data)
            
        elif action == 'ping' or action == 'keep_alive':
            # Health check or keep-alive
            response = {
                'status': 'ok'
            }
            
            # Send response
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big'))
            conn.sendall(response_data)
            
        else:
            # Unknown action
            response = {
                'success': False,
                'error': f"Unknown action: {action}"
            }
            response_data = json.dumps(response).encode('utf-8')
            conn.sendall(len(response_data).to_bytes(4, byteorder='big'))
            conn.sendall(response_data)
            
    except Exception as e:
        print(f"Error handling client: {e}")
        traceback.print_exc()
    finally:
        conn.close()

def main():
    if len(sys.argv) < 2:
        print("Usage: python_daemon.py SOCKET_PATH")
        sys.exit(1)
        
    socket_path = sys.argv[1]
    
    # Remove socket if it already exists
    if os.path.exists(socket_path):
        os.unlink(socket_path)
    
    # Create server socket
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    server.bind(socket_path)
    server.listen(5)
    
    print(f"Python daemon listening on {socket_path}")
    
    # Handle signals
    signal.signal(signal.SIGTERM, lambda sig, frame: os._exit(0))
    signal.signal(signal.SIGINT, lambda sig, frame: os._exit(0))
    
    try:
        while True:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.daemon = True
            thread.start()
    except KeyboardInterrupt:
        pass
    finally:
        server.close()
        if os.path.exists(socket_path):
            os.unlink(socket_path)

if __name__ == "__main__":
    main()
