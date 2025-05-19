import streamlit as st
import os
import sys
import logging
import importlib
import time
import subprocess
import signal
import json
import socket
from typing import List, Dict, Any, Optional
import threading
import psutil
import pandas as pd
import requests

# Page config - MUST be the first Streamlit command
st.set_page_config(
    page_title="MCP Server | Data Mage",
    page_icon="🔌",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.common import get_cache, render_memory_usage

# Import UDF extraction functions from workbench
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.common import extract_udf_name, extract_udf_docs

# Session state for server
if "mcp_server_running" not in st.session_state:
    st.session_state.mcp_server_running = False
if "mcp_server_process" not in st.session_state:
    st.session_state.mcp_server_process = None
if "mcp_server_port" not in st.session_state:
    st.session_state.mcp_server_port = 8080
if "mcp_server_url" not in st.session_state:
    st.session_state.mcp_server_url = ""

def is_port_in_use(port: int) -> bool:
    """Check if a port is in use"""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) == 0

def find_available_port(start_port: int = 8080, max_attempts: int = 10) -> int:
    """Find an available port starting from start_port"""
    port = start_port
    for _ in range(max_attempts):
        if not is_port_in_use(port):
            return port
        port += 1
    return port

def get_udf_metadata(udf_code: str) -> dict:
    """Extract MCP-specific metadata from a UDF"""
    # Basic metadata
    metadata = {
        "name": extract_udf_name(udf_code),
        "description": extract_udf_docs(udf_code),
        "parameters": []
    }
    
    # Extract parameter info
    # This is a simplified version - a more complete parser would be needed
    # for complex parameter types
    import re
    params_match = re.search(r'def\s+[a-zA-Z0-9_]+\s*\((.*?)\):', udf_code, re.DOTALL)
    if params_match:
        params_str = params_match.group(1).strip()
        if params_str:
            for param in params_str.split(','):
                param = param.strip()
                if param == 'self' or not param:
                    continue
                    
                # Handle default values and type hints
                param_name = param.split('=')[0].split(':')[0].strip()
                param_type = "string"  # Default type
                required = True
                
                # Check for type hint
                if ':' in param:
                    type_hint = param.split(':')[1].split('=')[0].strip()
                    if 'int' in type_hint:
                        param_type = 'integer'
                    elif 'float' in type_hint or 'double' in type_hint:
                        param_type = 'number'
                    elif 'bool' in type_hint:
                        param_type = 'boolean'
                    elif 'list' in type_hint or 'List' in type_hint:
                        param_type = 'array'
                    elif 'dict' in type_hint or 'Dict' in type_hint:
                        param_type = 'object'
                
                # Check for default value
                if '=' in param:
                    required = False
                
                metadata["parameters"].append({
                    "name": param_name,
                    "type": param_type,
                    "required": required
                })
    
    return metadata

def get_available_udfs_from_session():
    """Get available UDFs from session state"""
    udfs = []
    
    # Make sure the UDF workbench has been initialized
    if "udf_codes" not in st.session_state or "udf_tabs" not in st.session_state:
        return []
    
    # Extract UDFs from session state
    for i, (tab_name, code) in enumerate(zip(st.session_state.udf_tabs, st.session_state.udf_codes)):
        if not code.strip():  # Skip empty UDFs
            continue
            
        try:
            metadata = get_udf_metadata(code)
            udfs.append({
                "name": metadata["name"],
                "display_name": tab_name,
                "description": metadata["description"],
                "parameters": metadata["parameters"],
                "code": code,
                "index": i
            })
        except Exception as e:
            logger.error(f"Error extracting metadata from UDF {tab_name}: {e}")
    
    return udfs

def save_udf_to_disk(udf: dict, udfs_dir: str = "udfs") -> str:
    """Save a UDF to disk so the MCP server can find it"""
    # Ensure the UDFs directory exists
    os.makedirs(udfs_dir, exist_ok=True)
    
    # Create a directory for this UDF
    udf_name = udf["name"]
    udf_dir = os.path.join(udfs_dir, udf_name)
    os.makedirs(udf_dir, exist_ok=True)
    
    # Write the UDF code
    udf_file = os.path.join(udf_dir, "main.py")
    with open(udf_file, "w") as f:
        f.write(udf["code"])
    
    # Create metadata file with MCP specific info
    metadata = {
        "fused:mcp": {
            "description": udf["description"] or f"UDF: {udf_name}",
            "parameters": udf["parameters"]
        }
    }
    
    metadata_file = os.path.join(udf_dir, "fused.json")
    with open(metadata_file, "w") as f:
        json.dump(metadata, f, indent=2)
    
    return udf_dir

def create_simplified_server_script():
    """Create a simplified server script that doesn't require fused module"""
    simplified_script = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "simplified_mcp_server.py"))
    
    # Create the script
    logger.info(f"Creating simplified MCP server script at {simplified_script}")
    with open(simplified_script, "w") as f:
        f.write('''
import argparse
import asyncio
import json
import logging
import os
import sys
import importlib.util
import inspect
from typing import Any, Dict, List, Optional

# Force-add the parent directory to sys.path to find modules
parent_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, parent_dir)

try:
    import pandas as pd
except ImportError:
    print("pandas module not found, installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas"])
    import pandas as pd

try:
    import mcp.server.stdio
    import mcp.types as types
    from mcp.server import NotificationOptions, Server
    from mcp.server.models import InitializationOptions
    from mcp.server.sse import SseServerTransport
except ImportError:
    print("mcp module not found, installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "mcp-python==0.6.0"])
    import mcp.server.stdio
    import mcp.types as types
    from mcp.server import NotificationOptions, Server
    from mcp.server.models import InitializationOptions
    from mcp.server.sse import SseServerTransport

try:
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.routing import Mount, Route
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from starlette.responses import JSONResponse
except ImportError:
    print("starlette module not found, installing...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "starlette uvicorn"])
    from starlette.applications import Starlette
    from starlette.requests import Request
    from starlette.routing import Mount, Route
    from starlette.middleware import Middleware
    from starlette.middleware.cors import CORSMiddleware
    from starlette.responses import JSONResponse

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

# Class to represent a UDF
class SimplifiedUdf:
    def __init__(self, name, description, main_func, parameters=None):
        self.name = name
        self.description = description
        self.main_func = main_func
        self.parameters = parameters or []
        
    def run(self, **kwargs):
        return self.main_func(**kwargs)
        
    @property
    def metadata(self):
        return {
            "fused:mcp": {
                "description": self.description,
                "parameters": self.parameters
            }
        }

# Load a UDF from a Python file
def load_udf_from_file(file_path, metadata_path=None):
    module_name = os.path.basename(os.path.dirname(file_path))
    
    # Load the Python module
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    
    # Get the main function
    main_func = getattr(module, "main", None)
    if not main_func:
        raise ValueError(f"No 'main' function found in {file_path}")
    
    # Get function docstring as description
    description = inspect.getdoc(main_func) or f"UDF: {module_name}"
    
    # Get parameters from function signature
    sig = inspect.signature(main_func)
    parameters = []
    for name, param in sig.parameters.items():
        param_type = "string"  # Default type
        if param.annotation != inspect.Parameter.empty:
            type_name = str(param.annotation)
            if "int" in type_name:
                param_type = "integer"
            elif "float" in type_name or "double" in type_name:
                param_type = "number"
            elif "bool" in type_name:
                param_type = "boolean"
            elif "list" in type_name or "List" in type_name:
                param_type = "array"
            elif "dict" in type_name or "Dict" in type_name:
                param_type = "object"
        
        required = param.default == inspect.Parameter.empty
        
        parameters.append({
            "name": name,
            "type": param_type,
            "description": f"Parameter: {name}",
            "required": required
        })
    
    # Override with metadata from JSON file if provided
    if metadata_path and os.path.exists(metadata_path):
        try:
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
                
            if "fused:mcp" in metadata:
                mcp_metadata = metadata["fused:mcp"]
                if "description" in mcp_metadata:
                    description = mcp_metadata["description"]
                if "parameters" in mcp_metadata:
                    # Replace parameters with those from the metadata
                    parameters = mcp_metadata["parameters"]
        except Exception as e:
            logger.error(f"Error loading metadata from {metadata_path}: {e}")
    
    return SimplifiedUdf(module_name, description, main_func, parameters)

class SimplifiedMcpServer:
    """MCP server for UDFs without requiring fused module"""

    def __init__(self, server_name: str):
        """Initialize the UDF MCP server"""
        self.server = Server(server_name)
        self.registered_udfs = {}
        self.tool_schemas = {}
        self._setup_handlers()

    def _setup_handlers(self):
        """Set up the MCP request handlers"""

        @self.server.list_tools()
        async def handle_list_tools() -> list[types.Tool]:
            """List all registered UDF tools"""
            tools = []

            for udf_name, udf in self.registered_udfs.items():
                # Get the schema for this tool
                schema = self.tool_schemas.get(
                    udf_name, {"type": "object", "properties": {}, "required": []}
                )

                # Create tool definition
                tools.append(
                    types.Tool(
                        name=udf_name,
                        description=udf.metadata.get("fused:mcp", {}).get(
                            "description", f"Function: {udf_name}"
                        ),
                        inputSchema=schema,
                    )
                )

            logger.info(f"Returning {len(tools)} tools")
            return tools

        @self.server.call_tool()
        async def handle_call_tool(
            name: str, arguments: Dict[str, Any] = None
        ) -> list[types.TextContent]:
            """Execute a registered UDF tool"""
            try:
                # Check if this is a registered UDF
                if name not in self.registered_udfs:
                    return [
                        types.TextContent(
                            type="text", text=f"Error: Unknown UDF tool '{name}'"
                        )
                    ]

                # Get the UDF
                udf = self.registered_udfs[name]
                arguments = arguments or {}

                logger.info(f"Executing UDF '{name}' with arguments: {arguments}")

                # Execute the UDF
                try:
                    result = udf.run(**arguments)

                    # Handle DataFrame results
                    if isinstance(result, pd.DataFrame):
                        text_output = result.to_string()
                    elif isinstance(result, dict) and 'data' in result:
                        text_output = json.dumps(result)
                    else:
                        text_output = str(result)
                        
                    return [types.TextContent(type="text", text=text_output)]

                except Exception as e:
                    logger.exception(f"Error executing UDF '{name}': {e}")
                    return [
                        types.TextContent(
                            type="text", text=f"Error executing UDF '{name}': {str(e)}"
                        )
                    ]

            except Exception as e:
                logger.exception(f"Error handling tool call: {e}")
                return [types.TextContent(type="text", text=f"Error: {str(e)}")]

    def register_udf(self, udf: SimplifiedUdf) -> bool:
        """Register a UDF as an MCP tool"""
        try:
            # Get the MCP metadata
            logger.info(f"Registering UDF tool '{udf.name}'")

            # Create schema from parameters
            schema = self._create_schema_from_parameters(udf.name, udf.metadata.get("fused:mcp", {}))

            # Store the UDF and schema
            self.registered_udfs[udf.name] = udf
            self.tool_schemas[udf.name] = schema

            logger.info(f"Successfully registered UDF tool '{udf.name}'")
            return True

        except Exception as e:
            logger.exception(f"Error registering UDF '{udf.name}': {e}")
            return False

    def _create_schema_from_parameters(
        self, udf_name: str, metadata: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Create a JSON Schema from UDF parameters"""
        # Initialize schema
        schema = {"type": "object", "properties": {}, "required": []}

        # Get parameters
        parameters = metadata.get("parameters", [])

        # Check if parameters is a JSON string that needs to be deserialized
        if isinstance(parameters, str):
            try:
                logger.debug(
                    f"Parameters for UDF '{udf_name}' is a string, attempting to parse as JSON"
                )
                parameters = json.loads(parameters)
            except json.JSONDecodeError as e:
                logger.warning(
                    f"Failed to parse parameters JSON for UDF '{udf_name}': {e}"
                )
                parameters = []

        if parameters is None:
            logger.info(f"No parameters defined for UDF '{udf_name}'")
            return schema

        # Process parameters
        for param in parameters:
            if not isinstance(param, dict) or "name" not in param:
                continue

            param_name = param["name"]
            param_type = param.get("type", "string").lower()
            param_desc = param.get("description", f"Parameter: {param_name}")
            required = param.get("required", True)

            if param_type in ("float", "double", "decimal", "number"):
                json_type = "number"
            elif param_type in ("int", "integer"):
                json_type = "number"
            elif param_type in ("bool", "boolean"):
                json_type = "boolean"
            elif param_type in ("array", "list"):
                json_type = "array"
            elif param_type in ("object", "dict", "map"):
                json_type = "object"
            else:
                json_type = "string"

            # Add property definition
            schema["properties"][param_name] = {
                "type": json_type,
                "description": param_desc,
            }

            # Add to required list if needed
            if required:
                schema["required"].append(param_name)

        logger.info(f"Created schema for UDF '{udf_name}': {schema}")
        return schema

    def create_starlette_app(self, debug: bool = False) -> Starlette:
        """Create a Starlette application for SSE transport"""
        sse = SseServerTransport("/messages/")

        async def handle_sse(request: Request) -> None:
            logger.info(f"New SSE connection from {request.client}")
            async with sse.connect_sse(
                request.scope,
                request.receive,
                request._send,
            ) as (read_stream, write_stream):
                await self.server.run(
                    read_stream,
                    write_stream,
                    InitializationOptions(
                        server_name=self.server.name,
                        server_version="1.0.0",
                        capabilities=self.server.get_capabilities(
                            notification_options=NotificationOptions(),
                            experimental_capabilities={},
                        ),
                    ),
                )

        # Set up middleware for CORS
        middleware = [
            Middleware(
                CORSMiddleware,
                allow_origins=["*"],
                allow_methods=["*"],
                allow_headers=["*"],
            )
        ]

        app = Starlette(
            debug=debug,
            routes=[
                Route("/sse", endpoint=handle_sse),
                Mount("/messages/", app=sse.handle_post_message),
            ],
            middleware=middleware
        )
        
        @app.route("/")
        async def root(request):
            return JSONResponse({
                "status": "running",
                "server_name": self.server.name,
                "registered_udfs": list(self.registered_udfs.keys())
            })
            
        return app

def create_server_from_folder_names(
    folder_names: List[str] = None,
    server_name: str = "udf-server",
) -> SimplifiedMcpServer:
    """Create and configure an MCP server with tools from local UDF folders."""
    # Initialize server
    server = SimplifiedMcpServer(server_name)

    # Register tools for each folder name
    if folder_names:
        for udf_name in folder_names:
            udf_name = udf_name.strip()  # Remove any whitespace
            if udf_name:
                try:
                    # Load UDF from folder
                    base_path = os.path.abspath(os.curdir)
                    folder_path = os.path.join(base_path, "udfs", udf_name)
                    main_py_path = os.path.join(folder_path, "main.py")
                    metadata_path = os.path.join(folder_path, "fused.json")
                    
                    logger.info(f"Loading UDF from folder: {folder_path}")
                    
                    # Check if the folder exists
                    if not os.path.exists(folder_path):
                        logger.error(f"UDF folder not found: {folder_path}")
                        continue
                        
                    # Check if the main.py file exists
                    if not os.path.exists(main_py_path):
                        logger.error(f"main.py not found in UDF folder: {folder_path}")
                        continue
                        
                    # Load the UDF
                    udf = load_udf_from_file(main_py_path, metadata_path)
                    logger.info(f"UDF loaded successfully with name: {udf.name}")
                    
                    # Register the UDF
                    success = server.register_udf(udf)
                    logger.info(f"UDF registration {'successful' if success else 'failed'}")

                except Exception as e:
                    logger.exception(f"Error loading UDF from folder '{udf_name}': {e}")

    return server

def run_simplified_server(
    server_name: str,
    host: str = "0.0.0.0",
    port: int = 8080,
    udf_names: List[str] = None,
    debug: bool = False
):
    """Run an MCP server with the specified configuration."""
    try:
        # Create server instance
        logger.info(f"Creating server from UDF folders: {udf_names}")
        server = create_server_from_folder_names(udf_names, server_name)
        
        if not server.registered_udfs:
            logger.warning("No UDFs were registered. Server will start but won't have any tools.")

        logger.info(f"Starting MCP server with name: {server_name}")
        logger.info(f"Server will be available at http://{host}:{port}/sse")
        
        # Create and run Starlette app with SSE transport
        import uvicorn
        starlette_app = server.create_starlette_app(debug=debug)
        uvicorn.run(starlette_app, host=host, port=port, log_level="info")

    except Exception as e:
        logger.exception(f"Error starting server: {e}")
        sys.exit(1)

def main():
    """Main entry point for the server"""
    parser = argparse.ArgumentParser(description="Run a simplified UDF MCP server")
    parser.add_argument(
        "--udf-names",
        help="Comma-separated list of UDF (folder) names to register as tools",
    )
    parser.add_argument("--name", default="simplified-udf-server", help="Server name")
    parser.add_argument("--host", default="0.0.0.0", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8080, help="Port to listen on")
    parser.add_argument(
        "--debug",
        action="store_true",
        help="Enable debug logging",
    )
    args = parser.parse_args()

    # Process UDF names
    udf_names = args.udf_names.split(",") if args.udf_names else []

    # Run the server
    run_simplified_server(
        server_name=args.name,
        host=args.host,
        port=args.port,
        udf_names=udf_names,
        debug=args.debug
    )

if __name__ == "__main__":
    main()
''')
    
    return simplified_script


def start_simplified_mcp_server(selected_udfs: List[str], port: int):
    """Start the simplified MCP server with the selected UDFs"""
    # Create the simplified server script
    server_script = create_simplified_server_script()
    
    # Prepare the command
    cmd = [
        sys.executable,
        server_script,
        "--name", "datamage-simplified-mcp",
        "--port", str(port),
        "--udf-names", ",".join(selected_udfs),
        "--debug"
    ]
    
    # Start the server as a subprocess
    try:
        # Log the server command
        logger.info(f"Starting simplified MCP server with command: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Store the process handle
        st.session_state.mcp_server_process = process
        st.session_state.mcp_server_running = True
        st.session_state.mcp_server_port = port
        st.session_state.mcp_server_url = f"http://localhost:{port}/sse"
        
        # Wait a bit to ensure server starts
        time.sleep(2)
        
        # Check if process is still running
        if process.poll() is not None:
            # Process exited
            stdout, stderr = process.communicate()
            error_msg = f"Server failed to start: {stderr}"
            logger.error(error_msg)
            st.error(error_msg)
            st.session_state.mcp_server_running = False
            return False
            
        # Try to verify the server is working by making a request to the root endpoint
        try:
            base_url = f"http://localhost:{port}"
            response = requests.get(base_url, timeout=3)
            if response.status_code == 200:
                server_info = response.json()
                logger.info(f"Server is running: {server_info}")
                
                if 'registered_udfs' in server_info:
                    num_udfs = len(server_info['registered_udfs'])
                    if num_udfs > 0:
                        logger.info(f"Server has {num_udfs} registered UDFs: {server_info['registered_udfs']}")
                    else:
                        logger.warning("Server is running but has no registered UDFs")
            else:
                logger.warning(f"Server health check returned status code {response.status_code}")
        except Exception as e:
            logger.warning(f"Failed to verify server health: {str(e)}")
        
        return True
    
    except Exception as e:
        error_msg = f"Failed to start server: {e}"
        logger.error(error_msg)
        st.error(error_msg)
        st.session_state.mcp_server_running = False
        return False

def stop_mcp_server():
    """Stop the running MCP server"""
    if st.session_state.mcp_server_process:
        process = st.session_state.mcp_server_process
        
        try:
            # First try to terminate gracefully
            parent = psutil.Process(process.pid)
            for child in parent.children(recursive=True):
                child.terminate()
            parent.terminate()
            
            # Wait for process to finish
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                # Force kill if it doesn't respond
                for child in parent.children(recursive=True):
                    child.kill()
                parent.kill()
        
        except Exception as e:
            st.error(f"Error stopping server: {e}")
        
        finally:
            st.session_state.mcp_server_running = False
            st.session_state.mcp_server_process = None

# Sidebar with memory usage
with st.sidebar:
    st.header("Memory Usage")
    render_memory_usage()

# Main content
st.title("🔌 MCP Server")
st.markdown("Expose your UDFs as MCP tools for AI agents")

# Create tabs
tabs = st.tabs(["Server Configuration", "Client Connection", "Usage Instructions"])

with tabs[0]:
    st.subheader("UDF Selection")
    
    # Get available UDFs from the workbench
    available_udfs = get_available_udfs_from_session()
    
    if not available_udfs:
        st.warning("No UDFs found. Please create some UDFs in the UDF Workbench first.")
    else:
        st.write(f"Found {len(available_udfs)} UDFs in the workbench:")
        
        # Show available UDFs in a table
        udf_data = []
        for udf in available_udfs:
            udf_data.append({
                "Name": udf["name"],
                "Display Name": udf["display_name"],
                "Description": udf["description"][:50] + "..." if len(udf["description"]) > 50 else udf["description"],
                "Parameters": len(udf["parameters"])
            })
        
        st.dataframe(pd.DataFrame(udf_data), use_container_width=True)
        
        # Multi-select for UDFs to expose
        selected_udf_names = [udf["name"] for udf in available_udfs]
        selected_udfs = st.multiselect(
            "Select UDFs to expose via MCP:",
            options=selected_udf_names,
            default=selected_udf_names[:1] if selected_udf_names else []
        )
        
        # Server configuration
        st.subheader("Server Configuration")
        
        col1, col2 = st.columns(2)
        with col1:
            port = st.number_input(
                "Port:", 
                min_value=1024, 
                max_value=65535, 
                value=st.session_state.mcp_server_port
            )
        
        with col2:
            # Find an available port button
            if st.button("Find available port"):
                available_port = find_available_port(port)
                if available_port != port:
                    st.info(f"Found available port: {available_port}")
                    port = available_port
        
        # Server controls
        if st.session_state.mcp_server_running:
            server_status = st.success("Server is running")
            if st.button("Stop Server", type="primary"):
                stop_mcp_server()
                st.rerun()
        else:
            server_status = st.info("Server is not running")
            
            if st.button("Start Server", type="primary", disabled=not selected_udfs):
                # First save the selected UDFs to disk
                st.info("Preparing UDFs...")
                udf_dirs = []
                
                for udf_name in selected_udfs:
                    # Find the UDF details
                    udf = next((u for u in available_udfs if u["name"] == udf_name), None)
                    if udf:
                        # Save UDF to disk
                        udf_dir = save_udf_to_disk(udf)
                        udf_dirs.append(os.path.basename(udf_dir))
                
                # Now start the server
                if udf_dirs:
                    st.info("Starting server...")
                    success = start_simplified_mcp_server(udf_dirs, port)
                    if success:
                        st.success("Server started successfully!")
                        st.rerun()
                    else:
                        st.error("Failed to start server. Check the logs for details.")
                else:
                    st.error("No UDFs were prepared successfully. Cannot start server.")
        
        # Server details when running
        if st.session_state.mcp_server_running:
            st.subheader("Server Details")
            
            # Show connection information
            st.code(f"Server URL: {st.session_state.mcp_server_url}")
            
            # Log output (if available)
            if st.session_state.mcp_server_process:
                process = st.session_state.mcp_server_process
                if process.stdout:
                    with st.expander("Server Logs"):
                        # Create a container for the logs
                        log_container = st.empty()
                        
                        def update_logs():
                            logs = []
                            # Read stdout without blocking
                            while True:
                                line = process.stdout.readline()
                                if not line:
                                    break
                                logs.append(line.strip())
                            return logs
                        
                        # Get initial logs
                        logs = update_logs()
                        if logs:
                            log_container.code("\n".join(logs))
                        
                        # Auto-refresh button
                        if st.button("Refresh Logs"):
                            logs = update_logs()
                            if logs:
                                log_container.code("\n".join(logs))

with tabs[1]:
    st.subheader("Client Connection")
    
    with st.expander("Python Client Code", expanded=True):
        st.markdown("Use this code to connect to your MCP server from Python:")
        
        client_code = f"""
```python
import asyncio
from mcp import ClientSession
from mcp.client.sse import sse_client
from anthropic import Anthropic

async def main():
    # Connect to the MCP server
    server_url = "{st.session_state.mcp_server_url or 'http://localhost:8080/sse'}"
    
    # Create the SSE client
    async with sse_client(url=server_url) as streams:
        # Create the MCP client session
        async with ClientSession(*streams) as session:
            # Initialize
            await session.initialize()
            
            # List available tools
            response = await session.list_tools()
            tools = response.tools
            print("Available tools:", [tool.name for tool in tools])
            
            # Example: Call a tool
            if tools:
                result = await session.call_tool(tools[0].name, {{"param1": "value1"}})
                print(f"Tool result: {{result.content}}")
            
            # Example: Use with Claude
            anthropic = Anthropic()
            
            # Convert MCP tools to Claude format
            claude_tools = [
                {{
                    "name": tool.name,
                    "description": tool.description,
                    "input_schema": tool.inputSchema,
                }}
                for tool in tools
            ]
            
            # Call Claude with tools
            response = anthropic.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=1000,
                messages=[{{"role": "user", "content": "Please help me analyze data using these tools"}}],
                tools=claude_tools,
            )
            
            print(response.content)

# Run the example
asyncio.run(main())
```
"""
        st.code(client_code, language="python")
    
    with st.expander("Command-line Client"):
        st.markdown("You can also use the provided client.py script to connect to your server:")
        
        st.code(f"""
# Run from command line:
python client.py {st.session_state.mcp_server_url or 'http://localhost:8080/sse'}
""", language="bash")

with tabs[2]:
    st.subheader("Usage Instructions")
    
    st.markdown("""
    ### How to use the MCP Server
    
    1. **Create UDFs in the UDF Workbench**
       - Go to the UDF Workbench page
       - Create or edit your UDFs
       - Make sure your UDFs have proper documentation with parameter descriptions
    
    2. **Configure and Start the Server**
       - Select which UDFs to expose
       - Configure the server port
       - Start the server
    
    3. **Connect with a Client**
       - Use the provided Python client code
       - Or use the command-line client
    
    4. **Use UDFs as Tools with Claude**
       - Your UDFs will be available as tools for Claude
       - Claude can execute your UDFs and use the results
    
    ### Important Notes
    
    - The server must be running for clients to connect
    - If you update your UDFs, you need to restart the server
    - UDFs must have proper metadata for parameters to work correctly
    """)
    
    st.info("When using UDFs as tools with Claude, be sure to provide instructions on what data is available and what operations are possible.") 