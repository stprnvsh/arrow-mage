import streamlit as st
import os
import sys
import logging
import asyncio
import json
import re
import pandas as pd
import time
from typing import List, Dict, Any, Optional
import concurrent.futures
import nest_asyncio
import requests
import ast
import subprocess
from io import StringIO

# Apply nest_asyncio to allow nested asyncio usage within Streamlit
nest_asyncio.apply()

# Page config - MUST be the first Streamlit command
st.set_page_config(
    page_title="MCP Client | Data Mage",
    page_icon="🔗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Import from utils
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utils.common import render_memory_usage

# Session state for MCP client
if "mcp_client_connected" not in st.session_state:
    st.session_state.mcp_client_connected = False
if "mcp_server_url" not in st.session_state:
    st.session_state.mcp_server_url = "http://localhost:8080/sse"
if "mcp_tools" not in st.session_state:
    st.session_state.mcp_tools = []
if "mcp_call_history" not in st.session_state:
    st.session_state.mcp_call_history = []

# Check if required packages are installed
requirements = ['mcp', 'asyncio', 'anthropic']
missing_packages = []

for package in requirements:
    try:
        __import__(package)
    except ImportError:
        missing_packages.append(package)

# Install missing packages if any
if missing_packages:
    st.warning(f"Required packages not found: {', '.join(missing_packages)}. Installing now...")
    
    import subprocess
    for package in missing_packages:
        try:
            subprocess.check_call([sys.executable, "-m", "pip", "install", package])
            st.success(f"Successfully installed {package}")
        except Exception as e:
            st.error(f"Failed to install {package}: {str(e)}")
            st.stop()
    
    st.rerun()

# Now import the required packages
try:
    from mcp import ClientSession
    from mcp.client.sse import sse_client
    from anthropic import Anthropic
except ImportError as e:
    st.error(f"Error importing required packages: {str(e)}")
    st.stop()

def str_to_value(value_str: str, param_type: str):
    """Convert a string to the appropriate type based on parameter type"""
    if not value_str:
        return None
        
    try:
        if param_type == "number":
            # Check if it's a float or int
            if "." in value_str:
                return float(value_str)
            else:
                return int(value_str)
        elif param_type == "boolean":
            return value_str.lower() in ["true", "yes", "1", "y"]
        elif param_type == "array":
            # Try to parse as JSON array
            return json.loads(value_str)
        elif param_type == "object":
            # Try to parse as JSON object
            return json.loads(value_str)
        else:
            # Default to string
            return value_str
    except Exception as e:
        st.warning(f"Error converting value: {str(e)}. Using as string.")
        return value_str

async def connect_to_mcp_server(server_url: str):
    """Connect to the MCP server and get available tools"""
    try:
        st.toast(f"Attempting to connect to {server_url}")
        logger.info(f"Connecting to MCP server at {server_url}")
        
        # Create the SSE client
        st.toast("Creating SSE client...")
        context = sse_client(url=server_url)
        async with context as streams:
            st.toast("SSE client created, establishing session...")
            # Create the MCP client session
            session_context = ClientSession(*streams)
            async with session_context as session:
                # Initialize
                st.toast("Initializing session...")
                await session.initialize()
                
                # List available tools
                st.toast("Getting available tools...")
                response = await session.list_tools()
                tools = response.tools
                
                # Log success
                logger.info(f"Successfully connected to server, found {len(tools)} tools")
                st.toast(f"Found {len(tools)} tools")
                
                # Transform tools into a more usable format
                tool_info = []
                for tool in tools:
                    params = {}
                    if hasattr(tool, "inputSchema") and tool.inputSchema:
                        schema = tool.inputSchema
                        if "properties" in schema:
                            for param_name, param_info in schema["properties"].items():
                                required = param_name in schema.get("required", [])
                                params[param_name] = {
                                    "type": param_info.get("type", "string"),
                                    "description": param_info.get("description", ""),
                                    "required": required
                                }
                    
                    tool_info.append({
                        "name": tool.name,
                        "description": tool.description,
                        "parameters": params
                    })
                
                return True, tool_info
    except Exception as e:
        error_msg = f"Error connecting to MCP server: {str(e)}"
        logger.error(error_msg)
        st.toast(error_msg, icon="❌")
        
        # Try to check if server is reachable
        try:
            st.toast("Checking if server is reachable...")
            # Remove /sse from the URL if present to check the base server
            base_url = server_url.split("/sse")[0] if "/sse" in server_url else server_url
            response = requests.get(base_url, timeout=3)
            if response.status_code == 200:
                return False, f"Server is reachable but MCP connection failed: {str(e)}"
            else:
                return False, f"Server returned status code {response.status_code}: {str(e)}"
        except requests.RequestException as re:
            return False, f"Server unreachable: {str(re)}"

async def call_mcp_tool(server_url: str, tool_name: str, arguments: Dict[str, Any]):
    """Call an MCP tool with arguments and return the result"""
    try:
        # Create the SSE client
        async with sse_client(url=server_url) as streams:
            # Create the MCP client session
            async with ClientSession(*streams) as session:
                # Initialize
                await session.initialize()
                
                # Call the tool
                result = await session.call_tool(tool_name, arguments)
                
                # Format result for display
                result_content = result.content
                
                # Parse result as DataFrame if it looks like tabular data
                if isinstance(result_content, str):
                    # Check if it's a DataFrame string representation
                    if re.search(r'\n\s+\d+\s+', result_content) and '\n' in result_content:
                        try:
                            # Try to parse as a DataFrame
                            lines = result_content.split('\n')
                            # Find headers line
                            header_line = next((i for i, line in enumerate(lines) if re.match(r'\s+\w+', line)), 0)
                            if header_line > 0:
                                # Skip potential index name in corner
                                headers = re.split(r'\s+', lines[header_line].strip())[1:]
                                data = []
                                for line in lines[header_line+1:]:
                                    if line.strip():
                                        row = re.split(r'\s+', line.strip())
                                        # First element might be index
                                        if len(row) > len(headers):
                                            row = row[1:]  # Skip index
                                        data.append(row[:len(headers)])  # Make sure we have the right number of columns
                                
                                if data:
                                    df = pd.DataFrame(data, columns=headers)
                                    return True, {"type": "dataframe", "data": df}
                        except Exception as e:
                            logger.warning(f"Failed to parse result as DataFrame: {e}")
                
                return True, {"type": "text", "data": result_content}
    except Exception as e:
        logger.error(f"Error calling MCP tool: {str(e)}")
        return False, str(e)

def run_async(coroutine):
    """Run an async coroutine in the Streamlit app"""
    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(asyncio.run, coroutine)
        return future.result()

# Sidebar with memory usage
with st.sidebar:
    st.header("Memory Usage")
    render_memory_usage()
    
    # Server configuration
    st.header("Server Connection")
    server_url = st.text_input(
        "MCP Server URL:",
        value=st.session_state.mcp_server_url
    )
    
    # Check if we should auto-detect the server URL from the MCP Server page
    if "mcp_server_url" in st.session_state and st.session_state.mcp_server_url:
        st.info(f"Server detected at: {st.session_state.mcp_server_url}")
        if st.button("Use detected server"):
            server_url = st.session_state.mcp_server_url
    
    # Connect button
    if not st.session_state.mcp_client_connected:
        if st.button("Connect to Server", type="primary"):
            with st.spinner("Connecting to MCP server..."):
                success, result = run_async(connect_to_mcp_server(server_url))
                
                if success:
                    st.session_state.mcp_client_connected = True
                    st.session_state.mcp_tools = result
                    st.session_state.mcp_server_url = server_url
                    st.success("Connected to MCP server!")
                    st.rerun()
                else:
                    st.error(f"Failed to connect: {result}")
    else:
        st.success("Connected to MCP server")
        if st.button("Disconnect"):
            st.session_state.mcp_client_connected = False
            st.rerun()
        
        if st.button("Refresh Tools"):
            with st.spinner("Refreshing tools..."):
                success, result = run_async(connect_to_mcp_server(server_url))
                
                if success:
                    st.session_state.mcp_tools = result
                    st.success("Tools refreshed!")
                    st.rerun()
                else:
                    st.error(f"Failed to refresh tools: {result}")

# Main content
st.title("🔗 MCP Client")
st.markdown("""
This page allows you to connect to an MCP server, view available UDF tools, and execute them directly 
within the Streamlit app. You can connect to the local server running alongside this app 
or to a remote MCP server.
""")

# Server connection section
with st.expander("Server Connection", expanded=not st.session_state.mcp_client_connected):
    st.markdown("### Connection Settings")
    col1, col2 = st.columns([3, 1])
    with col1:
        server_url = st.text_input("MCP Server URL", value=st.session_state.mcp_server_url)
    with col2:
        if st.button("Connect"):
            with st.spinner("Connecting to server..."):
                success, result = run_async(connect_to_mcp_server(server_url))
                
                if success:
                    st.session_state.mcp_client_connected = True
                    st.session_state.mcp_tools = result
                    st.session_state.mcp_server_url = server_url
                    st.success(f"Connected to {server_url}")
                    st.rerun()
                else:
                    st.error(f"Failed to connect: {result}")
    
    # Debug section
    st.markdown("### Debug Options")
    if st.checkbox("Enable debug mode"):
        debug_col1, debug_col2 = st.columns(2)
        
        with debug_col1:
            if st.button("Test server root endpoint"):
                try:
                    # Get base URL (without /sse)
                    base_url = server_url.split("/sse")[0] if "/sse" in server_url else server_url
                    st.write(f"Testing connection to: {base_url}")
                    
                    response = requests.get(base_url, timeout=5)
                    st.write(f"Status code: {response.status_code}")
                    
                    if response.status_code == 200:
                        try:
                            st.json(response.json())
                        except:
                            st.code(response.text)
                    else:
                        st.error(f"Server returned error status: {response.status_code}")
                        st.code(response.text)
                except Exception as e:
                    st.error(f"Error connecting to server: {str(e)}")
        
        with debug_col2:
            if st.button("Check UDF folder"):
                udfs_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "udfs"))
                st.write(f"UDFs directory: {udfs_dir}")
                
                if os.path.exists(udfs_dir):
                    folders = [f for f in os.listdir(udfs_dir) if os.path.isdir(os.path.join(udfs_dir, f))]
                    st.write(f"Found {len(folders)} UDF folders:")
                    for folder in folders:
                        folder_path = os.path.join(udfs_dir, folder)
                        main_py = os.path.join(folder_path, "main.py")
                        fused_json = os.path.join(folder_path, "fused.json")
                        
                        main_exists = os.path.exists(main_py)
                        json_exists = os.path.exists(fused_json)
                        
                        status = "✅ Valid" if main_exists and json_exists else "❌ Invalid"
                        st.write(f"- {folder}: {status}")
                        
                        if not main_exists:
                            st.write(f"  - ❌ Missing main.py")
                        if not json_exists:
                            st.write(f"  - ❌ Missing fused.json")
                else:
                    st.error(f"UDFs directory not found: {udfs_dir}")

        # Add direct custom URL test
        st.markdown("### Manual SSE Endpoint Test")
        manual_url = st.text_input("Enter exact SSE URL to test", value="http://localhost:8080/sse")
        if st.button("Test SSE Endpoint Directly"):
            with st.spinner("Testing SSE endpoint..."):
                try:
                    async def test_sse():
                        try:
                            # Create the SSE client with a short timeout
                            async with sse_client(url=manual_url) as streams:
                                # Just try to create a session
                                async with ClientSession(*streams) as session:
                                    # Initialize
                                    await session.initialize()
                                    # Try to list tools
                                    response = await session.list_tools()
                                    tools = response.tools
                                    return True, f"Connection successful! Found {len(tools)} tools: {[tool.name for tool in tools]}"
                        except Exception as e:
                            return False, f"SSE connection failed: {str(e)}"
                    
                    success, message = run_async(test_sse())
                    if success:
                        st.success(message)
                    else:
                        st.error(message)
                except Exception as e:
                    st.error(f"Error during SSE test: {str(e)}")

# Display content only if connected
if st.session_state.mcp_client_connected:
    # Create tabs
    tabs = st.tabs(["Available Tools", "Tool Execution", "Execution History"])
    
    with tabs[0]:
        st.subheader("Available UDF Tools")
        
        if not st.session_state.mcp_tools:
            st.warning("No tools available on the server.")
        else:
            # Show tools in a table
            tools_data = []
            for tool in st.session_state.mcp_tools:
                tools_data.append({
                    "Name": tool["name"],
                    "Description": tool["description"],
                    "Parameters": len(tool["parameters"]),
                })
            
            st.dataframe(pd.DataFrame(tools_data), use_container_width=True)
            
            # Show details for each tool
            st.subheader("Tool Details")
            for tool in st.session_state.mcp_tools:
                with st.expander(f"{tool['name']}: {tool['description'][:50]}..." if len(tool['description']) > 50 else tool['description']):
                    st.markdown(f"**Description:** {tool['description']}")
                    
                    if tool["parameters"]:
                        st.markdown("**Parameters:**")
                        params_data = []
                        for param_name, param_info in tool["parameters"].items():
                            params_data.append({
                                "Name": param_name,
                                "Type": param_info["type"],
                                "Required": "Yes" if param_info["required"] else "No",
                                "Description": param_info["description"]
                            })
                        
                        st.dataframe(pd.DataFrame(params_data), use_container_width=True)
                    else:
                        st.info("This tool has no parameters.")
    
    with tabs[1]:
        st.subheader("Execute UDF Tool")
        
        if not st.session_state.mcp_tools:
            st.warning("No tools available to execute.")
        else:
            # Select tool
            tool_names = [tool["name"] for tool in st.session_state.mcp_tools]
            selected_tool_name = st.selectbox("Select Tool:", tool_names)
            
            # Get the selected tool details
            selected_tool = next((tool for tool in st.session_state.mcp_tools if tool["name"] == selected_tool_name), None)
            
            if selected_tool:
                # Show tool description
                st.info(selected_tool["description"])
                
                # Parameter inputs
                parameters = {}
                if selected_tool["parameters"]:
                    st.markdown("**Parameters:**")
                    
                    for param_name, param_info in selected_tool["parameters"].items():
                        param_type = param_info["type"]
                        required = param_info["required"]
                        description = param_info["description"]
                        
                        # Create appropriate input field based on parameter type
                        if param_type == "boolean":
                            param_value = st.checkbox(
                                f"{param_name}" + (" (required)" if required else ""),
                                help=description
                            )
                            parameters[param_name] = param_value
                        elif param_type == "number":
                            param_value = st.number_input(
                                f"{param_name}" + (" (required)" if required else ""),
                                help=description
                            )
                            parameters[param_name] = param_value
                        elif param_type in ["array", "object"]:
                            param_value = st.text_area(
                                f"{param_name}" + (" (required)" if required else ""),
                                height=100,
                                help=f"{description} (Enter as JSON)"
                            )
                            if param_value:
                                try:
                                    parameters[param_name] = json.loads(param_value)
                                except json.JSONDecodeError:
                                    st.warning(f"Invalid JSON for parameter {param_name}")
                        else:
                            # Default to text input for string and other types
                            param_value = st.text_input(
                                f"{param_name}" + (" (required)" if required else ""),
                                help=description
                            )
                            if param_value:
                                parameters[param_name] = param_value
                
                # Execute button
                if st.button("Execute Tool", type="primary"):
                    # Check required parameters
                    missing_params = []
                    for param_name, param_info in selected_tool["parameters"].items():
                        if param_info["required"] and param_name not in parameters:
                            missing_params.append(param_name)
                    
                    if missing_params:
                        st.error(f"Missing required parameters: {', '.join(missing_params)}")
                    else:
                        with st.spinner(f"Executing {selected_tool_name}..."):
                            # Call the tool
                            execution_start = time.time()
                            success, result = run_async(call_mcp_tool(
                                server_url=st.session_state.mcp_server_url,
                                tool_name=selected_tool_name,
                                arguments=parameters
                            ))
                            execution_time = time.time() - execution_start
                            
                            # Record in history
                            history_entry = {
                                "tool": selected_tool_name,
                                "parameters": parameters,
                                "success": success,
                                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
                                "execution_time": execution_time
                            }
                            
                            if success:
                                history_entry["result"] = result
                                
                                # Display result
                                st.success(f"Tool executed successfully in {execution_time:.2f} seconds!")
                                
                                if result["type"] == "dataframe":
                                    df = result["data"]
                                    st.dataframe(df, use_container_width=True)
                                    
                                    # Download option
                                    csv = df.to_csv(index=False).encode('utf-8')
                                    st.download_button(
                                        "Download as CSV",
                                        csv,
                                        f"{selected_tool_name}_result.csv",
                                        "text/csv",
                                        key='download-csv'
                                    )
                                else:
                                    st.code(result["data"])
                            else:
                                history_entry["error"] = result
                                st.error(f"Error: {result}")
                            
                            # Save to history
                            st.session_state.mcp_call_history.append(history_entry)
            else:
                st.warning("Failed to get tool details.")
    
    with tabs[2]:
        st.subheader("Execution History")
        
        if not st.session_state.mcp_call_history:
            st.info("No execution history yet. Execute a tool to see results here.")
        else:
            # Clear history button
            if st.button("Clear History"):
                st.session_state.mcp_call_history = []
                st.rerun()
            
            # Display history in reverse chronological order
            for i, entry in enumerate(reversed(st.session_state.mcp_call_history)):
                with st.expander(f"{entry['timestamp']} - {entry['tool']}"):
                    st.markdown(f"**Tool:** {entry['tool']}")
                    st.markdown(f"**Parameters:** ```{json.dumps(entry['parameters'], indent=2)}```")
                    st.markdown(f"**Execution Time:** {entry['execution_time']:.2f} seconds")
                    
                    if entry['success']:
                        st.success("Execution successful")
                        
                        # Display result
                        result = entry["result"]
                        if result["type"] == "dataframe":
                            df = result["data"]
                            st.dataframe(df, use_container_width=True)
                        else:
                            st.code(result["data"])
                    else:
                        st.error(f"Execution failed: {entry.get('error', 'Unknown error')}")
                    
                    # Re-run button
                    if st.button("Re-run this tool", key=f"rerun_{i}"):
                        # Switch to execution tab
                        # TODO: Can't directly switch tabs in Streamlit, would need to use session state
                        # and check on app startup
                        st.info("Switch to the 'Tool Execution' tab to re-run with the same parameters.")
                        
                        # Pre-select the tool
                        for j, tool in enumerate(st.session_state.mcp_tools):
                            if tool["name"] == entry["tool"]:
                                # Can store the index to reuse later
                                st.session_state.selected_tool_index = j
                                break 