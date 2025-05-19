import streamlit as st
import os
import sys
import logging
import pandas as pd

# Configure logging for this page
logger = logging.getLogger(__name__)

# --- Add workspace root to sys.path for shared modules ---
script_dir = os.path.dirname(os.path.abspath(__file__))
page_dir = os.path.abspath(script_dir)
app_root_dir = os.path.abspath(os.path.join(page_dir, '..'))
workspace_root = os.path.abspath(os.path.join(app_root_dir, '..'))

if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
    logger.info(f"Connectors Page: Added workspace root {workspace_root} to sys.path")
if app_root_dir not in sys.path:
    sys.path.insert(0, app_root_dir)
    logger.info(f"Connectors Page: Added app root {app_root_dir} to sys.path")

# --- Import from arrow-cache-mcp --- 
try:
    from arrow_cache_mcp.core import (
        get_arrow_cache,
        import_data_directly, # Key function for DB connectors
        # CacheNotInitializedError was removed, will rely on general Exception for init issues
    )
    from arrow_cache_mcp.utils import clean_dataset_name
    # Placeholder for S3 and Flight specific loaders if they exist or are added to mcp
    # from arrow_cache_mcp.loaders import load_dataset_from_s3, load_dataset_from_flight

    cache = get_arrow_cache()
    logger.info("Connectors Page: Successfully accessed Arrow Cache and imported mcp components.")
except ImportError as e:
    logger.error(f"Connectors Page: Failed to import from arrow-cache-mcp: {e}", exc_info=True)
    st.error(f"Error loading required modules: {e}. Please ensure 'arrow-cache-mcp' is correctly installed.")
    st.stop()
# Removed specific CacheNotInitializedError, general Exception will catch init issues
except Exception as e:
    logger.error(f"Connectors Page: An unexpected error occurred: {e}", exc_info=True)
    st.error(f"An unexpected error occurred: {e}")
    st.stop()

# --- Page Configuration & Title --- 
st.title("🔌 Data Connectors")
st.markdown("Connect to various external data sources to load data into the studio cache.")

# --- Connector Tabs --- 
tab_postgres, tab_s3, tab_flight = st.tabs(["🗄️ PostgreSQL", "☁️ Amazon S3", "✈️ Apache Arrow Flight"])

# --- PostgreSQL Connector --- 
with tab_postgres:
    st.subheader("Connect to PostgreSQL Database")
    st.markdown("Import data from a PostgreSQL table or using a custom SQL query.")

    with st.form(key="postgres_form"):
        st.write("**Connection Details:**")
        pg_host = st.text_input("Host", value=st.session_state.get("pg_host", "localhost"))
        col1, col2 = st.columns(2)
        with col1:
            pg_port = st.text_input("Port", value=st.session_state.get("pg_port", "5432"))
            pg_user = st.text_input("Username", value=st.session_state.get("pg_user", ""))
        with col2:
            pg_dbname = st.text_input("Database Name", value=st.session_state.get("pg_dbname", ""))
            pg_password = st.text_input("Password", type="password", value=st.session_state.get("pg_password", ""))
        
        st.write("**Data Source:**")
        pg_source_type = st.radio(
            "Import Type:", 
            ["Single Table", "SQL Query"],
            horizontal=True, 
            key="pg_source_type_connector"
        )

        pg_dataset_name_default = "postgres_import"
        if pg_source_type == "Single Table":
            col1, col2 = st.columns(2)
            with col1:
                pg_schema = st.text_input("Schema", value=st.session_state.get("pg_schema", "public"))
            with col2:
                pg_table = st.text_input("Table Name", value=st.session_state.get("pg_table", ""))
            pg_dataset_name_default = clean_dataset_name(f"pg_{pg_schema}_{pg_table}" if pg_table else "postgres_table")
        else: # SQL Query
            pg_query = st.text_area("SQL Query", height=150, value=st.session_state.get("pg_query", "SELECT * FROM your_table LIMIT 100;"), help="Enter the SQL query to extract data.")
            pg_dataset_name_default = clean_dataset_name("postgres_query_result")
            pg_schema = None # Not applicable for query
            pg_table = None # Not applicable for query

        pg_dataset_name = st.text_input("Dataset Name in Cache", value=pg_dataset_name_default, help="This name will be used to reference the dataset in the cache.")
        
        # Add SSL mode for Azure and other cloud providers
        pg_sslmode = st.selectbox("SSL Mode", ["disable", "allow", "prefer", "require", "verify-ca", "verify-full"], index=3, help="Set SSL mode, 'require' is common for cloud DBs")

        submitted_pg = st.form_submit_button("📥 Import from PostgreSQL", type="primary")

    if submitted_pg:
        # Validate inputs
        if not all([pg_host, pg_dbname, pg_user, pg_dataset_name]):
            st.error("Host, Database Name, Username, and Dataset Name are required.")
        elif pg_source_type == "Single Table" and not pg_table:
            st.error("Table Name is required for Single Table import.")
        elif pg_source_type == "SQL Query" and not pg_query:
            st.error("SQL Query is required for SQL Query import.")
        else:
            # Store inputs in session state for persistence across reruns
            st.session_state.pg_host = pg_host
            st.session_state.pg_port = pg_port
            st.session_state.pg_dbname = pg_dbname
            st.session_state.pg_user = pg_user
            st.session_state.pg_password = pg_password # Note: storing password in session state
            st.session_state.pg_schema = pg_schema
            st.session_state.pg_table = pg_table
            st.session_state.pg_query = pg_query if pg_source_type == "SQL Query" else ""
            
            connection_string = f"postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_dbname}"
            if pg_sslmode and pg_sslmode != "disable":
                 connection_string += f"?sslmode={pg_sslmode}"

            with st.spinner(f"Importing data as '{pg_dataset_name}' from PostgreSQL..."):
                try:
                    import_args = {
                        "key": pg_dataset_name,
                        "source": connection_string,
                        "source_type": "postgres"
                    }
                    if pg_source_type == "Single Table":
                        import_args["table_name"] = pg_table
                        import_args["schema"] = pg_schema
                    else: # SQL Query
                        import_args["query"] = pg_query
                    
                    success, result = import_data_directly(**import_args)

                    if success:
                        st.success(f"Successfully imported data as '{result.get('name', pg_dataset_name)}'.")
                        st.json(result) # Display metadata of the imported dataset
                    else:
                        st.error(f"Failed to import from PostgreSQL: {result}")
                except Exception as e:
                    logger.error(f"PostgreSQL import error: {e}", exc_info=True)
                    st.error(f"An unexpected error occurred during PostgreSQL import: {e}")

# --- Amazon S3 Connector --- 
with tab_s3:
    st.subheader("Connect to Amazon S3")
    st.markdown("Load datasets from CSV, Parquet, or other supported files stored in S3 buckets.")
    st.info("💡 Note: S3 integration via `arrow-cache-mcp` might require `boto3` and `s3fs`. Ensure these are installed in your environment if you encounter issues.", icon="ℹ️")

    with st.form(key="s3_form"):
        st.write("**AWS Credentials & Configuration:**")
        s3_aws_access_key_id = st.text_input("AWS Access Key ID", value=st.session_state.get("s3_access_key", ""), help="Leave blank if using IAM roles or environment variables.")
        s3_aws_secret_access_key = st.text_input("AWS Secret Access Key", type="password", value=st.session_state.get("s3_secret_key", ""))
        s3_aws_session_token = st.text_input("AWS Session Token (optional)", type="password", value=st.session_state.get("s3_session_token", ""))
        s3_region_name = st.text_input("AWS Region", value=st.session_state.get("s3_region", "us-east-1"))
        
        st.write("**S3 Object Details:**")
        s3_bucket_name = st.text_input("S3 Bucket Name", value=st.session_state.get("s3_bucket", ""))
        s3_object_key = st.text_input("S3 Object Key (File Path)", value=st.session_state.get("s3_object_key", "path/to/your/file.parquet"))
        
        s3_dataset_name_default = clean_dataset_name(os.path.basename(s3_object_key) if s3_object_key else "s3_import")
        s3_dataset_name = st.text_input("Dataset Name in Cache", value=s3_dataset_name_default)
        
        # Format auto-detection will be based on S3 object key extension
        st.caption("File format (e.g., CSV, Parquet) will be auto-detected from the S3 object key extension.")
        
        submitted_s3 = st.form_submit_button("📥 Load from S3", type="primary")

    if submitted_s3:
        if not all([s3_bucket_name, s3_object_key, s3_dataset_name]):
            st.error("S3 Bucket Name, Object Key, and Dataset Name are required.")
        else:
            st.session_state.s3_access_key = s3_aws_access_key_id
            st.session_state.s3_secret_key = s3_aws_secret_access_key
            st.session_state.s3_session_token = s3_aws_session_token
            st.session_state.s3_region = s3_region_name
            st.session_state.s3_bucket = s3_bucket_name
            st.session_state.s3_object_key = s3_object_key

            s3_path = f"s3://{s3_bucket_name}/{s3_object_key.lstrip('/')}"
            storage_options = {}
            if s3_aws_access_key_id and s3_aws_secret_access_key:
                storage_options['key'] = s3_aws_access_key_id
                storage_options['secret'] = s3_aws_secret_access_key
                if s3_aws_session_token:
                    storage_options['token'] = s3_aws_session_token
            # s3fs should pick up region from env or default config if not specified here
            # if s3_region_name:
            #     storage_options['client_kwargs'] = {'region_name': s3_region_name}

            with st.spinner(f"Loading '{s3_dataset_name}' from S3 path: {s3_path}"):
                try:
                    # Assuming load_dataset_from_path in arrow-cache-mcp can handle s3:// paths
                    # if s3fs is installed and credentials are set correctly.
                    # This might need a dedicated load_dataset_from_s3 function in arrow-cache-mcp for robustness.
                    from arrow_cache_mcp.loaders import load_dataset_from_path # Re-import for clarity
                    
                    # load_dataset_from_path might internally use pd.read_parquet, pd.read_csv etc.
                    # which can take storage_options for s3fs.
                    success, result = load_dataset_from_path(
                        path=s3_path, 
                        dataset_name=s3_dataset_name,
                        storage_options=storage_options if storage_options else None
                        # format can be auto-detected by load_dataset_from_path
                    )
                    if success:
                        st.success(f"Dataset '{result.get('name', s3_dataset_name)}' loaded from S3.")
                        st.json(result)
                    else:
                        st.error(f"Failed to load from S3: {result}. Ensure 's3fs' and 'boto3' are installed and AWS credentials/permissions are correct.")
                except ImportError:
                    st.error("Failed to load from S3: `s3fs` library might be missing. Please install it (`pip install s3fs boto3`).")
                except Exception as e:
                    logger.error(f"S3 import error: {e}", exc_info=True)
                    st.error(f"An error occurred during S3 import: {e}")

# --- Apache Arrow Flight Connector --- 
with tab_flight:
    st.subheader("Connect to Apache Arrow Flight Server")
    st.markdown("Load datasets exposed via an Arrow Flight endpoint.")
    st.info("💡 Note: Arrow Flight integration requires `pyarrow` with Flight capabilities. This is usually included in standard `pyarrow` installs.", icon="ℹ️")

    with st.form(key="flight_form"):
        flight_uri = st.text_input("Flight Server URI", value=st.session_state.get("flight_uri", "grpc://localhost:8815"), help="e.g., grpc://host:port or grpc+tls://host:port")
        # TODO: Add options for authentication (e.g., bearer token)
        # TODO: Add options for listing flights (Info an FlightDescriptor) vs getting a specific flight (Ticket)
        flight_ticket_str = st.text_input("Flight Ticket (JSON or string)", value=st.session_state.get("flight_ticket", "{\"dataset_name\": \"my_flight_dataset\"}"), help="Usually a JSON string representing the ticket, or a simple name if server supports it.")
        
        flight_dataset_name_default = clean_dataset_name(f"flight_import_{flight_ticket_str[:20]}")
        flight_dataset_name = st.text_input("Dataset Name in Cache", value=flight_dataset_name_default)

        submitted_flight = st.form_submit_button("📥 Load from Arrow Flight", type="primary")

    if submitted_flight:
        if not all([flight_uri, flight_ticket_str, flight_dataset_name]):
            st.error("Flight URI, Ticket, and Dataset Name are required.")
        else:
            st.session_state.flight_uri = flight_uri
            st.session_state.flight_ticket = flight_ticket_str
            
            with st.spinner(f"Loading '{flight_dataset_name}' from Arrow Flight endpoint: {flight_uri}"):
                st.warning("Arrow Flight connector is a placeholder. Backend loading logic needs to be implemented in `arrow-cache-mcp`.")
                # try:
                #     # This would be the ideal call if arrow-cache-mcp supports it
                #     # success, result = load_dataset_from_flight(
                #     #     uri=flight_uri, 
                #     #     ticket=flight_ticket_str, # or parsed ticket object
                #     #     dataset_name=flight_dataset_name
                #     #     # auth_handler=... if needed
                #     # )
                #     # if success:
                #     #     st.success(f"Dataset '{result.get('name', flight_dataset_name)}' loaded from Arrow Flight.")
                #     #     st.json(result)
                #     # else:
                #     #     st.error(f"Failed to load from Arrow Flight: {result}")
                # except Exception as e:
                #     logger.error(f"Arrow Flight import error: {e}", exc_info=True)
                #     st.error(f"An error occurred during Arrow Flight import: {e}")
                st.info(f"Placeholder: Would attempt to load {flight_dataset_name} from URI {flight_uri} using ticket {flight_ticket_str}")


logger.info("Connectors page loaded.") 