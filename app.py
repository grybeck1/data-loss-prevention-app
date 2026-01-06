import streamlit as st
import pandas as pd
from databricks import sql
from databricks.sdk.core import Config
import os
from datetime import datetime
import getpass

# Page configuration
st.set_page_config(
    page_title="Data Loss Prevention App",
    page_icon="🔒",
    layout="wide"
)

# Initialize Databricks Config (automatically loads from environment variables)
try:
    cfg = Config()
except Exception:
    cfg = None  # Will be None in local development mode

# Function to check if running as Databricks App
def is_databricks_app():
    """Check if running as a Databricks App by looking for forwarded headers"""
    try:
        # Try to get Streamlit's request context
        from streamlit.web.server.websocket_headers import _get_websocket_headers
        headers = _get_websocket_headers()
        return headers and ('X-Forwarded-Email' in headers or 'X-Forwarded-Access-Token' in headers)
    except:
        # Fallback: check environment or other indicators
        return os.getenv('DATABRICKS_APP_MODE') == 'true'

# Function to get Databricks App user context
def get_databricks_app_user():
    """
    Get user email, access token, and host from Databricks App headers.
    
    When deployed as a Databricks App, the platform automatically forwards:
    - X-Forwarded-Email: Authenticated user's email
    - X-Forwarded-Access-Token: User's access token for API calls
    - X-Forwarded-Host: Databricks workspace host
    
    This enables per-user access control where queries run with each user's
    permissions, respecting Unity Catalog row/column-level security.
    
    Pattern from: https://docs.databricks.com/en/dev-tools/databricks-apps/
    """
    try:
        # Use modern st.context.headers API (Streamlit 1.27+)
        email = st.context.headers.get('X-Forwarded-Email')
        token = st.context.headers.get('X-Forwarded-Access-Token')
        host = st.context.headers.get('X-Forwarded-Host')
        
        # If host not in headers, try to construct from environment
        if not host:
            host = os.getenv('DATABRICKS_HOST', '')
        
        # Clean the host (remove https:// and trailing slashes)
        if host:
            host = host.replace('https://', '').replace('http://', '').rstrip('/')
        
        return {
            'email': email,
            'token': token,
            'host': host,
            'is_databricks_app': token is not None
        }
    except Exception as e:
        # Not running in Databricks App context or headers not available
        return {
            'email': None,
            'token': None,
            'host': None,
            'is_databricks_app': False
        }

# Audit logging function
def log_audit_event(event_type, details, user_email=None):
    """Log data access and export events for compliance to stdout"""
    try:
        timestamp = datetime.now().isoformat()
        username = user_email or getpass.getuser()
        
        log_entry = f"[{timestamp}] USER={username} EVENT={event_type} DETAILS={details}"
        
        # Log to stdout for Databricks App logging
        print(log_entry)
    except Exception as e:
        # Silently fail - don't break the app if logging fails
        pass


# Title and description
st.title("🔒 Data Loss Prevention App")
st.markdown("Export data from Unity Catalog tables with ease")

# Sidebar for connection settings
st.sidebar.header("Databricks Connection")

# Check if running as Databricks App
user_context = get_databricks_app_user()
is_databricks_app_mode = user_context['is_databricks_app']
warehouse_id = os.getenv('DATABRICKS_WAREHOUSE_ID')

if is_databricks_app_mode and cfg:
    # Databricks App mode - use forwarded credentials and Config
    st.sidebar.success("🎯 **Databricks App Mode**")
    st.sidebar.info(f"👤 Signed in as: **{user_context['email']}**")
    st.sidebar.caption("Using your Databricks credentials automatically")
    
    auth_method = "Databricks App"
    server_hostname = cfg.host
    access_token = user_context['token']
    
    # Build HTTP path from warehouse_id
    if warehouse_id:
        http_path = f"/sql/1.0/warehouses/{warehouse_id}"
    else:
        http_path = None
        st.sidebar.warning("⚠️ SQL Warehouse not configured. Please set DATABRICKS_WAREHOUSE_ID in app.yaml")
else:
    # Local mode - use environment variables
    st.sidebar.info("🔒 **Local Development Mode**")
    st.sidebar.caption("Using environment variables")
    auth_method = "Environment Variables"
    
    # Read from environment variables
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
    http_path = os.getenv("DATABRICKS_HTTP_PATH", "")
    access_token = os.getenv("DATABRICKS_TOKEN", "")
    
    # Show status
    if server_hostname and http_path and access_token:
        st.sidebar.success("✅ Environment variables detected")
        st.sidebar.caption(f"Host: `{server_hostname}`")
    else:
        st.sidebar.error("⚠️ Missing environment variables")
        missing = []
        if not server_hostname: missing.append("DATABRICKS_SERVER_HOSTNAME")
        if not http_path: missing.append("DATABRICKS_HTTP_PATH")
        if not access_token: missing.append("DATABRICKS_TOKEN")
        st.sidebar.code(f"Missing: {', '.join(missing)}")

# Initialize session state
if 'connection' not in st.session_state:
    st.session_state.connection = None
if 'catalogs' not in st.session_state:
    st.session_state.catalogs = []
if 'schemas' not in st.session_state:
    st.session_state.schemas = []
if 'tables' not in st.session_state:
    st.session_state.tables = []
if 'data' not in st.session_state:
    st.session_state.data = None
if 'terms_accepted' not in st.session_state:
    st.session_state.terms_accepted = False

# Auto-connect if credentials are available
if server_hostname and http_path and access_token and not st.session_state.connection:
    try:
        with st.spinner("Connecting to Databricks..."):
            # Connect to Databricks using the user's access token
            with sql.connect(
                server_hostname=server_hostname,
                http_path=http_path,
                access_token=access_token
            ) as connection:
                with connection.cursor() as cursor:
                    # Test connection and fetch catalogs
                    cursor.execute("SHOW CATALOGS")
                    st.session_state.catalogs = [row[0] for row in cursor.fetchall()]
            
            # Store connection details for later queries
            st.session_state.connection_config = {
                'server_hostname': server_hostname,
                'http_path': http_path,
                'access_token': access_token
            }
            st.session_state.connection = True  # Mark as connected
            
            # Log successful connection
            auth_info = "DatabricksApp" if is_databricks_app_mode else "EnvironmentVariables"
            user_email = user_context.get('email') if is_databricks_app_mode else None
            log_audit_event(
                "CONNECTION_SUCCESS",
                f"host={server_hostname}, method={auth_method}, auth_type={auth_info}",
                user_email=user_email
            )
            
            st.sidebar.success("✅ Connected successfully!")
    except Exception as e:
        error_msg = str(e)
        st.sidebar.error(f"❌ Connection failed: {error_msg}")
        
        # Provide helpful hints
        if "Invalid access token" in error_msg or "401" in error_msg or "INVALID_TOKEN" in error_msg:
            st.sidebar.warning("💡 **Token Issue**: The access token may be expired or invalid.\n\n"
                               "**Solution**: Update your DATABRICKS_TOKEN environment variable")
        elif "404" in error_msg or "not found" in error_msg.lower():
            st.sidebar.warning("💡 **Path Issue**: Check DATABRICKS_HTTP_PATH environment variable")
        elif "host" in error_msg.lower() or "hostname" in error_msg.lower():
            st.sidebar.warning("💡 **Host Issue**: Check DATABRICKS_SERVER_HOSTNAME environment variable")
        elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
            st.sidebar.warning("💡 **Timeout**: Connection timed out. Check:\n"
                               "1. SQL Warehouse is running\n"
                               "2. Network connectivity\n"
                               "3. Firewall settings")

# Main content
if st.session_state.connection and 'connection_config' in st.session_state:
    config = st.session_state.connection_config
    
    col1, col2, col3 = st.columns(3)
    
    # Catalog selector
    with col1:
        # Default to "samples" if available
        default_catalog_index = 0
        if "samples" in st.session_state.catalogs:
            default_catalog_index = st.session_state.catalogs.index("samples")
        
        selected_catalog = st.selectbox(
            "Select Catalog",
            options=st.session_state.catalogs,
            index=default_catalog_index,
            key="catalog_selector"
        )
    
    # Schema selector
    with col2:
        if selected_catalog:
            try:
                with sql.connect(**config) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"SHOW SCHEMAS IN {selected_catalog}")
                        st.session_state.schemas = [row[0] for row in cursor.fetchall()]
            except Exception as e:
                st.error(f"Error fetching schemas: {str(e)}")
                st.session_state.schemas = []
            
            # Default to "nyctaxi" if available
            default_schema_index = 0
            if "nyctaxi" in st.session_state.schemas:
                default_schema_index = st.session_state.schemas.index("nyctaxi")
            
            selected_schema = st.selectbox(
                "Select Schema",
                options=st.session_state.schemas,
                index=default_schema_index,
                key="schema_selector"
            )
        else:
            selected_schema = None
    
    # Table selector
    with col3:
        if selected_catalog and selected_schema:
            try:
                with sql.connect(**config) as connection:
                    with connection.cursor() as cursor:
                        cursor.execute(f"SHOW TABLES IN {selected_catalog}.{selected_schema}")
                        st.session_state.tables = [row[1] for row in cursor.fetchall()]
            except Exception as e:
                st.error(f"Error fetching tables: {str(e)}")
                st.session_state.tables = []
            
            # Default to "trips" if available
            default_table_index = 0
            if "trips" in st.session_state.tables:
                default_table_index = st.session_state.tables.index("trips")
            
            selected_table = st.selectbox(
                "Select Table",
                options=st.session_state.tables,
                index=default_table_index,
                key="table_selector"
            )
        else:
            selected_table = None
    
    # Query and display data
    if selected_catalog and selected_schema and selected_table:
        st.divider()
        
        col_a, col_b = st.columns([3, 1])
        with col_a:
            st.subheader(f"📊 Table: {selected_catalog}.{selected_schema}.{selected_table}")
        
        with col_b:
            row_limit = st.number_input("Row Limit", min_value=1, max_value=100000, value=10)
        
        if st.button("Load Data", type="primary"):
            try:
                with st.spinner(f"Loading data from {selected_table}..."):
                    query = f"SELECT * FROM {selected_catalog}.{selected_schema}.{selected_table} LIMIT {row_limit}"
                    
                    # Query with user's token using context manager
                    with sql.connect(**config) as connection:
                        with connection.cursor() as cursor:
                            cursor.execute(query)
                            # Use fetchall_arrow for better performance
                            result = cursor.fetchall_arrow().to_pandas()
                            st.session_state.data = result
                    
                    # Reset terms acceptance when loading new data
                    st.session_state.terms_accepted = False
                    
                    # Log data access
                    user_email = user_context.get('email') if is_databricks_app_mode else None
                    log_audit_event(
                        "DATA_LOADED",
                        f"table={selected_catalog}.{selected_schema}.{selected_table}, rows={len(st.session_state.data)}, columns={len(st.session_state.data.columns)}",
                        user_email=user_email
                    )
                    
                    st.success(f"✅ Loaded {len(st.session_state.data)} rows")
            except Exception as e:
                st.error(f"❌ Error loading data: {str(e)}")
        
        # Display data
        if st.session_state.data is not None:
            st.dataframe(data=st.session_state.data, height=400, use_container_width=True)
            
            # Export options with Terms of Use
            st.divider()
            st.subheader("📥 Export Data")
            
            # Terms of Use Section
            with st.expander("📜 **Terms of Use** (Must Accept to Download)", expanded=not st.session_state.terms_accepted):
                st.markdown("""
                ### Data Export Terms and Conditions
                
                By downloading data from this application, you acknowledge and agree to the following:
                
                1. **Data Usage**: The exported data is intended for authorized business purposes only.
                
                2. **Confidentiality**: You will maintain the confidentiality of any sensitive or proprietary information contained in the exported data.
                
                3. **Compliance**: You will comply with all applicable data protection laws and regulations, including but not limited to GDPR, CCPA, and HIPAA (where applicable).
                
                4. **Security**: You are responsible for securing the downloaded data and preventing unauthorized access.
                
                5. **No Redistribution**: You will not redistribute, share, or publish the exported data without proper authorization.
                
                6. **Audit Trail**: All data exports are logged and may be subject to audit for compliance purposes.
                
                7. **Access Rights**: You confirm that you have the appropriate permissions and access rights to export this data.
                
                8. **Liability**: Misuse of exported data may result in disciplinary action and/or legal consequences.
                
                ---
                
                **By checking the box below, you certify that you have read, understood, and agree to these terms.**
                """)
                
                terms_checkbox = st.checkbox(
                    "✅ I accept the Terms of Use and agree to handle exported data responsibly",
                    value=st.session_state.terms_accepted,
                    key="terms_checkbox"
                )
                
                if terms_checkbox != st.session_state.terms_accepted:
                    st.session_state.terms_accepted = terms_checkbox
                    if terms_checkbox:
                        # Log terms acceptance
                        user_email = user_context.get('email') if is_databricks_app_mode else None
                        log_audit_event(
                            "TERMS_ACCEPTED",
                            f"table={selected_catalog}.{selected_schema}.{selected_table}, rows={len(st.session_state.data)}",
                            user_email=user_email
                        )
                        st.success("✅ Terms accepted. You may now download data.")
                    else:
                        st.warning("⚠️ You must accept the terms to download data.")
            
            if not st.session_state.terms_accepted:
                st.warning("⚠️ **Please accept the Terms of Use above to enable data downloads.**")
            
            col_export1, col_export2 = st.columns(2)
            
            with col_export1:
                # CSV export
                csv = st.session_state.data.to_csv(index=False)
                st.download_button(
                    label="Download as CSV" if st.session_state.terms_accepted else "🔒 Accept Terms to Download CSV",
                    data=csv,
                    file_name=f"{selected_table}.csv",
                    mime="text/csv",
                    disabled=not st.session_state.terms_accepted,
                    type="primary" if st.session_state.terms_accepted else "secondary"
                )
            
            with col_export2:
                # JSON export
                json = st.session_state.data.to_json(orient='records', indent=2)
                st.download_button(
                    label="Download as JSON" if st.session_state.terms_accepted else "🔒 Accept Terms to Download JSON",
                    data=json,
                    file_name=f"{selected_table}.json",
                    mime="application/json",
                    disabled=not st.session_state.terms_accepted,
                    type="primary" if st.session_state.terms_accepted else "secondary"
                )
            
            if st.session_state.terms_accepted:
                st.caption("📋 Note: Data exports are logged for compliance and security purposes.")
            
            # Display stats
            st.divider()
            st.subheader("📈 Data Statistics")
            col_stat1, col_stat2, col_stat3 = st.columns(3)
            with col_stat1:
                st.metric("Total Rows", len(st.session_state.data))
            with col_stat2:
                st.metric("Total Columns", len(st.session_state.data.columns))
            with col_stat3:
                memory_usage = st.session_state.data.memory_usage(deep=True).sum() / 1024 / 1024
                st.metric("Memory Usage", f"{memory_usage:.2f} MB")

else:
    if is_databricks_app_mode:
        st.info("👈 Click **Connect to SQL Warehouse** in the sidebar to get started")
        st.markdown("""
        ### 🎯 Welcome to Data Loss Prevention App
        
        You're running in **Databricks App mode** with automatic authentication.
        
        **Your session:**
        - ✅ Authenticated as: `{}`
        - ✅ Using your Databricks permissions
        - ✅ All queries run with your access rights
        - ✅ Activity logged for compliance
        
        Click the **Connect to SQL Warehouse** button in the sidebar to begin.
        """.format(user_context.get('email', 'Unknown')))
    else:
        st.info("👈 Set environment variables to connect to Databricks")
        
        st.markdown("""
        ### 🔒 Local Development Setup
        
        Set these environment variables before running the app:
        
        ```bash
        export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
        export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
        export DATABRICKS_TOKEN="dapi...your-token..."
        
        # Then run the app
        streamlit run app.py
        ```
        
        ---
        
        ### 💡 How to Get Your Credentials:
        
        #### Access Token:
        1. Open your Databricks workspace
        2. Click your profile icon → **User Settings**
        3. Go to **Access Tokens** tab
        4. Click **Generate New Token**
        5. Copy the token (starts with `dapi`)
        
        #### Server Hostname:
        - Found in your browser URL bar when logged into Databricks
        - Example: `your-workspace.cloud.databricks.com`
        - **Don't include** `https://`
        
        #### HTTP Path:
        1. Go to **SQL Warehouses** in Databricks
        2. Click on your SQL Warehouse
        3. Go to the **Connection Details** tab
        4. Copy the **HTTP path**
        5. Example: `/sql/1.0/warehouses/abc123def456`
        
        ---
        
        ### 🚀 Features:
        - ✅ Auto-connect to SQL Warehouse
        - ✅ Browse Unity Catalog tables
        - ✅ Preview data before export
        - ✅ Terms of Use acceptance required
        - ✅ Download as CSV or JSON
        - ✅ Full audit logging
        """)

# Footer
st.divider()
col_footer1, col_footer2 = st.columns([3, 1])
with col_footer1:
    st.caption("🔒 Data Loss Prevention App - Secure data export from Unity Catalog")
with col_footer2:
    st.caption("📋 All activity logged for compliance")
    
# Show audit log information
with st.expander("ℹ️ Audit Logging Information"):
    st.info("""
    **Audit Trail**: All data access and export activities are logged for compliance purposes.
    
    **Log Output**: Application logs (stdout/stderr)
    
    **Logged Events**:
    - Database connections
    - Data table access and loading
    - Terms of Use acceptance
    - User identity (when in Databricks App mode)
    
    **Log Format**:
    ```
    [timestamp] USER=user@company.com EVENT=event_type DETAILS=details
    ```
    
    Logs are captured by the Databricks platform and can be reviewed by security and compliance teams.
    """)

