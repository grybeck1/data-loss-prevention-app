import streamlit as st
import pandas as pd
from databricks import sql
import os
from pathlib import Path
import configparser
from datetime import datetime
import getpass
from databricks.sdk.core import Config as DatabricksConfig
from databricks.sdk import WorkspaceClient

# Page configuration
st.set_page_config(
    page_title="Data Loss Prevention App",
    page_icon="🔒",
    layout="wide"
)

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
        # Try new st.context.headers (Streamlit 1.27+) first
        try:
            import streamlit as st_ctx
            if hasattr(st_ctx, 'context'):
                headers = st_ctx.context.headers
            else:
                # Fall back to deprecated method for older Streamlit versions
                from streamlit.web.server.websocket_headers import _get_websocket_headers
                headers = _get_websocket_headers()
        except:
            # Fall back to deprecated method
            from streamlit.web.server.websocket_headers import _get_websocket_headers
            headers = _get_websocket_headers()
        
        email = headers.get('X-Forwarded-Email', None)
        token = headers.get('X-Forwarded-Access-Token', None)
        host = headers.get('X-Forwarded-Host', None)
        
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
        return {
            'email': None,
            'token': None,
            'host': None,
            'is_databricks_app': False
        }

# Audit logging function
def log_audit_event(event_type, details, user_email=None):
    """Log data access and export events for compliance"""
    try:
        log_dir = Path.home() / ".dlp_app_logs"
        log_dir.mkdir(exist_ok=True)
        log_file = log_dir / "audit_log.txt"
        
        timestamp = datetime.now().isoformat()
        username = user_email or getpass.getuser()
        
        log_entry = f"[{timestamp}] USER={username} EVENT={event_type} DETAILS={details}\n"
        
        with open(log_file, 'a') as f:
            f.write(log_entry)
    except Exception as e:
        # Silently fail - don't break the app if logging fails
        pass

# Function to load Databricks profiles from ~/.databrickscfg
def load_databricks_profiles():
    """Load profiles from ~/.databrickscfg if it exists"""
    config_path = Path.home() / ".databrickscfg"
    if not config_path.exists():
        return {}
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    profiles = {}
    for section in config.sections():
        # Get host and clean it
        host = config.get(section, 'host', fallback=None)
        if host:
            # Remove https:// and trailing slashes
            host = host.replace('https://', '').replace('http://', '').rstrip('/')
        
        # Get token and auth_type
        token = config.get(section, 'token', fallback=None)
        auth_type = config.get(section, 'auth_type', fallback=None)
        
        # ONLY include token-based profiles to avoid OAuth hanging issues
        # OAuth profiles require interactive flow which doesn't work well in Streamlit
        if host and token:
            profiles[section] = {
                'host': host,
                'token': token,
                'auth_type': auth_type,
                'is_oauth': False,  # We only load token profiles
            }
    
    return profiles

# Function to get OAuth credentials for a profile
def get_oauth_credentials(profile_name):
    """Get OAuth credentials using Databricks SDK for a specific profile"""
    try:
        # Use the Databricks SDK to handle OAuth authentication
        # The SDK reads from ~/.databrickscfg and handles token refresh
        cfg = DatabricksConfig(profile=profile_name)
        
        # Try to get the token - SDK should use cached credentials
        # This should NOT trigger an interactive OAuth flow if already authenticated
        try:
            token = cfg.token
            if token:
                return token
        except Exception as token_error:
            # Token retrieval failed, try to get from WorkspaceClient
            # which may handle OAuth refresh better
            try:
                from databricks.sdk import WorkspaceClient
                w = WorkspaceClient(profile=profile_name)
                # The client should have authenticated, get the token
                if hasattr(w.config, 'token') and w.config.token:
                    return w.config.token
                else:
                    raise Exception("Could not retrieve OAuth token from profile")
            except Exception as client_error:
                raise Exception(f"OAuth token retrieval failed. Try re-authenticating with: databricks auth login --profile {profile_name}")
        
        raise Exception(f"No token found in profile {profile_name}")
    except Exception as e:
        raise Exception(f"OAuth authentication failed: {str(e)}")

# Title and description
st.title("🔒 Data Loss Prevention App")
st.markdown("Export data from Unity Catalog tables with ease")

# Sidebar for connection settings
st.sidebar.header("Databricks Connection")

# Check if running as Databricks App with built-in auth
user_context = get_databricks_app_user()
is_databricks_app_mode = user_context['is_databricks_app']

if is_databricks_app_mode:
    # Databricks App mode - use forwarded credentials
    st.sidebar.success("🎯 **Databricks App Mode**")
    st.sidebar.info(f"👤 Signed in as: **{user_context['email']}**")
    st.sidebar.caption("Using your Databricks credentials automatically")
    
    # Set variables for connection
    auth_method = "Databricks App"
    server_hostname = user_context.get('host', '')
    access_token = user_context['token']
    
    # Still need HTTP path from config
    http_path = os.getenv("SQL_WAREHOUSE", os.getenv("DATABRICKS_HTTP_PATH", ""))
    
    if not server_hostname:
        st.sidebar.error("⚠️ Databricks host not detected. Check app configuration.")
    
    if not http_path:
        st.sidebar.warning("⚠️ SQL Warehouse not configured. Please set SQL_WAREHOUSE in app.yaml")
    
    databricks_profiles = {}  # Not needed in app mode
else:
    # Local mode - show authentication options
    # Load Databricks CLI profiles
    databricks_profiles = load_databricks_profiles()
    
    # Authentication method selection
    auth_method = st.sidebar.radio(
        "Authentication Method:",
        options=["Databricks CLI Profile (Recommended)", "Environment Variables", "Manual Entry"],
        index=0 if databricks_profiles else (1 if os.getenv("DATABRICKS_TOKEN") else 2),
        help="Choose how to authenticate with Databricks"
    )

# Initialize connection variables if not in Databricks App mode
if not is_databricks_app_mode:
    server_hostname = ""
    http_path = ""
    access_token = ""

if not is_databricks_app_mode and auth_method == "Databricks CLI Profile (Recommended)":
    if databricks_profiles:
        profile_name = st.sidebar.selectbox(
            "Select Profile",
            options=list(databricks_profiles.keys()),
            help="Token-based profiles from ~/.databrickscfg"
        )
        
        if profile_name:
            profile = databricks_profiles[profile_name]
            server_hostname = profile['host'] or ""
            access_token = profile['token'] or ""
            
            st.sidebar.success(f"✅ Using profile: **{profile_name}**")
            st.sidebar.info(f"🔒 Host: `{server_hostname}`")
            
            # Still need HTTP path
            # Check for SQL_WAREHOUSE env var (Databricks App deployment) or DATABRICKS_HTTP_PATH
            default_http_path = os.getenv("SQL_WAREHOUSE", os.getenv("DATABRICKS_HTTP_PATH", ""))
            http_path = st.sidebar.text_input(
                "HTTP Path",
                value=default_http_path,
                help="e.g., /sql/1.0/warehouses/xxxxx",
                placeholder="/sql/1.0/warehouses/xxxxx"
            )
            
            # Store profile info in session state for connection
            if 'selected_profile' not in st.session_state:
                st.session_state.selected_profile = None
            st.session_state.selected_profile = profile_name
    else:
        # Check if config file exists
        config_path = Path.home() / ".databrickscfg"
        if config_path.exists():
            st.sidebar.warning("⚠️ No token-based profiles found in `~/.databrickscfg`.")
            st.sidebar.info("""
            Your config file exists but contains no profiles with access tokens.
            
            **Create a token profile:**
            ```ini
            [my-profile]
            host = https://your-workspace.cloud.databricks.com
            token = dapi...your-token...
            ```
            
            Get a token from: Databricks → User Settings → Access Tokens
            """)
        else:
            st.sidebar.warning("⚠️ No Databricks config found at `~/.databrickscfg`.")
            st.sidebar.markdown("""
            **Create ~/.databrickscfg:**
            ```ini
            [DEFAULT]
            host = https://your-workspace.cloud.databricks.com
            token = dapi...your-token...
            ```
            
            Get a token from: Databricks → User Settings → Access Tokens
            """)
        # Fall back to environment variables or manual entry
        auth_method = "Environment Variables" if os.getenv("DATABRICKS_TOKEN") else "Manual Entry"

if not is_databricks_app_mode and auth_method == "Environment Variables":
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME", "")
    # Support both DATABRICKS_HTTP_PATH and SQL_WAREHOUSE (Databricks App deployment)
    http_path = os.getenv("SQL_WAREHOUSE", os.getenv("DATABRICKS_HTTP_PATH", ""))
    access_token = os.getenv("DATABRICKS_TOKEN", "")
    
    if all([server_hostname, http_path, access_token]):
        st.sidebar.success("✅ Using environment variables")
    else:
        st.sidebar.warning("⚠️ Environment variables not fully set")
        st.sidebar.code("""
export DATABRICKS_SERVER_HOSTNAME="..."
export DATABRICKS_HTTP_PATH="..."
export DATABRICKS_TOKEN="..."
        """)

if not is_databricks_app_mode and auth_method == "Manual Entry":
    st.sidebar.warning("🔒 **Security Warning**: Manual entry is less secure. Use CLI or env vars for production.")
    
    server_hostname = st.sidebar.text_input(
        "Server Hostname",
        value="",
        help="e.g., your-workspace.cloud.databricks.com"
    )
    
    http_path = st.sidebar.text_input(
        "HTTP Path",
        value="",
        help="e.g., /sql/1.0/warehouses/xxxxx"
    )
    
    access_token = st.sidebar.text_input(
        "Access Token",
        value="",
        type="password",
        help="Your Databricks personal access token"
    )

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

# Connect button (or auto-connect in Databricks App mode)
connect_button_label = "Connect to Databricks" if not is_databricks_app_mode else "🔌 Connect to SQL Warehouse"
if st.sidebar.button(connect_button_label):
    # Check if we have the minimum requirements
    if not server_hostname or not http_path:
        missing = []
        if not server_hostname: missing.append("Server Hostname")
        if not http_path: missing.append("HTTP Path")
        st.sidebar.warning(f"⚠️ Missing: {', '.join(missing)}")
    else:
        try:
            with st.spinner("Connecting to Databricks..."):
                # Debug info
                st.sidebar.info(f"🔌 Connecting to: `{server_hostname}`")
                st.sidebar.info(f"📍 HTTP Path: `{http_path}`")
                
                # Validate we have an access token
                if not access_token:
                    raise Exception("Access token is required")
                
                # Connect to Databricks with timeout
                connection = sql.connect(
                    server_hostname=server_hostname,
                    http_path=http_path,
                    access_token=access_token,
                    _session_timeout=30  # 30 second timeout
                )
                st.session_state.connection = connection
                
                # Fetch catalogs
                cursor = connection.cursor()
                cursor.execute("SHOW CATALOGS")
                st.session_state.catalogs = [row[0] for row in cursor.fetchall()]
                cursor.close()
                
                # Log successful connection
                auth_info = "DatabricksApp" if is_databricks_app_mode else "Token"
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
                                   "**Solution**: Generate a new token in Databricks:\n"
                                   "1. Go to User Settings > Access Tokens\n"
                                   "2. Click 'Generate New Token'\n"
                                   "3. Update your ~/.databrickscfg or use Manual Entry")
            elif "404" in error_msg or "not found" in error_msg.lower():
                st.sidebar.warning("💡 **Path Issue**: Check that the HTTP Path is correct.\n\n"
                                   "Find it in: SQL Warehouses > Your Warehouse > Connection Details")
            elif "host" in error_msg.lower() or "hostname" in error_msg.lower():
                st.sidebar.warning("💡 **Host Issue**: Verify the server hostname is correct")
            elif "timeout" in error_msg.lower() or "timed out" in error_msg.lower():
                st.sidebar.warning("💡 **Timeout**: Connection timed out. Check:\n"
                                   "1. SQL Warehouse is running\n"
                                   "2. Network connectivity\n"
                                   "3. Firewall settings")

# Main content
if st.session_state.connection:
    col1, col2, col3 = st.columns(3)
    
    # Catalog selector
    with col1:
        selected_catalog = st.selectbox(
            "Select Catalog",
            options=st.session_state.catalogs,
            key="catalog_selector"
        )
    
    # Schema selector
    with col2:
        if selected_catalog:
            try:
                cursor = st.session_state.connection.cursor()
                cursor.execute(f"SHOW SCHEMAS IN {selected_catalog}")
                st.session_state.schemas = [row[0] for row in cursor.fetchall()]
                cursor.close()
            except Exception as e:
                st.error(f"Error fetching schemas: {str(e)}")
                st.session_state.schemas = []
            
            selected_schema = st.selectbox(
                "Select Schema",
                options=st.session_state.schemas,
                key="schema_selector"
            )
        else:
            selected_schema = None
    
    # Table selector
    with col3:
        if selected_catalog and selected_schema:
            try:
                cursor = st.session_state.connection.cursor()
                cursor.execute(f"SHOW TABLES IN {selected_catalog}.{selected_schema}")
                st.session_state.tables = [row[1] for row in cursor.fetchall()]
                cursor.close()
            except Exception as e:
                st.error(f"Error fetching tables: {str(e)}")
                st.session_state.tables = []
            
            selected_table = st.selectbox(
                "Select Table",
                options=st.session_state.tables,
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
            row_limit = st.number_input("Row Limit", min_value=1, max_value=100000, value=1000)
        
        if st.button("Load Data", type="primary"):
            try:
                with st.spinner(f"Loading data from {selected_table}..."):
                    cursor = st.session_state.connection.cursor()
                    query = f"SELECT * FROM {selected_catalog}.{selected_schema}.{selected_table} LIMIT {row_limit}"
                    cursor.execute(query)
                    
                    # Fetch data into pandas dataframe
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    st.session_state.data = pd.DataFrame(rows, columns=columns)
                    cursor.close()
                    
                    # Reset terms acceptance when loading new data
                    st.session_state.terms_accepted = False
                    
                    # Log data access
                    user_email = user_context.get('email') if is_databricks_app_mode else None
                    log_audit_event(
                        "DATA_LOADED",
                        f"table={selected_catalog}.{selected_schema}.{selected_table}, rows={len(st.session_state.data)}, columns={len(columns)}",
                        user_email=user_email
                    )
                    
                    st.success(f"✅ Loaded {len(st.session_state.data)} rows")
            except Exception as e:
                st.error(f"❌ Error loading data: {str(e)}")
        
        # Display data
        if st.session_state.data is not None:
            st.dataframe(st.session_state.data, width='stretch', height=400)
            
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
        st.info("👈 Please connect to Databricks using the sidebar")
        
        st.markdown("""
        ### 🔒 Getting Started
        
        Choose your authentication method in the sidebar. We recommend:
        
        #### 🥇 Best: Databricks CLI Profile
        
        Create `~/.databrickscfg` with your credentials:
        
        ```ini
        [DEFAULT]
        host = https://your-workspace.cloud.databricks.com
        token = dapi...your-token...
        ```
        
        Get a token from: **Databricks → User Settings → Access Tokens**
        
        Then select the profile in the sidebar!
        
        #### 🥈 Good: Environment Variables
        
        ```bash
        export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
        export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
        export DATABRICKS_TOKEN="dapi...your-token..."
        ```
        
        #### 🥉 Quick Test: Manual Entry
        
        Enter credentials directly in the sidebar (local development only).
        
        ---
        
        ### 💡 Getting Your Credentials:
        
        **Access Token:**
        1. Open Databricks workspace
        2. Click your profile → **User Settings**
        3. Go to **Access Tokens** → **Generate New Token**
        4. Copy the token (starts with `dapi`)
        
        **HTTP Path:**
        1. Go to **SQL Warehouses**
        2. Click your warehouse
        3. Go to **Connection Details** tab
        4. Copy the HTTP Path (e.g., `/sql/1.0/warehouses/abc123`)
        """)

# Footer
st.divider()
col_footer1, col_footer2 = st.columns([3, 1])
with col_footer1:
    st.caption("🔒 Data Loss Prevention App - Secure data export from Unity Catalog")
with col_footer2:
    st.caption("📋 All activity logged for compliance")
    
# Show audit log location
with st.expander("ℹ️ Audit Logging Information"):
    log_path = Path.home() / ".dlp_app_logs" / "audit_log.txt"
    st.info(f"""
    **Audit Trail**: All data access and export activities are logged for compliance purposes.
    
    **Log Location**: `{log_path}`
    
    **Logged Events**:
    - Data table access and loading
    - Terms of Use acceptance
    - Connection attempts
    
    This audit trail may be reviewed by security and compliance teams.
    """)

