# 🔒 Data Loss Prevention App

A simple Streamlit application for securely exporting data from Databricks Unity Catalog tables.

> **⚠️ LOCAL DEVELOPMENT ONLY**: This app is designed for local development and testing. For production deployment, implement proper OAuth authentication, secret management, and security controls. See [Security Best Practices](#-security-best-practices) below.

## Features

- 🎯 **Native Databricks App** with built-in authentication via forwarded headers
- 🔐 **Simple authentication**: Manual entry for local dev, automatic for Databricks Apps
- 📊 Browse Unity Catalog: catalogs, schemas, and tables
- 👀 Preview table data before export
- 📥 Export data in multiple formats (CSV, JSON)
- 📈 View data statistics (row count, column count, memory usage)
- 📜 **Terms of Use acceptance** required before data export
- 📋 **Audit logging** of all data access and export activities with user identity
- 🛡️ **Compliance-ready** with automatic activity tracking
- 🔒 **Per-user access control** - respects Unity Catalog permissions
- ⚡ **Zero configuration** when deployed as Databricks App

## Installation

1. Clone this repository or download the files
2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Run the app:

```bash
streamlit run app.py
```

## Usage

### 🖥️ Local Development

Run the app locally and enter credentials in the sidebar:

```bash
streamlit run app.py
```

In the app sidebar, enter:
1. **Server Hostname** - Your Databricks workspace URL (e.g., `your-workspace.cloud.databricks.com`)
2. **HTTP Path** - SQL Warehouse path (e.g., `/sql/1.0/warehouses/xxxxx`)
3. **Access Token** - Personal access token from Databricks

**Get credentials from:**
- **Access Token**: Databricks → User Settings → Access Tokens → Generate New Token
- **HTTP Path**: SQL Warehouses → Your Warehouse → Connection Details

---

### 🚀 Databricks App Deployment (Production)

When deployed as a Databricks App, authentication is automatic - no credentials needed!

```bash
databricks apps deploy data-loss-prevention-app \
  --source-code-path . \
  --config app.yaml
```

The app automatically uses the authenticated user's credentials via forwarded headers.

### Authentication Methods Summary

The app supports two authentication modes:

| Mode | Authentication | Configuration | Use Case |
|------|---------------|---------------|----------|
| **Databricks App** | Automatic via forwarded headers | Zero config needed | **Production deployment** |
| **Local Development** | Manual token entry | Enter credentials in UI | Local testing and development |

**✨ When deployed as a Databricks App, users authenticate automatically with their Databricks credentials!**

### How to Get Credentials (Local Development)

#### Access Token:
1. Open your Databricks workspace
2. Click on your profile icon (top right)
3. Go to **User Settings**
4. Click **Access Tokens** tab
5. Click **Generate New Token**
6. Give it a name and set expiration (optional)
7. Click **Generate**
8. Copy the token (starts with `dapi`) - **save it securely!**

**Security Note**: Use read-only permissions if possible.

#### Server Hostname:
- Found in your Databricks workspace URL
- Example: If your URL is `https://abc-123.cloud.databricks.com/`, your hostname is `abc-123.cloud.databricks.com`
- **Don't include** `https://` or trailing slashes

#### HTTP Path:
1. Go to **SQL Warehouses** in Databricks
2. Select your SQL Warehouse
3. Go to **Connection Details** tab
4. Copy the **HTTP Path**
5. Example format: `/sql/1.0/warehouses/abc123def456`

## Workflow

1. **Connect**: Enter credentials and click "Connect to Databricks"
2. **Browse**: Select catalog, schema, and table from dropdowns
3. **Load**: Click "Load Data" to preview the table (with optional row limit)
4. **Accept Terms**: Review and accept the Terms of Use
5. **Export**: Download data as CSV or JSON format

## 📜 Terms of Use & Compliance

### Data Export Terms
Before downloading any data, users must accept the Terms of Use, which include:

- Authorization and proper business use
- Confidentiality and data protection compliance
- Security responsibilities
- No unauthorized redistribution
- Acknowledgment of audit logging

### Audit Logging
All activity is automatically logged to stdout for compliance and security purposes:

**Logged Events:**
- Database connections (timestamp, user, host)
- Data table access (table name, row count, columns)
- Terms of Use acceptance
- User identity (email in Databricks App mode)

**Log Output:** Application stdout (captured by Databricks platform logs)

**Log Format:**
```
[2026-01-06T12:34:56] USER=user@company.com EVENT=DATA_LOADED DETAILS=table=catalog.schema.table, rows=1000
[2026-01-06T12:35:10] USER=user@company.com EVENT=TERMS_ACCEPTED DETAILS=table=catalog.schema.table, rows=1000
```

**Viewing Logs:**
- **Databricks App**: View in Databricks App logs interface
- **Local Development**: Check terminal output where you ran `streamlit run app.py`

These audit logs can be reviewed by security and compliance teams to track data access patterns and ensure policy compliance.

## 🔐 Security Best Practices

### For Local Development:
- ✅ Never commit access tokens to version control
- ✅ Use tokens with **read-only permissions** scoped to specific catalogs/schemas
- ✅ Rotate tokens regularly and revoke unused tokens
- ✅ Don't share tokens between users
- ✅ Set token expiration dates for added security
- ✅ Use strong passwords for your Databricks account

### For Production/Shared Environments:
**⚠️ This app is designed for local development only.** For production deployment, you should implement:

1. **OAuth 2.0 Authentication**: Replace personal access tokens with OAuth flows
2. **Secret Management**: Use services like AWS Secrets Manager, Azure Key Vault, or HashiCorp Vault
3. **HTTPS/TLS**: Encrypt all communications with proper certificates
4. **Audit Logging**: Track all data access and exports
5. **Role-Based Access Control (RBAC)**: Verify user permissions before allowing data access
6. **Token Encryption**: Encrypt tokens at rest and in transit
7. **Session Management**: Implement proper session timeouts and invalidation
8. **Databricks Authentication Context**: For Databricks App Store, use native Databricks auth instead of tokens

### Why Databricks App Mode is Most Secure:

**Built-in Authentication:**
- Uses OAuth 2.0 via Databricks platform
- Automatic token rotation and renewal
- Per-user access control
- No credential management needed

**Benefits:**
- Each user authenticates with their own Databricks account
- Unity Catalog enforces row/column-level security per user
- Full audit trail with user identity
- Zero configuration for end users
- No manual token handling or exposure

**For Local Development:**
- Tokens are only used temporarily
- Not stored permanently in the app
- Use strong, expiring tokens
- Keep your Databricks account secure

## Requirements

- Python 3.8+
- Databricks workspace with Unity Catalog enabled
- SQL Warehouse running in Databricks
- Valid Databricks access token (or OAuth via Databricks CLI)

## 🚀 Databricks App Deployment

This app can be deployed to Databricks as a native Databricks App.

### Deployment Files

- **`app.yaml`** - Databricks App configuration
- **`requirements.txt`** - Python dependencies
- **`.streamlit/config.toml`** - Streamlit configuration for production deployment

### Deploy to Databricks

1. Install Databricks CLI (if not already installed):
```bash
pip install databricks-cli
databricks auth login --host https://your-workspace.cloud.databricks.com
```

2. Deploy the app:
```bash
databricks apps create data-loss-prevention-app
databricks apps deploy data-loss-prevention-app \
  --source-code-path . \
  --config app.yaml
```

3. Configure the SQL Warehouse:
   - In the Databricks workspace, go to your app
   - Set the `sql-warehouse` resource to your SQL Warehouse ID

### app.yaml Configuration

The app uses the following environment variables when deployed:
- `STREAMLIT_BROWSER_GATHER_USAGE_STATS`: Disabled for privacy
- `DATABRICKS_APP_MODE`: Set to "true" to enable Databricks App mode
- `DATABRICKS_WAREHOUSE_ID`: Automatically populated from Databricks App resources (references the warehouse ID)

### Authentication in Databricks Apps

When deployed as a Databricks App, the app uses **built-in Databricks App authentication**:

- ✅ **Automatic user authentication** via `X-Forwarded-Email` and `X-Forwarded-Access-Token` headers
- ✅ **Per-user access control** - queries run with each user's permissions
- ✅ **No token configuration** - Databricks handles all authentication
- ✅ **Seamless UX** - users just sign in to Databricks

#### How it works:

1. User accesses the app in Databricks
2. Databricks authenticates the user and forwards their identity:
   - `X-Forwarded-Email`: User's email address
   - `X-Forwarded-Access-Token`: User's access token
3. App uses the forwarded token to query SQL Warehouse
4. All queries execute with the user's permissions
5. Activity is logged with the user's email for audit compliance

This ensures data access respects Unity Catalog permissions and provides full audit trails.

#### Authentication Flow Diagram:

```
┌─────────────┐
│   User      │
└──────┬──────┘
       │ 1. Access app
       ▼
┌─────────────────────┐
│  Databricks Apps    │
│  Platform           │
└──────┬──────────────┘
       │ 2. Forwards headers:
       │    - X-Forwarded-Email
       │    - X-Forwarded-Access-Token
       ▼
┌─────────────────────┐
│  DLP Streamlit App  │
│                     │
│  • Extracts token   │
│  • Identifies user  │
└──────┬──────────────┘
       │ 3. Query with user token
       ▼
┌─────────────────────┐
│  SQL Warehouse      │
│  + Unity Catalog    │
│                     │
│  Enforces user's    │
│  permissions        │
└─────────────────────┘
```

**Key Benefits:**
- ✅ No credential management required
- ✅ Queries run as the authenticated user
- ✅ Unity Catalog enforces row/column-level security
- ✅ Full audit trail with user identity

## License

MIT License
