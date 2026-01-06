# 🔒 Data Loss Prevention App

A simple Streamlit application for securely exporting data from Databricks Unity Catalog tables.

> **⚠️ LOCAL DEVELOPMENT ONLY**: This app is designed for local development and testing. For production deployment, implement proper OAuth authentication, secret management, and security controls. See [Security Best Practices](#-security-best-practices) below.

## Features

- 🎯 **Native Databricks App** with built-in authentication via forwarded headers
- 🔐 Multiple authentication methods: Databricks App (built-in), OAuth, tokens
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

3. **(Recommended)** Install and configure Databricks CLI with OAuth:

```bash
pip install databricks-cli
databricks auth login --host https://your-workspace.cloud.databricks.com
```

This will authenticate via OAuth 2.0 and securely store your credentials in `~/.databrickscfg`. **No access token needed!**

## Usage

### 🥇 Method 1: Databricks CLI with OAuth (BEST - No Token Needed!)

The most secure and convenient method using OAuth 2.0:

```bash
# One-time setup: Authenticate with Databricks CLI
databricks auth login --host https://your-workspace.cloud.databricks.com

# Run the app
streamlit run app.py
```

In the app:
1. Select **"Databricks CLI Profile (Recommended)"**
2. Choose your OAuth profile (marked with 🔐 OAuth)
3. Enter your SQL Warehouse HTTP path
4. Click **Connect** - the app will automatically use OAuth (no token needed!)

**Benefits:**
- 🔐 **OAuth 2.0** authentication (no manual access tokens!)
- 🔄 **Automatic token rotation** and refresh
- 💾 Credentials stored securely in `~/.databrickscfg`
- ✨ No environment variables needed
- 🛡️ **Most secure** - tokens never exposed in the app

---

### 🥈 Method 2: Environment Variables

For automation or CI/CD pipelines:

```bash
# Set environment variables
export DATABRICKS_SERVER_HOSTNAME="your-workspace.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxxx"
export DATABRICKS_TOKEN="your-access-token"

# Run the app
streamlit run app.py
```

**Pro Tip**: Create a `.env` file (already in `.gitignore`):

```bash
# .env
DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/xxxxx
DATABRICKS_TOKEN=your-access-token-here
```

Then source it:
```bash
set -a; source .env; set +a
streamlit run app.py
```

---

### 🥉 Method 3: Manual Entry (Local Dev Only)

Enter credentials directly in the UI sidebar. **Not recommended** - use only for quick testing on your local machine.

### Authentication Methods Summary

The app supports three authentication methods, ranked by security:

| Method | Security | Token Required? | Convenience | Use Case |
|--------|----------|----------------|-------------|----------|
| **Databricks CLI (OAuth)** | 🔐🔐🔐 Best | ❌ No! | ⭐⭐⭐ Easy | **Recommended for all users** |
| **Environment Variables** | 🔐🔐 Good | ✅ Yes | ⭐⭐ Medium | CI/CD, automation |
| **Manual Entry** | 🔐 Low | ✅ Yes | ⭐ Hard | Quick local testing only |

**✨ OAuth profiles (marked with 🔐) don't require any access token - the app handles authentication automatically using secure OAuth 2.0!**

### How to Get Credentials

#### For Databricks CLI with OAuth (Recommended - No Token!):
1. Install: `pip install databricks-cli`
2. Run: `databricks auth login --host https://your-workspace.cloud.databricks.com`
3. Complete OAuth flow in browser (one-time setup)
4. Your OAuth credentials are automatically saved to `~/.databrickscfg`
5. In the app, look for profiles marked with 🔐 OAuth

**Note**: OAuth profiles in `~/.databrickscfg` look like this:
```
[my-profile]
host = https://your-workspace.cloud.databricks.com/
auth_type = databricks-cli
```

The app will handle OAuth token refresh automatically - no manual token management needed!

#### For Manual/Environment Variable Setup:
1. **Server Hostname**: Found in your Databricks workspace URL (e.g., `your-workspace.cloud.databricks.com`)
2. **Access Token**:
   - In Databricks, click on your user profile
   - Go to User Settings > Access Tokens
   - Click "Generate New Token"
   - Set appropriate permissions (read-only recommended)

#### HTTP Path (Required for all methods):
- Go to **SQL Warehouses** in Databricks
- Select your warehouse
- Go to **Connection Details** tab
- Copy the **HTTP Path** (e.g., `/sql/1.0/warehouses/xxxxx`)

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
All activity is automatically logged for compliance and security purposes:

**Logged Events:**
- Database connections (timestamp, user, host)
- Data table access (table name, row count, columns)
- Terms of Use acceptance
- Data export preparations

**Log Location:** `~/.dlp_app_logs/audit_log.txt`

**Log Format:**
```
[2026-01-06T12:34:56] USER=username EVENT=DATA_LOADED DETAILS=table=catalog.schema.table, rows=1000
[2026-01-06T12:35:10] USER=username EVENT=TERMS_ACCEPTED DETAILS=table=catalog.schema.table, rows=1000
```

These audit logs can be reviewed by security and compliance teams to track data access patterns and ensure policy compliance.

## 🔐 Security Best Practices

### For Local Development:
- ✅ **Use Databricks CLI authentication** (`databricks auth login`) - OAuth 2.0 with automatic token rotation
- ✅ **Second choice: environment variables** - better than manual entry
- ✅ Never commit access tokens to version control (`.env` files are in `.gitignore`)
- ✅ Use tokens with **read-only permissions** scoped to specific catalogs/schemas
- ✅ Rotate tokens regularly and revoke unused tokens
- ✅ Use `~/.databrickscfg` for CLI-based auth (automatically managed)

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

### Why CLI/Environment Variables Are Safer:

**Databricks CLI:**
- Uses OAuth 2.0 (no manual token handling)
- Automatic token rotation and renewal
- Credentials encrypted and stored in secure config file
- Browser-based authentication flow

**Environment Variables:**
- Not stored in browser memory or accessible via developer tools
- Not transmitted between browser and server
- Can be managed by secure shell environments and CI/CD pipelines
- Easier to rotate without code changes

**Both avoid:**
- Manual token entry in UI
- Token exposure in browser developer tools
- Accidental token leakage through screenshots or screen sharing

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
- `SQL_WAREHOUSE`: Automatically populated from Databricks App resources

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
