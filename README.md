# LIZARD – Fraud Pattern Visualizer

<p align="center">
  <strong>Visualized Indicators for Zonal Anomaly Risk DETECTION​.
</p>

<p align="center">
  <a href="#-features">Features</a> •
  <a href="#-quick-start">Quick Start</a> •
  <a href="#-dashboard-panels">Dashboard Panels</a> •
  <a href="#️-cloud-integration">Cloud Integration</a> •
  <a href="#-api-documentation">API Docs</a> •
  <a href="#-development">Development</a>
</p>

---

## ✨ Features

### 🔍 Investigation Dashboard

Interactive visualization panels for comprehensive fraud analysis:

| Panel | Description | Key Features |
|-------|-------------|--------------|
| **Timeline** | Temporal event analysis | Anomaly detection, bucket aggregation, drill-down |
| **Globe/Map** | Geographic visualization | Hex aggregation, travel routes, clustering |
| **Pie Chart** | Categorical distribution | Multi-field grouping, drill-down, anomaly coloring |
| **Bar Chart** | Comparative analysis | Horizontal/vertical, sorting, drill-down |
| **Scatter Plot** | Correlation analysis | X/Y field selection, regression line, outlier detection |
| **Link Graph** | Entity relationships | Network visualization, community detection |
| **Job Progress** | Scheduler monitoring | Live job status, run/pause controls (Cloud mode) |

### 🤖 Analytics Engine

- **Simple Mode** – Fast statistical detection (Z-Score + EWMA + MAD)
- **Advanced Mode** – ML-based detection (Isolation Forest)
- **Configurable Sensitivity** – High/Balanced/Low presets
- **Real-time Scoring** – Event-level anomaly scores with explanations

### 📊 Data Management

- **AI-Assisted Mapping** – Automatic schema detection and field mapping
- **Data Workbench** – Query builder, views, and data exploration
- **Multi-source Support** – CSV upload with validation
- **Custom Fields** – Extend schema with domain-specific fields

### ☁️ Cloud Integration

LIZARD can operate in two execution modes — **Local** and **Cloud** — switchable at runtime via the UI header toggle or the Settings page.

| Capability | Local Mode | Cloud Mode |
|------------|-----------|------------|
| Data storage | SQLite / local files | Azure Blob Storage, ADLS Gen2, DBFS |
| Compute | In-process pandas | Databricks Spark clusters & SQL Warehouses |
| Connectivity | Direct | Direct or via Application Gateways |
| Authentication | N/A | Service Principal (OAuth), PAT, Username/Password |
| Scheduler | Inactive | Background health checks, config sync, stale detection |
| Export | Local download | Export to Azure Blob or DBFS |

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.12+**
- **Node.js 18+** (for UI development)
- **Docker** (optional, for containerized deployment)

### Installation

```bash
# 1. Clone the repository
git clone https://github.com/pmboust1_amadeus/Lizard.git
cd Lizard

# 2. Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate

# 3. Install Python dependencies
make install-all

# 4. Install UI dependencies
make ui-install

# 5. Generate synthetic fraud data
make data
```

### Running the Application

```bash
# Terminal 1: Start the API
make api
# API available at:  http://localhost:8000/docs

# Terminal 2: Start the UI
make ui
# UI available at: http://localhost:5173
```

### Upload Data

1. Open the UI at http://localhost:5173
2. Navigate to **Mapping** page
3. Upload CSV files from `./data/` directory
4. The AI mapper will suggest field mappings automatically

---

## 📊 Dashboard Panels

### Timeline Panel
Visualize events over time with anomaly detection.

**Features:**
- Configurable time buckets (30s to 1 day)
- Metrics: Count, Sum, Average, Max, Min
- Simple (Z-Score) and Advanced (Isolation Forest) analytics
- Click on anomalous buckets to see detailed events
- Data source filtering

### Globe/Map Panel
Geographic visualization of events with travel route analysis.

**Features:**
- 2D Map and 3D Globe views
- Hex aggregation for dense data
- User travel routes with arc visualization
- Color by anomaly score or category
- Click on locations for event details

### Pie Chart Panel
Analyze categorical distribution of events.

**Features:**
- Multi-field grouping (composite keys)
- Unlimited categories (3-200 slider)
- "Others" aggregation toggle
- Click-to-drill-down with breadcrumb navigation
- Anomaly severity coloring
- Detail drawer with anomaly events table

### Bar Chart Panel
Compare event counts/metrics across categories.

**Features:**
- Vertical and horizontal orientation
- Multi-field grouping
- Sorting by value, label, or anomaly score
- Click-to-drill-down with breadcrumb navigation
- Detail drawer with anomaly events table
- Show/hide value labels

### Scatter Plot Panel
Correlation and outlier analysis.

**Features:**
- Select any fields for X/Y axes
- Color by category or anomaly score
- Size by numeric field
- Optional regression line
- Log scale toggle for axes
- Click points for detail drawer

### Job Progress Panel
Live scheduler monitoring dashboard panel (Cloud mode only).

**Features:**
- Real-time job status (running/stopped, OK/error counts)
- Per-job enable/disable toggle
- Manual "Run Now" trigger for any job
- Run count, error count, and last-run duration display
- Start/stop scheduler from the panel

---

## ☁️ Cloud Integration

LIZARD's cloud integration is layered across six phases, each building on the last. All cloud features are **opt-in** — the application runs fully locally by default.

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│  React UI                                                       │
│  ┌──────────────┐ ┌───────────────┐ ┌────────────────────────┐  │
│  │ Cloud Toggle │ │ Cloud Browser │ │ Settings Page           │  │
│  │ (AppBar)     │ │ (Storage/DBFS)│ │ (Gateways/Connections) │  │
│  └──────┬───────┘ └───────┬───────┘ └───────────┬────────────┘  │
│         │                 │                      │               │
│  ┌──────┴─────────────────┴──────────────────────┴────────────┐  │
│  │  CloudContext (mode, config, scheduler, API helpers)        │  │
│  └────────────────────────────┬────────────────────────────────┘  │
└───────────────────────────────┼──────────────────────────────────┘
                                │ REST API
┌───────────────────────────────┼──────────────────────────────────┐
│  FastAPI Backend              │                                   │
│  ┌────────────────────────────┴───────────────────────────────┐  │
│  │  cloud_api.py (config, test, browse, engine, export,       │  │
│  │               analytics, cluster, scheduler, health)       │  │
│  └────────────────────────────┬───────────────────────────────┘  │
│                               │                                   │
│  ┌────────────────────────────┴───────────────────────────────┐  │
│  │  cloud/ package                                             │  │
│  │  ├── config.py          YAML config + runtime state         │  │
│  │  ├── connectivity.py    Gateway registry + endpoint resolve │  │
│  │  ├── diagnostics.py     Error types + connection testing    │  │
│  │  ├── audit.py           Ring-buffer audit log               │  │
│  │  ├── cluster_manager.py Databricks cluster/warehouse ops    │  │
│  │  ├── output_storage.py  Export to Blob / DBFS               │  │
│  │  ├── analytics_engine.py  Anomaly + clustering wrappers     │  │
│  │  ├── scheduler.py       Background job scheduler            │  │
│  │  └── execution/                                             │  │
│  │      ├── base.py        ExecutionEngine ABC + ExecutionResult│  │
│  │      ├── local_engine.py  Pandas engine (wraps existing)    │  │
│  │      ├── spark_engine.py  Databricks Spark SQL engine       │  │
│  │      └── engine_factory.py  Engine selection + caching      │  │
│  └─────────────────────────────────────────────────────────────┘  │
│                               │                                   │
│              ┌────────────────┼────────────────┐                  │
│              ▼                ▼                ▼                  │
│    Azure Blob / ADLS    Databricks         DBFS                  │
│    (via azure-sdk)      (via databricks-sdk)                     │
└──────────────────────────────────────────────────────────────────┘
```

### Execution Modes

**Local Mode** (default) — All computation runs in-process with pandas. Data lives in the local SQLite database. No cloud credentials required.

**Cloud Mode** — Data is read from Azure Blob Storage, ADLS Gen2, or Databricks DBFS. Pipelines are translated to Spark SQL and submitted to a Databricks cluster or SQL Warehouse. Results are collected back as pandas DataFrames for the API response.

Switch modes at any time using the toggle in the app bar, or via `POST /cloud/mode`.

### Configuration

Cloud configuration is stored in `lizard-cloud.yaml` (auto-created on first use) and can be edited via the Settings UI or directly in YAML.

```yaml
# lizard-cloud.yaml
mode: local  # or "cloud"

gateways:
  - name: tst-gateway
    fqdn: gateway-tst.corp.com
    environment: TST
    exposed_workspaces: ["123456789"]
    exposed_storage_accounts: ["myaccount"]

databricks_connections:
  - name: dev-workspace
    workspace_id: "123456789"
    connectivity: gateway        # "direct" or "gateway"
    gateway_name: tst-gateway    # required when connectivity=gateway
    auth:
      type: service_principal    # or "developer_token", "username_password"
      tenant_id: ${AZURE_TENANT_ID}
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
    cluster:
      cluster_id: 0123-456789-abc
      cluster_name: analytics-cluster

storage_connections:
  - name: fraud-lake
    account_name: myaccount
    container: raw-data
    endpoint_type: dfs           # "blob" or "dfs" (ADLS Gen2)
    connectivity: gateway
    gateway_name: tst-gateway
    auth:
      type: service_principal
      tenant_id: ${AZURE_TENANT_ID}
      client_id: ${AZURE_CLIENT_ID}
      client_secret: ${AZURE_CLIENT_SECRET}
```

> **Note:** Environment variable references (`${VAR_NAME}`) are resolved at load time, so secrets never need to appear in the YAML file.

### Application Gateways

When direct network access to Azure resources is blocked (common in enterprise environments), LIZARD routes traffic through Application Gateways. The connectivity system:

1. Looks up the gateway by name from the config
2. Validates that the target resource (workspace or storage account) is exposed through that gateway
3. Rewrites the endpoint URL to route through the gateway FQDN

| Resource | Direct URL | Gateway URL |
|----------|-----------|-------------|
| Databricks | `https://adb-{workspace_id}.azuredatabricks.net` | `https://{gateway_fqdn}/adb-{workspace_id}` |
| Blob Storage | `https://{account}.blob.core.windows.net` | `https://{gateway_fqdn}/{account}.blob.core.windows.net` |
| ADLS Gen2 | `https://{account}.dfs.core.windows.net` | `https://{gateway_fqdn}/{account}.dfs.core.windows.net` |

### Connection Testing

The Settings page provides a multi-step connection test that validates:

1. **Config validation** — Connection name exists, required fields present
2. **Endpoint resolution** — URL is correctly built (direct or gateway)
3. **Network reachability** — HTTP(S) connectivity to the endpoint
4. **Authentication** — Token exchange succeeds with the configured credentials
5. **Resource access** — Can list clusters, containers, or files

Each step returns a status (`ok`, `warning`, `error`, `skipped`) with actionable remediation hints.

### Execution Engine

The execution engine abstracts over pandas (local) and Spark SQL (cloud):

| Engine | Class | When Used |
|--------|-------|-----------|
| **Local** | `LocalPandasEngine` | Default — wraps the existing `PipelineExecutor` |
| **Spark** | `SparkDatabricksEngine` | When a Databricks connection with a cluster or warehouse is configured |

The `engine_factory` module selects the appropriate engine based on the current config and caches it. Pipelines are translated to SQL for the Spark engine:

```python
# Pipeline step types → SQL
source   → FROM {table}
filter   → WHERE {conditions}
aggregate → GROUP BY ... HAVING ...
sort     → ORDER BY ...
select   → SELECT {columns}
join     → JOIN ... ON ...
distinct → SELECT DISTINCT ...
```

### Cluster Management

In cloud mode, LIZARD can manage Databricks compute resources:

- **List clusters** — View all clusters in a workspace with their state
- **Start / Stop clusters** — On-demand cluster lifecycle management
- **Get cluster status** — Detailed info including autoscale, Spark version, workers
- **List SQL Warehouses** — View serverless/pro warehouses

### Export & Output Storage

Pipeline results can be exported directly to cloud storage:

- **Azure Blob Storage** — Upload as CSV or Parquet to any configured container
- **Databricks DBFS** — Write to the Databricks file system

```bash
POST /cloud/export
{
  "pipeline": [...],
  "connection_type": "storage",  # or "dbfs"
  "connection_name": "fraud-lake",
  "format": "parquet",
  "container": "exports",
  "path_prefix": "/lizard-results"
}
```

### Cloud Analytics

The cloud analytics engine wraps the existing anomaly detection and clustering algorithms to work through the execution engine:

- **Anomaly Detection** (`POST /cloud/analytics/anomaly`) — Simple (Z-Score + EWMA + MAD) or Advanced (Isolation Forest) detection on pipeline results
- **Geo-temporal Clustering** (`POST /cloud/analytics/clustering`) — DBSCAN clustering on latitude/longitude/time features

In local mode, these run in-process with pandas. In cloud mode, data is fetched via Spark SQL and then analyzed locally (Spark-native analytics planned for a future release).

### Background Scheduler

When cloud mode is active, a lightweight background scheduler runs periodic maintenance tasks:

| Job | Interval | Description |
|-----|----------|-------------|
| `health_check` | 5 min | Pings all configured connections, logs results to audit |
| `config_sync` | 10 min | Re-reads `lizard-cloud.yaml` to pick up external changes |
| `audit_trim` | 1 hour | Trims the audit ring buffer (prevents unbounded growth) |
| `stale_detection` | 30 min | Warns about connections that haven't been tested recently |

The scheduler is thread-based (no external dependencies like Celery). Jobs can be enabled/disabled, triggered manually, or monitored from the **Job Progress** dashboard panel.

### Audit Log

All cloud operations are recorded in an in-memory ring-buffer audit log:

- Connection tests, health checks, exports, analytics runs
- Categorized by `system`, `config`, `connection`, `analytics`, `export`
- Queryable via `GET /cloud/health` or the audit API
- Automatically trimmed by the scheduler

### Cloud Data Browser

The **Cloud Browser** page (available only in cloud mode) lets you interactively browse:

- **Azure Storage** — List containers → list blobs → preview datasets
- **DBFS** — Navigate the Databricks file system with breadcrumb navigation
- **Dataset Preview** — Read CSV/Parquet/JSON files and display the first N rows in a table

---

## 📁 Project Structure

```
Lizard/
├── app/                        # FastAPI backend
│   ├── main.py                 # Main API routes & viz endpoints
│   ├── mapping_api.py          # Data mapping endpoints
│   ├── rules_api.py            # Rules engine endpoints
│   ├── workbench_api.py        # Data workbench endpoints
│   └── cloud_api.py            # Cloud mode API (config, browse, engine,
│                                #   cluster, export, analytics, scheduler, health)
├── analytics/                  # Analytics modules
│   ├── simple_anomaly.py       # Z-Score + EWMA detection
│   ├── advanced_anomaly.py     # Isolation Forest detection
│   ├── rules_engine.py         # Rules evaluation engine
│   └── clustering.py           # Geo-temporal clustering
├── cloud/                      # Cloud integration package
│   ├── __init__.py             # Feature flag (CLOUD_DEPS_AVAILABLE)
│   ├── config.py               # YAML config: load, save, get_config singleton
│   ├── constants.py            # URL templates, endpoint types, defaults
│   ├── connectivity.py         # GatewayRegistry + EndpointResolver
│   ├── diagnostics.py          # ConfigurationError, ConnectivityError, test helpers
│   ├── audit.py                # Ring-buffer audit log (record, get_entries, get_stats)
│   ├── cluster_manager.py      # Databricks cluster/warehouse operations
│   ├── output_storage.py       # Export to Azure Blob / DBFS
│   ├── analytics_engine.py     # Anomaly + clustering wrappers
│   ├── scheduler.py            # Background task scheduler
│   └── execution/              # Execution engine abstraction
│       ├── __init__.py
│       ├── base.py             # ExecutionEngine ABC + ExecutionResult
│       ├── local_engine.py     # LocalPandasEngine (wraps PipelineExecutor)
│       ├── spark_engine.py     # SparkDatabricksEngine (Spark SQL)
│       └── engine_factory.py   # Engine selection, caching, reset
├── domain/                     # Domain models
│   ├── models.py               # SQLAlchemy ORM models
│   └── schemas.py              # Pydantic schemas
├── mapping/                    # Data mapping
│   ├── ai_mapper.py            # AI-assisted field mapping
│   ├── validation.py           # Data validation rules
│   └── expr.py                 # Expression evaluation
├── connectors/                 # Data connectors
│   └── csv/                    # CSV loader
├── ui-react/                   # React frontend (Vite + MUI)
│   ├── src/
│   │   ├── api.ts              # API client (viz, cloud, scheduler)
│   │   ├── components/
│   │   │   ├── AppLayout.tsx           # App shell with nav + cloud toggle
│   │   │   ├── CloudModeToggle.tsx     # Cloud/Local switch in header
│   │   │   ├── JobProgressPanel.tsx    # Scheduler dashboard panel
│   │   │   ├── AddVisualizationDialog.tsx # Panel picker (incl. jobs)
│   │   │   ├── AnomalyDetailDrawer.tsx
│   │   │   ├── GlobeDeck.tsx
│   │   │   ├── MappingManager.tsx
│   │   │   └── RulesManager.tsx
│   │   ├── sections/           # Dashboard visualization panels
│   │   │   ├── TimelinePanel.tsx
│   │   │   ├── GlobePanel.tsx
│   │   │   ├── MapPanel.tsx
│   │   │   ├── PieChartPanel.tsx
│   │   │   ├── BarChartPanel.tsx
│   │   │   ├── ScatterPlotPanel.tsx
│   │   │   └── LinkGraphPanel.tsx
│   │   ├── pages/
│   │   │   ├── Dashboard.tsx           # Drag-and-drop panel dashboard
│   │   │   ├── MappingPage.tsx
│   │   │   ├── RulesPage.tsx
│   │   │   ├── WorkbenchPage.tsx
│   │   │   ├── DataManagerPage.tsx
│   │   │   ├── CloudBrowserPage.tsx    # Azure Storage / DBFS browser
│   │   │   └── SettingsPage.tsx        # Cloud config, gateways, connections
│   │   └── context/
│   │       ├── FiltersContext.tsx       # Global date/source filters
│   │       └── CloudContext.tsx         # Cloud mode, config, scheduler state
│   └── package.json
├── scripts/                    # Utility scripts
│   └── generate_synthetic.py
├── infra/                      # Docker & deployment
│   ├── Dockerfile.api
│   ├── Dockerfile.ui
│   ├── docker-compose.yml
│   └── nginx.conf
├── tests/                      # Test suite
│   ├── test_execution.py       # Execution engine tests
│   ├── test_cluster_manager.py # Cluster manager tests
│   ├── test_output_storage.py  # Export / output storage tests
│   ├── test_analytics_engine.py # Cloud analytics tests
│   ├── test_scheduler.py       # Scheduler tests
│   ├── test_api_ingest.py      # Ingest API tests
│   └── ...
├── Makefile                    # Development commands
├── pyproject.toml              # Python dependencies
├── lizard-cloud.yaml           # Cloud configuration (auto-created)
└── README.md
```

---

## 🔧 Configuration

Copy `.env.sample` to `.env` and configure:

```bash
cp .env.sample .env
```

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `LIZARD_DB_URL` | Database connection URL | `sqlite+aiosqlite:///./lizard.db` |
| `VITE_API_BASE` | API base URL for UI | `http://localhost:8000` |
| `OPENAI_API_KEY` | OpenAI API key (for AI mapping) | - |
| `OPENAI_MODEL` | OpenAI model to use | `gpt-4o-mini` |

### Cloud Settings (optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `AZURE_TENANT_ID` | Azure AD tenant for service principal auth | - |
| `AZURE_CLIENT_ID` | Service principal client ID | - |
| `AZURE_CLIENT_SECRET` | Service principal client secret | - |
| `LIZARD_CLOUD_CONFIG` | Path to cloud config YAML | `lizard-cloud.yaml` |

> Cloud dependencies (`azure-identity`, `azure-storage-blob`, `databricks-sdk`) are optional. LIZARD detects their presence at startup and disables cloud features gracefully if they are missing.

---

## 📖 API Documentation

Once the API is running, access:
- **Swagger UI**: http://localhost:8000/docs
- **ReDoc**: http://localhost:8000/redoc

### Core Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check |
| `/upload/csv` | POST | Upload CSV with mapping |
| `/viz/grid` | GET | Query events with analytics |
| `/viz/globe` | GET | Globe visualization data |
| `/viz/schema` | GET | Get field schema |
| `/viz/distinct/{field}` | GET | Get distinct values |
| `/viz/top-users` | GET | Top users by event count |
| `/viz/data-sources` | GET | List data sources |
| `/rules/` | GET | List detection rules |
| `/rules/evaluate` | POST | Evaluate rules on data |
| `/mapping/templates` | GET | List mapping templates |
| `/mapping/suggest` | POST | AI-suggest field mapping |
| `/workbench/query` | POST | Execute workbench query |

### Cloud Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cloud/config` | GET | Get cloud configuration (secrets redacted) |
| `/cloud/config` | PUT | Update and persist cloud configuration |
| `/cloud/mode` | POST | Switch between local and cloud mode |
| `/cloud/test-connection` | POST | Multi-step connection test |
| `/cloud/health` | GET | Comprehensive cloud health check |
| `/cloud/status` | GET | Lightweight cloud status summary |
| `/cloud/providers` | GET | List configured data-source providers |

### Cloud Data Browsing

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cloud/browse/storage/{conn}/containers` | GET | List Azure Storage containers |
| `/cloud/browse/storage/{conn}/blobs` | GET | List blobs in a container |
| `/cloud/browse/dbfs/{conn}` | GET | List DBFS directory contents |
| `/cloud/browse/preview` | POST | Preview a cloud dataset |

### Cloud Compute & Execution

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cloud/clusters/{conn}` | GET | List Databricks clusters |
| `/cloud/clusters/{conn}/{id}` | GET | Get cluster status |
| `/cloud/clusters/{conn}/{id}/start` | POST | Start a cluster |
| `/cloud/clusters/{conn}/{id}/stop` | POST | Stop a cluster |
| `/cloud/warehouses/{conn}` | GET | List SQL Warehouses |
| `/cloud/engine/status` | GET | Current execution engine status |
| `/cloud/engine/set-compute` | POST | Set cluster/warehouse for Spark |
| `/cloud/engine/reset` | POST | Reset cached engines |

### Cloud Analytics & Export

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cloud/analytics/anomaly` | POST | Run anomaly detection on pipeline |
| `/cloud/analytics/clustering` | POST | Run geo-temporal clustering on pipeline |
| `/cloud/export` | POST | Export pipeline results to Blob/DBFS |

### Scheduler

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/cloud/scheduler` | GET | Scheduler status and registered jobs |
| `/cloud/scheduler/start` | POST | Start the background scheduler |
| `/cloud/scheduler/stop` | POST | Stop the background scheduler |
| `/cloud/scheduler/jobs/{name}/run` | POST | Trigger a job immediately |
| `/cloud/scheduler/jobs/{name}/enable` | POST | Enable a scheduled job |
| `/cloud/scheduler/jobs/{name}/disable` | POST | Disable a scheduled job |

---

## 🧪 Development

```bash
# Install all dependencies
make install-all
make ui-install

# Run linting
make lint

# Auto-fix linting issues
make lint-fix

# Run tests
make test

# Run tests with coverage
make test-cov

# Type checking
make type

# Clean build artifacts
make clean

# Full reset (including database)
make reset
```

---

## 🐳 Docker Deployment

```bash
# Build and start all services
make docker-up

# Access:
# - UI: http://localhost:3000
# - API: http://localhost:8000/docs

# Stop services
make docker-down

# View logs
docker compose -f infra/docker-compose.yml logs -f
```

---

## 📝 License

This project is licensed under the MIT License.

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📧 Contact

LIZARD Team - mehdi.boustala@amadeus.com

Project Link: [https://github.com/pmboust1_amadeus/Lizard](https://github.com/pmboust1_amadeus/Lizard)