# Getting Started with LIZARD

This guide walks you through setting up and running LIZARD locally.

## Prerequisites

- **Python 3.12+**
- **Node.js 18+** (for UI development)
- **Docker** (optional, for containerized deployment)

## Installation

1. Clone the repository

        git clone https://github.com/AmadeusITGroup/lizard.git
        cd lizard

2. Create and activate virtual environment

        python -m venv .venv
        source .venv/bin/activate  # Windows: .venv\Scripts\activate

3. Install Python dependencies

        make install-all

4. Install UI dependencies

        make ui-install

5. Generate synthetic fraud data

        make data

## Running the Application

    # Terminal 1: Start the API
    make api
    # API available at:  http://localhost:8000/docs

    # Terminal 2: Start the UI
    make ui
    # UI available at: http://localhost:5173

## Upload Data

1. Open the UI at http://localhost:5173
2. Navigate to the **Mapping** page
3. Upload CSV files from the `./data/` directory
4. The AI mapper will suggest field mappings automatically

## Configuration

Copy `.env.sample` to `.env` and configure:

    cp .env.sample .env

### Core Settings

| Variable | Description | Default |
|----------|-------------|---------|
| `LIZARD_DB_URL` | Database connection URL | `sqlite+aiosqlite:///./lizard.db` |
| `VITE_API_BASE` | API base URL for UI | `http://localhost:8000` |
| `OPENAI_API_KEY` | OpenAI API key (for AI mapping) | — |
| `OPENAI_MODEL` | OpenAI model to use | `gpt-4o-mini` |

### Cloud Settings (optional)

| Variable | Description | Default |
|----------|-------------|---------|
| `AZURE_TENANT_ID` | Azure AD tenant for service principal auth | — |
| `AZURE_CLIENT_ID` | Service principal client ID | — |
| `AZURE_CLIENT_SECRET` | Service principal client secret | — |
| `LIZARD_CLOUD_CONFIG` | Path to cloud config YAML | `lizard-cloud.yaml` |

> Cloud dependencies (`azure-identity`, `azure-storage-blob`, `databricks-sdk`) are optional. LIZARD detects their presence at startup and disables cloud features gracefully if they are missing.

## Docker Deployment

    # Build and start all services
    make docker-up

    # Access:
    # - UI: http://localhost:3000
    # - API: http://localhost:8000/docs

    # Stop services
    make docker-down

    # View logs
    docker compose -f infra/docker-compose.yml logs -f

## Development Commands

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

## Next Steps

- Explore the [Dashboard Panels](README.md#-dashboard-panels) to visualize your data
- Check the [API Documentation](README.md#-api-documentation) for endpoint details
- Set up [Cloud Integration](README.md#%EF%B8%8F-cloud-integration) for Azure/Databricks connectivity
- Read the [Contributing Guide](CONTRIBUTING.md) if you'd like to contribute