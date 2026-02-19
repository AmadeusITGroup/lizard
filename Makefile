# Makefile for LIZARD - Fraud Pattern Visualizer
# Usage: make [target]

.PHONY: help install install-dev install-all api ui ui-install ui-build dev \
        up down build lint lint-fix test test-cov type data clean reset \
        docker-build docker-up docker-down

# Default target
help:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════════╗"
	@echo "║           LIZARD - Fraud Pattern Visualizer                     ║"
	@echo "╚══════════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "SETUP:"
	@echo "  make install        Install production dependencies"
	@echo "  make install-dev    Install with dev/test dependencies"
	@echo "  make install-all    Install all dependencies (dev + test + ai)"
	@echo "  make ui-install     Install UI dependencies only"
	@echo ""
	@echo "DEVELOPMENT:"
	@echo "  make api            Start FastAPI backend (port 8000)"
	@echo "  make ui             Start React UI (port 5173)"
	@echo "  make ui-build       Build React UI for production"
	@echo ""
	@echo "DATA:"
	@echo "  make data           Generate synthetic fraud scenario data"
	@echo ""
	@echo "QUALITY:"
	@echo "  make lint           Run linting (ruff)"
	@echo "  make lint-fix       Auto-fix linting issues"
	@echo "  make test           Run pytest"
	@echo "  make test-cov       Run tests with coverage"
	@echo "  make type           Run type checking (mypy)"
	@echo ""
	@echo "DOCKER:"
	@echo "  make docker-build   Build Docker images"
	@echo "  make docker-up      Start services with Docker Compose"
	@echo "  make docker-down    Stop Docker Compose services"
	@echo ""
	@echo "CLEANUP:"
	@echo "  make clean          Remove cache files and build artifacts"
	@echo "  make reset          Full reset:  clean + remove database"
	@echo ""

# ============================================================
# Setup
# ============================================================

install:
	pip install --upgrade pip
	pip install -e .

install-dev:
	pip install --upgrade pip
	pip install -e ".[dev,test]"

install-all:
	pip install --upgrade pip
	pip install -e ".[dev,test,ai,cloud]"

ui-install:
	cd ui-react && npm install --legacy-peer-deps

# ============================================================
# Development
# ============================================================

api:
	uvicorn app.main:app --reload --port 8000 \
		--reload-exclude "ui-react/*" \
		--reload-exclude "*/node_modules/*" \
		--reload-exclude ".git/*" \
		--reload-exclude "data/*"

ui:
	cd ui-react && npm run dev

ui-build:
	cd ui-react && npm install --legacy-peer-deps && npm run build

# Alias for starting both (instructions only)
dev:
	@echo ""
	@echo "To start LIZARD in development mode:"
	@echo ""
	@echo "  Terminal 1:  make api"
	@echo "  Terminal 2:  make ui"
	@echo ""
	@echo "Or use Docker:   make docker-up"
	@echo ""

# ============================================================
# Data Management
# ============================================================

data:
	@mkdir -p data
	python -m scripts.generate_synthetic --out ./data
	@echo ""
	@echo "✓ Synthetic data generated in ./data/"
	@echo ""
	@echo "To load data into the database:"
	@echo "  1.Start the API:   make api"
	@echo "  2.Use the Mapping page in the UI to upload CSV files"
	@echo "  3.Or use curl:"
	@echo "     curl -X POST http://localhost:8000/upload/csv -F 'file=@./data/auth_events.csv'"
	@echo ""

# ============================================================
# Code Quality
# ============================================================

lint:
	ruff check app domain connectors mapping analytics scripts tests
	ruff format --check app domain connectors mapping analytics scripts tests

lint-fix:
	ruff check --fix app domain connectors mapping analytics scripts tests
	ruff format app domain connectors mapping analytics scripts tests

test:
	pytest tests/ -v --tb=short

test-cov:
	pytest tests/ -v --cov=app --cov=domain --cov=analytics --cov-report=html
	@echo "Coverage report:  htmlcov/index.html"

type:
	mypy app domain analytics mapping --ignore-missing-imports

# ============================================================
# Docker
# ============================================================

docker-build:
	docker compose -f infra/docker-compose.yml build

docker-up:
	docker compose -f infra/docker-compose.yml up --build

docker-down:
	docker compose -f infra/docker-compose.yml down -v

# Legacy aliases
up:  docker-up
down: docker-down
build: docker-build

# ============================================================
# Cleanup
# ============================================================

clean:
	rm -rf .pytest_cache
	rm -rf .ruff_cache
	rm -rf .mypy_cache
	rm -rf htmlcov
	rm -rf .coverage
	rm -rf dist
	rm -rf build
	rm -rf *.egg-info
	rm -rf exports
	rm -rf ui-react/dist
	rm -rf ui-react/node_modules/.vite
	find .-type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find .-type f -name "*.pyc" -delete 2>/dev/null || true
	find .-type f -name ".DS_Store" -delete 2>/dev/null || true
	@echo "✓ Cleaned build artifacts and caches"

reset:  clean
	rm -f lizard.db
	rm -rf data/*.csv
	@echo "✓ Database and generated data removed"
	@echo "  Run 'make data' to regenerate synthetic data"

# Add to Makefile

demo-data: demo-data
	@mkdir -p data
	python -m scripts.generate_demo_data --out ./data --users 50 --days 30
	@echo ""
	@echo "✅ Demo data generated!"
	@echo "   See data/anomaly_scenarios.json for injected fraud scenarios"
	@echo ""

# Reset all data (database, caches, workbench)
reset-data:
	@echo "Resetting all data..."
	rm -f lizard.db
	@echo "✓ Database deleted"
	@echo ""
	@echo "Note:  Restart the API server to complete reset"
	@echo "      The in-memory caches will be cleared on restart"

# Full clean including data
clean-all:  clean reset-data
	rm -rf data/*. csv
	rm -rf data/*.json
	@echo "✓ All data files removed"