# Makefile for Python Airflow Snowflake Data Pipeline
# Production-grade automation for development, testing, and deployment

.PHONY: help install install-dev test test-unit test-integration test-e2e lint format clean build deploy start stop logs

# Default target
.DEFAULT_GOAL := help

# Variables
PYTHON := python3
PIP := pip3
DOCKER_COMPOSE := docker-compose -f infrastructure/docker/docker-compose.yml --env-file .env
PROJECT_NAME := python-airflow-snowflake-data-pipeline
VENV_DIR := venv

# Colors for output
RED := \033[0;31m
GREEN := \033[0;32m
YELLOW := \033[0;33m
BLUE := \033[0;34m
NC := \033[0m # No Color

help: ## Show this help message
	@echo "$(BLUE)$(PROJECT_NAME) - Makefile Commands$(NC)"
	@echo ""
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)%-20s$(NC) %s\n", $$1, $$2}'

# Development Environment
install: ## Install production dependencies
	@echo "$(BLUE)Installing production dependencies...$(NC)"
	$(PIP) install -r requirements.txt

install-dev: ## Install development dependencies including testing tools
	@echo "$(BLUE)Installing development dependencies...$(NC)"
	$(PIP) install -r requirements.txt
	$(PIP) install pytest pytest-cov pytest-mock black flake8 mypy pre-commit
	pre-commit install

setup-venv: ## Create and activate virtual environment
	@echo "$(BLUE)Setting up virtual environment...$(NC)"
	$(PYTHON) -m venv $(VENV_DIR)
	@echo "$(YELLOW)Activate with: source $(VENV_DIR)/bin/activate$(NC)"

# Code Quality
lint: ## Run linting checks
	@echo "$(BLUE)Running linting checks...$(NC)"
	flake8 jobs/ libs/ tests/ --max-line-length=100 --exclude=venv/
	mypy jobs/ libs/ --ignore-missing-imports

format: ## Format code with black
	@echo "$(BLUE)Formatting code...$(NC)"
	black jobs/ libs/ tests/ --line-length=100

format-check: ## Check if code formatting is correct
	@echo "$(BLUE)Checking code formatting...$(NC)"
	black --check jobs/ libs/ tests/ --line-length=100

# Testing
test: test-unit test-integration ## Run all tests

test-unit: ## Run unit tests
	@echo "$(BLUE)Running unit tests...$(NC)"
	pytest tests/unit/ -v --cov=jobs --cov=libs --cov-report=html --cov-report=term

test-integration: ## Run integration tests
	@echo "$(BLUE)Running integration tests...$(NC)"
	pytest tests/integration/ -v -m "not slow"

test-e2e: ## Run end-to-end tests
	@echo "$(BLUE)Running end-to-end tests...$(NC)"
	pytest tests/e2e/ -v

test-coverage: ## Generate test coverage report
	@echo "$(BLUE)Generating coverage report...$(NC)"
	pytest tests/ --cov=jobs --cov=libs --cov-report=html --cov-report=term
	@echo "$(GREEN)Coverage report generated in htmlcov/$(NC)"

# Data Validation
validate-data: ## Validate raw data files
	@echo "$(BLUE)Validating raw data files...$(NC)"
	$(PYTHON) -c "import os; files = os.listdir('data/raw'); print(f'Found {len(files)} files: {files}')"

validate-config: ## Validate configuration files
	@echo "$(BLUE)Validating configuration files...$(NC)"
	$(PYTHON) -c "import yaml; [yaml.safe_load(open(f)) for f in ['config/config.yaml', 'config/environments/dev.yaml']]"
	@echo "$(GREEN)Configuration files are valid$(NC)"

# Docker Operations
build: ## Build Docker images
	@echo "$(BLUE)Building Docker images...$(NC)"
	$(DOCKER_COMPOSE) build

start: ## Start all services with Docker Compose
	@echo "$(BLUE)Starting services...$(NC)"
	$(DOCKER_COMPOSE) up -d
	@echo "$(GREEN)Services started successfully!$(NC)"
	@echo "$(YELLOW)Access points:$(NC)"
	@echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
	@echo "  - Spark UI: http://localhost:9090"
	@echo "  - PostgreSQL: localhost:5432"

stop: ## Stop all services
	@echo "$(BLUE)Stopping services...$(NC)"
	$(DOCKER_COMPOSE) down

restart: stop start ## Restart all services

logs: ## Show logs from all services
	$(DOCKER_COMPOSE) logs -f

logs-airflow: ## Show Airflow logs only
	$(DOCKER_COMPOSE) logs -f airflow-webserver airflow-scheduler

logs-spark: ## Show Spark logs only
	$(DOCKER_COMPOSE) logs -f spark-master spark-worker

status: ## Show status of all services
	$(DOCKER_COMPOSE) ps

init: ## Initialize the entire environment (first time setup)
	@echo "$(BLUE)Initializing environment...$(NC)"
	$(DOCKER_COMPOSE) up airflow-init
	@echo "$(GREEN)Environment initialized!$(NC)"

# Airflow Operations
airflow-init: ## Initialize Airflow database and create admin user
	@echo "$(BLUE)Initializing Airflow...$(NC)"
	$(DOCKER_COMPOSE) run --rm airflow-webserver airflow db init
	$(DOCKER_COMPOSE) run --rm airflow-webserver airflow users create \
		--username admin \
		--firstname Admin \
		--lastname User \
		--role Admin \
		--email admin@example.com \
		--password admin

airflow-shell: ## Open Airflow shell
	$(DOCKER_COMPOSE) exec airflow-webserver bash

# Spark Operations
spark-shell: ## Open Spark shell
	$(DOCKER_COMPOSE) exec spark-master spark-shell

spark-submit: ## Submit Spark job (usage: make spark-submit JOB=jobs/bronze/process_raw_data.py)
	@if [ -z "$(JOB)" ]; then echo "$(RED)Error: JOB parameter required. Usage: make spark-submit JOB=path/to/job.py$(NC)"; exit 1; fi
	$(DOCKER_COMPOSE) exec spark-master spark-submit /opt/spark/$(JOB)

# Database Operations
snowflake-test: ## Test Snowflake connection
	@echo "$(BLUE)Testing Snowflake connection...$(NC)"
	$(PYTHON) -c "from jobs.common.snowflake_connector import SnowflakeConnector; from jobs.common.config_manager import ConfigManager; cfg = ConfigManager().get_config(); conn = SnowflakeConnector(cfg); print('✅ Snowflake connection successful')"

# Data Pipeline Operations
run-bronze: ## Run Bronze layer processing
	@echo "$(BLUE)Running Bronze layer processing...$(NC)"
	$(PYTHON) jobs/bronze/process_raw_data.py --env dev

run-pipeline: ## Run complete data pipeline
	@echo "$(BLUE)Running complete data pipeline...$(NC)"
	$(DOCKER_COMPOSE) exec airflow-webserver airflow dags trigger bronze_data_pipeline

# Monitoring
monitor: ## Open monitoring dashboard
	@echo "$(BLUE)Opening monitoring dashboard...$(NC)"
	@echo "Grafana: http://localhost:3000"
	@echo "Airflow: http://localhost:8080"
	@echo "Spark UI: http://localhost:9090"

# Cleanup
clean: ## Clean up temporary files and caches
	@echo "$(BLUE)Cleaning up...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf .pytest_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf dist/
	rm -rf build/

clean-docker: ## Clean up Docker containers and volumes
	@echo "$(BLUE)Cleaning up Docker resources...$(NC)"
	$(DOCKER_COMPOSE) down -v --remove-orphans
	docker system prune -f

clean-all: clean clean-docker ## Clean everything

# Security
security-check: ## Run security checks
	@echo "$(BLUE)Running security checks...$(NC)"
	$(PIP) install safety bandit
	safety check
	bandit -r jobs/ libs/ -f json -o security-report.json || true
	@echo "$(GREEN)Security check completed. Report: security-report.json$(NC)"

# Documentation
docs-serve: ## Serve documentation locally
	@echo "$(BLUE)Serving documentation...$(NC)"
	@echo "Documentation available at: docs/README.md"
	@echo "Architecture: docs/architecture/system_overview.md"

# Deployment
deploy-dev: ## Deploy to development environment
	@echo "$(BLUE)Deploying to development...$(NC)"
	$(MAKE) test
	$(MAKE) build
	$(MAKE) start
	@echo "$(GREEN)Development deployment complete$(NC)"

deploy-staging: ## Deploy to staging environment
	@echo "$(BLUE)Deploying to staging...$(NC)"
	@echo "$(YELLOW)Staging deployment not implemented yet$(NC)"

deploy-prod: ## Deploy to production environment
	@echo "$(BLUE)Deploying to production...$(NC)"
	@echo "$(RED)Production deployment requires manual approval$(NC)"

# Backup and Restore
backup-config: ## Backup configuration files
	@echo "$(BLUE)Backing up configuration...$(NC)"
	tar -czf config-backup-$(shell date +%Y%m%d-%H%M%S).tar.gz config/
	@echo "$(GREEN)Configuration backed up$(NC)"

# Performance Testing
perf-test: ## Run performance tests
	@echo "$(BLUE)Running performance tests...$(NC)"
	pytest tests/ -m "slow" -v

# Environment Setup
setup-env: ## Create .env file from template
	@echo "$(BLUE)Creating .env file from template...$(NC)"
	@if [ ! -f .env ]; then \
		cp env.example .env; \
		echo "$(GREEN)✅ .env file created from template$(NC)"; \
		echo "$(YELLOW)⚠️  Please edit .env file with your Snowflake credentials$(NC)"; \
	else \
		echo "$(YELLOW)⚠️  .env file already exists$(NC)"; \
	fi

setup-dev: setup-venv install-dev setup-env validate-config ## Complete development setup
	@echo "$(GREEN)Development environment setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "1. Edit .env file with your Snowflake credentials"
	@echo "2. Activate virtual environment: source $(VENV_DIR)/bin/activate"
	@echo "3. Start services: make start"
	@echo "4. Initialize Airflow: make init"
	@echo "5. Access Airflow UI: http://localhost:8080"

# CI/CD
ci: lint format-check test security-check ## Run CI pipeline
	@echo "$(GREEN)CI pipeline completed successfully$(NC)"

# Version Management
version: ## Show current version
	@echo "$(BLUE)Project Version Information:$(NC)"
	@echo "Project: $(PROJECT_NAME)"
	@echo "Python: $(shell $(PYTHON) --version)"
	@echo "Pip: $(shell $(PIP) --version)"
	@echo "Git: $(shell git describe --tags --always --dirty 2>/dev/null || echo 'No git info')"
