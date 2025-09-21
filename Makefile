# Makefile for Real-Time Environmental Event Alerting System
# Simplifies common development and deployment tasks

.PHONY: help install test lint format deploy clean setup-env

# Default target
help:
	@echo "🌍 Real-Time Environmental Event Alerting System"
	@echo "=================================================="
	@echo ""
	@echo "Available commands:"
	@echo ""
	@echo "Setup & Installation:"
	@echo "  install           Install Python dependencies"
	@echo "  setup-env         Setup development environment"
	@echo "  setup-fabric      Setup Microsoft Fabric resources"
	@echo ""
	@echo "Development:"
	@echo "  test              Run all tests"
	@echo "  test-unit         Run unit tests only"
	@echo "  test-integration  Run integration tests"
	@echo "  lint              Run code linting"
	@echo "  format            Format code with black and isort"
	@echo "  type-check        Run mypy type checking"
	@echo ""
	@echo "Data Pipeline:"
	@echo "  ingest            Run data ingestion pipeline"
	@echo "  process           Run data processing pipeline"
	@echo "  detect-anomalies  Run anomaly detection"
	@echo "  test-alerts       Test notification system"
	@echo ""
	@echo "Deployment:"
	@echo "  deploy-staging    Deploy to staging environment"
	@echo "  deploy-production Deploy to production environment"
	@echo "  infrastructure    Deploy/update infrastructure"
	@echo "  rollback          Rollback to previous deployment"
	@echo ""
	@echo "Monitoring:"
	@echo "  logs              Show application logs"
	@echo "  monitor           Show system metrics"
	@echo "  health-check      Check system health"
	@echo ""
	@echo "Utilities:"
	@echo "  clean             Clean build artifacts"
	@echo "  docs              Generate documentation"
	@echo "  docker-build      Build Docker images"
	@echo "  docker-run        Run with Docker Compose"

# Variables
PYTHON := python3
VENV := venv
TERRAFORM_DIR := terraform
NOTEBOOKS_DIR := notebooks
SRC_DIR := src
TEST_DIR := tests

# Colors for output
RED := \033[31m
GREEN := \033[32m
YELLOW := \033[33m
BLUE := \033[34m
NC := \033[0m # No Color

# Installation and Setup
install:
	@echo "$(BLUE)📦 Installing dependencies...$(NC)"
	$(PYTHON) -m pip install --upgrade pip
	pip install -r requirements.txt
	pip install -r requirements-dev.txt
	@echo "$(GREEN)✅ Dependencies installed successfully$(NC)"

setup-env:
	@echo "$(BLUE)🔧 Setting up development environment...$(NC)"
	@if [ ! -d "$(VENV)" ]; then \
		echo "Creating virtual environment..."; \
		$(PYTHON) -m venv $(VENV); \
	fi
	@echo "Activating virtual environment and installing dependencies..."
	. $(VENV)/bin/activate && pip install --upgrade pip
	. $(VENV)/bin/activate && pip install -r requirements.txt
	. $(VENV)/bin/activate && pip install -r requirements-dev.txt
	@if [ ! -f ".env" ]; then \
		echo "Creating .env file from template..."; \
		cp .env.template .env; \
		echo "$(YELLOW)⚠️  Please configure your .env file$(NC)"; \
	fi
	@echo "$(GREEN)✅ Development environment setup complete$(NC)"

setup-fabric:
	@echo "$(BLUE)🏗️ Setting up Microsoft Fabric resources...$(NC)"
	$(PYTHON) scripts/setup_fabric_resources.py
	$(PYTHON) scripts/upload_notebooks.py
	@echo "$(GREEN)✅ Fabric resources configured$(NC)"

# Testing
test:
	@echo "$(BLUE)🧪 Running all tests...$(NC)"
	pytest $(TEST_DIR)/ -v --cov=$(SRC_DIR) --cov-report=html --cov-report=term-missing
	@echo "$(GREEN)✅ All tests completed$(NC)"

test-unit:
	@echo "$(BLUE)🔬 Running unit tests...$(NC)"
	pytest $(TEST_DIR)/unit/ -v
	@echo "$(GREEN)✅ Unit tests completed$(NC)"

test-integration:
	@echo "$(BLUE)🔗 Running integration tests...$(NC)"
	pytest $(TEST_DIR)/integration/ -v
	@echo "$(GREEN)✅ Integration tests completed$(NC)"

test-ingestion:
	@echo "$(BLUE)📥 Testing data ingestion...$(NC)"
	$(PYTHON) -m $(SRC_DIR).data_ingestion.earthquake_api
	$(PYTHON) -m $(SRC_DIR).data_ingestion.weather_api
	@echo "$(GREEN)✅ Ingestion tests completed$(NC)"

test-alerts:
	@echo "$(BLUE)🚨 Testing alert system...$(NC)"
	$(PYTHON) scripts/test_notifications.py
	@echo "$(GREEN)✅ Alert tests completed$(NC)"

# Code Quality
lint:
	@echo "$(BLUE)🔍 Running code linting...$(NC)"
	flake8 $(SRC_DIR) $(TEST_DIR) --max-line-length=88 --extend-ignore=E203,W503
	pylint $(SRC_DIR) --rcfile=.pylintrc
	@echo "$(GREEN)✅ Linting completed$(NC)"

format:
	@echo "$(BLUE)🎨 Formatting code...$(NC)"
	black $(SRC_DIR) $(TEST_DIR) --line-length=88
	isort $(SRC_DIR) $(TEST_DIR) --profile=black
	@echo "$(GREEN)✅ Code formatting completed$(NC)"

type-check:
	@echo "$(BLUE)🔧 Running type checking...$(NC)"
	mypy $(SRC_DIR) --ignore-missing-imports
	@echo "$(GREEN)✅ Type checking completed$(NC)"

# Data Pipeline Operations
ingest:
	@echo "$(BLUE)📥 Running data ingestion...$(NC)"
	@echo "Ingesting earthquake data..."
	$(PYTHON) -c "from $(SRC_DIR).data_ingestion.earthquake_api import create_earthquake_ingestion; client = create_earthquake_ingestion(); events = client.get_recent_earthquakes(); print(f'Ingested {len(events)} earthquake events')"
	@echo "Ingesting weather data..."
	$(PYTHON) -c "from $(SRC_DIR).data_ingestion.weather_api import create_weather_ingestion; client = create_weather_ingestion(); alerts = client.get_active_alerts(); print(f'Ingested {len(alerts)} weather alerts')"
	@echo "$(GREEN)✅ Data ingestion completed$(NC)"

process:
	@echo "$(BLUE)⚙️ Running data processing pipeline...$(NC)"
	@echo "This will run the Fabric notebooks in sequence..."
	@echo "1. Bronze layer ingestion"
	@echo "2. Silver layer processing"
	@echo "3. Gold layer aggregation"
	@echo "$(YELLOW)⚠️  Run this command in your Fabric workspace$(NC)"

detect-anomalies:
	@echo "$(BLUE)🤖 Running anomaly detection...$(NC)"
	$(PYTHON) scripts/run_anomaly_detection.py
	@echo "$(GREEN)✅ Anomaly detection completed$(NC)"

# Infrastructure and Deployment
infrastructure:
	@echo "$(BLUE)🏗️ Deploying infrastructure...$(NC)"
	cd $(TERRAFORM_DIR) && terraform init
	cd $(TERRAFORM_DIR) && terraform plan
	cd $(TERRAFORM_DIR) && terraform apply -auto-approve
	@echo "$(GREEN)✅ Infrastructure deployment completed$(NC)"

deploy-staging:
	@echo "$(BLUE)🚀 Deploying to staging environment...$(NC)"
	@echo "Running pre-deployment tests..."
	make test-unit
	@echo "Deploying to staging..."
	./scripts/deploy_staging.sh
	@echo "Running integration tests..."
	make test-integration
	@echo "$(GREEN)✅ Staging deployment completed$(NC)"

deploy-production:
	@echo "$(BLUE)🚀 Deploying to production environment...$(NC)"
	@echo "$(YELLOW)⚠️  This will deploy to production!$(NC)"
	@read -p "Are you sure? (y/N): " confirm && [ "$$confirm" = "y" ] || exit 1
	@echo "Running full test suite..."
	make test
	@echo "Deploying to production..."
	./scripts/deploy_production.sh
	@echo "$(GREEN)✅ Production deployment completed$(NC)"

rollback:
	@echo "$(BLUE)↩️ Rolling back deployment...$(NC)"
	./scripts/rollback.sh
	@echo "$(GREEN)✅ Rollback completed$(NC)"

# Monitoring and Maintenance
logs:
	@echo "$(BLUE)📋 Showing application logs...$(NC)"
	./scripts/show_logs.sh

monitor:
	@echo "$(BLUE)📊 Showing system metrics...$(NC)"
	$(PYTHON) scripts/monitor_system.py

health-check:
	@echo "$(BLUE)🔍 Performing health check...$(NC)"
	$(PYTHON) scripts/health_check.py
	@echo "$(GREEN)✅ Health check completed$(NC)"

# Docker Operations
docker-build:
	@echo "$(BLUE)🐳 Building Docker images...$(NC)"
	docker-compose build
	@echo "$(GREEN)✅ Docker images built$(NC)"

docker-run:
	@echo "$(BLUE)🐳 Running with Docker Compose...$(NC)"
	docker-compose up -d
	@echo "$(GREEN)✅ Services started with Docker$(NC)"

docker-stop:
	@echo "$(BLUE)🐳 Stopping Docker services...$(NC)"
	docker-compose down
	@echo "$(GREEN)✅ Docker services stopped$(NC)"

# Utilities
clean:
	@echo "$(BLUE)🧹 Cleaning build artifacts...$(NC)"
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .pytest_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .mypy_cache/
	@echo "$(GREEN)✅ Cleanup completed$(NC)"

docs:
	@echo "$(BLUE)📚 Generating documentation...$(NC)"
	cd docs && sphinx-build -b html . _build/html
	@echo "$(GREEN)✅ Documentation generated$(NC)"

# Development workflow shortcuts
dev-setup: setup-env setup-fabric
	@echo "$(GREEN)✅ Development setup completed$(NC)"

dev-test: format lint type-check test
	@echo "$(GREEN)✅ Development testing completed$(NC)"

full-deploy: infrastructure deploy-staging deploy-production
	@echo "$(GREEN)✅ Full deployment completed$(NC)"

# Continuous Integration shortcuts (for GitHub Actions)
ci-test:
	@echo "$(BLUE)🔄 Running CI test suite...$(NC)"
	make format
	make lint  
	make type-check
	make test-unit
	make test-integration
	@echo "$(GREEN)✅ CI tests completed$(NC)"

# Performance testing
performance-test:
	@echo "$(BLUE)⚡ Running performance tests...$(NC)"
	$(PYTHON) scripts/performance_test.py
	@echo "$(GREEN)✅ Performance testing completed$(NC)"

# Security scanning
security-scan:
	@echo "$(BLUE)🔒 Running security scan...$(NC)"
	safety check
	bandit -r $(SRC_DIR)
	@echo "$(GREEN)✅ Security scan completed$(NC)"

# Database operations (if needed)
db-migrate:
	@echo "$(BLUE)🗄️ Running database migrations...$(NC)"
	# Add database migration commands here if using a database
	@echo "$(GREEN)✅ Database migrations completed$(NC)"

# Environment-specific targets
local: setup-env ingest
	@echo "$(GREEN)✅ Local environment ready$(NC)"

staging: deploy-staging test-integration
	@echo "$(GREEN)✅ Staging environment ready$(NC)"

production: deploy-production monitor
	@echo "$(GREEN)✅ Production environment ready$(NC)"

# Show configuration
show-config:
	@echo "$(BLUE)📋 Current configuration:$(NC)"
	@echo "Python version: $$($(PYTHON) --version)"
	@echo "Virtual environment: $(VENV)"
	@echo "Source directory: $(SRC_DIR)"
	@echo "Test directory: $(TEST_DIR)"
	@echo "Terraform directory: $(TERRAFORM_DIR)"
	@if [ -f ".env" ]; then \
		echo "Environment file: .env (configured)"; \
	else \
		echo "Environment file: .env ($(RED)missing$(NC))"; \
	fi

# Project statistics
stats:
	@echo "$(BLUE)📊 Project statistics:$(NC)"
	@echo "Python files: $$(find $(SRC_DIR) -name '*.py' | wc -l)"
	@echo "Test files: $$(find $(TEST_DIR) -name '*.py' | wc -l)"
	@echo "Notebooks: $$(find $(NOTEBOOKS_DIR) -name '*.py' | wc -l)"
	@echo "Total lines of code: $$(find $(SRC_DIR) -name '*.py' -exec wc -l {} \; | awk '{sum += $$1} END {print sum}')"
	@echo "Test coverage: $$(pytest --cov=$(SRC_DIR) --cov-report=term-missing -q | grep TOTAL | awk '{print $$4}')"

# Quick start for new developers
quick-start:
	@echo "$(BLUE)🚀 Quick start for new developers...$(NC)"
	@echo "1. Setting up environment..."
	make setup-env
	@echo "2. Running tests..."
	make test-unit
	@echo "3. Testing data ingestion..."
	make test-ingestion
	@echo "4. Testing alerts..."
	make test-alerts
	@echo "$(GREEN)✅ Quick start completed!$(NC)"
	@echo ""
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "1. Configure your .env file"
	@echo "2. Set up Microsoft Fabric workspace"
	@echo "3. Import Power BI dashboard"
	@echo "4. Run 'make ingest' to start data ingestion"
