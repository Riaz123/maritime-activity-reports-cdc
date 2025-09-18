# Maritime Activity Reports CDC/CDF - Makefile with GCP deployment

.PHONY: help install test lint format clean setup-dev run-tests docker-build docker-run deploy-gcp

# Default target
help:
	@echo "Maritime Activity Reports CDC/CDF - Available commands:"
	@echo ""
	@echo "Development:"
	@echo "  install      Install dependencies using PDM"
	@echo "  setup-dev    Setup development environment"
	@echo "  test         Run all tests"
	@echo "  lint         Run linting checks"
	@echo "  format       Format code with black and isort"
	@echo "  clean        Clean up build artifacts"
	@echo ""
	@echo "Application:"
	@echo "  setup-tables Setup Delta tables with CDF"
	@echo "  start-stream Start streaming processors"
	@echo "  simulate     Generate simulated data"
	@echo ""
	@echo "GCP Deployment:"
	@echo "  deploy-gcp   Deploy to Google Cloud Platform"
	@echo "  build-gcp    Build and push Docker image to GCP"
	@echo "  infra-gcp    Deploy GCP infrastructure only"
	@echo "  dags-gcp     Deploy Airflow DAGs to Cloud Composer"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build Build Docker image"
	@echo "  docker-run   Run application in Docker"

# Development setup
install:
	@echo "Installing dependencies..."
	pdm install

setup-dev: install
	@echo "Setting up development environment..."
	pdm install --dev
	pre-commit install
	@echo "Creating example config..."
	@if [ ! -f config/config.yaml ]; then \
		cp config/config.example.yaml config/config.yaml; \
		echo "Created config/config.yaml from example"; \
	fi

# Testing
test:
	@echo "Running tests..."
	pdm run pytest -v --cov=maritime_activity_reports --cov-report=html --cov-report=term

test-unit:
	@echo "Running unit tests..."
	pdm run pytest src/maritime_activity_reports/tests/ -m unit -v

test-integration:
	@echo "Running integration tests..."
	pdm run pytest src/maritime_activity_reports/tests/ -m integration -v

# Code quality
lint:
	@echo "Running linting checks..."
	pdm run flake8 src/
	pdm run mypy src/
	pdm run black --check src/
	pdm run isort --check-only src/

format:
	@echo "Formatting code..."
	pdm run black src/ tests/
	pdm run isort src/ tests/

# Cleanup
clean:
	@echo "Cleaning up..."
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/
	rm -rf dist/
	rm -rf .coverage
	rm -rf htmlcov/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/

# Application commands
setup-tables:
	@echo "Setting up Delta tables..."
	pdm run maritime-reports setup-tables

start-stream:
	@echo "Starting streaming processors..."
	pdm run maritime-reports start-streaming

simulate:
	@echo "Generating simulated data..."
	pdm run maritime-reports simulate-data --vessels 5 --records 50

optimize:
	@echo "Optimizing Delta tables..."
	pdm run maritime-reports optimize-tables

health-check:
	@echo "Running health check..."
	pdm run maritime-reports health-check

# GCP Deployment commands
deploy-gcp: check-gcp-env
	@echo "🚀 Deploying to Google Cloud Platform..."
	./scripts/gcp/deploy.sh deploy

build-gcp: check-gcp-env
	@echo "🐳 Building and pushing Docker image to GCP..."
	./scripts/gcp/deploy.sh build

infra-gcp: check-gcp-env
	@echo "🏗️  Deploying GCP infrastructure..."
	./scripts/gcp/deploy.sh infrastructure

dags-gcp: check-gcp-env
	@echo "🎼 Deploying Airflow DAGs to Cloud Composer..."
	./scripts/gcp/deploy.sh airflow

monitoring-gcp: check-gcp-env
	@echo "📊 Setting up GCP monitoring..."
	./scripts/gcp/deploy.sh monitoring

# Check GCP environment variables
check-gcp-env:
	@if [ -z "$$GOOGLE_CLOUD_PROJECT" ]; then \
		echo "❌ GOOGLE_CLOUD_PROJECT environment variable is not set"; \
		echo "Set it with: export GOOGLE_CLOUD_PROJECT=your-project-id"; \
		exit 1; \
	fi
	@echo "✅ GCP Project: $$GOOGLE_CLOUD_PROJECT"

# Docker commands
docker-build:
	@echo "Building Docker image..."
	docker build -t maritime-reports-cdc:latest .

docker-build-prod:
	@echo "Building production Docker image..."
	docker build -f infrastructure/docker/Dockerfile.production -t maritime-reports-cdc:prod .

docker-run:
	@echo "Running application in Docker..."
	docker run --rm -v $(PWD)/config:/app/config maritime-reports-cdc:latest

docker-run-prod:
	@echo "Running production Docker image..."
	docker run --rm \
		-e GOOGLE_CLOUD_PROJECT=$$GOOGLE_CLOUD_PROJECT \
		-e ENVIRONMENT=$$ENVIRONMENT \
		-e STARTUP_MODE=health-check \
		-v $(PWD)/config:/app/config \
		maritime-reports-cdc:prod

# Package building
build:
	@echo "Building package..."
	pdm build

publish-test:
	@echo "Publishing to Test PyPI..."
	pdm publish --repository testpypi

publish:
	@echo "Publishing to PyPI..."
	pdm publish

# GCP specific utilities
gcp-auth:
	@echo "Authenticating with GCP..."
	gcloud auth login
	gcloud config set project $$GOOGLE_CLOUD_PROJECT

gcp-enable-apis:
	@echo "Enabling required GCP APIs..."
	gcloud services enable \
		compute.googleapis.com \
		dataproc.googleapis.com \
		bigquery.googleapis.com \
		storage.googleapis.com \
		pubsub.googleapis.com \
		composer.googleapis.com \
		run.googleapis.com \
		artifactregistry.googleapis.com

gcp-create-service-account:
	@echo "Creating GCP service account..."
	gcloud iam service-accounts create maritime-reports-$$ENVIRONMENT \
		--display-name="Maritime Reports Service Account - $$ENVIRONMENT" \
		--description="Service account for maritime activity reports CDC/CDF processing"

gcp-logs:
	@echo "Viewing recent GCP logs..."
	gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=maritime-streaming-$$ENVIRONMENT" --limit=50

gcp-status:
	@echo "Checking GCP deployment status..."
	@echo "Cloud Run services:"
	gcloud run services list --region=$$REGION
	@echo ""
	@echo "Dataproc batches (last 5):"
	gcloud dataproc batches list --region=$$REGION --limit=5
	@echo ""
	@echo "BigQuery datasets:"
	bq ls $$GOOGLE_CLOUD_PROJECT

# Environment info
env-info:
	@echo "Environment information:"
	@echo "Python version: $(shell python --version)"
	@echo "PDM version: $(shell pdm --version)"
	@echo "Spark version: $(shell python -c 'import pyspark; print(pyspark.__version__)' 2>/dev/null || echo 'Not installed')"
	@echo "Delta version: $(shell python -c 'import delta; print(delta.__version__)' 2>/dev/null || echo 'Not installed')"
	@echo "GCP Project: $$GOOGLE_CLOUD_PROJECT"
	@echo "Environment: $$ENVIRONMENT"
	@echo "Region: $$REGION"

# Verify installation
verify-installation:
	@echo "Verifying installation..."
	python3 scripts/verify_installation.py

# Quick start for new users
quick-start: install setup-dev verify-installation setup-tables simulate health-check
	@echo ""
	@echo "🎉 Quick start completed successfully!"
	@echo ""
	@echo "📊 System Status:"
	@pdm run maritime-reports health-check --component all
	@echo ""
	@echo "📖 Next steps:"
	@echo "  - Explore: docs/API.md"
	@echo "  - Tutorial: GETTING_STARTED.md"
	@echo "  - Deploy GCP: make deploy-gcp"

# Full GCP deployment for new projects
deploy-gcp-full: clean install gcp-enable-apis infra-gcp build-gcp deploy-gcp dags-gcp monitoring-gcp
	@echo "🎉 Full GCP deployment completed!"
	@echo "Check the deployment guide: deployment/gcp_deployment_guide.md"