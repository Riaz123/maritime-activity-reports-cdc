# Maritime Activity Reports with CDC/CDF

A comprehensive data engineering solution for processing maritime vessel activity data using Change Data Capture (CDC) and Change Data Feed (CDF) in a medallion architecture.

## 🚢 Overview

This project implements a real-time maritime data processing pipeline that captures vessel movements, analyzes activity patterns, and generates business-ready reports. The system uses a medallion architecture (Bronze → Silver → Gold) with Change Data Feed (CDF) for real-time change propagation.

## 🏗️ Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Bronze Layer  │───▶│  Silver Layer   │───▶│   Gold Layer    │
│   (Raw CDC)     │    │ (Cleaned CDF)   │    │ (Business CDF)  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
   Raw AIS Data           Enriched Data          Activity Reports
   Vessel Metadata        Zone Assignments       Journey Analytics
   Geospatial Zones       Data Quality           Port Performance
                          Spatial Enrichment     Materialized Views
```

### Key Features

- **Real-time CDC/CDF Processing**: Automatic change propagation across all layers
- **Maritime Domain Expertise**: Built-in understanding of vessel operations, zones, and compliance
- **Data Quality Assurance**: Comprehensive validation and scoring at each layer
- **Scalable Architecture**: Designed for high-volume AIS data processing
- **Business-Ready Analytics**: Pre-built reports and materialized views

## 📊 Data Layers

### Bronze Layer (Raw Data)
- **AIS Movement Data**: Raw vessel position and movement data
- **Vessel Metadata**: Ship registry information with SCD Type 2 handling  
- **Geospatial Zones**: Reference data for maritime zones and boundaries
- **CDC Operations**: Full audit trail of all data changes

### Silver Layer (Cleaned & Enriched)
- **Cleaned Movements**: Validated and quality-scored position data
- **Vessel Master**: Current vessel information with risk scoring
- **Zone Definitions**: Standardized geographic zone classifications
- **Spatial Enrichment**: Cached spatial lookups for performance

### Gold Layer (Business Ready)
- **Activity Reports**: Zone visits, time spent, journey segments
- **Port Analytics**: Performance metrics and operational insights
- **Compliance Reports**: Risk assessment and sanction screening
- **Real-time Dashboards**: Live vessel tracking and alerts

## 🚀 Quick Start

**New to this project? Start here:** 👉 **[GETTING_STARTED.md](GETTING_STARTED.md)** 👈

### 5-Minute Setup

```bash
# 1. Clone and setup
git clone <repository-url>
cd maritime-activity-reports-cdc
./scripts/setup_development.sh

# 2. Initialize system
make setup-tables
make simulate
make health-check
```

### Prerequisites

- **Python 3.9+** - [Download](https://python.org/downloads/)
- **Java 11+** - Required for Apache Spark
- **PDM** - Python dependency manager (`pip install pdm`)

**Optional for GCP:**
- **gcloud CLI** - [Installation Guide](https://cloud.google.com/sdk/docs/install)
- **Terraform** - [Installation Guide](https://learn.hashicorp.com/tutorials/terraform/install-cli)

### Configuration

Create a configuration file at `config/config.yaml`:

```yaml
# config/config.yaml
project_name: "maritime-activity-reports-cdc"
environment: "dev"

spark:
  app_name: "Maritime-Activity-Reports-CDC"
  executor_memory: "4g"
  executor_cores: 2
  executor_instances: 2

bronze:
  base_path: "gs://your-bucket/bronze"
  retention_days: 30

silver:
  base_path: "gs://your-bucket/silver"
  retention_days: 30

gold:
  base_path: "gs://your-bucket/gold"
  retention_days: 90

bigquery:
  project_id: "your-gcp-project"
  dataset: "maritime_reports"

gcs:
  bucket_name: "your-data-bucket"
```

## 📖 Usage

### Command Line Interface

```bash
# Setup all Delta tables with CDF
maritime-reports setup-tables

# Ingest CDC data
maritime-reports ingest-cdc --source ais --file path/to/data.json

# Start streaming processors
maritime-reports start-streaming --layer silver

# Generate activity reports
maritime-reports generate-reports --date 2024-01-15 --type port

# Create materialized views
maritime-reports create-materialized-views
```

### Python API

```python
from maritime_activity_reports import MaritimeConfig
from maritime_activity_reports.bronze import BronzeCDCLayer
from maritime_activity_reports.orchestrator import CDCCDFOrchestrator

# Load configuration
config = MaritimeConfig.from_file("config/config.yaml")

# Initialize orchestrator
orchestrator = CDCCDFOrchestrator(config)

# Setup all tables
orchestrator.setup_all_cdf_tables()

# Start streaming
queries = orchestrator.start_all_streaming_queries()

# Monitor streams
orchestrator.monitor_streaming_queries()
```

### Airflow Integration

```python
# airflow_dags/maritime_reports_dag.py
from maritime_activity_reports.airflow import create_maritime_dag

dag = create_maritime_dag(
    dag_id="maritime-reports-daily",
    config_path="config/config.yaml",
    schedule_interval="@daily"
)
```

## 🔧 Development

### Project Structure

```
maritime-activity-reports-cdc/
├── src/maritime_activity_reports/
│   ├── bronze/           # Bronze layer CDC ingestion
│   ├── silver/           # Silver layer CDF processing  
│   ├── gold/             # Gold layer analytics
│   ├── models/           # Data schemas and config
│   ├── utils/            # Common utilities
│   └── orchestrator/     # CDC/CDF coordination
├── config/               # Configuration files
├── docs/                 # Documentation
├── scripts/              # Utility scripts
├── airflow_dags/         # Airflow DAG definitions
├── notebooks/            # Jupyter notebooks
└── tests/                # Test suite
```

### Running Tests

```bash
# Run all tests
pdm run pytest

# Run with coverage
pdm run pytest --cov=maritime_activity_reports

# Run specific test category
pdm run pytest -m unit
pdm run pytest -m integration
```

### Code Quality

```bash
# Format code
pdm run black src/
pdm run isort src/

# Lint code
pdm run flake8 src/
pdm run mypy src/

# Pre-commit hooks
pdm run pre-commit install
pdm run pre-commit run --all-files
```

## 📈 Monitoring & Operations

### Health Checks

```bash
# Check streaming query health
maritime-reports health-check --component streaming

# Validate data quality
maritime-reports validate-quality --table silver.cleaned_vessel_movements

# Monitor CDC lag
maritime-reports monitor-cdc --source ais
```

### Performance Tuning

```bash
# Optimize Delta tables
maritime-reports optimize-tables --layer silver

# Vacuum old files
maritime-reports vacuum-tables --retention-hours 168

# Analyze table statistics
maritime-reports analyze-tables --table silver.vessel_master
```



### Development Guidelines

- Follow PEP 8 style guidelines
- Write comprehensive tests for new features
- Update documentation for API changes
- Use structured logging throughout
- Implement proper error handling

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🆘 Support

- **Documentation**: [docs/](docs/)
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Email**: data-team@your-company.com

## 🙏 Acknowledgments

- Apache Spark and Delta Lake communities
- Open Source Maritime data providers [AISHub](https://www.aishub.net/)
- Inspiration for implementation from https://open-ais.org/
- Open source geospatial libraries
- Google Cloud Platform team

---

**Built with ❤️ for the maritime industry**
