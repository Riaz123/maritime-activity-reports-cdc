# 🚢 Getting Started - Maritime Activity Reports CDC/CDF

Welcome! This guide will help you get the Maritime Activity Reports CDC/CDF system up and running on your local machine or deploy it to GCP.

## 🎯 Quick Start (5 minutes)

```bash
# 1. Clone the repository
git clone <repository-url>
cd maritime-activity-reports-cdc

# 2. Run automated setup
./scripts/setup_development.sh

# 3. Start the system
make setup-tables
make simulate
make health-check
```

**That's it!** You now have a working maritime data processing system with CDC/CDF capabilities.

---

## 📋 Detailed Setup Guide

### **Prerequisites**

Before starting, ensure you have:

| Requirement | Version | Installation |
|-------------|---------|--------------|
| **Python** | 3.9+ | [Download Python](https://python.org/downloads/) |
| **Java** | 11+ | `brew install openjdk@11` (macOS) or [Download](https://adoptium.net/) |
| **Git** | Latest | [Download Git](https://git-scm.com/downloads) |
| **PDM** | Latest | `pip install pdm` |

**Optional for GCP deployment:**
- **gcloud CLI**: [Installation Guide](https://cloud.google.com/sdk/docs/install)
- **Terraform**: [Installation Guide](https://learn.hashicorp.com/tutorials/terraform/install-cli)
- **Docker**: [Installation Guide](https://docs.docker.com/get-docker/)

### **Step 1: Clone and Setup**

```bash
# Clone the repository
git clone <repository-url>
cd maritime-activity-reports-cdc

# Check prerequisites
python3 --version  # Should be 3.9+
java -version      # Should be 11+
```

### **Step 2: Install Dependencies**

```bash
# Option A: Automated setup (recommended)
./scripts/setup_development.sh

# Option B: Manual setup
make install
make setup-dev
```

The automated setup will:
- ✅ Check prerequisites
- ✅ Install PDM and dependencies
- ✅ Setup pre-commit hooks
- ✅ Create configuration file
- ✅ Run basic tests
- ✅ Verify installation

### **Step 3: Configure the System**

```bash
# Configuration file is automatically created at:
config/config.yaml

# Edit with your settings (optional for local development)
vim config/config.yaml
```

**Default configuration works for local development!** You only need to edit it for:
- GCP deployment
- Custom storage paths
- Specific performance tuning

### **Step 4: Initialize the System**

```bash
# Create all Delta tables with Change Data Feed
make setup-tables

# This creates:
# - Bronze layer tables (raw CDC data)
# - Silver layer tables (cleaned CDF data)  
# - Gold layer tables (business analytics)
```

### **Step 5: Generate Test Data**

```bash
# Generate simulated maritime data
make simulate

# This creates:
# - 5 test vessels
# - 50 movement records per vessel
# - Realistic AIS data with quality scores
```

### **Step 6: Verify Everything Works**

```bash
# Run comprehensive health check
make health-check

# Run tests
make test

# Check data was created
pdm run python -c "
from maritime_activity_reports.utils.spark_utils import get_spark_session
from maritime_activity_reports.models.config import MaritimeConfig

config = MaritimeConfig.from_file('config/config.yaml')
spark = get_spark_session(config)

print('📊 Data Summary:')
bronze_count = spark.sql('SELECT COUNT(*) FROM bronze.ais_movements').collect()[0][0]
print(f'Bronze AIS records: {bronze_count}')

spark.stop()
print('✅ System verification completed!')
"
```

---

## 🛠️ **Development Workflow**

### **Daily Development**

```bash
# Start your development session
make health-check          # Verify system health
make simulate              # Generate fresh test data
make start-stream          # Start streaming processors (optional)

# During development
make test                  # Run tests
make lint                  # Check code quality
make format                # Format code

# End of session
make clean                 # Clean up artifacts
```

### **Working with Data**

```bash
# Generate more test data
pdm run maritime-reports simulate-data --vessels 10 --records 100

# Check data quality
pdm run maritime-reports validate-quality --table bronze.ais_movements

# Optimize tables
pdm run maritime-reports optimize-tables

# View system statistics
pdm run maritime-reports health-check --component all
```

### **Exploring the System**

```bash
# Start Jupyter for interactive exploration
pdm run jupyter lab

# Open the getting started notebook:
# notebooks/01_Getting_Started.ipynb (create this)

# Or explore with CLI
pdm run maritime-reports --help
```

---

## ☁️ **GCP Deployment (Optional)**

If you want to deploy to Google Cloud Platform:

### **Prerequisites for GCP**
```bash
# Install gcloud CLI
curl https://sdk.cloud.google.com | bash

# Authenticate
gcloud auth login
gcloud config set project YOUR_PROJECT_ID

# Install Terraform
brew install terraform  # macOS
# or download from https://terraform.io
```

### **Deploy to GCP**
```bash
# Set environment variables
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export ENVIRONMENT="dev"

# One-command deployment
make deploy-gcp-full

# Or step-by-step
make gcp-enable-apis       # Enable required APIs
make infra-gcp            # Deploy infrastructure
make build-gcp            # Build Docker image
make deploy-gcp           # Deploy to Cloud Run
make dags-gcp            # Deploy Airflow DAGs
```

---

## 🧪 **Testing Your Setup**

### **Basic Functionality Test**

```bash
# Test 1: Configuration loading
pdm run python -c "
from maritime_activity_reports.models.config import MaritimeConfig
config = MaritimeConfig.from_file('config/config.yaml')
print(f'✅ Config loaded: {config.project_name}')
"

# Test 2: Spark integration
pdm run python -c "
from maritime_activity_reports.utils.spark_utils import get_spark_session
from maritime_activity_reports.models.config import MaritimeConfig
config = MaritimeConfig.from_file('config/config.yaml')
spark = get_spark_session(config)
print(f'✅ Spark session: {spark.version}')
spark.stop()
"

# Test 3: Table creation
pdm run maritime-reports setup-tables
echo "✅ Tables created successfully"

# Test 4: Data generation
pdm run maritime-reports simulate-data --vessels 3 --records 10
echo "✅ Data generation successful"

# Test 5: Health check
pdm run maritime-reports health-check
echo "✅ Health check completed"
```

### **Advanced Testing**

```bash
# Run full test suite
make test

# Run only unit tests
make test-unit

# Run integration tests
make test-integration

# Performance test with larger dataset
pdm run maritime-reports simulate-data --vessels 50 --records 200
pdm run maritime-reports optimize-tables
```

---

## 🔍 **Exploring the System**

### **Understanding the Architecture**

```bash
# View project structure
find src -name "*.py" | head -20

# Key components:
# - src/maritime_activity_reports/bronze/    # CDC ingestion
# - src/maritime_activity_reports/silver/    # CDF processing  
# - src/maritime_activity_reports/gold/      # Business analytics
# - src/maritime_activity_reports/orchestrator/ # End-to-end coordination
```

### **Exploring the Data**

```python
# Start Python shell
pdm run python

# Explore data interactively
from maritime_activity_reports.utils.spark_utils import get_spark_session
from maritime_activity_reports.models.config import MaritimeConfig

config = MaritimeConfig.from_file('config/config.yaml')
spark = get_spark_session(config)

# View Bronze data
spark.sql("SELECT * FROM bronze.ais_movements LIMIT 5").show()

# Check data quality
spark.sql("""
    SELECT 
        AVG(data_quality_score) as avg_quality,
        COUNT(DISTINCT imo) as unique_vessels
    FROM bronze.ais_movements
""").show()

spark.stop()
```

---

## 🆘 **Common Issues & Solutions**

### **Issue 1: Java Not Found**
```bash
# Error: JAVA_HOME not set
# Solution:
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64  # Linux
export JAVA_HOME=/usr/local/opt/openjdk@11           # macOS
```

### **Issue 2: Permission Denied on Scripts**
```bash
# Error: Permission denied
# Solution:
chmod +x scripts/*.sh
chmod +x scripts/gcp/*.sh
```

### **Issue 3: Configuration File Missing**
```bash
# Error: Config file not found
# Solution:
cp config/config.example.yaml config/config.yaml
# Then edit config/config.yaml with your settings
```

### **Issue 4: Spark Session Issues**
```bash
# Error: Cannot create Spark session
# Solution:
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3
```

### **Issue 5: PDM Installation Issues**
```bash
# Error: PDM not found
# Solution:
pip install --user pdm
export PATH="$HOME/.local/bin:$PATH"
```

---

## 📚 **Learning Resources**

### **Understanding the Code**
1. **Start with**: `src/maritime_activity_reports/cli.py` - Main entry point
2. **Then explore**: `src/maritime_activity_reports/orchestrator/` - System coordination
3. **Deep dive**: `src/maritime_activity_reports/bronze/` - CDC ingestion
4. **Advanced**: `src/maritime_activity_reports/gold/materialized_views.py` - Business analytics

### **Key Concepts**
- **CDC (Change Data Capture)**: Capturing changes from source systems
- **CDF (Change Data Feed)**: Delta Lake's change tracking mechanism
- **Medallion Architecture**: Bronze → Silver → Gold data refinement
- **Materialized Views**: Pre-computed analytics for fast queries

### **Documentation**
- **API Reference**: `docs/API.md`
- **GCP Deployment**: `deployment/gcp_deployment_guide.md`
- **Configuration**: `config/config.example.yaml`

---

## 🎓 **Next Steps for Learning**

### **Beginner (First Week)**
1. ✅ Complete this getting started guide
2. 📊 Explore the simulated data
3. 🧪 Run the test suite
4. 📖 Read the API documentation

### **Intermediate (Second Week)**
1. 🔧 Modify configuration for your use case
2. 📈 Create custom analytics in Gold layer
3. 🔄 Understand CDC/CDF flow
4. 🚀 Deploy to GCP (dev environment)

### **Advanced (Third Week)**
1. 🏗️ Extend the system with new data sources
2. 📊 Create custom materialized views
3. 🔍 Implement custom data quality rules
4. 🚀 Deploy to production

---

## 💬 **Getting Help**

### **Self-Help**
```bash
# Check system health
make health-check

# View logs
tail -f logs/maritime-reports.log

# Run diagnostics
pdm run maritime-reports health-check --component all
```

### **Community Support**
- **Documentation**: `docs/` folder
- **Issues**: GitHub Issues
- **Discussions**: GitHub Discussions
- **Email**: data-team@your-company.com

### **Debug Mode**
```bash
# Enable debug logging
export MARITIME_DEBUG=true
pdm run maritime-reports health-check

# Verbose test output
make test-unit -v
```

---

## 🎉 **Success Checklist**

After setup, you should be able to:

- [ ] ✅ Load configuration without errors
- [ ] ✅ Create Spark session locally
- [ ] ✅ Setup all Delta tables
- [ ] ✅ Generate simulated data
- [ ] ✅ Run health checks successfully
- [ ] ✅ Pass all tests
- [ ] ✅ View data in Bronze/Silver/Gold layers
- [ ] ✅ (Optional) Deploy to GCP

**If all boxes are checked, you're ready to start developing! 🚀**

---

## 🔗 **Quick Reference**

### **Essential Commands**
```bash
make help                  # Show all commands
make setup-dev            # Setup development environment
make setup-tables         # Create Delta tables
make simulate             # Generate test data
make health-check         # System health check
make test                 # Run tests
make deploy-gcp          # Deploy to GCP
```

### **Configuration Files**
- `config/config.yaml` - Main configuration
- `config/gcp_config.yaml` - GCP-specific settings
- `pyproject.toml` - Project metadata and dependencies

### **Key Directories**
- `src/maritime_activity_reports/` - Main source code
- `airflow_dags/` - Airflow orchestration
- `infrastructure/` - Deployment infrastructure
- `docs/` - Documentation
- `scripts/` - Utility scripts

**Happy maritime data processing! ⚓🌊**
