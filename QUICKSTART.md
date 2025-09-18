# ⚡ Quick Start - Maritime Activity Reports CDC/CDF

**Get up and running in 5 minutes!**

## 🎯 One-Command Setup

```bash
git clone <repository-url>
cd maritime-activity-reports-cdc
make quick-start
```

**That's it!** The system is now running with simulated maritime data.

---

## 📋 What Just Happened?

The `make quick-start` command automatically:

1. ✅ **Installed dependencies** (PDM, Python packages)
2. ✅ **Setup development environment** (pre-commit hooks, config)
3. ✅ **Verified installation** (comprehensive system check)
4. ✅ **Created Delta tables** (Bronze, Silver, Gold with CDF enabled)
5. ✅ **Generated test data** (5 vessels, 50 movement records each)
6. ✅ **Ran health check** (verified all components working)

## 🔍 Verify Your Setup

### Check System Status
```bash
# Quick health check
make health-check

# Detailed verification
python3 scripts/verify_installation.py
```

### Explore the Data
```bash
# View generated data
pdm run python -c "
from maritime_activity_reports.utils.spark_utils import get_spark_session
from maritime_activity_reports.models.config import MaritimeConfig

config = MaritimeConfig.from_file('config/config.yaml')
spark = get_spark_session(config)

print('📊 Data Summary:')
spark.sql('SELECT COUNT(*) as total_records FROM bronze.ais_movements').show()
spark.sql('SELECT COUNT(DISTINCT imo) as unique_vessels FROM bronze.ais_movements').show()
spark.sql('SELECT AVG(data_quality_score) as avg_quality FROM bronze.ais_movements').show()

spark.stop()
"
```

### Test CDC/CDF Functionality
```bash
# Test Change Data Feed
pdm run python -c "
from maritime_activity_reports.utils.spark_utils import get_spark_session
from maritime_activity_reports.models.config import MaritimeConfig

config = MaritimeConfig.from_file('config/config.yaml')
spark = get_spark_session(config)

print('🔄 Testing Change Data Feed:')
cdf_data = spark.read.format('delta').option('readChangeFeed', 'true').option('startingVersion', '0').table('bronze.ais_movements')
print(f'CDF records available: {cdf_data.count()}')
cdf_data.groupBy('_change_type').count().show()

spark.stop()
"
```

---

## 🚀 Next Steps

### **Explore the System**
```bash
# View all available commands
make help

# Start interactive development
pdm run jupyter lab  # Opens Jupyter at http://localhost:8888

# Explore with CLI
pdm run maritime-reports --help
```

### **Generate More Data**
```bash
# Generate larger dataset
pdm run maritime-reports simulate-data --vessels 20 --records 200

# Optimize tables for performance
pdm run maritime-reports optimize-tables
```

### **Start Streaming (Advanced)**
```bash
# Start real-time CDC/CDF streaming
pdm run maritime-reports start-streaming

# Monitor streaming in another terminal
pdm run maritime-reports health-check --component streaming
```

### **Deploy to GCP (Production)**
```bash
# Set your GCP project
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"

# Deploy everything to GCP
make deploy-gcp-full
```

---

## 📚 **Learning Path**

### **Day 1: Basics**
1. ✅ Complete this quick start
2. 📖 Read [GETTING_STARTED.md](GETTING_STARTED.md)
3. 🧪 Run `make test` to see all tests pass
4. 📊 Explore the generated data

### **Day 2: Understanding**
1. 📖 Read [docs/API.md](docs/API.md)
2. 🔍 Explore `src/maritime_activity_reports/` code
3. 🧪 Modify configuration in `config/config.yaml`
4. 🎯 Understand Bronze → Silver → Gold flow

### **Day 3: Advanced**
1. 🚀 Deploy to GCP using [deployment/gcp_deployment_guide.md](deployment/gcp_deployment_guide.md)
2. 📊 Create custom materialized views
3. 🔄 Implement real CDC data sources
4. 📈 Setup monitoring and alerting

---

## 🆘 **Troubleshooting**

### **Common Issues**

**❌ "Java not found"**
```bash
# Install Java 11+
brew install openjdk@11  # macOS
sudo apt install openjdk-11-jdk  # Ubuntu

# Set JAVA_HOME
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
```

**❌ "PDM not found"**
```bash
pip install pdm
export PATH="$HOME/.local/bin:$PATH"
```

**❌ "Permission denied"**
```bash
chmod +x scripts/*.sh
chmod +x scripts/gcp/*.sh
```

**❌ "Configuration file not found"**
```bash
cp config/config.example.yaml config/config.yaml
```

### **Get Help**
```bash
# Run diagnostics
python3 scripts/verify_installation.py

# Check system health
make health-check

# View detailed help
make help
pdm run maritime-reports --help
```

---

## 🎉 **Success!**

If you've made it here, you now have:

- ✅ **Working maritime data processing system**
- ✅ **CDC/CDF capabilities** for real-time processing
- ✅ **Medallion architecture** (Bronze → Silver → Gold)
- ✅ **Business-ready analytics** with materialized views
- ✅ **Production deployment** capabilities for GCP

**Welcome to enterprise-grade maritime data engineering! 🚢⚓**

---

## 🔗 **Quick Reference**

| Command | Purpose |
|---------|---------|
| `make quick-start` | Complete setup in one command |
| `make health-check` | Verify system health |
| `make simulate` | Generate test data |
| `make test` | Run all tests |
| `make deploy-gcp` | Deploy to Google Cloud |
| `pdm run maritime-reports --help` | CLI help |

**Happy coding! 🚀**
