#!/bin/bash
# Development environment setup script

set -e

echo "🚢 Setting up Maritime Activity Reports CDC/CDF Development Environment"
echo "=================================================================="

# Check prerequisites
echo "📋 Checking prerequisites..."

# Check Python version
python_version=$(python3 --version 2>&1 | grep -o '[0-9]\+\.[0-9]\+' | head -1)
required_version="3.9"

if [ "$(printf '%s\n' "$required_version" "$python_version" | sort -V | head -n1)" != "$required_version" ]; then
    echo "❌ Python $required_version or higher required. Found: $python_version"
    exit 1
fi

echo "✅ Python version: $python_version"

# Check if PDM is installed
if ! command -v pdm &> /dev/null; then
    echo "📦 Installing PDM..."
    pip install pdm
fi

echo "✅ PDM installed: $(pdm --version)"

# Check if Java is installed (required for Spark)
if ! command -v java &> /dev/null; then
    echo "❌ Java is required for Spark but not found"
    echo "Please install Java 11 or higher"
    exit 1
fi

echo "✅ Java installed: $(java -version 2>&1 | head -1)"

# Install dependencies
echo "📦 Installing Python dependencies..."
pdm install --dev

# Setup pre-commit hooks
echo "🔧 Setting up pre-commit hooks..."
pdm run pre-commit install

# Create configuration file if it doesn't exist
if [ ! -f "config/config.yaml" ]; then
    echo "📝 Creating configuration file..."
    cp config/config.example.yaml config/config.yaml
    echo "✅ Created config/config.yaml from example"
    echo "⚠️  Please edit config/config.yaml with your specific settings"
else
    echo "✅ Configuration file already exists"
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p logs
mkdir -p data/test
mkdir -p notebooks
mkdir -p scripts/sql

# Set up local Spark environment
echo "⚡ Setting up local Spark environment..."
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=python3
export PYSPARK_DRIVER_PYTHON=python3

# Run comprehensive verification
echo "🧪 Running installation verification..."
python3 scripts/verify_installation.py

if [ $? -eq 0 ]; then
    echo ""
    echo "🎉 Installation verification passed!"
else
    echo ""
    echo "❌ Installation verification failed. Please check the issues above."
    exit 1
fi

echo ""
echo "🎉 Development environment setup completed!"
echo ""
echo "Next steps:"
echo "1. Edit config/config.yaml with your GCP project settings"
echo "2. Run 'make setup-tables' to create Delta tables"
echo "3. Run 'make simulate' to generate test data"
echo "4. Run 'make start-stream' to start streaming processors"
echo ""
echo "Useful commands:"
echo "  make help              - Show all available commands"
echo "  make test              - Run all tests"
echo "  make lint              - Run code quality checks"
echo "  make health-check      - Check system health"
echo "  make simulate          - Generate test data"
echo ""
echo "Happy coding! 🚀"
