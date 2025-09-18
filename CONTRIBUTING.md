# Contributing to Maritime Activity Reports CDC/CDF

Thank you for your interest in contributing to the Maritime Activity Reports CDC/CDF project! This document provides guidelines for contributing.

## 🚢 Getting Started

1. **Fork the repository** on GitHub
2. **Clone your fork** locally
3. **Setup development environment**:
   ```bash
   cd maritime-activity-reports-cdc
   make quick-start
   ```
4. **Create a feature branch**: `git checkout -b feature/amazing-feature`

## 🛠️ Development Workflow

### Before Making Changes

```bash
# Ensure everything is working
make health-check
make test

# Create feature branch
git checkout -b feature/your-feature-name
```

### During Development

```bash
# Run tests frequently
make test-unit

# Check code quality
make lint
make format

# Test with simulated data
make simulate
```

### Before Submitting

```bash
# Run full test suite
make test

# Verify installation
python3 scripts/verify_installation.py

# Check health
make health-check

# Format code
make format
```

## 📋 Code Guidelines

### Python Style
- **Follow PEP 8** style guidelines
- **Use Black** for code formatting (`make format`)
- **Use isort** for import sorting
- **Use type hints** for all functions
- **Maximum line length**: 88 characters

### Documentation
- **Docstrings** for all public functions and classes
- **Comments** for complex logic
- **Update API docs** for new features
- **Include examples** in docstrings

### Testing
- **Write tests** for all new features
- **Unit tests** for individual components
- **Integration tests** for cross-layer functionality
- **Test with simulated data** for maritime-specific logic

## 🏗️ Architecture Guidelines

### Medallion Architecture
- **Bronze Layer**: Raw CDC data, minimal processing
- **Silver Layer**: Cleaned, validated, enriched data
- **Gold Layer**: Business-ready analytics and reports

### CDC/CDF Patterns
- **Enable CDF** on all business-critical tables
- **Use streaming** for real-time processing
- **Implement proper watermarks** for late data
- **Handle schema evolution** gracefully

### Maritime Domain
- **Understand maritime concepts** (IMO, AIS, zones, ports)
- **Use proper maritime terminology**
- **Consider maritime regulations** and compliance
- **Validate maritime data** (coordinates, speeds, vessel types)

## 🧪 Testing Guidelines

### Test Categories

#### Unit Tests (`@pytest.mark.unit`)
```python
def test_ais_data_validation():
    """Test AIS data validation logic."""
    # Test individual component
    pass
```

#### Integration Tests (`@pytest.mark.integration`)
```python
def test_bronze_to_silver_flow():
    """Test data flow from Bronze to Silver layer."""
    # Test cross-component functionality
    pass
```

#### Slow Tests (`@pytest.mark.slow`)
```python
def test_large_dataset_processing():
    """Test processing with large datasets."""
    # Performance and scale tests
    pass
```

### Running Tests

```bash
# All tests
make test

# Specific categories
make test-unit
make test-integration
pytest -m "not slow"  # Skip slow tests
```

### Test Data

- **Use simulated data** for tests
- **Don't commit real maritime data**
- **Use small datasets** for unit tests
- **Use larger datasets** for integration tests

## 📊 Performance Guidelines

### Spark Optimization
- **Use appropriate partitioning** for large tables
- **Implement Z-ordering** for frequently queried columns
- **Cache DataFrames** only when reused multiple times
- **Use broadcast joins** for small lookup tables

### Delta Lake Best Practices
- **Enable auto-optimization** for production tables
- **Vacuum regularly** to manage storage costs
- **Use appropriate retention** policies
- **Monitor table statistics**

### Streaming Performance
- **Choose appropriate trigger intervals**
- **Use watermarks** for event-time processing
- **Handle backpressure** gracefully
- **Monitor streaming metrics**

## 🔍 Code Review Process

### Pull Request Requirements
- [ ] **All tests pass**
- [ ] **Code follows style guidelines**
- [ ] **Documentation updated**
- [ ] **No breaking changes** (or properly documented)
- [ ] **Performance impact** considered
- [ ] **Security implications** reviewed

### Review Checklist
- **Functionality**: Does the code do what it's supposed to do?
- **Performance**: Are there any performance implications?
- **Security**: Are there any security concerns?
- **Maintainability**: Is the code easy to understand and maintain?
- **Testing**: Are there adequate tests?
- **Documentation**: Is the code properly documented?

## 🌊 Maritime Data Considerations

### Data Quality
- **Validate coordinates** (latitude: -90 to 90, longitude: -180 to 180)
- **Check vessel speeds** (reasonable limits for different vessel types)
- **Verify IMO numbers** (7-digit format)
- **Handle missing data** gracefully

### Compliance
- **Respect data privacy** regulations
- **Handle sensitive locations** (military zones, restricted areas)
- **Implement audit trails** for compliance
- **Consider data retention** requirements

### Domain Knowledge
- **AIS Message Types**: Understand different AIS message formats
- **Maritime Zones**: EEZ, territorial waters, high seas
- **Vessel Classifications**: Cargo, tanker, passenger, fishing, etc.
- **Port Operations**: Arrival, departure, anchorage, berth

## 🚀 Deployment Contributions

### Infrastructure Changes
- **Test locally** with Docker Compose
- **Validate Terraform** plans before applying
- **Consider cost implications**
- **Document infrastructure changes**

### Configuration Updates
- **Maintain backward compatibility**
- **Provide migration guides** for breaking changes
- **Update example configurations**
- **Document new settings**

## 📝 Documentation Contributions

### Types of Documentation
- **API Reference**: Technical documentation for developers
- **User Guides**: Step-by-step instructions
- **Architecture Docs**: System design and patterns
- **Deployment Guides**: Environment-specific setup

### Documentation Standards
- **Clear and concise** writing
- **Include code examples**
- **Provide context** for maritime domain
- **Keep up-to-date** with code changes

## 🎯 Issue Guidelines

### Bug Reports
- **Use the bug report template**
- **Include reproduction steps**
- **Provide environment details**
- **Include relevant logs**

### Feature Requests
- **Use the feature request template**
- **Explain the maritime use case**
- **Consider implementation complexity**
- **Discuss alternatives**

## 🏆 Recognition

Contributors will be recognized in:
- **README.md** acknowledgments section
- **Release notes** for significant contributions
- **GitHub contributors** page

## 📞 Getting Help

- **GitHub Discussions**: For questions and ideas
- **GitHub Issues**: For bugs and feature requests
- **Email**: data-team@your-company.com
- **Documentation**: Check `docs/API.md` first

## 🎉 Thank You!

Every contribution, no matter how small, helps make this project better for the maritime data engineering community.

**Happy contributing! ⚓🌊**
