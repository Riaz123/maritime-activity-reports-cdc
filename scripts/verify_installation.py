#!/usr/bin/env python3
"""
Installation verification script for Maritime Activity Reports CDC/CDF.
Run this after setup to verify everything is working correctly.
"""

import sys
import os
import subprocess
from pathlib import Path

# Colors for output
class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    BOLD = '\033[1m'
    END = '\033[0m'

def print_status(message, status="info"):
    """Print colored status message."""
    if status == "success":
        print(f"{Colors.GREEN}✅ {message}{Colors.END}")
    elif status == "error":
        print(f"{Colors.RED}❌ {message}{Colors.END}")
    elif status == "warning":
        print(f"{Colors.YELLOW}⚠️  {message}{Colors.END}")
    elif status == "info":
        print(f"{Colors.BLUE}ℹ️  {message}{Colors.END}")
    else:
        print(f"📋 {message}")

def check_python_version():
    """Check Python version."""
    print_status("Checking Python version...")
    
    version = sys.version_info
    if version.major == 3 and version.minor >= 9:
        print_status(f"Python {version.major}.{version.minor}.{version.micro}", "success")
        return True
    else:
        print_status(f"Python {version.major}.{version.minor}.{version.micro} - Requires 3.9+", "error")
        return False

def check_java():
    """Check Java installation."""
    print_status("Checking Java installation...")
    
    try:
        result = subprocess.run(['java', '-version'], capture_output=True, text=True)
        if result.returncode == 0:
            # Java version is in stderr
            version_line = result.stderr.split('\n')[0]
            print_status(f"Java found: {version_line}", "success")
            return True
        else:
            print_status("Java not found", "error")
            return False
    except FileNotFoundError:
        print_status("Java not found", "error")
        return False

def check_pdm():
    """Check PDM installation."""
    print_status("Checking PDM installation...")
    
    try:
        result = subprocess.run(['pdm', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print_status(f"PDM found: {result.stdout.strip()}", "success")
            return True
        else:
            print_status("PDM not found", "error")
            return False
    except FileNotFoundError:
        print_status("PDM not found", "error")
        return False

def check_dependencies():
    """Check if dependencies are installed."""
    print_status("Checking Python dependencies...")
    
    required_packages = [
        'pyspark',
        'delta-spark', 
        'structlog',
        'pydantic',
        'typer'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
            print_status(f"✓ {package}", "success")
        except ImportError:
            print_status(f"✗ {package}", "error")
            missing_packages.append(package)
    
    if missing_packages:
        print_status(f"Missing packages: {', '.join(missing_packages)}", "error")
        print_status("Run 'pdm install' to install dependencies", "info")
        return False
    
    return True

def check_configuration():
    """Check configuration file."""
    print_status("Checking configuration...")
    
    config_path = Path("config/config.yaml")
    
    if not config_path.exists():
        print_status("Configuration file not found", "error")
        print_status("Run 'cp config/config.example.yaml config/config.yaml'", "info")
        return False
    
    try:
        # Try to load configuration
        sys.path.insert(0, 'src')
        from maritime_activity_reports.models.config import MaritimeConfig
        
        config = MaritimeConfig.from_file(config_path)
        print_status(f"Configuration loaded: {config.project_name}", "success")
        return True
        
    except Exception as e:
        print_status(f"Configuration loading failed: {e}", "error")
        return False

def check_spark_session():
    """Check if Spark session can be created."""
    print_status("Testing Spark session creation...")
    
    try:
        sys.path.insert(0, 'src')
        from maritime_activity_reports.utils.spark_utils import get_spark_session
        from maritime_activity_reports.models.config import MaritimeConfig
        
        config = MaritimeConfig.from_file("config/config.yaml")
        spark = get_spark_session(config)
        
        # Test basic Spark operation
        test_df = spark.sql("SELECT 1 as test_column")
        result = test_df.collect()[0].test_column
        
        if result == 1:
            print_status(f"Spark session working: {spark.version}", "success")
            spark.stop()
            return True
        else:
            print_status("Spark session test failed", "error")
            spark.stop()
            return False
            
    except Exception as e:
        print_status(f"Spark session creation failed: {e}", "error")
        return False

def check_cli():
    """Check CLI functionality."""
    print_status("Testing CLI functionality...")
    
    try:
        result = subprocess.run(['pdm', 'run', 'maritime-reports', '--help'], 
                              capture_output=True, text=True)
        if result.returncode == 0:
            print_status("CLI working correctly", "success")
            return True
        else:
            print_status(f"CLI test failed: {result.stderr}", "error")
            return False
    except Exception as e:
        print_status(f"CLI test failed: {e}", "error")
        return False

def check_project_structure():
    """Check project structure."""
    print_status("Checking project structure...")
    
    required_dirs = [
        "src/maritime_activity_reports/bronze",
        "src/maritime_activity_reports/silver", 
        "src/maritime_activity_reports/gold",
        "src/maritime_activity_reports/orchestrator",
        "src/maritime_activity_reports/utils",
        "config",
        "scripts",
        "docs"
    ]
    
    missing_dirs = []
    for dir_path in required_dirs:
        if not Path(dir_path).exists():
            missing_dirs.append(dir_path)
            print_status(f"✗ Missing: {dir_path}", "error")
        else:
            print_status(f"✓ Found: {dir_path}", "success")
    
    if missing_dirs:
        print_status(f"Missing directories: {', '.join(missing_dirs)}", "error")
        return False
    
    return True

def main():
    """Run all verification checks."""
    print(f"{Colors.BOLD}{Colors.BLUE}")
    print("🚢 Maritime Activity Reports CDC/CDF - Installation Verification")
    print("================================================================")
    print(f"{Colors.END}")
    
    checks = [
        ("Python Version", check_python_version),
        ("Java Installation", check_java),
        ("PDM Installation", check_pdm),
        ("Project Structure", check_project_structure),
        ("Configuration", check_configuration),
        ("Dependencies", check_dependencies),
        ("Spark Session", check_spark_session),
        ("CLI Interface", check_cli),
    ]
    
    passed = 0
    total = len(checks)
    
    for check_name, check_func in checks:
        print(f"\n{Colors.BOLD}🔍 {check_name}{Colors.END}")
        print("-" * (len(check_name) + 4))
        
        try:
            if check_func():
                passed += 1
            else:
                print_status(f"{check_name} check failed", "error")
        except Exception as e:
            print_status(f"{check_name} check error: {e}", "error")
    
    # Summary
    print(f"\n{Colors.BOLD}📊 Verification Summary{Colors.END}")
    print("=" * 25)
    
    if passed == total:
        print_status(f"All {total} checks passed! System is ready to use.", "success")
        print(f"\n{Colors.GREEN}{Colors.BOLD}🎉 Installation verified successfully!{Colors.END}")
        print(f"\n{Colors.BLUE}Next steps:{Colors.END}")
        print("1. Run 'make setup-tables' to create Delta tables")
        print("2. Run 'make simulate' to generate test data")
        print("3. Run 'make health-check' to verify system health")
        print("4. Explore 'docs/API.md' for detailed documentation")
        return 0
    else:
        print_status(f"{passed}/{total} checks passed. Please fix the issues above.", "error")
        print(f"\n{Colors.RED}{Colors.BOLD}❌ Installation verification failed!{Colors.END}")
        print(f"\n{Colors.YELLOW}Common solutions:{Colors.END}")
        print("• Install missing dependencies: pdm install")
        print("• Create configuration: cp config/config.example.yaml config/config.yaml")
        print("• Install Java: brew install openjdk@11 (macOS)")
        print("• Check JAVA_HOME: export JAVA_HOME=/path/to/java")
        return 1

if __name__ == "__main__":
    sys.exit(main())
