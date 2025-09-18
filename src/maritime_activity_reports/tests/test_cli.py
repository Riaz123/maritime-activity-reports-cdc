"""Tests for CLI functionality."""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
from typer.testing import CliRunner

from ..cli import app


class TestCLI:
    """Test command line interface."""
    
    def setup_method(self):
        """Setup test runner."""
        self.runner = CliRunner()
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    @patch('maritime_activity_reports.bronze.table_setup.BronzeTableSetup')
    def test_setup_tables_command(self, mock_bronze_setup, mock_config):
        """Test setup-tables CLI command."""
        # Mock configuration
        mock_config.return_value = Mock()
        
        # Mock table setup
        mock_bronze_instance = Mock()
        mock_bronze_setup.return_value = mock_bronze_instance
        
        # Run command
        result = self.runner.invoke(app, ["setup-tables", "--layer", "bronze"])
        
        # Verify command executed
        assert result.exit_code == 0
        mock_config.assert_called_once()
        mock_bronze_setup.assert_called_once()
        mock_bronze_instance.create_databases.assert_called_once()
        mock_bronze_instance.create_all_bronze_tables.assert_called_once()
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    @patch('maritime_activity_reports.orchestrator.cdc_cdf_orchestrator.CDCCDFOrchestrator')
    def test_start_streaming_command(self, mock_orchestrator, mock_config):
        """Test start-streaming CLI command."""
        # Mock configuration
        mock_config.return_value = Mock()
        
        # Mock orchestrator
        mock_orch_instance = Mock()
        mock_orchestrator.return_value = mock_orch_instance
        mock_orch_instance.start_all_streaming_queries.return_value = [Mock(), Mock()]
        
        # Mock monitoring to avoid infinite loop
        mock_orch_instance.monitor_streaming_queries.side_effect = KeyboardInterrupt()
        
        # Run command
        result = self.runner.invoke(app, ["start-streaming"])
        
        # Verify command executed (will exit with 1 due to KeyboardInterrupt, which is expected)
        mock_config.assert_called_once()
        mock_orchestrator.assert_called_once()
        mock_orch_instance.start_all_streaming_queries.assert_called_once()
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    @patch('maritime_activity_reports.orchestrator.cdc_cdf_orchestrator.CDCCDFOrchestrator')
    def test_ingest_cdc_command(self, mock_orchestrator, mock_config):
        """Test ingest-cdc CLI command."""
        # Mock configuration
        mock_config.return_value = Mock()
        
        # Mock orchestrator
        mock_orch_instance = Mock()
        mock_orchestrator.return_value = mock_orch_instance
        
        # Create a temporary test file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"imo": "9123456", "latitude": 51.5074}')
            temp_file = f.name
        
        try:
            # Run command
            result = self.runner.invoke(app, [
                "ingest-cdc", 
                "ais", 
                "--file", temp_file
            ])
            
            # Verify command executed
            assert result.exit_code == 0
            mock_config.assert_called_once()
            mock_orchestrator.assert_called_once()
            mock_orch_instance.ingest_ais_cdc_from_file.assert_called_once()
            
        finally:
            # Clean up temp file
            Path(temp_file).unlink()
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    @patch('maritime_activity_reports.orchestrator.cdc_cdf_orchestrator.CDCCDFOrchestrator')
    def test_health_check_command(self, mock_orchestrator, mock_config):
        """Test health-check CLI command."""
        # Mock configuration
        mock_config.return_value = Mock()
        
        # Mock orchestrator with healthy status
        mock_orch_instance = Mock()
        mock_orchestrator.return_value = mock_orch_instance
        mock_orch_instance.health_check.return_value = {
            "overall_status": "healthy",
            "components": {},
            "issues": []
        }
        
        # Run command
        result = self.runner.invoke(app, ["health-check"])
        
        # Verify command executed successfully
        assert result.exit_code == 0
        mock_config.assert_called_once()
        mock_orchestrator.assert_called_once()
        mock_orch_instance.health_check.assert_called_once_with("all")
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    @patch('maritime_activity_reports.orchestrator.cdc_cdf_orchestrator.CDCCDFOrchestrator')
    def test_health_check_unhealthy(self, mock_orchestrator, mock_config):
        """Test health-check CLI command with unhealthy status."""
        # Mock configuration
        mock_config.return_value = Mock()
        
        # Mock orchestrator with unhealthy status
        mock_orch_instance = Mock()
        mock_orchestrator.return_value = mock_orch_instance
        mock_orch_instance.health_check.return_value = {
            "overall_status": "unhealthy",
            "components": {"streaming": {"status": "unhealthy"}},
            "issues": ["No active streaming queries"]
        }
        
        # Run command
        result = self.runner.invoke(app, ["health-check"])
        
        # Should exit with code 1 for unhealthy status
        assert result.exit_code == 1
        mock_orch_instance.health_check.assert_called_once()
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    @patch('maritime_activity_reports.orchestrator.cdc_cdf_orchestrator.CDCCDFOrchestrator')
    def test_simulate_data_command(self, mock_orchestrator, mock_config):
        """Test simulate-data CLI command."""
        # Mock configuration
        mock_config.return_value = Mock()
        
        # Mock orchestrator
        mock_orch_instance = Mock()
        mock_orchestrator.return_value = mock_orch_instance
        
        # Run command
        result = self.runner.invoke(app, [
            "simulate-data", 
            "--vessels", "5",
            "--records", "20"
        ])
        
        # Verify command executed
        assert result.exit_code == 0
        mock_config.assert_called_once()
        mock_orchestrator.assert_called_once()
        mock_orch_instance.simulate_maritime_data.assert_called_once_with(5, 20)
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    @patch('maritime_activity_reports.bronze.table_setup.BronzeTableSetup')
    def test_optimize_tables_command(self, mock_bronze_setup, mock_config):
        """Test optimize-tables CLI command."""
        # Mock configuration
        mock_config.return_value = Mock()
        
        # Mock table setup
        mock_bronze_instance = Mock()
        mock_bronze_setup.return_value = mock_bronze_instance
        
        # Run command
        result = self.runner.invoke(app, ["optimize-tables", "--layer", "bronze"])
        
        # Verify command executed
        assert result.exit_code == 0
        mock_config.assert_called_once()
        mock_bronze_setup.assert_called_once()
        mock_bronze_instance.optimize_tables.assert_called_once()
    
    def test_cli_help(self):
        """Test CLI help functionality."""
        # Test main help
        result = self.runner.invoke(app, ["--help"])
        assert result.exit_code == 0
        assert "Maritime Activity Reports" in result.output
        
        # Test command help
        result = self.runner.invoke(app, ["setup-tables", "--help"])
        assert result.exit_code == 0
        assert "Setup Delta tables" in result.output
    
    def test_invalid_command(self):
        """Test invalid CLI command handling."""
        result = self.runner.invoke(app, ["invalid-command"])
        assert result.exit_code != 0
    
    @patch('maritime_activity_reports.models.config.MaritimeConfig.from_file')
    def test_config_file_error_handling(self, mock_config):
        """Test CLI error handling when config file is missing."""
        # Mock config file error
        mock_config.side_effect = FileNotFoundError("Config file not found")
        
        # Run command
        result = self.runner.invoke(app, ["health-check"])
        
        # Should exit with error code
        assert result.exit_code == 1
        mock_config.assert_called_once()
    
    def test_cli_with_custom_config_path(self):
        """Test CLI with custom configuration path."""
        # Create a mock config file
        import tempfile
        import yaml
        
        test_config = {
            "project_name": "test-maritime",
            "environment": "test",
            "spark": {"app_name": "test-app"},
            "bronze": {"base_path": "/tmp/bronze", "checkpoint_path": "/tmp/checkpoints/bronze", "retention_days": 1},
            "silver": {"base_path": "/tmp/silver", "checkpoint_path": "/tmp/checkpoints/silver", "retention_days": 1},
            "gold": {"base_path": "/tmp/gold", "checkpoint_path": "/tmp/checkpoints/gold", "retention_days": 1},
            "cdc": {"enable_cdf": True, "processing_time_seconds": 1, "checkpoint_location": "/tmp/cdc"},
            "bigquery": {"project_id": "test-project", "dataset": "test"},
            "gcs": {"bucket_name": "test-bucket"}
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(test_config, f)
            config_file = f.name
        
        try:
            with patch('maritime_activity_reports.orchestrator.cdc_cdf_orchestrator.CDCCDFOrchestrator') as mock_orch:
                mock_orch_instance = Mock()
                mock_orch.return_value = mock_orch_instance
                mock_orch_instance.health_check.return_value = {"overall_status": "healthy", "components": {}, "issues": []}
                
                # Run with custom config
                result = self.runner.invoke(app, [
                    "health-check",
                    "--config", config_file
                ])
                
                assert result.exit_code == 0
                mock_orch.assert_called_once()
                
        finally:
            # Clean up
            Path(config_file).unlink()
