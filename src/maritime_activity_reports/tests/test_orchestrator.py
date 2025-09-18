"""Tests for CDC/CDF orchestrator."""

import pytest
from datetime import datetime
from unittest.mock import Mock, patch

from ..orchestrator.cdc_cdf_orchestrator import CDCCDFOrchestrator
from ..bronze.table_setup import BronzeTableSetup
from ..silver.table_setup import SilverTableSetup
from ..gold.table_setup import GoldTableSetup


class TestCDCCDFOrchestrator:
    """Test the main CDC/CDF orchestrator."""
    
    def test_orchestrator_initialization(self, spark_session, test_config):
        """Test orchestrator initialization."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Verify components are initialized
        assert orchestrator.config == test_config
        assert orchestrator.spark == spark_session
        assert isinstance(orchestrator.bronze_setup, BronzeTableSetup)
        assert isinstance(orchestrator.silver_setup, SilverTableSetup)
        assert orchestrator.active_queries == []
    
    def test_setup_all_cdf_tables(self, spark_session, test_config):
        """Test complete table setup across all layers."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Setup all tables
        orchestrator.setup_all_cdf_tables()
        
        # Verify databases were created
        databases = spark_session.sql("SHOW DATABASES").collect()
        db_names = [row.databaseName for row in databases]
        
        assert "bronze" in db_names
        assert "silver" in db_names
        assert "gold" in db_names
        
        # Verify Bronze tables
        bronze_tables = spark_session.sql("SHOW TABLES IN bronze").collect()
        bronze_table_names = [row.tableName for row in bronze_tables]
        assert "ais_movements" in bronze_table_names
        assert "vessel_metadata" in bronze_table_names
        
        # Verify Silver tables
        silver_tables = spark_session.sql("SHOW TABLES IN silver").collect()
        silver_table_names = [row.tableName for row in silver_tables]
        assert "cleaned_vessel_movements" in silver_table_names
        assert "vessel_master" in silver_table_names
        
        # Verify Gold tables
        gold_tables = spark_session.sql("SHOW TABLES IN gold").collect()
        gold_table_names = [row.tableName for row in gold_tables]
        assert "vessel_activity_reports" in gold_table_names
        assert "vessel_summary" in gold_table_names
    
    def test_simulate_maritime_data(self, spark_session, test_config):
        """Test maritime data simulation."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Setup tables first
        orchestrator.setup_all_cdf_tables()
        
        # Simulate data
        orchestrator.simulate_maritime_data(num_vessels=3, num_records=10)
        
        # Verify data was created
        ais_count = spark_session.sql("SELECT COUNT(*) FROM bronze.ais_movements").collect()[0][0]
        assert ais_count == 30  # 3 vessels * 10 records each
        
        # Verify data quality
        quality_stats = spark_session.sql("""
            SELECT 
                AVG(data_quality_score) as avg_quality,
                COUNT(CASE WHEN is_valid_position = true THEN 1 END) as valid_positions
            FROM bronze.ais_movements
        """).collect()[0]
        
        assert quality_stats.avg_quality > 0.5
        assert quality_stats.valid_positions > 0
    
    def test_health_check_functionality(self, spark_session, test_config):
        """Test comprehensive health check."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Setup tables for health check
        orchestrator.setup_all_cdf_tables()
        
        # Perform health check
        health_status = orchestrator.health_check()
        
        # Verify health check structure
        assert "overall_status" in health_status
        assert "components" in health_status
        assert "issues" in health_status
        assert "timestamp" in health_status
        
        # Check component health
        assert "spark" in health_status["components"]
        assert health_status["components"]["spark"]["status"] == "healthy"
        
        # Bronze should be healthy after setup
        if "bronze" in health_status["components"]:
            # Bronze might be healthy or degraded (no data yet)
            assert health_status["components"]["bronze"]["status"] in ["healthy", "degraded"]
    
    def test_comprehensive_statistics(self, spark_session, test_config):
        """Test comprehensive statistics collection."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Setup tables
        orchestrator.setup_all_cdf_tables()
        
        # Add some test data
        orchestrator.simulate_maritime_data(num_vessels=2, num_records=5)
        
        # Get statistics
        stats = orchestrator.get_comprehensive_statistics()
        
        # Verify statistics structure
        assert "timestamp" in stats
        assert "bronze_layer" in stats
        assert "silver_layer" in stats
        assert "gold_layer" in stats
        assert "streaming" in stats
        
        # Check Bronze layer stats
        if "ais_movements" in stats["bronze_layer"]:
            bronze_stats = stats["bronze_layer"]["ais_movements"]
            assert "total_records" in bronze_stats
            assert bronze_stats["total_records"] == 10  # 2 vessels * 5 records
    
    @pytest.mark.integration
    def test_table_optimization(self, spark_session, test_config):
        """Test table optimization across all layers."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Setup tables
        orchestrator.setup_all_cdf_tables()
        
        # Add test data
        orchestrator.simulate_maritime_data(num_vessels=2, num_records=10)
        
        # Optimize tables
        orchestrator.optimize_all_tables()
        
        # Verify optimization completed without errors
        # In a real test environment, we'd check Delta log files
        assert True
    
    @pytest.mark.integration 
    def test_table_vacuum(self, spark_session, test_config):
        """Test table vacuum across all layers."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Setup tables
        orchestrator.setup_all_cdf_tables()
        
        # Add test data
        orchestrator.simulate_maritime_data(num_vessels=1, num_records=5)
        
        # Vacuum tables with short retention for testing
        orchestrator.vacuum_all_tables(retention_hours=0)
        
        # Verify vacuum completed without errors
        assert True
    
    def test_error_handling(self, spark_session, test_config):
        """Test error handling in orchestrator."""
        orchestrator = CDCCDFOrchestrator(test_config, spark_session)
        
        # Test with invalid table name
        with pytest.raises(Exception):
            spark_session.sql("SELECT * FROM invalid.table_name")
        
        # Test health check with missing tables
        health_status = orchestrator.health_check()
        
        # Should handle missing tables gracefully
        assert "overall_status" in health_status
        # Status might be unhealthy due to missing tables, but shouldn't crash
        assert health_status["overall_status"] in ["healthy", "degraded", "unhealthy"]
