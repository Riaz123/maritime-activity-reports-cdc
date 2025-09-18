"""Tests for Silver layer CDF processing."""

import pytest
from datetime import datetime, timedelta
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

from ..silver.table_setup import SilverTableSetup
from ..silver.cdf_processor import SilverCDFProcessor
from ..bronze.table_setup import BronzeTableSetup
from ..bronze.cdc_ingestion import BronzeCDCLayer


class TestSilverLayer:
    """Test Silver layer functionality."""
    
    def test_silver_table_setup(self, spark_session, test_config):
        """Test Silver table creation."""
        # Setup Bronze first (dependency)
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        # Setup Silver
        silver_setup = SilverTableSetup(test_config, spark_session)
        silver_setup.create_all_silver_tables()
        
        # Verify Silver tables were created
        tables = spark_session.sql("SHOW TABLES IN silver").collect()
        table_names = [row.tableName for row in tables]
        
        assert "cleaned_vessel_movements" in table_names
        assert "vessel_master" in table_names
        assert "zone_definitions" in table_names
        assert "spatial_enrichment_cache" in table_names
    
    def test_silver_views_creation(self, spark_session, test_config):
        """Test Silver layer views creation."""
        # Setup tables first
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        silver_setup = SilverTableSetup(test_config, spark_session)
        silver_setup.create_all_silver_tables()
        
        # Add some test data to Silver tables
        self._add_test_silver_data(spark_session)
        
        # Create views
        silver_setup.create_silver_views()
        
        # Test views work
        current_positions = spark_session.sql("SELECT * FROM silver.current_vessel_positions").collect()
        active_vessels = spark_session.sql("SELECT * FROM silver.active_vessels").collect()
        
        # Views should execute without error (even if empty)
        assert isinstance(current_positions, list)
        assert isinstance(active_vessels, list)
    
    def test_ais_movement_enrichment(self, spark_session, test_config):
        """Test AIS movement enrichment in Silver layer."""
        # Setup infrastructure
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        silver_setup = SilverTableSetup(test_config, spark_session)
        silver_setup.create_all_silver_tables()
        
        # Create Silver CDF processor
        silver_processor = SilverCDFProcessor(test_config, spark_session)
        
        # Create test movement data
        schema = StructType([
            StructField("imo", StringType(), False),
            StructField("movementdatetime", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("speed_over_ground", DoubleType(), True),
            StructField("course_over_ground", DoubleType(), True),
            StructField("heading", DoubleType(), True),
            StructField("navigational_status", StringType(), True),
            StructField("_change_type", StringType(), False),
            StructField("_commit_version", LongType(), True),
            StructField("_commit_timestamp", TimestampType(), True)
        ])
        
        # Sequential movement data for same vessel
        base_time = datetime(2024, 1, 15, 10, 0)
        test_data = [
            ("9123456", base_time, 51.5074, -0.1278, 12.5, 45.0, 47.0, "UNDER_WAY", "insert", 1, base_time),
            ("9123456", base_time + timedelta(minutes=5), 51.5080, -0.1270, 13.2, 46.0, 48.0, "UNDER_WAY", "insert", 2, base_time + timedelta(minutes=5)),
            ("9123456", base_time + timedelta(minutes=10), 51.5086, -0.1262, 14.1, 47.0, 49.0, "UNDER_WAY", "insert", 3, base_time + timedelta(minutes=10)),
        ]
        
        movements_df = spark_session.createDataFrame(test_data, schema)
        
        # Apply enrichment
        enriched_df = silver_processor._enrich_ais_movements(movements_df)
        
        # Verify enrichment fields were added
        enriched_columns = enriched_df.columns
        assert "distance_from_previous_nm" in enriched_columns
        assert "time_since_previous_minutes" in enriched_columns
        assert "calculated_speed_knots" in enriched_columns
        assert "movement_type" in enriched_columns
        
        # Check calculated values
        enriched_data = enriched_df.orderBy("movementdatetime").collect()
        
        # First record should have 0 distance (no previous)
        assert enriched_data[0].distance_from_previous_nm == 0.0
        
        # Second record should have calculated distance and time
        assert enriched_data[1].distance_from_previous_nm > 0.0
        assert enriched_data[1].time_since_previous_minutes == 5.0
    
    def test_vessel_master_transformation(self, spark_session, test_config):
        """Test vessel metadata transformation to Silver vessel master."""
        silver_processor = SilverCDFProcessor(test_config, spark_session)
        
        # Create test vessel metadata
        schema = StructType([
            StructField("imo", StringType(), False),
            StructField("vessel_name", StringType(), True),
            StructField("vessel_type", StringType(), True),
            StructField("gross_tonnage", DoubleType(), True),
            StructField("owner_code", StringType(), True),
            StructField("operator_code", StringType(), True),
            StructField("flag_country", StringType(), True),
            StructField("_change_type", StringType(), False),
            StructField("_commit_timestamp", TimestampType(), True),
            StructField("ingestion_timestamp", TimestampType(), True)
        ])
        
        test_data = [
            ("9123456", "Test Bulk Carrier", "Bulk Carrier", 50000.0, "OWNER001", "OPERATOR001", "Panama", "insert", datetime.now(), datetime.now()),
            ("9234567", "Test Container Ship", "Container Ship", 75000.0, "OWNER002", "OPERATOR002", "Liberia", "insert", datetime.now(), datetime.now()),
        ]
        
        metadata_df = spark_session.createDataFrame(test_data, schema)
        
        # Transform to vessel master format
        vessel_master_df = silver_processor._transform_to_vessel_master(metadata_df)
        
        # Verify transformation
        vessel_master_data = vessel_master_df.collect()
        
        # Check vessel type categorization
        bulk_carrier = [r for r in vessel_master_data if r.imo == "9123456"][0]
        assert bulk_carrier.vessel_type_category == "Bulk"
        
        container_ship = [r for r in vessel_master_data if r.imo == "9234567"][0]
        assert container_ship.vessel_type_category == "Container"
        
        # Check required fields are present
        assert bulk_carrier.current_owner_code == "OWNER001"
        assert bulk_carrier.flag_country == "Panama"
        assert bulk_carrier.data_lineage == "bronze->silver"
    
    def _add_test_silver_data(self, spark_session):
        """Add test data to Silver tables for view testing."""
        # Add test data to cleaned_vessel_movements
        spark_session.sql("""
            INSERT INTO silver.cleaned_vessel_movements VALUES
            ('9123456', '2024-01-15 10:30:00', 51.5074, -0.1278, 12.5, 45.0, 47.0, 'UNDER_WAY',
             true, true, 1.0, 'EXCELLENT', 'United Kingdom', 'GB', 'UK_EEZ', 'Europe', 'Western Europe',
             null, null, null, null, null, 0.0, 5.0, 12.3, 'SAILING', false, null, null,
             current_timestamp(), current_timestamp(), 'bronze->silver', 'insert', 1, current_timestamp())
        """)
        
        # Add test data to vessel_master
        spark_session.sql("""
            INSERT INTO silver.vessel_master VALUES
            ('9123456', 'Test Vessel', 'Bulk Carrier', 'Bulk', 50000.0, 80000.0, 200.0, 30.0, 'Panama', 'PA',
             'OWNER001', 'Test Owner', 'OPERATOR001', 'Test Operator', null, null, null, null,
             'MEDIUM', 0.75, false, current_timestamp(), null, true,
             current_timestamp(), current_timestamp(), 'bronze->silver', 'insert', 1, current_timestamp())
        """)
    
    @pytest.mark.integration
    def test_silver_table_metrics(self, spark_session, test_config):
        """Test Silver table metrics collection."""
        # Setup tables
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        silver_setup = SilverTableSetup(test_config, spark_session)
        silver_setup.create_all_silver_tables()
        
        # Get metrics
        metrics = silver_setup.get_silver_table_metrics()
        
        # Verify metrics structure
        assert isinstance(metrics, dict)
        assert "silver.cleaned_vessel_movements" in metrics
        assert "silver.vessel_master" in metrics
        assert "silver.zone_definitions" in metrics
        
        # Check metric fields
        for table, table_metrics in metrics.items():
            if "error" not in table_metrics:
                assert "record_count" in table_metrics
                assert "size_in_bytes" in table_metrics
                assert "num_files" in table_metrics
