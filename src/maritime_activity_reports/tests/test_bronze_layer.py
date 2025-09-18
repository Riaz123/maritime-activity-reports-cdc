"""Tests for Bronze layer CDC ingestion."""

import pytest
from datetime import datetime
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

from ..bronze.cdc_ingestion import BronzeCDCLayer
from ..bronze.table_setup import BronzeTableSetup
from ..models.schemas import CDCOperation


class TestBronzeCDCLayer:
    """Test Bronze layer CDC functionality."""
    
    def test_bronze_table_setup(self, spark_session, test_config):
        """Test Bronze table creation."""
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        
        # Create databases and tables
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        # Verify tables were created
        tables = spark_session.sql("SHOW TABLES IN bronze").collect()
        table_names = [row.tableName for row in tables]
        
        assert "ais_movements" in table_names
        assert "vessel_metadata" in table_names
        assert "geospatial_zones" in table_names
    
    def test_ais_cdc_ingestion(self, spark_session, test_config):
        """Test AIS CDC data ingestion."""
        bronze_cdc = BronzeCDCLayer(test_config, spark_session)
        
        # Setup tables first
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        # Create sample CDC data
        schema = StructType([
            StructField("imo", StringType(), False),
            StructField("movementdatetime", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("speed_over_ground", DoubleType(), True),
            StructField("course_over_ground", DoubleType(), True),
            StructField("heading", DoubleType(), True),
            StructField("navigational_status", StringType(), True),
            StructField("cdc_operation", StringType(), False)
        ])
        
        sample_data = [
            ("9123456", datetime(2024, 1, 15, 10, 30), 51.5074, -0.1278, 12.5, 45.0, 47.0, "UNDER_WAY", "INSERT"),
            ("9123456", datetime(2024, 1, 15, 10, 35), 51.5080, -0.1270, 13.2, 46.0, 48.0, "UNDER_WAY", "INSERT"),
            ("9234567", datetime(2024, 1, 15, 10, 30), 40.7128, -74.0060, 8.3, 90.0, 92.0, "ANCHORED", "INSERT")
        ]
        
        cdc_df = spark_session.createDataFrame(sample_data, schema)
        
        # Ingest CDC data
        result_df = bronze_cdc.ingest_ais_cdc_data(cdc_df, "TEST_SYSTEM")
        
        # Verify ingestion
        assert result_df.count() == 3
        
        # Check data quality fields were added
        columns = result_df.columns
        assert "is_valid_position" in columns
        assert "is_reasonable_speed" in columns
        assert "data_quality_score" in columns
        
        # Verify data was written to table
        table_count = spark_session.sql("SELECT COUNT(*) FROM bronze.ais_movements").collect()[0][0]
        assert table_count == 3
    
    def test_vessel_metadata_scd_type2(self, spark_session, test_config):
        """Test SCD Type 2 handling for vessel metadata."""
        bronze_cdc = BronzeCDCLayer(test_config, spark_session)
        
        # Setup tables
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        # Create initial vessel metadata
        initial_data = [
            ("9123456", "Test Vessel", "Bulk Carrier", 50000.0, 80000.0, 200.0, 30.0, "Panama", 
             "OWNER001", "OPERATOR001", None, None, None, None, None,
             datetime(2024, 1, 1), None, True, "INSERT", datetime.now(), 1, datetime.now(), None)
        ]
        
        schema = StructType([
            StructField("imo", StringType(), False),
            StructField("vessel_name", StringType(), True),
            StructField("vessel_type", StringType(), True),
            StructField("gross_tonnage", DoubleType(), True),
            StructField("deadweight_tonnage", DoubleType(), True),
            StructField("length_overall", DoubleType(), True),
            StructField("beam", DoubleType(), True),
            StructField("flag_country", StringType(), True),
            StructField("owner_code", StringType(), True),
            StructField("operator_code", StringType(), True),
            StructField("technical_manager_code", StringType(), True),
            StructField("ship_manager_code", StringType(), True),
            StructField("group_beneficial_owner_code", StringType(), True),
            StructField("document_of_compliance_doc_company_code", StringType(), True),
            StructField("bareboat_charter_company_code", StringType(), True),
            StructField("valid_from_datetime", TimestampType(), False),
            StructField("valid_to_datetime", TimestampType(), True),
            StructField("is_current_record", BooleanType(), True),
            StructField("cdc_operation", StringType(), False),
            StructField("cdc_timestamp", TimestampType(), True),
            StructField("cdc_sequence_number", LongType(), True),
            StructField("ingestion_timestamp", TimestampType(), True),
            StructField("processing_timestamp", TimestampType(), True)
        ])
        
        initial_df = spark_session.createDataFrame(initial_data, schema)
        bronze_cdc.ingest_vessel_metadata_cdc(initial_df, "TEST_SYSTEM")
        
        # Verify initial record
        current_records = spark_session.sql("""
            SELECT * FROM bronze.vessel_metadata 
            WHERE imo = '9123456' AND is_current_record = true
        """).collect()
        
        assert len(current_records) == 1
        assert current_records[0].vessel_name == "Test Vessel"
        
        # Create update data (name change)
        update_data = [
            ("9123456", "Updated Vessel Name", "Bulk Carrier", 50000.0, 80000.0, 200.0, 30.0, "Panama",
             "OWNER001", "OPERATOR001", None, None, None, None, None,
             datetime(2024, 1, 15), None, True, "UPDATE", datetime.now(), 2, datetime.now(), None)
        ]
        
        update_df = spark_session.createDataFrame(update_data, schema)
        bronze_cdc.ingest_vessel_metadata_cdc(update_df, "TEST_SYSTEM")
        
        # Verify SCD Type 2 logic
        all_records = spark_session.sql("""
            SELECT * FROM bronze.vessel_metadata 
            WHERE imo = '9123456'
            ORDER BY valid_from_datetime
        """).collect()
        
        assert len(all_records) == 2
        
        # Check old record is closed
        old_record = all_records[0]
        assert old_record.is_current_record == False
        assert old_record.valid_to_datetime is not None
        
        # Check new record is current
        new_record = all_records[1]
        assert new_record.is_current_record == True
        assert new_record.vessel_name == "Updated Vessel Name"
        assert new_record.valid_to_datetime is None
    
    def test_data_quality_validation(self, spark_session, test_config):
        """Test data quality validation in Bronze layer."""
        bronze_cdc = BronzeCDCLayer(test_config, spark_session)
        
        # Setup tables
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        # Create data with quality issues
        schema = StructType([
            StructField("imo", StringType(), False),
            StructField("movementdatetime", TimestampType(), False),
            StructField("latitude", DoubleType(), False),
            StructField("longitude", DoubleType(), False),
            StructField("speed_over_ground", DoubleType(), True),
            StructField("cdc_operation", StringType(), False)
        ])
        
        # Mix of good and bad data
        test_data = [
            ("9123456", datetime(2024, 1, 15, 10, 30), 51.5074, -0.1278, 12.5, "INSERT"),  # Good
            ("9123457", datetime(2024, 1, 15, 10, 30), 0.0, 0.0, 12.5, "INSERT"),        # Bad position
            ("9123458", datetime(2024, 1, 15, 10, 30), 51.5074, -0.1278, 150.0, "INSERT"), # Bad speed
            ("", datetime(2024, 1, 15, 10, 30), 51.5074, -0.1278, 12.5, "INSERT"),       # Missing IMO
        ]
        
        test_df = spark_session.createDataFrame(test_data, schema)
        
        # Ingest data
        result_df = bronze_cdc.ingest_ais_cdc_data(test_df, "TEST_SYSTEM")
        
        # Check quality scores
        quality_scores = result_df.select("imo", "data_quality_score").collect()
        
        # Good record should have high score
        good_record = [r for r in quality_scores if r.imo == "9123456"][0]
        assert good_record.data_quality_score >= 0.8
        
        # Bad position should have lower score
        bad_position = [r for r in quality_scores if r.imo == "9123457"][0]
        assert bad_position.data_quality_score < 0.8
        
        # Bad speed should have lower score
        bad_speed = [r for r in quality_scores if r.imo == "9123458"][0]
        assert bad_speed.data_quality_score < 0.8
    
    @pytest.mark.integration
    def test_bronze_table_optimization(self, spark_session, test_config):
        """Test Bronze table optimization."""
        bronze_setup = BronzeTableSetup(test_config, spark_session)
        
        # Setup and populate tables
        bronze_setup.create_databases()
        bronze_setup.create_all_bronze_tables()
        
        # Add some test data
        bronze_cdc = BronzeCDCLayer(test_config, spark_session)
        test_data = bronze_cdc.simulate_cdc_data(num_vessels=5, num_records_per_vessel=10)
        bronze_cdc.ingest_ais_cdc_data(test_data, "TEST_SYSTEM")
        
        # Optimize tables
        bronze_setup.optimize_tables()
        
        # Verify optimization completed without errors
        table_info = bronze_setup.get_table_info("bronze.ais_movements")
        assert "error" not in table_info
        assert table_info["table_name"] == "bronze.ais_movements"
