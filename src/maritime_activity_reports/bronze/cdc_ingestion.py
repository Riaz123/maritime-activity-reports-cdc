"""Bronze layer CDC ingestion and processing."""

import structlog
from typing import Optional, Dict, Any, List
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType, LongType

from ..models.config import MaritimeConfig
from ..models.schemas import VesselMovement, VesselMetadata, CDCOperation
from ..utils.spark_utils import get_spark_session
from ..utils.data_quality import DataQualityValidator

logger = structlog.get_logger(__name__)


class BronzeCDCLayer:
    """Handles CDC data ingestion into the Bronze layer."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        self.bronze_path = f"{config.gcs.bucket_name}/{config.gcs.bronze_prefix}"
        self.validator = DataQualityValidator(config)
        
    def ingest_ais_cdc_data(self, cdc_records: DataFrame, source_system: str = "AIS_FEED") -> DataFrame:
        """
        Ingest CDC AIS movement data into Bronze layer.
        
        Args:
            cdc_records: DataFrame containing CDC records
            source_system: Source system identifier
            
        Returns:
            DataFrame: Processed CDC records
        """
        logger.info("Starting AIS CDC data ingestion", 
                   record_count=cdc_records.count(),
                   source_system=source_system)
        
        # Add CDC metadata
        processed_records = (cdc_records
                           .withColumn("source_system", sf.lit(source_system))
                           .withColumn("cdc_timestamp", sf.current_timestamp())
                           .withColumn("ingestion_timestamp", sf.current_timestamp())
                           .withColumn("cdc_sequence_number", sf.monotonically_increasing_id())
                           .withColumn("processing_timestamp", sf.lit(None).cast(TimestampType())))
        
        # Apply basic data quality checks
        processed_records = self._apply_ais_data_quality(processed_records)
        
        # Write to Bronze Delta table
        try:
            (processed_records
             .write
             .format("delta")
             .mode("append")
             .option("mergeSchema", "true")
             .saveAsTable("bronze.ais_movements"))
            
            logger.info("AIS CDC data ingested successfully", 
                       record_count=processed_records.count())
            
            return processed_records
            
        except Exception as e:
            logger.error("Failed to ingest AIS CDC data", error=str(e))
            raise
    
    def ingest_vessel_metadata_cdc(self, cdc_records: DataFrame, source_system: str = "VESSEL_REGISTRY") -> DataFrame:
        """
        Ingest CDC vessel metadata with SCD Type 2 handling.
        
        Args:
            cdc_records: DataFrame containing vessel metadata CDC records
            source_system: Source system identifier
            
        Returns:
            DataFrame: Processed CDC records
        """
        logger.info("Starting vessel metadata CDC ingestion",
                   record_count=cdc_records.count(),
                   source_system=source_system)
        
        # Add CDC metadata
        processed_records = (cdc_records
                           .withColumn("source_system", sf.lit(source_system))
                           .withColumn("cdc_timestamp", sf.current_timestamp())
                           .withColumn("ingestion_timestamp", sf.current_timestamp())
                           .withColumn("cdc_sequence_number", sf.monotonically_increasing_id())
                           .withColumn("processing_timestamp", sf.lit(None).cast(TimestampType())))
        
        # Handle SCD Type 2 logic
        self._handle_vessel_metadata_scd_type2(processed_records)
        
        logger.info("Vessel metadata CDC data ingested successfully")
        return processed_records
    
    def ingest_geospatial_zones(self, zones_data: DataFrame, source_system: str = "GEOSPATIAL_REF") -> DataFrame:
        """
        Ingest geospatial zone reference data.
        
        Args:
            zones_data: DataFrame containing zone definitions
            source_system: Source system identifier
            
        Returns:
            DataFrame: Processed zone data
        """
        logger.info("Starting geospatial zones ingestion",
                   record_count=zones_data.count(),
                   source_system=source_system)
        
        processed_zones = (zones_data
                          .withColumn("source_system", sf.lit(source_system))
                          .withColumn("last_updated", sf.current_timestamp())
                          .withColumn("ingestion_timestamp", sf.current_timestamp())
                          .withColumn("processing_timestamp", sf.lit(None).cast(TimestampType())))
        
        try:
            (processed_zones
             .write
             .format("delta")
             .mode("overwrite")  # Reference data - full refresh
             .option("overwriteSchema", "true")
             .saveAsTable("bronze.geospatial_zones"))
            
            logger.info("Geospatial zones ingested successfully")
            return processed_zones
            
        except Exception as e:
            logger.error("Failed to ingest geospatial zones", error=str(e))
            raise
    
    def _apply_ais_data_quality(self, df: DataFrame) -> DataFrame:
        """Apply data quality checks to AIS data."""
        logger.debug("Applying AIS data quality checks")
        
        # Basic validation
        validated_df = (df
                       .filter(sf.col("imo").isNotNull())
                       .filter(sf.col("latitude").between(self.config.min_latitude, self.config.max_latitude))
                       .filter(sf.col("longitude").between(self.config.min_longitude, self.config.max_longitude))
                       .filter(sf.col("movementdatetime").isNotNull()))
        
        # Add data quality flags
        quality_df = (validated_df
                     .withColumn("is_valid_position", 
                               (sf.col("latitude") != 0) & (sf.col("longitude") != 0))
                     .withColumn("is_reasonable_speed", 
                               sf.coalesce(sf.col("speed_over_ground"), sf.lit(0)) <= self.config.max_speed_knots)
                     .withColumn("data_quality_score",
                               sf.when((sf.col("is_valid_position")) & 
                                      (sf.col("is_reasonable_speed")), 1.0)
                               .otherwise(0.5)))
        
        # Initialize zone columns (will be populated by Silver layer)
        zone_columns = [
            "zone_country", "zone_eez", "zone_continent", 
            "zone_hrz_v2", "zone_sanction", "zone_seca", "zone_port"
        ]
        
        for col in zone_columns:
            quality_df = quality_df.withColumn(col, sf.lit(None).cast(StringType()))
        
        return quality_df
    
    def _handle_vessel_metadata_scd_type2(self, cdc_records: DataFrame) -> None:
        """Handle SCD Type 2 logic for vessel metadata."""
        logger.debug("Applying SCD Type 2 logic for vessel metadata")
        
        # Create temporary view for MERGE operation
        cdc_records.createOrReplaceTempView("vessel_metadata_updates")
        
        merge_sql = """
            MERGE INTO bronze.vessel_metadata AS target
            USING vessel_metadata_updates AS source
            ON target.imo = source.imo AND target.is_current_record = true
            
            -- Handle updates: close current record and insert new one
            WHEN MATCHED AND source.cdc_operation IN ('UPDATE', 'update_postimage') THEN
                UPDATE SET 
                    is_current_record = false,
                    valid_to_datetime = source.cdc_timestamp,
                    processing_timestamp = current_timestamp()
            
            -- Handle deletes: close current record
            WHEN MATCHED AND source.cdc_operation = 'DELETE' THEN
                UPDATE SET 
                    is_current_record = false,
                    valid_to_datetime = source.cdc_timestamp,
                    processing_timestamp = current_timestamp()
            
            -- Insert new records for INSERT and UPDATE operations
            WHEN NOT MATCHED AND source.cdc_operation IN ('INSERT', 'UPDATE', 'update_postimage') THEN
                INSERT (
                    imo, vessel_name, vessel_type, gross_tonnage, deadweight_tonnage,
                    length_overall, beam, flag_country, owner_code, operator_code,
                    technical_manager_code, ship_manager_code, group_beneficial_owner_code,
                    document_of_compliance_doc_company_code, bareboat_charter_company_code,
                    valid_from_datetime, valid_to_datetime, is_current_record,
                    cdc_operation, cdc_timestamp, cdc_sequence_number,
                    ingestion_timestamp, processing_timestamp
                ) VALUES (
                    source.imo, source.vessel_name, source.vessel_type, 
                    source.gross_tonnage, source.deadweight_tonnage,
                    source.length_overall, source.beam, source.flag_country,
                    source.owner_code, source.operator_code, source.technical_manager_code,
                    source.ship_manager_code, source.group_beneficial_owner_code,
                    source.document_of_compliance_doc_company_code, 
                    source.bareboat_charter_company_code,
                    source.cdc_timestamp, null, true,
                    source.cdc_operation, source.cdc_timestamp, source.cdc_sequence_number,
                    source.ingestion_timestamp, current_timestamp()
                )
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.info("SCD Type 2 merge completed for vessel metadata")
        except Exception as e:
            logger.error("Failed to execute SCD Type 2 merge", error=str(e))
            raise
    
    def simulate_cdc_data(self, num_vessels: int = 10, num_records_per_vessel: int = 100) -> DataFrame:
        """
        Generate simulated CDC data for testing purposes.
        
        Args:
            num_vessels: Number of vessels to simulate
            num_records_per_vessel: Number of movement records per vessel
            
        Returns:
            DataFrame: Simulated AIS movement data
        """
        logger.info("Generating simulated CDC data", 
                   vessels=num_vessels, 
                   records_per_vessel=num_records_per_vessel)
        
        # Define schema for simulated data
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
        
        # Generate sample data
        import random
        from datetime import timedelta
        
        sample_data = []
        base_time = datetime.now()
        
        for vessel_idx in range(num_vessels):
            imo = f"912345{vessel_idx:03d}"
            base_lat = 51.5074 + random.uniform(-10, 10)  # Around London
            base_lon = -0.1278 + random.uniform(-10, 10)
            
            for record_idx in range(num_records_per_vessel):
                # Simulate vessel movement
                lat_offset = random.uniform(-0.01, 0.01)
                lon_offset = random.uniform(-0.01, 0.01)
                
                record = (
                    imo,
                    base_time + timedelta(minutes=record_idx * 5),
                    base_lat + lat_offset,
                    base_lon + lon_offset,
                    random.uniform(5, 25),  # speed
                    random.uniform(0, 360),  # course
                    random.uniform(0, 360),  # heading
                    random.choice(["UNDER_WAY", "ANCHORED", "MOORED"]),
                    "INSERT"
                )
                sample_data.append(record)
        
        simulated_df = self.spark.createDataFrame(sample_data, schema)
        logger.info("Simulated CDC data generated", total_records=simulated_df.count())
        
        return simulated_df
    
    def get_ingestion_stats(self, table_name: str, time_window_hours: int = 24) -> Dict[str, Any]:
        """
        Get ingestion statistics for a Bronze table.
        
        Args:
            table_name: Name of the table to analyze
            time_window_hours: Time window for statistics (hours)
            
        Returns:
            Dict: Ingestion statistics
        """
        try:
            cutoff_time = datetime.now() - timedelta(hours=time_window_hours)
            
            stats_df = self.spark.sql(f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT imo) as unique_vessels,
                    MIN(ingestion_timestamp) as earliest_ingestion,
                    MAX(ingestion_timestamp) as latest_ingestion,
                    COUNT(CASE WHEN cdc_operation = 'INSERT' THEN 1 END) as inserts,
                    COUNT(CASE WHEN cdc_operation = 'UPDATE' THEN 1 END) as updates,
                    COUNT(CASE WHEN cdc_operation = 'DELETE' THEN 1 END) as deletes,
                    AVG(data_quality_score) as avg_quality_score
                FROM {table_name}
                WHERE ingestion_timestamp >= '{cutoff_time}'
            """)
            
            stats = stats_df.collect()[0].asDict()
            
            logger.info("Ingestion statistics retrieved", 
                       table=table_name, 
                       time_window_hours=time_window_hours,
                       stats=stats)
            
            return stats
            
        except Exception as e:
            logger.error("Failed to get ingestion statistics", 
                        table=table_name, 
                        error=str(e))
            return {"error": str(e)}
