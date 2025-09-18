"""Silver layer CDF processing and streaming transformations."""

import structlog
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import Window
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session, optimize_spark_for_streaming
from ..utils.data_quality import DataQualityValidator

logger = structlog.get_logger(__name__)


class SilverCDFProcessor:
    """Processes Bronze CDF changes and transforms them for Silver layer."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        optimize_spark_for_streaming(self.spark, config)
        
        self.bronze_path = f"{config.gcs.bucket_name}/{config.gcs.bronze_prefix}"
        self.silver_path = f"{config.gcs.bucket_name}/{config.gcs.silver_prefix}"
        self.checkpoint_path = f"{config.gcs.bucket_name}/{config.gcs.checkpoint_prefix}/silver"
        
        self.validator = DataQualityValidator(config)
        
    def process_ais_movements_cdf_stream(self) -> StreamingQuery:
        """Process AIS movements CDF stream from Bronze to Silver."""
        logger.info("Starting AIS movements CDF stream processing")
        
        # Read CDF from Bronze AIS table
        bronze_cdf_stream = (self.spark
                           .readStream
                           .format("delta")
                           .option("readChangeFeed", "true")
                           .option("startingVersion", "latest")
                           .table("bronze.ais_movements"))
        
        def process_ais_microbatch(microBatchDF: DataFrame, batchId: int):
            """Process each microbatch of AIS movement changes."""
            logger.info("Processing AIS microbatch", batch_id=batchId, record_count=microBatchDF.count())
            
            if microBatchDF.count() == 0:
                return
            
            # Filter for relevant change types
            relevant_changes = microBatchDF.filter(
                sf.col("_change_type").isin(["insert", "update_postimage"])
            )
            
            if relevant_changes.count() == 0:
                logger.info("No relevant changes in microbatch", batch_id=batchId)
                return
            
            # Apply Silver layer transformations
            enriched_movements = self._enrich_ais_movements(relevant_changes)
            
            # Apply data quality validation
            validated_movements = self.validator.validate_ais_data(enriched_movements)
            
            # Add Silver layer metadata
            silver_movements = (validated_movements
                              .withColumn("silver_processing_timestamp", sf.current_timestamp())
                              .withColumn("bronze_source_timestamp", sf.col("ingestion_timestamp"))
                              .withColumn("data_lineage", sf.lit("bronze->silver"))
                              .withColumn("_change_type", sf.col("_change_type"))
                              .withColumn("_commit_version", sf.col("_commit_version"))
                              .withColumn("_commit_timestamp", sf.col("_commit_timestamp")))
            
            # Write to Silver table using MERGE
            self._merge_to_silver_movements(silver_movements)
            
            logger.info("AIS microbatch processed successfully", 
                       batch_id=batchId, 
                       processed_count=silver_movements.count())
        
        # Start streaming query
        query = (bronze_cdf_stream
                .writeStream
                .foreachBatch(process_ais_microbatch)
                .option("checkpointLocation", f"{self.checkpoint_path}/ais_movements")
                .trigger(processingTime=f"{self.config.cdc.processing_time_seconds} seconds")
                .start())
        
        logger.info("AIS movements CDF stream started", query_id=query.id)
        return query
    
    def process_vessel_metadata_cdf_stream(self) -> StreamingQuery:
        """Process vessel metadata CDF stream from Bronze to Silver."""
        logger.info("Starting vessel metadata CDF stream processing")
        
        bronze_metadata_cdf = (self.spark
                             .readStream
                             .format("delta")
                             .option("readChangeFeed", "true")
                             .option("startingVersion", "latest")
                             .table("bronze.vessel_metadata"))
        
        def process_metadata_microbatch(microBatchDF: DataFrame, batchId: int):
            """Process each microbatch of vessel metadata changes."""
            logger.info("Processing metadata microbatch", batch_id=batchId, record_count=microBatchDF.count())
            
            if microBatchDF.count() == 0:
                return
            
            # Filter for relevant changes
            relevant_changes = microBatchDF.filter(
                sf.col("_change_type").isin(["insert", "update_postimage"])
            )
            
            if relevant_changes.count() == 0:
                return
            
            # Transform to Silver vessel master format
            vessel_master_updates = self._transform_to_vessel_master(relevant_changes)
            
            # Apply data quality validation
            validated_metadata = self.validator.validate_vessel_metadata(vessel_master_updates)
            
            # Apply SCD Type 2 merge to Silver vessel master
            self._merge_to_vessel_master(validated_metadata)
            
            logger.info("Metadata microbatch processed successfully", batch_id=batchId)
        
        query = (bronze_metadata_cdf
                .writeStream
                .foreachBatch(process_metadata_microbatch)
                .option("checkpointLocation", f"{self.checkpoint_path}/vessel_metadata")
                .trigger(processingTime=f"{self.config.cdc.processing_time_seconds} seconds")
                .start())
        
        logger.info("Vessel metadata CDF stream started", query_id=query.id)
        return query
    
    def _enrich_ais_movements(self, df: DataFrame) -> DataFrame:
        """Enrich AIS movements with additional derived fields."""
        logger.debug("Enriching AIS movements with derived fields")
        
        # Window for calculating movement metrics
        vessel_window = Window.partitionBy("imo").orderBy("movementdatetime")
        
        enriched_df = (df
                      # Previous position for distance calculation
                      .withColumn("prev_latitude", sf.lag("latitude").over(vessel_window))
                      .withColumn("prev_longitude", sf.lag("longitude").over(vessel_window))
                      .withColumn("prev_timestamp", sf.lag("movementdatetime").over(vessel_window))
                      
                      # Next position for zone transition detection
                      .withColumn("next_latitude", sf.lead("latitude").over(vessel_window))
                      .withColumn("next_longitude", sf.lead("longitude").over(vessel_window))
                      
                      # Calculate distance from previous position (simplified Haversine)
                      .withColumn("distance_from_previous_nm", 
                                sf.when(sf.col("prev_latitude").isNotNull(),
                                       self._calculate_distance_nm(
                                           sf.col("prev_latitude"), sf.col("prev_longitude"),
                                           sf.col("latitude"), sf.col("longitude")
                                       )).otherwise(0.0))
                      
                      # Calculate time since previous position
                      .withColumn("time_since_previous_minutes",
                                sf.when(sf.col("prev_timestamp").isNotNull(),
                                       (sf.col("movementdatetime").cast("long") - 
                                        sf.col("prev_timestamp").cast("long")) / 60.0)
                                .otherwise(0.0))
                      
                      # Calculate actual speed from positions
                      .withColumn("calculated_speed_knots",
                                sf.when((sf.col("distance_from_previous_nm") > 0) & 
                                       (sf.col("time_since_previous_minutes") > 0),
                                       sf.col("distance_from_previous_nm") / 
                                       (sf.col("time_since_previous_minutes") / 60.0))
                                .otherwise(0.0))
                      
                      # Movement type classification
                      .withColumn("movement_type",
                                sf.when(sf.coalesce(sf.col("speed_over_ground"), sf.lit(0)) < 0.5, "ANCHORED")
                                .when(sf.coalesce(sf.col("speed_over_ground"), sf.lit(0)) < 2.0, "SLOW")
                                .when(sf.coalesce(sf.col("speed_over_ground"), sf.lit(0)) > 25.0, "FAST")
                                .otherwise("SAILING"))
                      
                      # Zone transition detection (simplified - would use actual spatial joins)
                      .withColumn("is_zone_transition", sf.lit(False))
                      .withColumn("previous_zone", sf.lit(None).cast(StringType()))
                      .withColumn("next_zone", sf.lit(None).cast(StringType()))
                      
                      # Initialize zone fields (will be populated by spatial enrichment)
                      .withColumn("zone_country", sf.lit("UNKNOWN"))
                      .withColumn("zone_country_code", sf.lit("UNK"))
                      .withColumn("zone_eez", sf.lit("UNKNOWN"))
                      .withColumn("zone_continent", sf.lit("UNKNOWN"))
                      .withColumn("zone_subcontinent", sf.lit("UNKNOWN"))
                      .withColumn("zone_hrz_v2", sf.lit("UNKNOWN"))
                      .withColumn("zone_sanction", sf.lit("UNKNOWN"))
                      .withColumn("zone_seca", sf.lit("UNKNOWN"))
                      .withColumn("zone_port", sf.lit("UNKNOWN"))
                      .withColumn("zone_port_id", sf.lit(None).cast(StringType())))
        
        return enriched_df
    
    def _calculate_distance_nm(self, lat1, lon1, lat2, lon2):
        """Calculate distance between two points using Haversine formula."""
        # Simplified distance calculation (would use proper geospatial functions in production)
        return sf.sqrt(
            sf.pow(lat2 - lat1, 2) + sf.pow(lon2 - lon1, 2)
        ) * 60.0  # Rough conversion to nautical miles
    
    def _transform_to_vessel_master(self, df: DataFrame) -> DataFrame:
        """Transform Bronze vessel metadata to Silver vessel master format."""
        logger.debug("Transforming vessel metadata to Silver format")
        
        vessel_master_df = (df
                           # Map Bronze fields to Silver fields
                           .withColumn("current_owner_code", sf.col("owner_code"))
                           .withColumn("current_owner_name", sf.lit("Unknown"))  # Would lookup from company master
                           .withColumn("current_operator_code", sf.col("operator_code"))
                           .withColumn("current_operator_name", sf.lit("Unknown"))
                           .withColumn("current_technical_manager_code", sf.col("technical_manager_code"))
                           .withColumn("current_technical_manager_name", sf.lit("Unknown"))
                           .withColumn("current_ship_manager_code", sf.col("ship_manager_code"))
                           .withColumn("current_ship_manager_name", sf.lit("Unknown"))
                           
                           # Vessel type categorization
                           .withColumn("vessel_type_category",
                                     sf.when(sf.col("vessel_type").like("%Bulk%"), "Bulk")
                                     .when(sf.col("vessel_type").like("%Container%"), "Container")
                                     .when(sf.col("vessel_type").like("%Tanker%"), "Tanker")
                                     .when(sf.col("vessel_type").like("%Cargo%"), "General Cargo")
                                     .otherwise("Other"))
                           
                           # Risk and compliance indicators (simplified)
                           .withColumn("risk_category", sf.lit("MEDIUM"))
                           .withColumn("compliance_score", sf.lit(0.75))
                           .withColumn("sanction_flag", sf.lit(False))
                           
                           # Flag country code (simplified mapping)
                           .withColumn("flag_country_code", 
                                     sf.when(sf.col("flag_country") == "United Kingdom", "GB")
                                     .when(sf.col("flag_country") == "United States", "US")
                                     .when(sf.col("flag_country") == "Germany", "DE")
                                     .otherwise("XX"))
                           
                           # Processing metadata
                           .withColumn("silver_processing_timestamp", sf.current_timestamp())
                           .withColumn("bronze_source_timestamp", sf.col("ingestion_timestamp"))
                           .withColumn("data_lineage", sf.lit("bronze->silver")))
        
        return vessel_master_df
    
    def _merge_to_silver_movements(self, df: DataFrame) -> None:
        """Merge processed movements into Silver cleaned_vessel_movements table."""
        df.createOrReplaceTempView("movement_updates")
        
        merge_sql = """
            MERGE INTO silver.cleaned_vessel_movements AS target
            USING movement_updates AS source
            ON target.imo = source.imo 
               AND target.movementdatetime = source.movementdatetime
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.debug("Successfully merged movements to Silver table")
        except Exception as e:
            logger.error("Failed to merge movements to Silver table", error=str(e))
            raise
    
    def _merge_to_vessel_master(self, df: DataFrame) -> None:
        """Apply SCD Type 2 merge to Silver vessel master table."""
        df.createOrReplaceTempView("vessel_master_updates")
        
        merge_sql = """
            MERGE INTO silver.vessel_master AS target
            USING vessel_master_updates AS source
            ON target.imo = source.imo AND target.is_current_record = true
            
            -- Close existing current record if data changed
            WHEN MATCHED AND (
                target.vessel_name != source.vessel_name OR
                target.vessel_type != source.vessel_type OR
                target.current_owner_code != source.current_owner_code OR
                target.current_operator_code != source.current_operator_code
            ) THEN
                UPDATE SET 
                    is_current_record = false,
                    valid_to_datetime = source._commit_timestamp,
                    silver_processing_timestamp = current_timestamp()
            
            -- Insert new current record
            WHEN NOT MATCHED THEN
                INSERT (
                    imo, vessel_name, vessel_type, vessel_type_category,
                    gross_tonnage, deadweight_tonnage, length_overall, beam,
                    flag_country, flag_country_code,
                    current_owner_code, current_owner_name,
                    current_operator_code, current_operator_name,
                    current_technical_manager_code, current_technical_manager_name,
                    current_ship_manager_code, current_ship_manager_name,
                    risk_category, compliance_score, sanction_flag,
                    valid_from_datetime, valid_to_datetime, is_current_record,
                    silver_processing_timestamp, bronze_source_timestamp, data_lineage,
                    _change_type, _commit_version, _commit_timestamp
                ) VALUES (
                    source.imo, source.vessel_name, source.vessel_type, source.vessel_type_category,
                    source.gross_tonnage, source.deadweight_tonnage, source.length_overall, source.beam,
                    source.flag_country, source.flag_country_code,
                    source.current_owner_code, source.current_owner_name,
                    source.current_operator_code, source.current_operator_name,
                    source.current_technical_manager_code, source.current_technical_manager_name,
                    source.current_ship_manager_code, source.current_ship_manager_name,
                    source.risk_category, source.compliance_score, source.sanction_flag,
                    source._commit_timestamp, null, true,
                    source.silver_processing_timestamp, source.bronze_source_timestamp, source.data_lineage,
                    source._change_type, source._commit_version, source._commit_timestamp
                )
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.debug("Successfully applied SCD Type 2 merge to vessel master")
        except Exception as e:
            logger.error("Failed to merge to vessel master", error=str(e))
            raise
    
    def get_stream_metrics(self, query: StreamingQuery) -> Dict[str, Any]:
        """Get metrics for a streaming query."""
        if not query.isActive:
            return {"status": "inactive", "exception": str(query.exception()) if query.exception() else None}
        
        progress = query.lastProgress
        return {
            "query_id": query.id,
            "status": "active",
            "batch_id": progress.get("batchId", -1),
            "input_rows_per_second": progress.get("inputRowsPerSecond", 0),
            "processed_rows_per_second": progress.get("processedRowsPerSecond", 0),
            "batch_duration_ms": progress.get("batchDuration", 0),
            "num_input_rows": progress.get("numInputRows", 0),
            "timestamp": progress.get("timestamp", "")
        }
