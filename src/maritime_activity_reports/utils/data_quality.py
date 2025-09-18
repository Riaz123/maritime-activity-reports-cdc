"""Data quality validation utilities."""

import structlog
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

from ..models.config import MaritimeConfig

logger = structlog.get_logger(__name__)


class DataQualityValidator:
    """Data quality validation and scoring for maritime data."""
    
    def __init__(self, config: MaritimeConfig):
        self.config = config
        
    def validate_ais_data(self, df: DataFrame) -> DataFrame:
        """
        Validate AIS movement data and add quality scores.
        
        Args:
            df: DataFrame containing AIS data
            
        Returns:
            DataFrame: Data with quality flags and scores
        """
        logger.info("Starting AIS data quality validation", 
                   record_count=df.count())
        
        # Basic field validation
        validated_df = (df
                       .withColumn("has_required_fields", 
                                 sf.col("imo").isNotNull() & 
                                 sf.col("movementdatetime").isNotNull() &
                                 sf.col("latitude").isNotNull() &
                                 sf.col("longitude").isNotNull())
                       
                       # Position validation
                       .withColumn("is_valid_latitude", 
                                 sf.col("latitude").between(self.config.min_latitude, 
                                                           self.config.max_latitude))
                       .withColumn("is_valid_longitude",
                                 sf.col("longitude").between(self.config.min_longitude, 
                                                            self.config.max_longitude))
                       .withColumn("is_valid_position",
                                 (sf.col("latitude") != 0) & 
                                 (sf.col("longitude") != 0) &
                                 sf.col("is_valid_latitude") &
                                 sf.col("is_valid_longitude"))
                       
                       # Speed validation
                       .withColumn("is_reasonable_speed",
                                 sf.coalesce(sf.col("speed_over_ground"), sf.lit(0)) <= self.config.max_speed_knots)
                       
                       # Course and heading validation
                       .withColumn("is_valid_course",
                                 sf.coalesce(sf.col("course_over_ground"), sf.lit(0)).between(0, 360))
                       .withColumn("is_valid_heading",
                                 sf.coalesce(sf.col("heading"), sf.lit(0)).between(0, 360))
                       
                       # IMO validation (basic format check)
                       .withColumn("is_valid_imo_format",
                                 sf.col("imo").rlike("^[0-9]{7}$")))
        
        # Calculate overall quality score
        quality_df = validated_df.withColumn("data_quality_score",
            sf.when(
                sf.col("has_required_fields") &
                sf.col("is_valid_position") &
                sf.col("is_reasonable_speed") &
                sf.col("is_valid_course") &
                sf.col("is_valid_heading") &
                sf.col("is_valid_imo_format"), 1.0
            ).when(
                sf.col("has_required_fields") &
                sf.col("is_valid_position") &
                sf.col("is_reasonable_speed"), 0.8
            ).when(
                sf.col("has_required_fields") &
                sf.col("is_valid_position"), 0.6
            ).when(
                sf.col("has_required_fields"), 0.4
            ).otherwise(0.0)
        )
        
        # Add quality category
        quality_df = quality_df.withColumn("quality_category",
            sf.when(sf.col("data_quality_score") >= 0.9, "EXCELLENT")
            .when(sf.col("data_quality_score") >= 0.7, "GOOD")
            .when(sf.col("data_quality_score") >= 0.5, "FAIR")
            .when(sf.col("data_quality_score") >= 0.3, "POOR")
            .otherwise("INVALID")
        )
        
        logger.info("AIS data quality validation completed")
        return quality_df
    
    def validate_vessel_metadata(self, df: DataFrame) -> DataFrame:
        """
        Validate vessel metadata and add quality scores.
        
        Args:
            df: DataFrame containing vessel metadata
            
        Returns:
            DataFrame: Data with quality flags and scores
        """
        logger.info("Starting vessel metadata quality validation",
                   record_count=df.count())
        
        validated_df = (df
                       # Required field validation
                       .withColumn("has_required_fields",
                                 sf.col("imo").isNotNull() &
                                 sf.col("vessel_name").isNotNull())
                       
                       # IMO validation
                       .withColumn("is_valid_imo_format",
                                 sf.col("imo").rlike("^[0-9]{7}$"))
                       
                       # Tonnage validation
                       .withColumn("is_valid_tonnage",
                                 sf.coalesce(sf.col("gross_tonnage"), sf.lit(0)) > 0)
                       
                       # Dimensions validation
                       .withColumn("is_valid_dimensions",
                                 (sf.coalesce(sf.col("length_overall"), sf.lit(0)) > 0) &
                                 (sf.coalesce(sf.col("beam"), sf.lit(0)) > 0))
                       
                       # Temporal validity
                       .withColumn("has_valid_dates",
                                 sf.col("valid_from_datetime").isNotNull())
                       
                       # Company codes validation
                       .withColumn("has_company_info",
                                 sf.col("owner_code").isNotNull() |
                                 sf.col("operator_code").isNotNull() |
                                 sf.col("technical_manager_code").isNotNull()))
        
        # Calculate quality score
        quality_df = validated_df.withColumn("data_quality_score",
            sf.when(
                sf.col("has_required_fields") &
                sf.col("is_valid_imo_format") &
                sf.col("is_valid_tonnage") &
                sf.col("is_valid_dimensions") &
                sf.col("has_valid_dates") &
                sf.col("has_company_info"), 1.0
            ).when(
                sf.col("has_required_fields") &
                sf.col("is_valid_imo_format") &
                sf.col("has_valid_dates"), 0.7
            ).when(
                sf.col("has_required_fields") &
                sf.col("is_valid_imo_format"), 0.5
            ).when(
                sf.col("has_required_fields"), 0.3
            ).otherwise(0.0)
        )
        
        logger.info("Vessel metadata quality validation completed")
        return quality_df
    
    def get_quality_report(self, df: DataFrame, table_name: str) -> Dict:
        """
        Generate a comprehensive data quality report.
        
        Args:
            df: DataFrame to analyze
            table_name: Name of the table being analyzed
            
        Returns:
            Dict: Quality report with statistics
        """
        logger.info("Generating quality report", table=table_name)
        
        try:
            # Overall statistics
            total_records = df.count()
            
            if total_records == 0:
                return {
                    "table_name": table_name,
                    "total_records": 0,
                    "error": "No data to analyze"
                }
            
            # Quality score statistics
            quality_stats = df.agg(
                sf.avg("data_quality_score").alias("avg_quality_score"),
                sf.min("data_quality_score").alias("min_quality_score"),
                sf.max("data_quality_score").alias("max_quality_score"),
                sf.stddev("data_quality_score").alias("stddev_quality_score")
            ).collect()[0]
            
            # Quality category distribution
            quality_distribution = (df
                                  .groupBy("quality_category")
                                  .count()
                                  .collect())
            
            quality_dist_dict = {row.quality_category: row.count for row in quality_distribution}
            
            # Data completeness for key fields
            completeness_stats = {}
            key_fields = ["imo", "movementdatetime", "latitude", "longitude"]
            
            for field in key_fields:
                if field in df.columns:
                    non_null_count = df.filter(sf.col(field).isNotNull()).count()
                    completeness_stats[field] = {
                        "non_null_count": non_null_count,
                        "completeness_percentage": (non_null_count / total_records) * 100
                    }
            
            report = {
                "table_name": table_name,
                "total_records": total_records,
                "quality_statistics": {
                    "average_score": quality_stats.avg_quality_score,
                    "minimum_score": quality_stats.min_quality_score,
                    "maximum_score": quality_stats.max_quality_score,
                    "standard_deviation": quality_stats.stddev_quality_score
                },
                "quality_distribution": quality_dist_dict,
                "data_completeness": completeness_stats,
                "quality_thresholds": {
                    "excellent": ">= 0.9",
                    "good": ">= 0.7",
                    "fair": ">= 0.5",
                    "poor": ">= 0.3",
                    "invalid": "< 0.3"
                }
            }
            
            logger.info("Quality report generated", 
                       table=table_name,
                       total_records=total_records,
                       avg_quality=quality_stats.avg_quality_score)
            
            return report
            
        except Exception as e:
            logger.error("Failed to generate quality report", 
                        table=table_name, 
                        error=str(e))
            return {
                "table_name": table_name,
                "error": str(e)
            }
    
    def filter_by_quality_threshold(self, df: DataFrame, 
                                   min_quality_score: float = 0.5) -> DataFrame:
        """
        Filter DataFrame by minimum quality threshold.
        
        Args:
            df: DataFrame to filter
            min_quality_score: Minimum quality score threshold
            
        Returns:
            DataFrame: Filtered data above quality threshold
        """
        filtered_df = df.filter(sf.col("data_quality_score") >= min_quality_score)
        
        original_count = df.count()
        filtered_count = filtered_df.count()
        
        logger.info("Data filtered by quality threshold",
                   min_quality_score=min_quality_score,
                   original_count=original_count,
                   filtered_count=filtered_count,
                   retention_percentage=(filtered_count / original_count) * 100 if original_count > 0 else 0)
        
        return filtered_df
    
    def identify_anomalies(self, df: DataFrame) -> DataFrame:
        """
        Identify potential data anomalies in maritime data.
        
        Args:
            df: DataFrame to analyze for anomalies
            
        Returns:
            DataFrame: Data with anomaly flags
        """
        logger.info("Identifying data anomalies")
        
        anomaly_df = (df
                     # Speed anomalies
                     .withColumn("speed_anomaly",
                               sf.coalesce(sf.col("speed_over_ground"), sf.lit(0)) > 40)
                     
                     # Position anomalies (on land for certain vessel types)
                     .withColumn("potential_land_position",
                               (sf.col("latitude") == 0) & (sf.col("longitude") == 0))
                     
                     # Temporal anomalies (future dates)
                     .withColumn("future_timestamp",
                               sf.col("movementdatetime") > sf.current_timestamp())
                     
                     # Duplicate position anomalies (same position for extended time)
                     .withColumn("position_hash",
                               sf.hash(sf.col("latitude"), sf.col("longitude")))
                     
                     # Overall anomaly flag
                     .withColumn("has_anomaly",
                               sf.col("speed_anomaly") |
                               sf.col("potential_land_position") |
                               sf.col("future_timestamp")))
        
        anomaly_count = anomaly_df.filter(sf.col("has_anomaly")).count()
        total_count = anomaly_df.count()
        
        logger.info("Anomaly detection completed",
                   anomaly_count=anomaly_count,
                   total_count=total_count,
                   anomaly_percentage=(anomaly_count / total_count) * 100 if total_count > 0 else 0)
        
        return anomaly_df
