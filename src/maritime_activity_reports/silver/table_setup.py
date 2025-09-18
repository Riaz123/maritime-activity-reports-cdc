"""Silver layer table setup and configuration."""

import structlog
from typing import Dict, Any
from pyspark.sql import SparkSession

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session

logger = structlog.get_logger(__name__)


class SilverTableSetup:
    """Handles Silver layer table creation and configuration."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        self.silver_path = f"{config.gcs.bucket_name}/{config.gcs.silver_prefix}"
        
    def create_all_silver_tables(self) -> None:
        """Create all Silver layer tables with CDF enabled."""
        logger.info("Creating Silver layer tables")
        
        self._create_cleaned_vessel_movements_table()
        self._create_vessel_master_table()
        self._create_zone_definitions_table()
        self._create_spatial_enrichment_table()
        
        logger.info("Silver layer tables created successfully")
    
    def _create_cleaned_vessel_movements_table(self) -> None:
        """Create the cleaned vessel movements table."""
        table_name = "silver.cleaned_vessel_movements"
        table_path = f"{self.silver_path}/cleaned_vessel_movements"
        
        logger.info("Creating cleaned vessel movements table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                imo STRING NOT NULL,
                movementdatetime TIMESTAMP NOT NULL,
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL,
                speed_over_ground DOUBLE,
                course_over_ground DOUBLE,
                heading DOUBLE,
                navigational_status STRING,
                
                -- Data quality fields (from Bronze)
                is_valid_position BOOLEAN,
                is_reasonable_speed BOOLEAN,
                data_quality_score DOUBLE,
                quality_category STRING,
                
                -- Enriched zone information
                zone_country STRING,
                zone_country_code STRING,
                zone_eez STRING,
                zone_continent STRING,
                zone_subcontinent STRING,
                zone_hrz_v2 STRING,
                zone_sanction STRING,
                zone_seca STRING,
                zone_port STRING,
                zone_port_id STRING,
                
                -- Derived fields
                distance_from_previous_nm DOUBLE,
                time_since_previous_minutes DOUBLE,
                calculated_speed_knots DOUBLE,
                
                -- Movement classification
                movement_type STRING, -- SAILING, ANCHORED, DOCKED, etc.
                is_zone_transition BOOLEAN,
                previous_zone STRING,
                next_zone STRING,
                
                -- Processing metadata
                silver_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                bronze_source_timestamp TIMESTAMP,
                data_lineage STRING DEFAULT 'bronze->silver',
                
                -- CDF metadata
                _change_type STRING,
                _commit_version BIGINT,
                _commit_timestamp TIMESTAMP
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.silver.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (DATE(movementdatetime), zone_country)
        """)
        
        logger.info("Cleaned vessel movements table created", table=table_name)
    
    def _create_vessel_master_table(self) -> None:
        """Create the vessel master dimension table."""
        table_name = "silver.vessel_master"
        table_path = f"{self.silver_path}/vessel_master"
        
        logger.info("Creating vessel master table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                imo STRING NOT NULL,
                vessel_name STRING,
                vessel_type STRING,
                vessel_type_category STRING, -- Bulk, Container, Tanker, etc.
                gross_tonnage DOUBLE,
                deadweight_tonnage DOUBLE,
                length_overall DOUBLE,
                beam DOUBLE,
                flag_country STRING,
                flag_country_code STRING,
                
                -- Company information (current)
                current_owner_code STRING,
                current_owner_name STRING,
                current_operator_code STRING,
                current_operator_name STRING,
                current_technical_manager_code STRING,
                current_technical_manager_name STRING,
                current_ship_manager_code STRING,
                current_ship_manager_name STRING,
                
                -- Risk and compliance indicators
                risk_category STRING, -- LOW, MEDIUM, HIGH
                compliance_score DOUBLE,
                sanction_flag BOOLEAN DEFAULT false,
                
                -- Temporal validity (SCD Type 2)
                valid_from_datetime TIMESTAMP NOT NULL,
                valid_to_datetime TIMESTAMP,
                is_current_record BOOLEAN DEFAULT true,
                
                -- Processing metadata
                silver_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                bronze_source_timestamp TIMESTAMP,
                data_lineage STRING DEFAULT 'bronze->silver',
                
                -- CDF metadata
                _change_type STRING,
                _commit_version BIGINT,
                _commit_timestamp TIMESTAMP
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.silver.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (is_current_record, vessel_type_category)
        """)
        
        logger.info("Vessel master table created", table=table_name)
    
    def _create_zone_definitions_table(self) -> None:
        """Create the standardized zone definitions table."""
        table_name = "silver.zone_definitions"
        table_path = f"{self.silver_path}/zone_definitions"
        
        logger.info("Creating zone definitions table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                zone_id STRING NOT NULL,
                zone_name STRING NOT NULL,
                zone_type STRING NOT NULL, -- country, eez, port, hrz_v2, sanction, seca
                zone_category STRING,
                zone_level STRING,
                
                -- Standardized attributes
                standardized_name STRING,
                iso_country_code STRING,
                continent STRING,
                subcontinent STRING,
                
                -- Geometric information
                geometry_wkt STRING, -- Well-Known Text format
                centroid_latitude DOUBLE,
                centroid_longitude DOUBLE,
                bounding_box_north DOUBLE,
                bounding_box_south DOUBLE,
                bounding_box_east DOUBLE,
                bounding_box_west DOUBLE,
                
                -- Hierarchical relationships
                parent_zone_id STRING,
                child_zone_ids ARRAY<STRING>,
                
                -- Risk and compliance attributes
                risk_level STRING, -- LOW, MEDIUM, HIGH
                sanction_status BOOLEAN DEFAULT false,
                seca_status BOOLEAN DEFAULT false,
                
                -- Metadata
                source_system STRING,
                last_updated TIMESTAMP,
                is_active BOOLEAN DEFAULT true,
                
                -- Processing metadata
                silver_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                data_lineage STRING DEFAULT 'bronze->silver'
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.silver.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (zone_type, continent)
        """)
        
        logger.info("Zone definitions table created", table=table_name)
    
    def _create_spatial_enrichment_table(self) -> None:
        """Create table for spatial enrichment lookup cache."""
        table_name = "silver.spatial_enrichment_cache"
        table_path = f"{self.silver_path}/spatial_enrichment_cache"
        
        logger.info("Creating spatial enrichment cache table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                position_hash STRING NOT NULL, -- Hash of lat/lon for lookup
                latitude DOUBLE NOT NULL,
                longitude DOUBLE NOT NULL,
                
                -- Zone assignments
                country_zone STRING,
                country_code STRING,
                eez_zone STRING,
                continent STRING,
                subcontinent STRING,
                hrz_v2_zone STRING,
                sanction_zone STRING,
                seca_zone STRING,
                port_zone STRING,
                port_id STRING,
                
                -- Additional spatial context
                distance_to_shore_km DOUBLE,
                water_depth_m DOUBLE,
                
                -- Cache metadata
                first_seen TIMESTAMP DEFAULT current_timestamp(),
                last_accessed TIMESTAMP DEFAULT current_timestamp(),
                access_count BIGINT DEFAULT 1,
                
                -- Processing metadata
                silver_processing_timestamp TIMESTAMP DEFAULT current_timestamp()
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.silver.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (country_code)
        """)
        
        logger.info("Spatial enrichment cache table created", table=table_name)
    
    def optimize_silver_tables(self) -> None:
        """Optimize Silver layer tables with Z-ordering."""
        optimization_configs = [
            ("silver.cleaned_vessel_movements", ["imo", "movementdatetime"]),
            ("silver.vessel_master", ["imo", "is_current_record"]),
            ("silver.zone_definitions", ["zone_type", "zone_id"]),
            ("silver.spatial_enrichment_cache", ["position_hash"])
        ]
        
        for table, zorder_columns in optimization_configs:
            logger.info("Optimizing Silver table", table=table, zorder_columns=zorder_columns)
            
            zorder_clause = ", ".join(zorder_columns)
            self.spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_clause})")
            
        logger.info("Silver layer tables optimized")
    
    def create_silver_views(self) -> None:
        """Create useful views on Silver layer tables."""
        logger.info("Creating Silver layer views")
        
        # Current vessel positions view
        self.spark.sql("""
            CREATE OR REPLACE VIEW silver.current_vessel_positions AS
            SELECT DISTINCT
                imo,
                FIRST_VALUE(movementdatetime) OVER (
                    PARTITION BY imo 
                    ORDER BY movementdatetime DESC
                ) as latest_timestamp,
                FIRST_VALUE(latitude) OVER (
                    PARTITION BY imo 
                    ORDER BY movementdatetime DESC
                ) as latest_latitude,
                FIRST_VALUE(longitude) OVER (
                    PARTITION BY imo 
                    ORDER BY movementdatetime DESC
                ) as latest_longitude,
                FIRST_VALUE(zone_country) OVER (
                    PARTITION BY imo 
                    ORDER BY movementdatetime DESC
                ) as current_zone_country,
                FIRST_VALUE(navigational_status) OVER (
                    PARTITION BY imo 
                    ORDER BY movementdatetime DESC
                ) as current_status
            FROM silver.cleaned_vessel_movements
            WHERE data_quality_score >= 0.7
        """)
        
        # Active vessels view
        self.spark.sql("""
            CREATE OR REPLACE VIEW silver.active_vessels AS
            SELECT 
                vm.imo,
                vm.vessel_name,
                vm.vessel_type,
                vm.flag_country,
                cvp.latest_timestamp,
                cvp.latest_latitude,
                cvp.latest_longitude,
                cvp.current_zone_country,
                cvp.current_status
            FROM silver.vessel_master vm
            INNER JOIN silver.current_vessel_positions cvp ON vm.imo = cvp.imo
            WHERE vm.is_current_record = true
            AND cvp.latest_timestamp >= current_timestamp() - INTERVAL 7 DAYS
        """)
        
        logger.info("Silver layer views created")
    
    def get_silver_table_metrics(self) -> Dict[str, Any]:
        """Get metrics for all Silver layer tables."""
        tables = [
            "silver.cleaned_vessel_movements",
            "silver.vessel_master",
            "silver.zone_definitions",
            "silver.spatial_enrichment_cache"
        ]
        
        metrics = {}
        
        for table in tables:
            try:
                # Get record count
                count = self.spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0].count
                
                # Get table size
                details_df = self.spark.sql(f"DESCRIBE DETAIL {table}")
                details = details_df.collect()[0].asDict()
                
                metrics[table] = {
                    "record_count": count,
                    "size_in_bytes": details.get("sizeInBytes", 0),
                    "num_files": details.get("numFiles", 0),
                    "location": details.get("location", "")
                }
                
            except Exception as e:
                logger.error("Failed to get metrics for table", table=table, error=str(e))
                metrics[table] = {"error": str(e)}
        
        logger.info("Silver layer metrics collected", metrics=metrics)
        return metrics
