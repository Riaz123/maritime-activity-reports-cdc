"""Bronze layer table setup and initialization."""

import structlog
from typing import Dict, Any
from pyspark.sql import SparkSession

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session

logger = structlog.get_logger(__name__)


class BronzeTableSetup:
    """Handles Bronze layer table creation and configuration."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        self.bronze_path = f"{config.gcs.bucket_name}/{config.gcs.bronze_prefix}"
        
    def create_all_bronze_tables(self) -> None:
        """Create all Bronze layer tables with CDF enabled."""
        logger.info("Creating Bronze layer tables")
        
        self._create_ais_movements_table()
        self._create_vessel_metadata_table()
        self._create_geospatial_zones_table()
        
        logger.info("Bronze layer tables created successfully")
    
    def _create_ais_movements_table(self) -> None:
        """Create the AIS movements table."""
        table_name = "bronze.ais_movements"
        table_path = f"{self.bronze_path}/ais_movements"
        
        logger.info("Creating AIS movements table", table=table_name)
        
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
                source_system STRING,
                
                -- Data quality fields
                is_valid_position BOOLEAN DEFAULT true,
                is_reasonable_speed BOOLEAN DEFAULT true,
                data_quality_score DOUBLE DEFAULT 1.0,
                
                -- Zone information (populated by Silver layer)
                zone_country STRING,
                zone_eez STRING,
                zone_continent STRING,
                zone_hrz_v2 STRING,
                zone_sanction STRING,
                zone_seca STRING,
                zone_port STRING,
                
                -- CDC metadata
                cdc_operation STRING,
                cdc_timestamp TIMESTAMP,
                cdc_sequence_number BIGINT,
                
                -- Processing metadata
                ingestion_timestamp TIMESTAMP DEFAULT current_timestamp(),
                processing_timestamp TIMESTAMP
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.bronze.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (DATE(movementdatetime))
        """)
        
        logger.info("AIS movements table created", table=table_name, path=table_path)
    
    def _create_vessel_metadata_table(self) -> None:
        """Create the vessel metadata table."""
        table_name = "bronze.vessel_metadata"
        table_path = f"{self.bronze_path}/vessel_metadata"
        
        logger.info("Creating vessel metadata table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                imo STRING NOT NULL,
                vessel_name STRING,
                vessel_type STRING,
                gross_tonnage DOUBLE,
                deadweight_tonnage DOUBLE,
                length_overall DOUBLE,
                beam DOUBLE,
                flag_country STRING,
                
                -- Company information
                owner_code STRING,
                operator_code STRING,
                technical_manager_code STRING,
                ship_manager_code STRING,
                group_beneficial_owner_code STRING,
                document_of_compliance_doc_company_code STRING,
                bareboat_charter_company_code STRING,
                
                -- Temporal validity (SCD Type 2)
                valid_from_datetime TIMESTAMP NOT NULL,
                valid_to_datetime TIMESTAMP,
                is_current_record BOOLEAN DEFAULT true,
                
                -- CDC metadata
                cdc_operation STRING,
                cdc_timestamp TIMESTAMP,
                cdc_sequence_number BIGINT,
                
                -- Processing metadata
                ingestion_timestamp TIMESTAMP DEFAULT current_timestamp(),
                processing_timestamp TIMESTAMP
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.bronze.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (DATE(valid_from_datetime))
        """)
        
        logger.info("Vessel metadata table created", table=table_name, path=table_path)
    
    def _create_geospatial_zones_table(self) -> None:
        """Create the geospatial zones reference table."""
        table_name = "bronze.geospatial_zones"
        table_path = f"{self.bronze_path}/geospatial_zones"
        
        logger.info("Creating geospatial zones table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                zone_id STRING NOT NULL,
                zone_name STRING NOT NULL,
                zone_type STRING NOT NULL,
                zone_category STRING,
                zone_level STRING,
                geometry STRING,  -- Well-Known Text (WKT) format
                
                -- Hierarchical information
                parent_zone_id STRING,
                country_code STRING,
                continent STRING,
                
                -- Metadata
                source_system STRING,
                last_updated TIMESTAMP,
                
                -- Processing metadata
                ingestion_timestamp TIMESTAMP DEFAULT current_timestamp(),
                processing_timestamp TIMESTAMP
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.bronze.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (zone_type)
        """)
        
        logger.info("Geospatial zones table created", table=table_name, path=table_path)
    
    def create_databases(self) -> None:
        """Create the required databases."""
        databases = ["bronze", "silver", "gold"]
        
        for db in databases:
            logger.info("Creating database", database=db)
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
            
        logger.info("All databases created successfully")
    
    def optimize_tables(self) -> None:
        """Optimize Bronze layer tables."""
        tables = [
            "bronze.ais_movements",
            "bronze.vessel_metadata", 
            "bronze.geospatial_zones"
        ]
        
        for table in tables:
            logger.info("Optimizing table", table=table)
            self.spark.sql(f"OPTIMIZE {table}")
            
        logger.info("Bronze layer tables optimized")
    
    def vacuum_tables(self, retention_hours: int = None) -> None:
        """Vacuum Bronze layer tables."""
        retention_hours = retention_hours or self.config.bronze.vacuum_hours
        
        tables = [
            "bronze.ais_movements",
            "bronze.vessel_metadata",
            "bronze.geospatial_zones"
        ]
        
        for table in tables:
            logger.info("Vacuuming table", table=table, retention_hours=retention_hours)
            self.spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
            
        logger.info("Bronze layer tables vacuumed")
    
    def get_table_info(self, table_name: str) -> Dict[str, Any]:
        """Get information about a Bronze table."""
        try:
            # Get table details
            details_df = self.spark.sql(f"DESCRIBE DETAIL {table_name}")
            details = details_df.collect()[0].asDict()
            
            # Get table properties
            properties_df = self.spark.sql(f"SHOW TBLPROPERTIES {table_name}")
            properties = {row.key: row.value for row in properties_df.collect()}
            
            return {
                "table_name": table_name,
                "location": details.get("location"),
                "num_files": details.get("numFiles"),
                "size_in_bytes": details.get("sizeInBytes"),
                "properties": properties,
                "partitioning": details.get("partitionColumns", [])
            }
            
        except Exception as e:
            logger.error("Failed to get table info", table=table_name, error=str(e))
            return {"error": str(e)}
