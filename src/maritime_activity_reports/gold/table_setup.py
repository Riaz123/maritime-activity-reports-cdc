"""Gold layer table setup and configuration."""

import structlog
from typing import Dict, Any
from pyspark.sql import SparkSession

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session

logger = structlog.get_logger(__name__)


class GoldTableSetup:
    """Handles Gold layer table creation and configuration."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        self.gold_path = f"{config.gcs.bucket_name}/{config.gcs.gold_prefix}"
        
    def create_all_gold_tables(self) -> None:
        """Create all Gold layer tables with CDF enabled."""
        logger.info("Creating Gold layer tables")
        
        self._create_vessel_activity_reports_table()
        self._create_port_performance_table()
        self._create_journey_analytics_table()
        self._create_compliance_activity_table()
        self._create_vessel_summary_table()
        
        logger.info("Gold layer tables created successfully")
    
    def _create_vessel_activity_reports_table(self) -> None:
        """Create the main vessel activity reports table."""
        table_name = "gold.vessel_activity_reports"
        table_path = f"{self.gold_path}/vessel_activity_reports"
        
        logger.info("Creating vessel activity reports table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                vesselimo STRING NOT NULL,
                vesselname STRING,
                vesseltypename STRING,
                vesselownername STRING,
                
                -- Activity timing
                entrytime TIMESTAMP NOT NULL,
                exittime TIMESTAMP,
                timespent DOUBLE,
                
                -- Geographic information
                geoareaname STRING NOT NULL,
                geoareatype STRING NOT NULL,
                geoareacategory STRING,
                geoarealevel STRING,
                
                -- Port-specific fields
                geoareaid STRING,
                geoarealeveleezsearegion STRING,
                geoarealevelwarzone STRING,
                geoarealevelcountry STRING,
                geoarealevelcontinent STRING,
                geoarealevelsubcontinent STRING,
                
                -- Report metadata
                report_type STRING NOT NULL,
                business_date DATE NOT NULL,
                data_lineage STRING DEFAULT 'bronze->silver->gold',
                
                -- Risk and compliance indicators
                risk_level STRING,
                activity_classification STRING,
                sanction_zone_flag BOOLEAN DEFAULT false,
                high_risk_zone_flag BOOLEAN DEFAULT false,
                
                -- Processing metadata
                gold_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                silver_source_timestamp TIMESTAMP,
                
                -- CDF metadata
                _change_type STRING,
                _commit_version BIGINT,
                _commit_timestamp TIMESTAMP
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.gold.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (business_date, report_type)
        """)
        
        logger.info("Vessel activity reports table created", table=table_name)
    
    def _create_port_performance_table(self) -> None:
        """Create the port performance analytics table."""
        table_name = "gold.port_performance"
        table_path = f"{self.gold_path}/port_performance"
        
        logger.info("Creating port performance table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                port_name STRING NOT NULL,
                port_id STRING,
                country STRING,
                continent STRING,
                
                -- Time period
                analysis_date DATE NOT NULL,
                period_start_date DATE NOT NULL,
                period_end_date DATE NOT NULL,
                
                -- Traffic metrics
                unique_vessels INT,
                total_visits INT,
                total_vessel_hours DOUBLE,
                avg_stay_duration_hours DOUBLE,
                median_stay_duration_hours DOUBLE,
                min_stay_duration_hours DOUBLE,
                max_stay_duration_hours DOUBLE,
                
                -- Vessel diversity
                vessel_types_served INT,
                vessel_diversity_ratio DOUBLE,
                
                -- Operational efficiency
                quick_turnarounds INT,
                extended_stays INT,
                quick_turnaround_rate DOUBLE,
                extended_stay_rate DOUBLE,
                
                -- Temporal patterns
                weekend_arrivals INT,
                night_arrivals INT,
                weekend_arrival_rate DOUBLE,
                night_arrival_rate DOUBLE,
                
                -- Performance scoring
                port_efficiency_score DOUBLE,
                performance_category STRING,
                
                -- Processing metadata
                gold_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                data_lineage STRING DEFAULT 'bronze->silver->gold'
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.gold.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (analysis_date, country)
        """)
        
        logger.info("Port performance table created", table=table_name)
    
    def _create_journey_analytics_table(self) -> None:
        """Create the journey analytics table."""
        table_name = "gold.journey_analytics"
        table_path = f"{self.gold_path}/journey_analytics"
        
        logger.info("Creating journey analytics table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                vesselimo STRING NOT NULL,
                vesselname STRING,
                vesseltypename STRING,
                vesselownername STRING,
                
                -- Journey identification
                journey_id STRING NOT NULL,
                journey_sequence INT,
                
                -- Journey details
                departuretime TIMESTAMP NOT NULL,
                departure_port STRING,
                departure_country STRING,
                arrivaltime TIMESTAMP,
                arrival_port STRING,
                arrival_country STRING,
                
                -- Journey metrics
                journey_duration_hours DOUBLE,
                estimated_distance_nm DOUBLE,
                avg_speed_knots DOUBLE,
                port_stay_duration_hours DOUBLE,
                
                -- Journey classification
                route_type STRING, -- LINEAR, CIRCULAR, RETURN
                journey_classification STRING, -- SHORT_HOP, DAY_JOURNEY, WEEK_JOURNEY, LONG_HAUL
                port_stay_classification STRING, -- QUICK_STOP, SHORT_STAY, MEDIUM_STAY, EXTENDED_STAY
                speed_classification STRING, -- HIGH_SPEED, NORMAL_SPEED, SLOW_SPEED, VERY_SLOW
                
                -- Risk indicators
                crosses_high_risk_zones BOOLEAN DEFAULT false,
                crosses_sanction_zones BOOLEAN DEFAULT false,
                unusual_route_flag BOOLEAN DEFAULT false,
                
                -- Business date
                business_date DATE NOT NULL,
                
                -- Processing metadata
                gold_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                data_lineage STRING DEFAULT 'bronze->silver->gold'
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.gold.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (business_date, vesseltypename)
        """)
        
        logger.info("Journey analytics table created", table=table_name)
    
    def _create_compliance_activity_table(self) -> None:
        """Create the vessel compliance activity table."""
        table_name = "gold.vessel_compliance_activity"
        table_path = f"{self.gold_path}/vessel_compliance_activity"
        
        logger.info("Creating vessel compliance activity table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                vesselimo STRING NOT NULL,
                vesselname STRING,
                vesseltypename STRING,
                flag_country STRING,
                
                -- Analysis period
                analysis_date DATE NOT NULL,
                analysis_period_days INT DEFAULT 30,
                
                -- Activity summary
                total_activities INT,
                unique_zones_visited INT,
                active_days INT,
                activity_intensity_percentage DOUBLE,
                
                -- Risk activity counters
                sanction_zone_activities INT,
                high_risk_activities INT,
                seca_zone_activities INT,
                port_visits INT,
                extended_port_stays INT,
                
                -- Risk timing
                last_sanction_zone_visit TIMESTAMP,
                last_hrz_visit TIMESTAMP,
                days_since_sanction_visit INT,
                days_since_hrz_visit INT,
                
                -- Time spent in risk zones
                total_sanction_zone_time_hours DOUBLE,
                total_hrz_time_hours DOUBLE,
                
                -- Risk scoring
                activity_risk_score INT, -- 0-100
                risk_level STRING, -- LOW, MEDIUM, HIGH, CRITICAL
                
                -- Compliance flags
                has_sanction_zone_activity BOOLEAN,
                has_significant_hrz_activity BOOLEAN,
                recent_sanction_activity BOOLEAN,
                recent_hrz_activity BOOLEAN,
                
                -- Activity classification
                activity_classification STRING, -- VERY_ACTIVE, ACTIVE, MODERATE, LOW_ACTIVITY
                
                -- Processing metadata
                gold_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                data_lineage STRING DEFAULT 'bronze->silver->gold'
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.gold.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (analysis_date, risk_level)
        """)
        
        logger.info("Vessel compliance activity table created", table=table_name)
    
    def _create_vessel_summary_table(self) -> None:
        """Create the vessel summary table for current status."""
        table_name = "gold.vessel_summary"
        table_path = f"{self.gold_path}/vessel_summary"
        
        logger.info("Creating vessel summary table", table=table_name)
        
        self.spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                vesselimo STRING NOT NULL,
                vesselname STRING,
                vesseltypename STRING,
                vessel_type_category STRING,
                vesselownername STRING,
                flag_country STRING,
                
                -- Current position and status
                latest_timestamp TIMESTAMP,
                latest_latitude DOUBLE,
                latest_longitude DOUBLE,
                current_zone_country STRING,
                current_zone_type STRING,
                current_navigational_status STRING,
                
                -- Recent activity summary (last 30 days)
                recent_total_activities INT,
                recent_unique_zones INT,
                recent_active_days INT,
                recent_port_visits INT,
                recent_journey_count INT,
                
                -- Risk indicators
                current_risk_level STRING,
                compliance_score DOUBLE,
                sanction_flag BOOLEAN,
                recent_sanction_activity BOOLEAN,
                recent_hrz_activity BOOLEAN,
                
                -- Performance metrics
                avg_speed_last_30_days DOUBLE,
                total_distance_last_30_days_nm DOUBLE,
                port_efficiency_score DOUBLE,
                
                -- Data freshness
                last_activity_timestamp TIMESTAMP,
                data_staleness_hours DOUBLE,
                is_active_vessel BOOLEAN, -- active in last 7 days
                
                -- Processing metadata
                summary_date DATE NOT NULL,
                gold_processing_timestamp TIMESTAMP DEFAULT current_timestamp(),
                data_lineage STRING DEFAULT 'bronze->silver->gold'
            ) USING DELTA
            LOCATION '{table_path}'
            TBLPROPERTIES (
                'delta.enableChangeDataFeed' = 'true',
                'delta.logRetentionDuration' = '{self.config.gold.retention_days} days',
                'delta.deletedFileRetentionDuration' = '7 days',
                'delta.autoOptimize.optimizeWrite' = 'true',
                'delta.autoOptimize.autoCompact' = 'true'
            )
            PARTITIONED BY (summary_date, vessel_type_category)
        """)
        
        logger.info("Vessel summary table created", table=table_name)
    
    def optimize_gold_tables(self) -> None:
        """Optimize Gold layer tables with Z-ordering."""
        optimization_configs = [
            ("gold.vessel_activity_reports", ["vesselimo", "business_date", "report_type"]),
            ("gold.port_performance", ["port_name", "analysis_date"]),
            ("gold.journey_analytics", ["vesselimo", "business_date"]),
            ("gold.vessel_compliance_activity", ["vesselimo", "analysis_date", "risk_level"]),
            ("gold.vessel_summary", ["vesselimo", "summary_date"])
        ]
        
        for table, zorder_columns in optimization_configs:
            logger.info("Optimizing Gold table", table=table, zorder_columns=zorder_columns)
            
            try:
                zorder_clause = ", ".join(zorder_columns)
                self.spark.sql(f"OPTIMIZE {table} ZORDER BY ({zorder_clause})")
                logger.info("Table optimized successfully", table=table)
            except Exception as e:
                logger.error("Failed to optimize table", table=table, error=str(e))
    
    def vacuum_gold_tables(self, retention_hours: int = None) -> None:
        """Vacuum Gold layer tables."""
        retention_hours = retention_hours or self.config.gold.vacuum_hours
        
        tables = [
            "gold.vessel_activity_reports",
            "gold.port_performance",
            "gold.journey_analytics",
            "gold.vessel_compliance_activity",
            "gold.vessel_summary"
        ]
        
        for table in tables:
            logger.info("Vacuuming Gold table", table=table, retention_hours=retention_hours)
            
            try:
                self.spark.sql(f"VACUUM {table} RETAIN {retention_hours} HOURS")
                logger.info("Table vacuumed successfully", table=table)
            except Exception as e:
                logger.error("Failed to vacuum table", table=table, error=str(e))
    
    def create_gold_views(self) -> None:
        """Create useful views on Gold layer tables."""
        logger.info("Creating Gold layer views")
        
        # Current active vessels view
        self.spark.sql("""
            CREATE OR REPLACE VIEW gold.active_vessels_current AS
            SELECT 
                vesselimo,
                vesselname,
                vesseltypename,
                flag_country,
                latest_timestamp,
                latest_latitude,
                latest_longitude,
                current_zone_country,
                current_navigational_status,
                current_risk_level,
                compliance_score,
                is_active_vessel
            FROM gold.vessel_summary
            WHERE summary_date = CURRENT_DATE()
            AND is_active_vessel = true
        """)
        
        # High risk vessels view
        self.spark.sql("""
            CREATE OR REPLACE VIEW gold.high_risk_vessels AS
            SELECT 
                vca.vesselimo,
                vs.vesselname,
                vs.vesseltypename,
                vs.flag_country,
                vca.activity_risk_score,
                vca.risk_level,
                vca.sanction_zone_activities,
                vca.high_risk_activities,
                vca.last_sanction_zone_visit,
                vca.recent_sanction_activity,
                vs.latest_latitude,
                vs.latest_longitude,
                vs.current_zone_country
            FROM gold.vessel_compliance_activity vca
            INNER JOIN gold.vessel_summary vs ON vca.vesselimo = vs.vesselimo
            WHERE vca.analysis_date = CURRENT_DATE()
            AND vca.risk_level IN ('HIGH', 'CRITICAL')
            AND vs.summary_date = CURRENT_DATE()
        """)
        
        # Port efficiency ranking view
        self.spark.sql("""
            CREATE OR REPLACE VIEW gold.port_efficiency_ranking AS
            SELECT 
                port_name,
                country,
                continent,
                port_efficiency_score,
                total_visits,
                unique_vessels,
                avg_stay_duration_hours,
                quick_turnaround_rate,
                RANK() OVER (PARTITION BY country ORDER BY port_efficiency_score DESC) as country_rank,
                RANK() OVER (ORDER BY port_efficiency_score DESC) as global_rank
            FROM gold.port_performance
            WHERE analysis_date = CURRENT_DATE()
            AND total_visits >= 10
            ORDER BY port_efficiency_score DESC
        """)
        
        # Daily activity summary view
        self.spark.sql("""
            CREATE OR REPLACE VIEW gold.daily_activity_summary AS
            SELECT 
                business_date,
                report_type,
                COUNT(DISTINCT vesselimo) as unique_vessels,
                COUNT(*) as total_activities,
                SUM(timespent) as total_time_spent_hours,
                AVG(timespent) as avg_time_spent_hours,
                COUNT(DISTINCT geoareaname) as unique_zones,
                COUNT(CASE WHEN sanction_zone_flag = true THEN 1 END) as sanction_activities,
                COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) as high_risk_activities
            FROM gold.vessel_activity_reports
            WHERE business_date >= CURRENT_DATE() - INTERVAL 30 DAYS
            GROUP BY business_date, report_type
            ORDER BY business_date DESC, report_type
        """)
        
        logger.info("Gold layer views created successfully")
    
    def get_gold_table_metrics(self) -> Dict[str, Any]:
        """Get metrics for all Gold layer tables."""
        tables = [
            "gold.vessel_activity_reports",
            "gold.port_performance",
            "gold.journey_analytics",
            "gold.vessel_compliance_activity",
            "gold.vessel_summary"
        ]
        
        metrics = {}
        
        for table in tables:
            try:
                # Get record count
                count = self.spark.sql(f"SELECT COUNT(*) as count FROM {table}").collect()[0].count
                
                # Get table size and details
                details_df = self.spark.sql(f"DESCRIBE DETAIL {table}")
                details = details_df.collect()[0].asDict()
                
                # Get recent data count (last 7 days)
                if "business_date" in [col.name for col in self.spark.table(table).schema]:
                    recent_count = self.spark.sql(f"""
                        SELECT COUNT(*) as count FROM {table} 
                        WHERE business_date >= CURRENT_DATE() - INTERVAL 7 DAYS
                    """).collect()[0].count
                elif "analysis_date" in [col.name for col in self.spark.table(table).schema]:
                    recent_count = self.spark.sql(f"""
                        SELECT COUNT(*) as count FROM {table} 
                        WHERE analysis_date >= CURRENT_DATE() - INTERVAL 7 DAYS
                    """).collect()[0].count
                elif "summary_date" in [col.name for col in self.spark.table(table).schema]:
                    recent_count = self.spark.sql(f"""
                        SELECT COUNT(*) as count FROM {table} 
                        WHERE summary_date >= CURRENT_DATE() - INTERVAL 7 DAYS
                    """).collect()[0].count
                else:
                    recent_count = 0
                
                metrics[table] = {
                    "total_records": count,
                    "recent_records_7_days": recent_count,
                    "size_in_bytes": details.get("sizeInBytes", 0),
                    "num_files": details.get("numFiles", 0),
                    "location": details.get("location", ""),
                    "last_modified": str(details.get("lastModified", ""))
                }
                
            except Exception as e:
                logger.error("Failed to get metrics for table", table=table, error=str(e))
                metrics[table] = {"error": str(e)}
        
        logger.info("Gold layer metrics collected", metrics=metrics)
        return metrics
