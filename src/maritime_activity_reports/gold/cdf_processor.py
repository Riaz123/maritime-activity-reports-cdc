"""Gold layer CDF processing for business analytics."""

import structlog
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as sf
from pyspark.sql import Window
from pyspark.sql.streaming import StreamingQuery
from datetime import datetime, timedelta

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session, optimize_spark_for_streaming
from .materialized_views import GoldMaterializedViews

logger = structlog.get_logger(__name__)


class GoldCDFProcessor:
    """Processes Silver CDF changes and creates business-ready Gold analytics."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        optimize_spark_for_streaming(self.spark, config)
        
        self.silver_path = f"{config.gcs.bucket_name}/{config.gcs.silver_prefix}"
        self.gold_path = f"{config.gcs.bucket_name}/{config.gcs.gold_prefix}"
        self.checkpoint_path = f"{config.gcs.bucket_name}/{config.gcs.checkpoint_prefix}/gold"
        
        self.materialized_views = GoldMaterializedViews(config, self.spark)
        
    def process_silver_movements_to_activity_reports(self) -> StreamingQuery:
        """Process Silver movements CDF to generate Gold activity reports."""
        logger.info("Starting Silver movements to Gold activity reports stream")
        
        # Read CDF from Silver cleaned vessel movements
        silver_movements_cdf = (self.spark
                               .readStream
                               .format("delta")
                               .option("readChangeFeed", "true")
                               .option("startingVersion", "latest")
                               .table("silver.cleaned_vessel_movements"))
        
        def process_movements_to_reports(microBatchDF: DataFrame, batchId: int):
            """Transform Silver movements into Gold activity reports."""
            logger.info("Processing movements to activity reports", 
                       batch_id=batchId, 
                       record_count=microBatchDF.count())
            
            if microBatchDF.count() == 0:
                return
            
            # Filter for relevant changes
            relevant_changes = microBatchDF.filter(
                sf.col("_change_type").isin(["insert", "update_postimage"])
            )
            
            if relevant_changes.count() == 0:
                return
            
            # Apply existing ActivityReport business logic in streaming context
            activity_reports = self._generate_activity_reports_from_movements(relevant_changes)
            
            if activity_reports.count() > 0:
                # Write to Gold activity reports table
                self._merge_to_gold_activity_reports(activity_reports)
                
                # Update related Gold tables
                self._update_vessel_summary(activity_reports)
                
                logger.info("Activity reports generated and merged", 
                           batch_id=batchId,
                           reports_count=activity_reports.count())
        
        query = (silver_movements_cdf
                .writeStream
                .foreachBatch(process_movements_to_reports)
                .option("checkpointLocation", f"{self.checkpoint_path}/activity_reports")
                .trigger(processingTime=f"{self.config.cdc.processing_time_seconds * 2} seconds")
                .start())
        
        logger.info("Silver movements to activity reports stream started", query_id=query.id)
        return query
    
    def process_activity_reports_to_analytics(self) -> StreamingQuery:
        """Process activity reports CDF to generate derived analytics."""
        logger.info("Starting activity reports to analytics stream")
        
        # Read CDF from Gold activity reports
        activity_reports_cdf = (self.spark
                               .readStream
                               .format("delta")
                               .option("readChangeFeed", "true")
                               .option("startingVersion", "latest")
                               .table("gold.vessel_activity_reports"))
        
        def process_reports_to_analytics(microBatchDF: DataFrame, batchId: int):
            """Generate analytics from activity reports changes."""
            logger.info("Processing reports to analytics", 
                       batch_id=batchId, 
                       record_count=microBatchDF.count())
            
            if microBatchDF.count() == 0:
                return
            
            # Filter for relevant changes
            relevant_changes = microBatchDF.filter(
                sf.col("_change_type").isin(["insert", "update_postimage"])
            )
            
            if relevant_changes.count() == 0:
                return
            
            # Generate port performance analytics
            self._update_port_performance_analytics(relevant_changes)
            
            # Generate compliance activity analytics
            self._update_compliance_analytics(relevant_changes)
            
            # Trigger materialized view refresh for affected data
            self._trigger_materialized_view_refresh(relevant_changes)
            
            logger.info("Analytics updated from activity reports", batch_id=batchId)
        
        query = (activity_reports_cdf
                .writeStream
                .foreachBatch(process_reports_to_analytics)
                .option("checkpointLocation", f"{self.checkpoint_path}/analytics")
                .trigger(processingTime=f"{self.config.cdc.processing_time_seconds * 3} seconds")
                .start())
        
        logger.info("Activity reports to analytics stream started", query_id=query.id)
        return query
    
    def _generate_activity_reports_from_movements(self, movements_df: DataFrame) -> DataFrame:
        """Generate activity reports from vessel movements using existing business logic."""
        logger.debug("Generating activity reports from movements")
        
        # Window for detecting zone transitions
        vessel_window = Window.partitionBy("imo").orderBy("movementdatetime")
        
        # Detect zone transitions and generate reports
        activity_reports = (movements_df
                           # Add zone transition detection
                           .withColumn("prev_zone_country", sf.lag("zone_country").over(vessel_window))
                           .withColumn("next_zone_country", sf.lead("zone_country").over(vessel_window))
                           
                           # Identify zone entries and exits
                           .withColumn("is_zone_entry",
                                     (sf.col("zone_country") != sf.col("prev_zone_country")) |
                                     sf.col("prev_zone_country").isNull())
                           .withColumn("is_zone_exit", 
                                     (sf.col("zone_country") != sf.col("next_zone_country")) |
                                     sf.col("next_zone_country").isNull())
                           
                           # Filter for zone transitions only
                           .filter(sf.col("is_zone_entry") | sf.col("is_zone_exit"))
                           
                           # Transform to Gold activity report format
                           .select(
                               sf.col("imo").alias("vesselimo"),
                               sf.coalesce(sf.col("vessel_name"), sf.lit("Unknown")).alias("vesselname"),
                               sf.coalesce(sf.col("vessel_type"), sf.lit("Unknown")).alias("vesseltypename"),
                               sf.coalesce(sf.col("vessel_owner"), sf.lit("Unknown")).alias("vesselownername"),
                               
                               # Activity timing
                               sf.col("movementdatetime").alias("entrytime"),
                               sf.col("movementdatetime").alias("exittime"),  # Would calculate actual exit
                               sf.lit(0.0).alias("timespent"),  # Would calculate actual time spent
                               
                               # Geographic information
                               sf.col("zone_country").alias("geoareaname"),
                               sf.lit("country").alias("geoareatype"),
                               sf.lit("geographic").alias("geoareacategory"),
                               sf.lit("country").alias("geoarealevel"),
                               
                               # Port-specific fields (simplified)
                               sf.col("zone_port").alias("geoareaid"),
                               sf.col("zone_eez").alias("geoarealeveleezsearegion"),
                               sf.lit(None).cast("string").alias("geoarealevelwarzone"),
                               sf.col("zone_country").alias("geoarealevelcountry"),
                               sf.col("zone_continent").alias("geoarealevelcontinent"),
                               sf.col("zone_subcontinent").alias("geoarealevelsubcontinent"),
                               
                               # Report metadata
                               sf.lit("country_un_name").alias("report_type"),
                               sf.col("movementdatetime").cast("date").alias("business_date"),
                               sf.lit("bronze->silver->gold").alias("data_lineage"),
                               
                               # Risk indicators
                               sf.when(sf.col("zone_sanction").isNotNull(), "HIGH")
                               .when(sf.col("zone_hrz_v2").isNotNull(), "MEDIUM")
                               .otherwise("LOW").alias("risk_level"),
                               
                               sf.col("movement_type").alias("activity_classification"),
                               (sf.col("zone_sanction").isNotNull()).alias("sanction_zone_flag"),
                               (sf.col("zone_hrz_v2").isNotNull()).alias("high_risk_zone_flag"),
                               
                               # Processing metadata
                               sf.current_timestamp().alias("gold_processing_timestamp"),
                               sf.col("silver_processing_timestamp").alias("silver_source_timestamp")
                           ))
        
        return activity_reports
    
    def _merge_to_gold_activity_reports(self, activity_reports_df: DataFrame) -> None:
        """Merge activity reports into Gold table."""
        activity_reports_df.createOrReplaceTempView("activity_reports_updates")
        
        merge_sql = """
            MERGE INTO gold.vessel_activity_reports AS target
            USING activity_reports_updates AS source
            ON target.vesselimo = source.vesselimo 
               AND target.entrytime = source.entrytime
               AND target.report_type = source.report_type
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.debug("Activity reports merged to Gold table successfully")
        except Exception as e:
            logger.error("Failed to merge activity reports to Gold table", error=str(e))
            raise
    
    def _update_vessel_summary(self, activity_reports_df: DataFrame) -> None:
        """Update vessel summary table with latest activity."""
        logger.debug("Updating vessel summary from activity reports")
        
        # Get unique vessels from the activity reports
        affected_vessels = (activity_reports_df
                           .select("vesselimo")
                           .distinct()
                           .collect())
        
        vessel_list = [row.vesselimo for row in affected_vessels]
        
        if not vessel_list:
            return
        
        # Generate updated vessel summaries
        for vessel_imo in vessel_list:
            self._update_single_vessel_summary(vessel_imo)
    
    def _update_single_vessel_summary(self, vessel_imo: str) -> None:
        """Update summary for a single vessel."""
        summary_sql = f"""
            MERGE INTO gold.vessel_summary AS target
            USING (
                WITH vessel_latest AS (
                    SELECT 
                        '{vessel_imo}' as vesselimo,
                        MAX(entrytime) as latest_timestamp,
                        FIRST_VALUE(geoareaname) OVER (ORDER BY entrytime DESC) as current_zone_country,
                        FIRST_VALUE(geoareatype) OVER (ORDER BY entrytime DESC) as current_zone_type,
                        COUNT(*) as recent_total_activities,
                        COUNT(DISTINCT geoareaname) as recent_unique_zones,
                        COUNT(DISTINCT DATE(entrytime)) as recent_active_days,
                        COUNT(CASE WHEN report_type = 'port' THEN 1 END) as recent_port_visits,
                        MAX(CASE WHEN sanction_zone_flag THEN entrytime END) as last_sanction_activity,
                        MAX(CASE WHEN high_risk_zone_flag THEN entrytime END) as last_hrz_activity
                    FROM gold.vessel_activity_reports
                    WHERE vesselimo = '{vessel_imo}'
                    AND business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                ),
                vessel_info AS (
                    SELECT 
                        imo,
                        vessel_name,
                        vessel_type,
                        vessel_type_category,
                        current_owner_name as vesselownername,
                        flag_country
                    FROM silver.vessel_master
                    WHERE imo = '{vessel_imo}' AND is_current_record = true
                )
                SELECT 
                    vl.vesselimo,
                    vi.vessel_name as vesselname,
                    vi.vessel_type as vesseltypename,
                    vi.vessel_type_category,
                    vi.vesselownername,
                    vi.flag_country,
                    vl.latest_timestamp,
                    null as latest_latitude,  -- Would get from latest position
                    null as latest_longitude,
                    vl.current_zone_country,
                    vl.current_zone_type,
                    'SAILING' as current_navigational_status,  -- Simplified
                    vl.recent_total_activities,
                    vl.recent_unique_zones,
                    vl.recent_active_days,
                    vl.recent_port_visits,
                    0 as recent_journey_count,  -- Would calculate from journey table
                    CASE 
                        WHEN vl.last_sanction_activity IS NOT NULL THEN 'HIGH'
                        WHEN vl.last_hrz_activity IS NOT NULL THEN 'MEDIUM'
                        ELSE 'LOW'
                    END as current_risk_level,
                    0.75 as compliance_score,  -- Simplified
                    vl.last_sanction_activity IS NOT NULL as sanction_flag,
                    DATE_DIFF(CURRENT_DATE(), DATE(vl.last_sanction_activity), DAY) <= 30 as recent_sanction_activity,
                    DATE_DIFF(CURRENT_DATE(), DATE(vl.last_hrz_activity), DAY) <= 7 as recent_hrz_activity,
                    0.0 as avg_speed_last_30_days,  -- Would calculate
                    0.0 as total_distance_last_30_days_nm,  -- Would calculate
                    0.75 as port_efficiency_score,  -- Would calculate
                    vl.latest_timestamp as last_activity_timestamp,
                    DATETIME_DIFF(CURRENT_TIMESTAMP(), vl.latest_timestamp, HOUR) as data_staleness_hours,
                    DATETIME_DIFF(CURRENT_TIMESTAMP(), vl.latest_timestamp, HOUR) <= 168 as is_active_vessel,
                    CURRENT_DATE() as summary_date
                FROM vessel_latest vl
                LEFT JOIN vessel_info vi ON vl.vesselimo = vi.imo
            ) AS source
            ON target.vesselimo = source.vesselimo AND target.summary_date = source.summary_date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        
        try:
            self.spark.sql(summary_sql)
            logger.debug("Vessel summary updated", vessel_imo=vessel_imo)
        except Exception as e:
            logger.error("Failed to update vessel summary", vessel_imo=vessel_imo, error=str(e))
    
    def _update_port_performance_analytics(self, activity_reports_df: DataFrame) -> None:
        """Update port performance analytics from activity reports."""
        logger.debug("Updating port performance analytics")
        
        # Get affected ports
        affected_ports = (activity_reports_df
                         .filter(sf.col("report_type") == "port")
                         .select("geoareaname")
                         .distinct()
                         .collect())
        
        # Update performance metrics for each affected port
        for port_row in affected_ports:
            port_name = port_row.geoareaname
            self._update_single_port_performance(port_name)
    
    def _update_single_port_performance(self, port_name: str) -> None:
        """Update performance metrics for a single port."""
        logger.debug("Updating port performance", port_name=port_name)
        
        port_performance_sql = f"""
            MERGE INTO gold.port_performance AS target
            USING (
                SELECT 
                    '{port_name}' as port_name,
                    FIRST_VALUE(geoareaid) OVER (ORDER BY entrytime DESC) as port_id,
                    FIRST_VALUE(geoarealevelcountry) OVER (ORDER BY entrytime DESC) as country,
                    FIRST_VALUE(geoarealevelcontinent) OVER (ORDER BY entrytime DESC) as continent,
                    CURRENT_DATE() as analysis_date,
                    DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) as period_start_date,
                    CURRENT_DATE() as period_end_date,
                    COUNT(DISTINCT vesselimo) as unique_vessels,
                    COUNT(*) as total_visits,
                    SUM(timespent) as total_vessel_hours,
                    AVG(timespent) as avg_stay_duration_hours,
                    PERCENTILE_CONT(timespent, 0.5) OVER() as median_stay_duration_hours,
                    MIN(timespent) as min_stay_duration_hours,
                    MAX(timespent) as max_stay_duration_hours,
                    COUNT(DISTINCT vesseltypename) as vessel_types_served,
                    COUNT(DISTINCT vesselimo) / COUNT(*) as vessel_diversity_ratio,
                    COUNT(CASE WHEN timespent < 6 THEN 1 END) as quick_turnarounds,
                    COUNT(CASE WHEN timespent > 72 THEN 1 END) as extended_stays,
                    COUNT(CASE WHEN timespent < 6 THEN 1 END) / COUNT(*) * 100 as quick_turnaround_rate,
                    COUNT(CASE WHEN timespent > 72 THEN 1 END) / COUNT(*) * 100 as extended_stay_rate,
                    COUNT(CASE WHEN EXTRACT(DAYOFWEEK FROM entrytime) IN (1, 7) THEN 1 END) as weekend_arrivals,
                    COUNT(CASE WHEN EXTRACT(HOUR FROM entrytime) BETWEEN 22 AND 6 THEN 1 END) as night_arrivals,
                    COUNT(CASE WHEN EXTRACT(DAYOFWEEK FROM entrytime) IN (1, 7) THEN 1 END) / COUNT(*) * 100 as weekend_arrival_rate,
                    COUNT(CASE WHEN EXTRACT(HOUR FROM entrytime) BETWEEN 22 AND 6 THEN 1 END) / COUNT(*) * 100 as night_arrival_rate,
                    -- Calculate efficiency score
                    ROUND(
                        (COUNT(CASE WHEN timespent < 6 THEN 1 END) / COUNT(*) * 30) +
                        (LEAST(COUNT(DISTINCT vesselimo) / 100, 1) * 20) +
                        (LEAST(COUNT(DISTINCT vesseltypename) / 20, 1) * 20) +
                        (CASE WHEN AVG(timespent) BETWEEN 12 AND 48 THEN 30 ELSE 0 END),
                        2
                    ) as port_efficiency_score,
                    CASE
                        WHEN ROUND(
                            (COUNT(CASE WHEN timespent < 6 THEN 1 END) / COUNT(*) * 30) +
                            (LEAST(COUNT(DISTINCT vesselimo) / 100, 1) * 20) +
                            (LEAST(COUNT(DISTINCT vesseltypename) / 20, 1) * 20) +
                            (CASE WHEN AVG(timespent) BETWEEN 12 AND 48 THEN 30 ELSE 0 END),
                            2
                        ) >= 80 THEN 'EXCELLENT'
                        WHEN ROUND(
                            (COUNT(CASE WHEN timespent < 6 THEN 1 END) / COUNT(*) * 30) +
                            (LEAST(COUNT(DISTINCT vesselimo) / 100, 1) * 20) +
                            (LEAST(COUNT(DISTINCT vesseltypename) / 20, 1) * 20) +
                            (CASE WHEN AVG(timespent) BETWEEN 12 AND 48 THEN 30 ELSE 0 END),
                            2
                        ) >= 60 THEN 'GOOD'
                        WHEN ROUND(
                            (COUNT(CASE WHEN timespent < 6 THEN 1 END) / COUNT(*) * 30) +
                            (LEAST(COUNT(DISTINCT vesselimo) / 100, 1) * 20) +
                            (LEAST(COUNT(DISTINCT vesseltypename) / 20, 1) * 20) +
                            (CASE WHEN AVG(timespent) BETWEEN 12 AND 48 THEN 30 ELSE 0 END),
                            2
                        ) >= 40 THEN 'FAIR'
                        ELSE 'POOR'
                    END as performance_category
                FROM gold.vessel_activity_reports
                WHERE geoareaname = '{port_name}'
                AND report_type = 'port'
                AND business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                GROUP BY geoareaname
            ) AS source
            ON target.port_name = source.port_name AND target.analysis_date = source.analysis_date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        
        try:
            self.spark.sql(port_performance_sql)
            logger.debug("Port performance updated", port_name=port_name)
        except Exception as e:
            logger.error("Failed to update port performance", port_name=port_name, error=str(e))
    
    def _update_compliance_analytics(self, activity_reports_df: DataFrame) -> None:
        """Update compliance analytics from activity reports."""
        logger.debug("Updating compliance analytics")
        
        # Get affected vessels with risk activities
        risk_vessels = (activity_reports_df
                       .filter((sf.col("sanction_zone_flag") == True) | 
                              (sf.col("high_risk_zone_flag") == True))
                       .select("vesselimo")
                       .distinct()
                       .collect())
        
        # Update compliance scores for affected vessels
        for vessel_row in risk_vessels:
            vessel_imo = vessel_row.vesselimo
            self._update_vessel_compliance_score(vessel_imo)
    
    def _update_vessel_compliance_score(self, vessel_imo: str) -> None:
        """Update compliance score for a vessel."""
        logger.debug("Updating compliance score", vessel_imo=vessel_imo)
        
        compliance_sql = f"""
            MERGE INTO gold.vessel_compliance_activity AS target
            USING (
                SELECT 
                    '{vessel_imo}' as vesselimo,
                    FIRST_VALUE(vesselname) OVER (ORDER BY entrytime DESC) as vesselname,
                    FIRST_VALUE(vesseltypename) OVER (ORDER BY entrytime DESC) as vesseltypename,
                    FIRST_VALUE(geoarealevelcountry) OVER (ORDER BY entrytime DESC) as flag_country,
                    CURRENT_DATE() as analysis_date,
                    30 as analysis_period_days,
                    COUNT(*) as total_activities,
                    COUNT(DISTINCT geoareaname) as unique_zones_visited,
                    COUNT(DISTINCT DATE(entrytime)) as active_days,
                    ROUND(COUNT(DISTINCT DATE(entrytime)) / 30.0 * 100, 2) as activity_intensity_percentage,
                    COUNT(CASE WHEN sanction_zone_flag = true THEN 1 END) as sanction_zone_activities,
                    COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) as high_risk_activities,
                    COUNT(CASE WHEN geoareatype = 'seca' THEN 1 END) as seca_zone_activities,
                    COUNT(CASE WHEN report_type = 'port' THEN 1 END) as port_visits,
                    COUNT(CASE WHEN report_type = 'port' AND timespent > 72 THEN 1 END) as extended_port_stays,
                    MAX(CASE WHEN sanction_zone_flag = true THEN entrytime END) as last_sanction_zone_visit,
                    MAX(CASE WHEN high_risk_zone_flag = true THEN entrytime END) as last_hrz_visit,
                    CASE 
                        WHEN MAX(CASE WHEN sanction_zone_flag = true THEN entrytime END) IS NOT NULL 
                        THEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(CASE WHEN sanction_zone_flag = true THEN entrytime END)), DAY)
                        ELSE NULL 
                    END as days_since_sanction_visit,
                    CASE 
                        WHEN MAX(CASE WHEN high_risk_zone_flag = true THEN entrytime END) IS NOT NULL 
                        THEN DATE_DIFF(CURRENT_DATE(), DATE(MAX(CASE WHEN high_risk_zone_flag = true THEN entrytime END)), DAY)
                        ELSE NULL 
                    END as days_since_hrz_visit,
                    SUM(CASE WHEN sanction_zone_flag = true THEN timespent ELSE 0 END) as total_sanction_zone_time_hours,
                    SUM(CASE WHEN high_risk_zone_flag = true THEN timespent ELSE 0 END) as total_hrz_time_hours,
                    -- Risk scoring
                    CASE
                        WHEN COUNT(CASE WHEN sanction_zone_flag = true THEN 1 END) > 0 THEN 100
                        WHEN COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) > 10 THEN 75
                        WHEN COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) > 5 THEN 50
                        WHEN COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) > 0 THEN 25
                        ELSE 0
                    END as activity_risk_score,
                    CASE
                        WHEN COUNT(CASE WHEN sanction_zone_flag = true THEN 1 END) > 0 THEN 'CRITICAL'
                        WHEN COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) > 10 THEN 'HIGH'
                        WHEN COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) > 5 THEN 'MEDIUM'
                        ELSE 'LOW'
                    END as risk_level,
                    -- Compliance flags
                    COUNT(CASE WHEN sanction_zone_flag = true THEN 1 END) > 0 as has_sanction_zone_activity,
                    COUNT(CASE WHEN high_risk_zone_flag = true THEN 1 END) > 5 as has_significant_hrz_activity,
                    DATE_DIFF(CURRENT_DATE(), DATE(MAX(CASE WHEN sanction_zone_flag = true THEN entrytime END)), DAY) <= 30 as recent_sanction_activity,
                    DATE_DIFF(CURRENT_DATE(), DATE(MAX(CASE WHEN high_risk_zone_flag = true THEN entrytime END)), DAY) <= 7 as recent_hrz_activity,
                    -- Activity classification
                    CASE 
                        WHEN COUNT(DISTINCT DATE(entrytime)) > 25 THEN 'VERY_ACTIVE'
                        WHEN COUNT(DISTINCT DATE(entrytime)) > 15 THEN 'ACTIVE'
                        WHEN COUNT(DISTINCT DATE(entrytime)) > 5 THEN 'MODERATE'
                        ELSE 'LOW_ACTIVITY'
                    END as activity_classification
                FROM gold.vessel_activity_reports
                WHERE vesselimo = '{vessel_imo}'
                AND business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
                GROUP BY vesselimo
            ) AS source
            ON target.vesselimo = source.vesselimo AND target.analysis_date = source.analysis_date
            WHEN MATCHED THEN
                UPDATE SET *
            WHEN NOT MATCHED THEN
                INSERT *
        """
        
        try:
            self.spark.sql(compliance_sql)
            logger.debug("Compliance analytics updated", vessel_imo=vessel_imo)
        except Exception as e:
            logger.error("Failed to update compliance analytics", vessel_imo=vessel_imo, error=str(e))
    
    def _trigger_materialized_view_refresh(self, activity_reports_df: DataFrame) -> None:
        """Trigger refresh of relevant materialized views."""
        logger.debug("Triggering materialized view refresh")
        
        # In a real implementation, this would selectively refresh views
        # based on the data that changed
        try:
            # Example: refresh vessel activity summary for affected vessels
            affected_vessels = (activity_reports_df
                               .select("vesselimo")
                               .distinct()
                               .count())
            
            if affected_vessels > 0:
                logger.info("Materialized views need refresh", affected_vessels=affected_vessels)
                # self.materialized_views.refresh_materialized_view("vessel_activity_summary_mv")
                
        except Exception as e:
            logger.error("Failed to trigger materialized view refresh", error=str(e))
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get processing statistics for Gold layer."""
        try:
            # Get record counts for Gold tables
            activity_reports_count = self.spark.sql("SELECT COUNT(*) FROM gold.vessel_activity_reports").collect()[0][0]
            vessel_summary_count = self.spark.sql("SELECT COUNT(*) FROM gold.vessel_summary").collect()[0][0]
            
            # Get recent processing statistics
            recent_reports = self.spark.sql("""
                SELECT COUNT(*) as count
                FROM gold.vessel_activity_reports 
                WHERE gold_processing_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
            """).collect()[0][0]
            
            return {
                "timestamp": datetime.now().isoformat(),
                "table_counts": {
                    "vessel_activity_reports": activity_reports_count,
                    "vessel_summary": vessel_summary_count
                },
                "recent_processing": {
                    "reports_last_hour": recent_reports
                },
                "materialized_views": self.materialized_views.get_materialized_view_status()
            }
            
        except Exception as e:
            logger.error("Failed to get Gold processing statistics", error=str(e))
            return {"error": str(e)}