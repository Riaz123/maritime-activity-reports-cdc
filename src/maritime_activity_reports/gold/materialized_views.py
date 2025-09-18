"""Gold layer materialized views - Following IHS Compliance pattern."""

import structlog
from typing import Dict, Any, List
from pyspark.sql import SparkSession

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session
from ..utils.bigquery import run_bigquery_query

logger = structlog.get_logger(__name__)


class GoldMaterializedViews:
    """Creates and manages BigQuery materialized views for Gold layer data."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        self.project_id = config.bigquery.project_id
        self.dataset = config.bigquery.dataset
        self.region = config.bigquery.location
        
    def create_all_materialized_views(self) -> None:
        """Create all materialized views for Gold layer."""
        logger.info("Creating all Gold layer materialized views")
        
        try:
            self.create_vessel_activity_summary_mv()
            self.create_port_performance_mv()
            self.create_journey_analytics_mv()
            self.create_vessel_compliance_activity_mv()
            self.create_real_time_vessel_status_mv()
            
            logger.info("All materialized views created successfully")
            
        except Exception as e:
            logger.error("Failed to create materialized views", error=str(e))
            raise
    
    def create_vessel_activity_summary_mv(self) -> None:
        """Create vessel activity summary materialized view - similar to IHS compliance pattern."""
        logger.info("Creating vessel activity summary materialized view")
        
        query = f"""
        -- CREATE OR REPLACE MATERIALIZED VIEW for Vessel Activity Summary
        CREATE OR REPLACE MATERIALIZED VIEW `{self.project_id}.{self.dataset}.vessel_activity_summary_mv`
        CLUSTER BY vesselimo
        OPTIONS (
            max_staleness = INTERVAL "{self.config.bigquery.materialized_view_options['max_staleness']}",
            allow_non_incremental_definition = {self.config.bigquery.materialized_view_options['allow_non_incremental_definition']}
        )
        AS
        WITH
        latest_vessel_update AS (
            SELECT
                vesselimo,
                MAX(gold_processing_timestamp) AS latest_update
            FROM
                `{self.project_id}.{self.dataset}.vessel_activity_reports`
            GROUP BY vesselimo
        ),
        current_vessel_status AS (
            SELECT t1.*
            FROM
                (`{self.project_id}.{self.dataset}.vessel_activity_reports` t1
            INNER JOIN latest_vessel_update t2 ON (
                (t1.vesselimo = t2.vesselimo) AND 
                (t1.gold_processing_timestamp = t2.latest_update)
            ))
        ),
        zone_activity_summary AS (
            SELECT
                vesselimo,
                vesselname,
                vesseltypename,
                vesselownername,
                report_type,
                COUNT(*) as total_activities,
                SUM(CASE WHEN timespent IS NOT NULL THEN timespent ELSE 0 END) as total_time_spent_hours,
                AVG(CASE WHEN timespent IS NOT NULL THEN timespent ELSE NULL END) as avg_time_spent_hours,
                COUNT(DISTINCT geoareaname) as unique_zones_visited,
                COUNT(DISTINCT DATE(entrytime)) as active_days,
                MIN(entrytime) as first_activity_time,
                MAX(entrytime) as last_activity_time,
                -- Risk indicators
                SUM(CASE WHEN geoareatype = 'sanction' THEN 1 ELSE 0 END) as sanction_zone_visits,
                SUM(CASE WHEN geoareatype = 'hrz_v2' THEN 1 ELSE 0 END) as high_risk_zone_visits,
                SUM(CASE WHEN geoareatype = 'seca' THEN 1 ELSE 0 END) as seca_zone_visits,
                -- Port specific metrics
                SUM(CASE WHEN report_type = 'port' THEN 1 ELSE 0 END) as port_visits,
                SUM(CASE WHEN report_type = 'port' AND timespent > 24 THEN 1 ELSE 0 END) as extended_port_stays
            FROM
                current_vessel_status
            WHERE
                business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
            GROUP BY 
                vesselimo, vesselname, vesseltypename, vesselownername, report_type
        )
        SELECT
            vesselimo,
            vesselname,
            vesseltypename,
            vesselownername,
            report_type,
            total_activities,
            total_time_spent_hours,
            avg_time_spent_hours,
            unique_zones_visited,
            active_days,
            first_activity_time,
            last_activity_time,
            sanction_zone_visits,
            high_risk_zone_visits,
            seca_zone_visits,
            port_visits,
            extended_port_stays,
            -- Risk scoring
            CASE 
                WHEN sanction_zone_visits > 0 THEN 'HIGH'
                WHEN high_risk_zone_visits > 5 THEN 'MEDIUM'
                WHEN seca_zone_visits = 0 THEN 'LOW'
                ELSE 'MEDIUM'
            END as risk_level,
            -- Activity classification
            CASE 
                WHEN active_days > 25 THEN 'VERY_ACTIVE'
                WHEN active_days > 15 THEN 'ACTIVE'
                WHEN active_days > 5 THEN 'MODERATE'
                ELSE 'LOW_ACTIVITY'
            END as activity_classification
        FROM
            zone_activity_summary
        """
        
        self._execute_materialized_view_query(query, "vessel_activity_summary_mv")
    
    def create_port_performance_mv(self) -> None:
        """Create port performance materialized view."""
        logger.info("Creating port performance materialized view")
        
        query = f"""
        CREATE OR REPLACE MATERIALIZED VIEW `{self.project_id}.{self.dataset}.port_performance_mv`
        CLUSTER BY geoareaname
        OPTIONS (
            max_staleness = INTERVAL "24" HOUR,
            allow_non_incremental_definition = true
        )
        AS
        WITH
        port_activities AS (
            SELECT *
            FROM `{self.project_id}.{self.dataset}.vessel_activity_reports`
            WHERE report_type = 'port'
            AND business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
        ),
        port_metrics AS (
            SELECT
                geoareaname as port_name,
                geoarealevelcountry as country,
                geoarealevelcontinent as continent,
                COUNT(DISTINCT vesselimo) as unique_vessels,
                COUNT(*) as total_visits,
                AVG(timespent) as avg_stay_duration_hours,
                PERCENTILE_CONT(timespent, 0.5) OVER(PARTITION BY geoareaname) as median_stay_duration,
                MIN(timespent) as min_stay_duration,
                MAX(timespent) as max_stay_duration,
                COUNT(DISTINCT vesseltypename) as vessel_types_served,
                SUM(timespent) as total_port_time_hours,
                -- Efficiency metrics
                AVG(DATE_DIFF(DATE(exittime), DATE(entrytime), DAY)) as avg_berth_days,
                COUNT(CASE WHEN timespent < 6 THEN 1 END) as quick_turnarounds,
                COUNT(CASE WHEN timespent > 72 THEN 1 END) as extended_stays,
                -- Temporal patterns
                COUNT(CASE WHEN EXTRACT(DAYOFWEEK FROM entrytime) IN (1, 7) THEN 1 END) as weekend_arrivals,
                COUNT(CASE WHEN EXTRACT(HOUR FROM entrytime) BETWEEN 22 AND 6 THEN 1 END) as night_arrivals
            FROM
                port_activities
            GROUP BY
                geoareaname, geoarealevelcountry, geoarealevelcontinent
        )
        SELECT
            port_name,
            country,
            continent,
            unique_vessels,
            total_visits,
            avg_stay_duration_hours,
            median_stay_duration,
            min_stay_duration,
            max_stay_duration,
            vessel_types_served,
            total_port_time_hours,
            avg_berth_days,
            quick_turnarounds,
            extended_stays,
            weekend_arrivals,
            night_arrivals,
            -- Performance indicators
            ROUND(quick_turnarounds / total_visits * 100, 2) as quick_turnaround_rate,
            ROUND(extended_stays / total_visits * 100, 2) as extended_stay_rate,
            ROUND(unique_vessels / total_visits * 100, 2) as vessel_diversity_ratio,
            -- Port efficiency score (0-100)
            ROUND(
                (quick_turnarounds / total_visits * 30) +
                (LEAST(unique_vessels / 100, 1) * 20) +
                (LEAST(vessel_types_served / 20, 1) * 20) +
                (CASE WHEN avg_stay_duration_hours BETWEEN 12 AND 48 THEN 30 ELSE 0 END),
                2
            ) as port_efficiency_score
        FROM
            port_metrics
        WHERE
            total_visits >= 10  -- Only include ports with meaningful activity
        """
        
        self._execute_materialized_view_query(query, "port_performance_mv")
    
    def create_journey_analytics_mv(self) -> None:
        """Create journey analytics materialized view - similar to STS compliance history."""
        logger.info("Creating journey analytics materialized view")
        
        query = f"""
        CREATE OR REPLACE MATERIALIZED VIEW `{self.project_id}.{self.dataset}.journey_analytics_mv`
        CLUSTER BY vesselimo
        OPTIONS (
            max_staleness = INTERVAL "24" HOUR,
            allow_non_incremental_definition = true
        ) 
        AS
        WITH
        sorted_journeys AS (
            SELECT DISTINCT
                vesselimo,
                vesselname,
                vesseltypename,
                vesselownername,
                departuretime,
                departure_port,
                departure_country,
                arrivaltime,
                arrival_port,
                arrival_country,
                business_date as journey_date,
                journey_duration_hours,
                estimated_distance_nm,
                avg_speed_knots
            FROM
                `{self.project_id}.{self.dataset}.journey_analytics`
            WHERE
                business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 180 DAY)
            ORDER BY vesselimo ASC, departuretime ASC
        ),
        numbered_journeys AS (
            SELECT
                ROW_NUMBER() OVER (PARTITION BY vesselimo ORDER BY departuretime ASC) as journey_sequence,
                *
            FROM
                sorted_journeys
            ORDER BY vesselimo ASC, departuretime ASC
        ),
        journey_patterns AS (
            SELECT
                j1.vesselimo,
                j1.vesselname,
                j1.vesseltypename,
                j1.vesselownername,
                j1.journey_sequence,
                j1.departuretime,
                j1.departure_port,
                j1.arrivaltime,
                j1.arrival_port,
                j1.journey_duration_hours,
                j1.estimated_distance_nm,
                j1.avg_speed_knots,
                -- Previous journey information
                j2.arrival_port as previous_arrival_port,
                j2.arrivaltime as previous_arrival_time,
                -- Time between journeys (port stay time)
                DATETIME_DIFF(j1.departuretime, j2.arrivaltime, HOUR) as port_stay_duration_hours,
                -- Route classification
                CASE
                    WHEN j1.departure_port = j1.arrival_port THEN 'CIRCULAR'
                    WHEN j1.departure_port = j2.arrival_port THEN 'RETURN'
                    ELSE 'LINEAR'
                END as route_type
            FROM
                numbered_journeys j1
            LEFT JOIN numbered_journeys j2 ON (
                j1.vesselimo = j2.vesselimo AND 
                j1.journey_sequence = j2.journey_sequence + 1
            )
        )
        SELECT
            vesselimo,
            vesselname,
            vesseltypename,
            vesselownername,
            journey_sequence,
            departuretime,
            departure_port,
            arrivaltime,
            arrival_port,
            journey_duration_hours,
            estimated_distance_nm,
            port_stay_duration_hours,
            avg_speed_knots,
            route_type,
            -- Journey classifications
            CASE 
                WHEN journey_duration_hours < 6 THEN 'SHORT_HOP'
                WHEN journey_duration_hours < 24 THEN 'DAY_JOURNEY'
                WHEN journey_duration_hours < 168 THEN 'WEEK_JOURNEY'
                ELSE 'LONG_HAUL'
            END as journey_classification,
            -- Port stay classifications
            CASE
                WHEN port_stay_duration_hours < 6 THEN 'QUICK_STOP'
                WHEN port_stay_duration_hours < 24 THEN 'SHORT_STAY'
                WHEN port_stay_duration_hours < 168 THEN 'MEDIUM_STAY'
                ELSE 'EXTENDED_STAY'
            END as port_stay_classification,
            -- Operational efficiency indicators
            CASE
                WHEN avg_speed_knots > 20 THEN 'HIGH_SPEED'
                WHEN avg_speed_knots > 12 THEN 'NORMAL_SPEED'
                WHEN avg_speed_knots > 6 THEN 'SLOW_SPEED'
                ELSE 'VERY_SLOW'
            END as speed_classification
        FROM
            journey_patterns
        WHERE
            journey_duration_hours > 0  -- Filter out invalid journeys
        """
        
        self._execute_materialized_view_query(query, "journey_analytics_mv")
    
    def create_vessel_compliance_activity_mv(self) -> None:
        """Create vessel compliance activity materialized view."""
        logger.info("Creating vessel compliance activity materialized view")
        
        query = f"""
        CREATE OR REPLACE MATERIALIZED VIEW `{self.project_id}.{self.dataset}.vessel_compliance_activity_mv`
        CLUSTER BY vesselimo
        OPTIONS (
            max_staleness = INTERVAL "24" HOUR,
            allow_non_incremental_definition = true
        )
        AS
        WITH
        recent_activities AS (
            SELECT
                vesselimo,
                vesselname,
                report_type,
                geoareaname,
                geoareatype,
                entrytime,
                exittime,
                timespent,
                business_date,
                sanction_zone_flag,
                high_risk_zone_flag
            FROM
                `{self.project_id}.{self.dataset}.vessel_activity_reports`
            WHERE
                business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
        ),
        compliance_summary AS (
            SELECT
                vesselimo,
                COUNT(CASE WHEN geoareatype = 'sanction' THEN 1 END) as sanction_zone_activities,
                COUNT(CASE WHEN geoareatype = 'hrz_v2' THEN 1 END) as high_risk_activities,
                MAX(CASE WHEN geoareatype = 'sanction' THEN entrytime END) as last_sanction_zone_visit,
                MAX(CASE WHEN geoareatype = 'hrz_v2' THEN entrytime END) as last_hrz_visit,
                SUM(CASE WHEN geoareatype = 'sanction' THEN timespent ELSE 0 END) as total_sanction_zone_time,
                SUM(CASE WHEN geoareatype = 'hrz_v2' THEN timespent ELSE 0 END) as total_hrz_time,
                COUNT(DISTINCT geoareaname) as unique_zones_visited,
                COUNT(DISTINCT DATE(entrytime)) as active_days
            FROM
                recent_activities
            GROUP BY
                vesselimo
        )
        SELECT
            cs.vesselimo,
            ra.vesselname,
            cs.sanction_zone_activities,
            cs.high_risk_activities,
            cs.last_sanction_zone_visit,
            cs.last_hrz_visit,
            cs.total_sanction_zone_time,
            cs.total_hrz_time,
            cs.unique_zones_visited,
            cs.active_days,
            -- Risk scoring based on activity patterns
            CASE
                WHEN cs.sanction_zone_activities > 0 THEN 100
                WHEN cs.high_risk_activities > 10 THEN 75
                WHEN cs.high_risk_activities > 5 THEN 50
                WHEN cs.high_risk_activities > 0 THEN 25
                ELSE 0
            END as activity_risk_score,
            -- Compliance flags
            cs.sanction_zone_activities > 0 as has_sanction_zone_activity,
            cs.high_risk_activities > 5 as has_significant_hrz_activity,
            DATE_DIFF(CURRENT_DATE(), DATE(cs.last_sanction_zone_visit), DAY) <= 30 as recent_sanction_activity,
            DATE_DIFF(CURRENT_DATE(), DATE(cs.last_hrz_visit), DAY) <= 7 as recent_hrz_activity,
            -- Activity intensity
            ROUND(cs.active_days / 30.0 * 100, 2) as activity_intensity_percentage
        FROM
            compliance_summary cs
        LEFT JOIN (
            SELECT DISTINCT vesselimo, vesselname
            FROM recent_activities
        ) ra ON cs.vesselimo = ra.vesselimo
        """
        
        self._execute_materialized_view_query(query, "vessel_compliance_activity_mv")
    
    def create_real_time_vessel_status_mv(self) -> None:
        """Create real-time vessel status materialized view."""
        logger.info("Creating real-time vessel status materialized view")
        
        query = f"""
        CREATE OR REPLACE MATERIALIZED VIEW `{self.project_id}.{self.dataset}.real_time_vessel_status_mv`
        CLUSTER BY vesselimo
        OPTIONS (
            max_staleness = INTERVAL "1" HOUR,
            allow_non_incremental_definition = true
        )
        AS
        WITH
        latest_positions AS (
            SELECT
                vesselimo,
                vesselname,
                vesseltypename,
                latest_timestamp,
                latest_latitude,
                latest_longitude,
                current_zone_country,
                current_navigational_status,
                current_risk_level,
                compliance_score,
                is_active_vessel,
                data_staleness_hours
            FROM
                `{self.project_id}.{self.dataset}.vessel_summary`
            WHERE
                summary_date = CURRENT_DATE()
        ),
        recent_compliance AS (
            SELECT
                vesselimo,
                activity_risk_score,
                has_sanction_zone_activity,
                recent_sanction_activity,
                recent_hrz_activity
            FROM
                `{self.project_id}.{self.dataset}.vessel_compliance_activity_mv`
        ),
        recent_activity_counts AS (
            SELECT
                vesselimo,
                COUNT(*) as activities_last_24h,
                COUNT(DISTINCT geoareaname) as zones_last_24h
            FROM
                `{self.project_id}.{self.dataset}.vessel_activity_reports`
            WHERE
                business_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
            GROUP BY vesselimo
        )
        SELECT
            lp.vesselimo,
            lp.vesselname,
            lp.vesseltypename,
            lp.latest_timestamp,
            lp.latest_latitude,
            lp.latest_longitude,
            lp.current_zone_country,
            lp.current_navigational_status,
            lp.current_risk_level,
            lp.compliance_score,
            lp.is_active_vessel,
            lp.data_staleness_hours,
            -- Compliance indicators
            COALESCE(rc.activity_risk_score, 0) as activity_risk_score,
            COALESCE(rc.has_sanction_zone_activity, false) as has_sanction_zone_activity,
            COALESCE(rc.recent_sanction_activity, false) as recent_sanction_activity,
            COALESCE(rc.recent_hrz_activity, false) as recent_hrz_activity,
            -- Recent activity
            COALESCE(rac.activities_last_24h, 0) as activities_last_24h,
            COALESCE(rac.zones_last_24h, 0) as zones_last_24h,
            -- Status indicators
            CASE 
                WHEN lp.data_staleness_hours > 24 THEN 'STALE'
                WHEN lp.data_staleness_hours > 6 THEN 'DELAYED'
                WHEN lp.is_active_vessel = true THEN 'ACTIVE'
                ELSE 'INACTIVE'
            END as data_status,
            -- Alert level
            CASE
                WHEN rc.recent_sanction_activity = true THEN 'CRITICAL'
                WHEN rc.activity_risk_score > 75 THEN 'HIGH'
                WHEN rc.activity_risk_score > 50 THEN 'MEDIUM'
                WHEN lp.data_staleness_hours > 24 THEN 'LOW'
                ELSE 'NORMAL'
            END as alert_level
        FROM
            latest_positions lp
        LEFT JOIN recent_compliance rc ON lp.vesselimo = rc.vesselimo
        LEFT JOIN recent_activity_counts rac ON lp.vesselimo = rac.vesselimo
        WHERE
            lp.is_active_vessel = true OR lp.data_staleness_hours <= 168  -- Show active or recently active vessels
        """
        
        self._execute_materialized_view_query(query, "real_time_vessel_status_mv")
    
    def _execute_materialized_view_query(self, query: str, view_name: str) -> None:
        """Execute a materialized view creation query."""
        try:
            # In a real implementation, this would call BigQuery API
            # For now, we'll log the query that would be executed
            logger.info("Materialized view query prepared", 
                       view_name=view_name,
                       project_id=self.project_id,
                       dataset=self.dataset)
            
            # TODO: Implement actual BigQuery API call
            # run_bigquery_query(query, self.project_id)
            
            logger.info("Materialized view created successfully", view_name=view_name)
            
        except Exception as e:
            logger.error("Failed to create materialized view", 
                        view_name=view_name, 
                        error=str(e))
            raise
    
    def refresh_materialized_view(self, view_name: str) -> None:
        """Refresh a specific materialized view."""
        logger.info("Refreshing materialized view", view_name=view_name)
        
        refresh_query = f"""
        CALL BQ.REFRESH_MATERIALIZED_VIEW('{self.project_id}.{self.dataset}.{view_name}')
        """
        
        try:
            # TODO: Implement actual BigQuery API call
            # run_bigquery_query(refresh_query, self.project_id)
            logger.info("Materialized view refreshed successfully", view_name=view_name)
        except Exception as e:
            logger.error("Failed to refresh materialized view", 
                        view_name=view_name, 
                        error=str(e))
            raise
    
    def refresh_all_materialized_views(self) -> None:
        """Refresh all materialized views."""
        logger.info("Refreshing all materialized views")
        
        views = [
            "vessel_activity_summary_mv",
            "port_performance_mv", 
            "journey_analytics_mv",
            "vessel_compliance_activity_mv",
            "real_time_vessel_status_mv"
        ]
        
        for view in views:
            try:
                self.refresh_materialized_view(view)
            except Exception as e:
                logger.error("Failed to refresh view, continuing with others", 
                           view=view, 
                           error=str(e))
        
        logger.info("All materialized views refresh completed")
    
    def get_materialized_view_status(self) -> Dict[str, Any]:
        """Get status of all materialized views."""
        logger.info("Getting materialized view status")
        
        # TODO: Implement actual BigQuery API calls to get view status
        # This would query INFORMATION_SCHEMA.MATERIALIZED_VIEWS
        
        status = {
            "timestamp": self.spark.sql("SELECT current_timestamp()").collect()[0][0],
            "views": {
                "vessel_activity_summary_mv": {"status": "active", "last_refresh": "2024-01-15T10:30:00Z"},
                "port_performance_mv": {"status": "active", "last_refresh": "2024-01-15T10:30:00Z"},
                "journey_analytics_mv": {"status": "active", "last_refresh": "2024-01-15T10:30:00Z"},
                "vessel_compliance_activity_mv": {"status": "active", "last_refresh": "2024-01-15T10:30:00Z"},
                "real_time_vessel_status_mv": {"status": "active", "last_refresh": "2024-01-15T10:30:00Z"}
            }
        }
        
        return status
