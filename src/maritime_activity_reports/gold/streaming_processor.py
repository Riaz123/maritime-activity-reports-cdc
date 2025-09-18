"""Gold layer streaming processor for business analytics."""

import structlog
from typing import List, Dict, Any
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session
from .table_setup import GoldTableSetup
from .cdf_processor import GoldCDFProcessor
from .materialized_views import GoldMaterializedViews

logger = structlog.get_logger(__name__)


class GoldStreamingProcessor:
    """Manages all Gold layer streaming queries and analytics processing."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        
        self.table_setup = GoldTableSetup(config, self.spark)
        self.cdf_processor = GoldCDFProcessor(config, self.spark)
        self.materialized_views = GoldMaterializedViews(config, self.spark)
        
        self.active_queries: List[StreamingQuery] = []
        self.query_metrics: Dict[str, Dict] = {}
        
    def initialize_gold_layer(self) -> None:
        """Initialize Gold layer tables, views, and materialized views."""
        logger.info("Initializing Gold layer")
        
        try:
            # Create all Gold tables
            self.table_setup.create_all_gold_tables()
            
            # Create Gold layer views
            self.table_setup.create_gold_views()
            
            # Optimize tables for better performance
            self.table_setup.optimize_gold_tables()
            
            # Create materialized views
            self.materialized_views.create_all_materialized_views()
            
            logger.info("Gold layer initialization completed")
            
        except Exception as e:
            logger.error("Failed to initialize Gold layer", error=str(e))
            raise
    
    def start_all_streaming_queries(self) -> List[StreamingQuery]:
        """Start all Gold layer streaming queries."""
        logger.info("Starting all Gold layer streaming queries")
        
        try:
            # Start Silver movements to activity reports stream
            movements_query = self.cdf_processor.process_silver_movements_to_activity_reports()
            self.active_queries.append(movements_query)
            
            # Start activity reports to analytics stream
            analytics_query = self.cdf_processor.process_activity_reports_to_analytics()
            self.active_queries.append(analytics_query)
            
            # Start materialized view refresh scheduler
            mv_refresh_query = self.start_materialized_view_refresh_scheduler()
            self.active_queries.append(mv_refresh_query)
            
            logger.info("All Gold streaming queries started", 
                       num_queries=len(self.active_queries))
            
            return self.active_queries
            
        except Exception as e:
            logger.error("Failed to start Gold streaming queries", error=str(e))
            self.stop_all_queries()
            raise
    
    def start_materialized_view_refresh_scheduler(self) -> StreamingQuery:
        """Start a scheduler for periodic materialized view refresh."""
        logger.info("Starting materialized view refresh scheduler")
        
        # Create a dummy streaming query that triggers MV refresh periodically
        # In practice, this might be handled by a separate scheduler service
        dummy_stream = (self.spark
                       .readStream
                       .format("rate")
                       .option("rowsPerSecond", 1)
                       .option("numPartitions", 1)
                       .load())
        
        def refresh_materialized_views(microBatchDF, batchId):
            """Periodically refresh materialized views."""
            logger.info("Checking materialized views for refresh", batch_id=batchId)
            
            # Only refresh every 10 batches (roughly every 10 minutes with default trigger)
            if batchId % 10 == 0:
                try:
                    logger.info("Refreshing materialized views", batch_id=batchId)
                    self.materialized_views.refresh_all_materialized_views()
                    logger.info("Materialized views refreshed successfully")
                except Exception as e:
                    logger.error("Failed to refresh materialized views", error=str(e))
        
        query = (dummy_stream
                .writeStream
                .foreachBatch(refresh_materialized_views)
                .option("checkpointLocation", f"{self.config.gold.checkpoint_path}/mv_refresh")
                .trigger(processingTime="60 seconds")
                .start())
        
        logger.info("Materialized view refresh scheduler started", query_id=query.id)
        return query
    
    def monitor_streaming_queries(self, monitoring_interval_seconds: int = 60) -> None:
        """Monitor all active Gold streaming queries."""
        import time
        
        logger.info("Starting Gold streaming query monitoring", 
                   interval_seconds=monitoring_interval_seconds)
        
        try:
            while self.active_queries:
                active_count = 0
                
                for query in self.active_queries[:]:
                    if query.isActive:
                        active_count += 1
                        
                        # Get query metrics
                        metrics = self._get_stream_metrics(query)
                        self.query_metrics[query.id] = metrics
                        
                        logger.info("Gold query status", 
                                   query_id=query.id,
                                   batch_id=metrics.get("batch_id", -1),
                                   input_rows_per_sec=metrics.get("input_rows_per_second", 0),
                                   processed_rows_per_sec=metrics.get("processed_rows_per_second", 0))
                    else:
                        # Query stopped, remove from active list
                        logger.warning("Gold query stopped", 
                                     query_id=query.id,
                                     exception=str(query.exception()) if query.exception() else "Unknown")
                        self.active_queries.remove(query)
                
                if active_count == 0:
                    logger.info("No active Gold queries remaining, stopping monitoring")
                    break
                
                # Log Gold layer health periodically
                if len(self.query_metrics) > 0:
                    self._log_gold_layer_health()
                
                time.sleep(monitoring_interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Gold monitoring interrupted by user")
        except Exception as e:
            logger.error("Error during Gold monitoring", error=str(e))
        finally:
            logger.info("Gold streaming query monitoring stopped")
    
    def stop_all_queries(self) -> None:
        """Stop all active Gold streaming queries."""
        logger.info("Stopping all Gold layer streaming queries")
        
        stopped_count = 0
        for query in self.active_queries:
            try:
                if query.isActive:
                    query.stop()
                    stopped_count += 1
                    logger.info("Gold query stopped", query_id=query.id)
            except Exception as e:
                logger.error("Failed to stop Gold query", query_id=query.id, error=str(e))
        
        self.active_queries.clear()
        logger.info("All Gold queries stopped", stopped_count=stopped_count)
    
    def restart_failed_queries(self) -> List[StreamingQuery]:
        """Restart any failed Gold streaming queries."""
        logger.info("Checking for failed Gold queries to restart")
        
        failed_queries = [q for q in self.active_queries if not q.isActive]
        
        if not failed_queries:
            logger.info("No failed Gold queries found")
            return []
        
        logger.warning("Found failed Gold queries", count=len(failed_queries))
        
        # Remove failed queries from active list
        for query in failed_queries:
            self.active_queries.remove(query)
            logger.warning("Removed failed Gold query", 
                         query_id=query.id,
                         exception=str(query.exception()) if query.exception() else "Unknown")
        
        # Restart all Gold streaming queries
        restarted_queries = self.start_all_streaming_queries()
        
        logger.info("Gold queries restarted", count=len(restarted_queries))
        return restarted_queries
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics for Gold layer."""
        try:
            # Get table metrics
            table_metrics = self.table_setup.get_gold_table_metrics()
            
            # Get streaming metrics
            streaming_metrics = {
                "active_queries": len(self.active_queries),
                "query_details": self.query_metrics
            }
            
            # Get CDF processor statistics
            cdf_stats = self.cdf_processor.get_processing_statistics()
            
            # Get materialized view status
            mv_status = self.materialized_views.get_materialized_view_status()
            
            return {
                "timestamp": self.spark.sql("SELECT current_timestamp()").collect()[0][0],
                "table_metrics": table_metrics,
                "streaming_metrics": streaming_metrics,
                "cdf_processing": cdf_stats,
                "materialized_views": mv_status,
                "gold_layer_status": "healthy" if len(self.active_queries) > 0 else "inactive"
            }
            
        except Exception as e:
            logger.error("Failed to get Gold processing statistics", error=str(e))
            return {"error": str(e)}
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on Gold layer components."""
        health_status = {
            "overall_status": "healthy",
            "components": {},
            "issues": []
        }
        
        try:
            # Check streaming queries
            active_queries = len([q for q in self.active_queries if q.isActive])
            health_status["components"]["streaming_queries"] = {
                "status": "healthy" if active_queries > 0 else "unhealthy",
                "active_count": active_queries,
                "total_count": len(self.active_queries)
            }
            
            if active_queries == 0:
                health_status["issues"].append("No active Gold streaming queries")
                health_status["overall_status"] = "unhealthy"
            
            # Check Gold table accessibility
            try:
                activity_reports_count = self.spark.sql("SELECT COUNT(*) FROM gold.vessel_activity_reports").collect()[0][0]
                health_status["components"]["activity_reports_table"] = {
                    "status": "healthy",
                    "record_count": activity_reports_count
                }
            except Exception as e:
                health_status["components"]["activity_reports_table"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_status["issues"].append(f"Cannot access vessel_activity_reports table: {str(e)}")
                health_status["overall_status"] = "unhealthy"
            
            # Check recent Gold data processing
            try:
                recent_data = self.spark.sql("""
                    SELECT MAX(gold_processing_timestamp) as latest_processing
                    FROM gold.vessel_activity_reports
                """).collect()[0].latest_processing
                
                if recent_data:
                    from datetime import datetime, timedelta
                    if datetime.now() - recent_data > timedelta(hours=4):
                        health_status["issues"].append("No recent Gold data processing detected")
                        health_status["overall_status"] = "degraded"
                    
                    health_status["components"]["data_freshness"] = {
                        "status": "healthy",
                        "latest_processing": str(recent_data)
                    }
                else:
                    health_status["components"]["data_freshness"] = {
                        "status": "unhealthy",
                        "latest_processing": None
                    }
                    health_status["issues"].append("No Gold processed data found")
                    
            except Exception as e:
                health_status["components"]["data_freshness"] = {
                    "status": "unknown",
                    "error": str(e)
                }
            
            # Check materialized views status
            try:
                mv_status = self.materialized_views.get_materialized_view_status()
                health_status["components"]["materialized_views"] = {
                    "status": "healthy",
                    "view_count": len(mv_status.get("views", {}))
                }
            except Exception as e:
                health_status["components"]["materialized_views"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_status["issues"].append("Materialized views status check failed")
            
        except Exception as e:
            health_status["overall_status"] = "unhealthy"
            health_status["issues"].append(f"Gold health check failed: {str(e)}")
        
        return health_status
    
    def _get_stream_metrics(self, query: StreamingQuery) -> Dict[str, Any]:
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
    
    def _log_gold_layer_health(self) -> None:
        """Log Gold layer health summary."""
        try:
            # Get quick health metrics
            total_queries = len(self.active_queries)
            active_queries = len([q for q in self.active_queries if q.isActive])
            
            # Get recent activity reports count
            recent_reports = self.spark.sql("""
                SELECT COUNT(*) as count
                FROM gold.vessel_activity_reports 
                WHERE gold_processing_timestamp >= CURRENT_TIMESTAMP() - INTERVAL 1 HOUR
            """).collect()[0][0]
            
            logger.info("Gold layer health summary",
                       total_queries=total_queries,
                       active_queries=active_queries,
                       recent_reports_hour=recent_reports,
                       health_status="healthy" if active_queries == total_queries else "degraded")
            
        except Exception as e:
            logger.error("Failed to log Gold layer health", error=str(e))
