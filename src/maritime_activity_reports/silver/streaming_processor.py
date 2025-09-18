"""Silver layer streaming processor for real-time CDF processing."""

import structlog
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session
from .cdf_processor import SilverCDFProcessor
from .table_setup import SilverTableSetup

logger = structlog.get_logger(__name__)


class SilverStreamingProcessor:
    """Manages all Silver layer streaming queries and processing."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        
        self.cdf_processor = SilverCDFProcessor(config, self.spark)
        self.table_setup = SilverTableSetup(config, self.spark)
        
        self.active_queries: List[StreamingQuery] = []
        self.query_metrics: Dict[str, Dict] = {}
        
    def initialize_silver_layer(self) -> None:
        """Initialize Silver layer tables and views."""
        logger.info("Initializing Silver layer")
        
        try:
            # Create all Silver tables
            self.table_setup.create_all_silver_tables()
            
            # Create views for easier querying
            self.table_setup.create_silver_views()
            
            # Optimize tables for better performance
            self.table_setup.optimize_silver_tables()
            
            logger.info("Silver layer initialization completed")
            
        except Exception as e:
            logger.error("Failed to initialize Silver layer", error=str(e))
            raise
    
    def start_all_streaming_queries(self) -> List[StreamingQuery]:
        """Start all Silver layer streaming queries."""
        logger.info("Starting all Silver layer streaming queries")
        
        try:
            # Start AIS movements CDF processing
            ais_query = self.cdf_processor.process_ais_movements_cdf_stream()
            self.active_queries.append(ais_query)
            
            # Start vessel metadata CDF processing
            metadata_query = self.cdf_processor.process_vessel_metadata_cdf_stream()
            self.active_queries.append(metadata_query)
            
            # Start spatial enrichment processor (if implemented)
            # spatial_query = self.start_spatial_enrichment_stream()
            # self.active_queries.append(spatial_query)
            
            logger.info("All Silver streaming queries started", 
                       num_queries=len(self.active_queries))
            
            return self.active_queries
            
        except Exception as e:
            logger.error("Failed to start streaming queries", error=str(e))
            self.stop_all_queries()
            raise
    
    def start_spatial_enrichment_stream(self) -> StreamingQuery:
        """Start spatial enrichment streaming processor."""
        logger.info("Starting spatial enrichment stream")
        
        # Read from cleaned vessel movements CDF
        movements_cdf = (self.spark
                        .readStream
                        .format("delta")
                        .option("readChangeFeed", "true")
                        .option("startingVersion", "latest")
                        .table("silver.cleaned_vessel_movements"))
        
        def enrich_spatial_data(microBatchDF, batchId):
            """Enrich movements with spatial zone information."""
            logger.info("Processing spatial enrichment batch", batch_id=batchId)
            
            if microBatchDF.count() == 0:
                return
            
            # Filter for new/updated positions
            new_positions = microBatchDF.filter(
                microBatchDF._change_type.isin(["insert", "update_postimage"])
            )
            
            if new_positions.count() == 0:
                return
            
            # Apply spatial enrichment (simplified - would use actual spatial joins)
            enriched_positions = self._apply_spatial_enrichment(new_positions)
            
            # Update spatial enrichment cache
            self._update_spatial_cache(enriched_positions)
            
            logger.info("Spatial enrichment batch completed", 
                       batch_id=batchId, 
                       processed_count=enriched_positions.count())
        
        query = (movements_cdf
                .writeStream
                .foreachBatch(enrich_spatial_data)
                .option("checkpointLocation", 
                       f"{self.config.silver.checkpoint_path}/spatial_enrichment")
                .trigger(processingTime="60 seconds")
                .start())
        
        logger.info("Spatial enrichment stream started", query_id=query.id)
        return query
    
    def _apply_spatial_enrichment(self, df):
        """Apply spatial zone enrichment to position data."""
        from pyspark.sql import functions as sf
        
        # Simplified spatial enrichment - in production would use actual spatial joins
        enriched_df = (df
                      .withColumn("enriched_zone_country", 
                                sf.when((sf.col("latitude").between(49, 61)) & 
                                       (sf.col("longitude").between(-8, 2)), "United Kingdom")
                                .when((sf.col("latitude").between(54, 58)) & 
                                      (sf.col("longitude").between(3, 15)), "Germany")
                                .when((sf.col("latitude").between(42, 51)) & 
                                      (sf.col("longitude").between(-5, 8)), "France")
                                .otherwise("International Waters"))
                      
                      .withColumn("enriched_zone_continent",
                                sf.when(sf.col("enriched_zone_country").isin(
                                    ["United Kingdom", "Germany", "France"]), "Europe")
                                .otherwise("Unknown"))
                      
                      # Add more spatial enrichment logic here
                      .withColumn("spatial_enrichment_timestamp", sf.current_timestamp()))
        
        return enriched_df
    
    def _update_spatial_cache(self, df):
        """Update the spatial enrichment cache."""
        from pyspark.sql import functions as sf
        
        # Create position hash for caching
        cache_updates = (df
                        .withColumn("position_hash", 
                                  sf.hash(sf.col("latitude"), sf.col("longitude")))
                        .select("position_hash", "latitude", "longitude", 
                               "enriched_zone_country", "enriched_zone_continent")
                        .distinct())
        
        # Merge into cache table
        cache_updates.createOrReplaceTempView("cache_updates")
        
        merge_sql = """
            MERGE INTO silver.spatial_enrichment_cache AS target
            USING cache_updates AS source
            ON target.position_hash = source.position_hash
            WHEN MATCHED THEN
                UPDATE SET 
                    last_accessed = current_timestamp(),
                    access_count = target.access_count + 1
            WHEN NOT MATCHED THEN
                INSERT (
                    position_hash, latitude, longitude, 
                    country_zone, continent,
                    first_seen, last_accessed, access_count,
                    silver_processing_timestamp
                ) VALUES (
                    source.position_hash, source.latitude, source.longitude,
                    source.enriched_zone_country, source.enriched_zone_continent,
                    current_timestamp(), current_timestamp(), 1,
                    current_timestamp()
                )
        """
        
        try:
            self.spark.sql(merge_sql)
            logger.debug("Spatial cache updated successfully")
        except Exception as e:
            logger.error("Failed to update spatial cache", error=str(e))
    
    def monitor_streaming_queries(self, monitoring_interval_seconds: int = 30) -> None:
        """Monitor all active streaming queries."""
        import time
        
        logger.info("Starting streaming query monitoring", 
                   interval_seconds=monitoring_interval_seconds)
        
        try:
            while self.active_queries:
                active_count = 0
                
                for query in self.active_queries[:]:  # Create copy to avoid modification during iteration
                    if query.isActive:
                        active_count += 1
                        
                        # Get query metrics
                        metrics = self.cdf_processor.get_stream_metrics(query)
                        self.query_metrics[query.id] = metrics
                        
                        logger.info("Query status", 
                                   query_id=query.id,
                                   batch_id=metrics.get("batch_id", -1),
                                   input_rows_per_sec=metrics.get("input_rows_per_second", 0),
                                   processed_rows_per_sec=metrics.get("processed_rows_per_second", 0))
                    else:
                        # Query stopped, remove from active list
                        logger.warning("Query stopped", 
                                     query_id=query.id,
                                     exception=str(query.exception()) if query.exception() else "Unknown")
                        self.active_queries.remove(query)
                
                if active_count == 0:
                    logger.info("No active queries remaining, stopping monitoring")
                    break
                
                time.sleep(monitoring_interval_seconds)
                
        except KeyboardInterrupt:
            logger.info("Monitoring interrupted by user")
        except Exception as e:
            logger.error("Error during monitoring", error=str(e))
        finally:
            logger.info("Streaming query monitoring stopped")
    
    def stop_all_queries(self) -> None:
        """Stop all active streaming queries."""
        logger.info("Stopping all Silver layer streaming queries")
        
        stopped_count = 0
        for query in self.active_queries:
            try:
                if query.isActive:
                    query.stop()
                    stopped_count += 1
                    logger.info("Query stopped", query_id=query.id)
            except Exception as e:
                logger.error("Failed to stop query", query_id=query.id, error=str(e))
        
        self.active_queries.clear()
        logger.info("All queries stopped", stopped_count=stopped_count)
    
    def restart_failed_queries(self) -> List[StreamingQuery]:
        """Restart any failed streaming queries."""
        logger.info("Checking for failed queries to restart")
        
        failed_queries = [q for q in self.active_queries if not q.isActive]
        
        if not failed_queries:
            logger.info("No failed queries found")
            return []
        
        logger.warning("Found failed queries", count=len(failed_queries))
        
        # Remove failed queries from active list
        for query in failed_queries:
            self.active_queries.remove(query)
            logger.warning("Removed failed query", 
                         query_id=query.id,
                         exception=str(query.exception()) if query.exception() else "Unknown")
        
        # Restart all streaming queries
        restarted_queries = self.start_all_streaming_queries()
        
        logger.info("Queries restarted", count=len(restarted_queries))
        return restarted_queries
    
    def get_processing_statistics(self) -> Dict[str, Any]:
        """Get comprehensive processing statistics for Silver layer."""
        try:
            # Get table metrics
            table_metrics = self.table_setup.get_silver_table_metrics()
            
            # Get streaming metrics
            streaming_metrics = {
                "active_queries": len(self.active_queries),
                "query_details": self.query_metrics
            }
            
            # Get data quality metrics
            quality_stats = self._get_data_quality_statistics()
            
            return {
                "timestamp": self.spark.sql("SELECT current_timestamp()").collect()[0][0],
                "table_metrics": table_metrics,
                "streaming_metrics": streaming_metrics,
                "data_quality": quality_stats,
                "silver_layer_status": "healthy" if len(self.active_queries) > 0 else "inactive"
            }
            
        except Exception as e:
            logger.error("Failed to get processing statistics", error=str(e))
            return {"error": str(e)}
    
    def _get_data_quality_statistics(self) -> Dict[str, Any]:
        """Get data quality statistics for Silver layer tables."""
        try:
            # Quality stats for cleaned vessel movements
            movements_quality = self.spark.sql("""
                SELECT 
                    COUNT(*) as total_records,
                    AVG(data_quality_score) as avg_quality_score,
                    COUNT(CASE WHEN data_quality_score >= 0.9 THEN 1 END) as excellent_quality,
                    COUNT(CASE WHEN data_quality_score >= 0.7 THEN 1 END) as good_quality,
                    COUNT(CASE WHEN data_quality_score < 0.5 THEN 1 END) as poor_quality,
                    COUNT(DISTINCT imo) as unique_vessels,
                    MAX(silver_processing_timestamp) as latest_processing
                FROM silver.cleaned_vessel_movements
                WHERE DATE(silver_processing_timestamp) = CURRENT_DATE()
            """).collect()[0].asDict()
            
            # Quality stats for vessel master
            vessel_master_quality = self.spark.sql("""
                SELECT 
                    COUNT(*) as total_vessels,
                    COUNT(CASE WHEN is_current_record = true THEN 1 END) as current_vessels,
                    AVG(data_quality_score) as avg_quality_score,
                    COUNT(CASE WHEN sanction_flag = true THEN 1 END) as sanctioned_vessels
                FROM silver.vessel_master
            """).collect()[0].asDict()
            
            return {
                "movements": movements_quality,
                "vessel_master": vessel_master_quality
            }
            
        except Exception as e:
            logger.error("Failed to get quality statistics", error=str(e))
            return {"error": str(e)}
    
    def health_check(self) -> Dict[str, Any]:
        """Perform health check on Silver layer components."""
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
                health_status["issues"].append("No active streaming queries")
                health_status["overall_status"] = "unhealthy"
            
            # Check table accessibility
            try:
                movements_count = self.spark.sql("SELECT COUNT(*) FROM silver.cleaned_vessel_movements").collect()[0][0]
                health_status["components"]["cleaned_movements_table"] = {
                    "status": "healthy",
                    "record_count": movements_count
                }
            except Exception as e:
                health_status["components"]["cleaned_movements_table"] = {
                    "status": "unhealthy",
                    "error": str(e)
                }
                health_status["issues"].append(f"Cannot access cleaned_vessel_movements table: {str(e)}")
                health_status["overall_status"] = "unhealthy"
            
            # Check recent data processing
            try:
                recent_data = self.spark.sql("""
                    SELECT MAX(silver_processing_timestamp) as latest_processing
                    FROM silver.cleaned_vessel_movements
                """).collect()[0].latest_processing
                
                if recent_data:
                    from datetime import datetime, timedelta
                    if datetime.now() - recent_data > timedelta(hours=2):
                        health_status["issues"].append("No recent data processing detected")
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
                    health_status["issues"].append("No processed data found")
                    
            except Exception as e:
                health_status["components"]["data_freshness"] = {
                    "status": "unknown",
                    "error": str(e)
                }
            
        except Exception as e:
            health_status["overall_status"] = "unhealthy"
            health_status["issues"].append(f"Health check failed: {str(e)}")
        
        return health_status
