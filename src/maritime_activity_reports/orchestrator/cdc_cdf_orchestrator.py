"""CDC/CDF orchestrator for coordinating data flow across all layers."""

import structlog
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQuery

from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session
from ..bronze.table_setup import BronzeTableSetup
from ..bronze.cdc_ingestion import BronzeCDCLayer
from ..silver.table_setup import SilverTableSetup
from ..silver.streaming_processor import SilverStreamingProcessor

logger = structlog.get_logger(__name__)


class CDCCDFOrchestrator:
    """Orchestrates CDC/CDF processing across Bronze, Silver, and Gold layers."""
    
    def __init__(self, config: MaritimeConfig, spark_session: SparkSession = None):
        self.config = config
        self.spark = spark_session or get_spark_session(config)
        
        # Initialize layer components
        self.bronze_setup = BronzeTableSetup(config, self.spark)
        self.bronze_cdc = BronzeCDCLayer(config, self.spark)
        self.silver_setup = SilverTableSetup(config, self.spark)
        self.silver_processor = SilverStreamingProcessor(config, self.spark)
        
        # Track active streaming queries
        self.active_queries: List[StreamingQuery] = []
        
    def setup_all_cdf_tables(self) -> None:
        """Setup all Delta tables with CDF enabled across all layers."""
        logger.info("Setting up all CDF tables across layers")
        
        try:
            # Create databases
            self.bronze_setup.create_databases()
            
            # Setup Bronze layer
            logger.info("Setting up Bronze layer tables")
            self.bronze_setup.create_all_bronze_tables()
            
            # Setup Silver layer
            logger.info("Setting up Silver layer tables")
            self.silver_setup.create_all_silver_tables()
            self.silver_setup.create_silver_views()
            
            # TODO: Setup Gold layer when implemented
            # self.gold_setup.create_all_gold_tables()
            
            logger.info("All CDF tables setup completed successfully")
            
        except Exception as e:
            logger.error("Failed to setup CDF tables", error=str(e))
            raise
    
    def start_all_streaming_queries(self) -> List[StreamingQuery]:
        """Start all streaming queries across all layers."""
        logger.info("Starting all streaming queries")
        
        try:
            # Initialize Silver layer first
            self.silver_processor.initialize_silver_layer()
            
            # Start Silver layer streaming
            silver_queries = self.silver_processor.start_all_streaming_queries()
            self.active_queries.extend(silver_queries)
            
            # TODO: Start Gold layer streaming when implemented
            # gold_queries = self.gold_processor.start_all_streaming_queries()
            # self.active_queries.extend(gold_queries)
            
            logger.info("All streaming queries started successfully", 
                       total_queries=len(self.active_queries))
            
            return self.active_queries
            
        except Exception as e:
            logger.error("Failed to start streaming queries", error=str(e))
            self.stop_all_streams()
            raise
    
    def start_silver_streams(self) -> List[StreamingQuery]:
        """Start only Silver layer streaming queries."""
        logger.info("Starting Silver layer streaming queries")
        
        try:
            self.silver_processor.initialize_silver_layer()
            silver_queries = self.silver_processor.start_all_streaming_queries()
            self.active_queries.extend(silver_queries)
            
            logger.info("Silver streaming queries started", count=len(silver_queries))
            return silver_queries
            
        except Exception as e:
            logger.error("Failed to start Silver streams", error=str(e))
            raise
    
    def start_gold_streams(self) -> List[StreamingQuery]:
        """Start only Gold layer streaming queries."""
        logger.info("Starting Gold layer streaming queries")
        
        # TODO: Implement when Gold layer is ready
        logger.warning("Gold layer streaming not yet implemented")
        return []
    
    def ingest_ais_cdc_from_file(self, file_path: Path) -> None:
        """Ingest AIS CDC data from a file."""
        logger.info("Ingesting AIS CDC data from file", file_path=str(file_path))
        
        try:
            # Read data from file
            df = self.spark.read.json(str(file_path))
            
            # Ingest into Bronze layer
            self.bronze_cdc.ingest_ais_cdc_data(df)
            
            logger.info("AIS CDC data ingestion completed")
            
        except Exception as e:
            logger.error("Failed to ingest AIS CDC data", error=str(e))
            raise
    
    def ingest_vessel_metadata_cdc_from_file(self, file_path: Path) -> None:
        """Ingest vessel metadata CDC data from a file."""
        logger.info("Ingesting vessel metadata CDC data from file", file_path=str(file_path))
        
        try:
            # Read data from file
            df = self.spark.read.json(str(file_path))
            
            # Ingest into Bronze layer
            self.bronze_cdc.ingest_vessel_metadata_cdc(df)
            
            logger.info("Vessel metadata CDC data ingestion completed")
            
        except Exception as e:
            logger.error("Failed to ingest vessel metadata CDC data", error=str(e))
            raise
    
    def ingest_zones_from_file(self, file_path: Path) -> None:
        """Ingest geospatial zones data from a file."""
        logger.info("Ingesting zones data from file", file_path=str(file_path))
        
        try:
            # Read data from file (could be JSON, Parquet, etc.)
            if file_path.suffix.lower() == '.json':
                df = self.spark.read.json(str(file_path))
            elif file_path.suffix.lower() == '.parquet':
                df = self.spark.read.parquet(str(file_path))
            else:
                raise ValueError(f"Unsupported file format: {file_path.suffix}")
            
            # Ingest into Bronze layer
            self.bronze_cdc.ingest_geospatial_zones(df)
            
            logger.info("Zones data ingestion completed")
            
        except Exception as e:
            logger.error("Failed to ingest zones data", error=str(e))
            raise
    
    def simulate_maritime_data(self, num_vessels: int = 10, num_records: int = 100) -> None:
        """Generate and ingest simulated maritime data for testing."""
        logger.info("Generating simulated maritime data", 
                   vessels=num_vessels, 
                   records=num_records)
        
        try:
            # Generate simulated AIS data
            simulated_ais = self.bronze_cdc.simulate_cdc_data(num_vessels, num_records)
            
            # Ingest simulated data
            self.bronze_cdc.ingest_ais_cdc_data(simulated_ais, "SIMULATION")
            
            logger.info("Simulated maritime data generation completed")
            
        except Exception as e:
            logger.error("Failed to generate simulated data", error=str(e))
            raise
    
    def generate_activity_reports(self, date: datetime, report_type: str = "all") -> None:
        """Generate activity reports for a specific date."""
        logger.info("Generating activity reports", 
                   date=date.strftime("%Y-%m-%d"), 
                   report_type=report_type)
        
        # TODO: Implement when Gold layer is ready
        logger.warning("Activity report generation not yet implemented")
    
    def monitor_streaming_queries(self) -> None:
        """Monitor all active streaming queries."""
        logger.info("Starting comprehensive streaming query monitoring")
        
        try:
            # Use Silver processor's monitoring (which monitors all Silver queries)
            self.silver_processor.monitor_streaming_queries()
            
        except KeyboardInterrupt:
            logger.info("Monitoring interrupted, stopping all streams")
            self.stop_all_streams()
        except Exception as e:
            logger.error("Error during monitoring", error=str(e))
            raise
    
    def stop_all_streams(self) -> None:
        """Stop all active streaming queries."""
        logger.info("Stopping all streaming queries")
        
        # Stop Silver layer streams
        self.silver_processor.stop_all_queries()
        
        # TODO: Stop Gold layer streams when implemented
        
        # Clear our tracking list
        self.active_queries.clear()
        
        logger.info("All streaming queries stopped")
    
    def restart_failed_queries(self) -> List[StreamingQuery]:
        """Restart any failed streaming queries."""
        logger.info("Restarting failed streaming queries")
        
        restarted_queries = []
        
        # Restart Silver layer queries
        silver_restarted = self.silver_processor.restart_failed_queries()
        restarted_queries.extend(silver_restarted)
        
        # TODO: Restart Gold layer queries when implemented
        
        # Update our tracking list
        self.active_queries = [q for q in self.active_queries if q.isActive]
        self.active_queries.extend(restarted_queries)
        
        logger.info("Query restart completed", restarted_count=len(restarted_queries))
        return restarted_queries
    
    def optimize_all_tables(self) -> None:
        """Optimize all Delta tables across all layers."""
        logger.info("Optimizing all Delta tables")
        
        try:
            # Optimize Bronze tables
            logger.info("Optimizing Bronze layer tables")
            self.bronze_setup.optimize_tables()
            
            # Optimize Silver tables
            logger.info("Optimizing Silver layer tables")
            self.silver_setup.optimize_silver_tables()
            
            # TODO: Optimize Gold tables when implemented
            
            logger.info("All table optimization completed")
            
        except Exception as e:
            logger.error("Failed to optimize tables", error=str(e))
            raise
    
    def vacuum_all_tables(self, retention_hours: int = 168) -> None:
        """Vacuum all Delta tables across all layers."""
        logger.info("Vacuuming all Delta tables", retention_hours=retention_hours)
        
        try:
            # Vacuum Bronze tables
            logger.info("Vacuuming Bronze layer tables")
            self.bronze_setup.vacuum_tables(retention_hours)
            
            # TODO: Vacuum Silver tables (add method to SilverTableSetup)
            # TODO: Vacuum Gold tables when implemented
            
            logger.info("All table vacuum completed")
            
        except Exception as e:
            logger.error("Failed to vacuum tables", error=str(e))
            raise
    
    def get_comprehensive_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics across all layers."""
        try:
            stats = {
                "timestamp": datetime.now().isoformat(),
                "bronze_layer": {},
                "silver_layer": {},
                "gold_layer": {},
                "streaming": {
                    "active_queries": len([q for q in self.active_queries if q.isActive]),
                    "total_queries": len(self.active_queries)
                }
            }
            
            # Bronze layer statistics
            try:
                bronze_stats = self.bronze_cdc.get_ingestion_stats("bronze.ais_movements")
                stats["bronze_layer"]["ais_movements"] = bronze_stats
            except Exception as e:
                stats["bronze_layer"]["error"] = str(e)
            
            # Silver layer statistics
            try:
                silver_stats = self.silver_processor.get_processing_statistics()
                stats["silver_layer"] = silver_stats
            except Exception as e:
                stats["silver_layer"]["error"] = str(e)
            
            # TODO: Gold layer statistics when implemented
            stats["gold_layer"]["status"] = "not_implemented"
            
            return stats
            
        except Exception as e:
            logger.error("Failed to get comprehensive statistics", error=str(e))
            return {"error": str(e)}
    
    def health_check(self, component: str = "all") -> Dict[str, Any]:
        """Perform comprehensive health check."""
        logger.info("Performing health check", component=component)
        
        health_status = {
            "overall_status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "components": {},
            "issues": []
        }
        
        try:
            # Check Spark session
            if component in ["all", "spark"]:
                try:
                    self.spark.sql("SELECT 1").collect()
                    health_status["components"]["spark"] = {"status": "healthy"}
                except Exception as e:
                    health_status["components"]["spark"] = {"status": "unhealthy", "error": str(e)}
                    health_status["issues"].append(f"Spark session issue: {str(e)}")
                    health_status["overall_status"] = "unhealthy"
            
            # Check Bronze layer
            if component in ["all", "bronze"]:
                try:
                    bronze_count = self.spark.sql("SELECT COUNT(*) FROM bronze.ais_movements").collect()[0][0]
                    health_status["components"]["bronze"] = {
                        "status": "healthy",
                        "ais_movements_count": bronze_count
                    }
                except Exception as e:
                    health_status["components"]["bronze"] = {"status": "unhealthy", "error": str(e)}
                    health_status["issues"].append(f"Bronze layer issue: {str(e)}")
                    if health_status["overall_status"] == "healthy":
                        health_status["overall_status"] = "degraded"
            
            # Check Silver layer
            if component in ["all", "silver"]:
                silver_health = self.silver_processor.health_check()
                health_status["components"]["silver"] = silver_health
                
                if silver_health["overall_status"] != "healthy":
                    health_status["issues"].extend(silver_health.get("issues", []))
                    if health_status["overall_status"] == "healthy":
                        health_status["overall_status"] = silver_health["overall_status"]
            
            # Check streaming queries
            if component in ["all", "streaming"]:
                active_queries = len([q for q in self.active_queries if q.isActive])
                total_queries = len(self.active_queries)
                
                health_status["components"]["streaming"] = {
                    "status": "healthy" if active_queries > 0 else "unhealthy",
                    "active_queries": active_queries,
                    "total_queries": total_queries
                }
                
                if active_queries == 0 and total_queries > 0:
                    health_status["issues"].append("No active streaming queries")
                    health_status["overall_status"] = "unhealthy"
            
        except Exception as e:
            health_status["overall_status"] = "unhealthy"
            health_status["issues"].append(f"Health check failed: {str(e)}")
            logger.error("Health check failed", error=str(e))
        
        logger.info("Health check completed", 
                   overall_status=health_status["overall_status"],
                   issues_count=len(health_status["issues"]))
        
        return health_status
