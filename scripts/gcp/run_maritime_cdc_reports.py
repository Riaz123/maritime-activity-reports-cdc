#!/usr/bin/env python3
"""
Main execution script for Maritime Activity Reports CDC/CDF on Dataproc Serverless.
This script is uploaded to GCS and executed by Dataproc batch jobs.
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add the package to Python path
sys.path.insert(0, '/home/spark')

try:
    from maritime_activity_reports.models.config import MaritimeConfig
    from maritime_activity_reports.orchestrator.cdc_cdf_orchestrator import CDCCDFOrchestrator
    from maritime_activity_reports.utils.logging import setup_logging
    from maritime_activity_reports.utils.spark_utils import get_spark_session
    import structlog
except ImportError as e:
    print(f"❌ Failed to import maritime_activity_reports: {e}")
    print("Ensure the package is properly installed on the Dataproc cluster")
    sys.exit(1)


def setup_gcp_logging():
    """Setup logging for GCP environment."""
    setup_logging(level="INFO", json_logs=True)
    
    # Also setup Python logging for Spark
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Reduce Spark logging noise
    logging.getLogger("py4j").setLevel(logging.WARN)
    logging.getLogger("pyspark").setLevel(logging.WARN)


def load_gcp_config():
    """Load configuration for GCP environment."""
    config_path = "/home/spark/maritime_config.yaml"
    
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    
    # Load base config
    config = MaritimeConfig.from_file(config_path)
    
    # Override with GCP-specific settings from environment variables
    if os.getenv("GOOGLE_CLOUD_PROJECT"):
        project_id = os.getenv("GOOGLE_CLOUD_PROJECT")
        environment = os.getenv("ENVIRONMENT", "dev")
        
        # Update paths with actual project ID and environment
        config.bronze.base_path = config.bronze.base_path.replace("PROJECT_ID", project_id).replace("ENVIRONMENT", environment)
        config.silver.base_path = config.silver.base_path.replace("PROJECT_ID", project_id).replace("ENVIRONMENT", environment)
        config.gold.base_path = config.gold.base_path.replace("PROJECT_ID", project_id).replace("ENVIRONMENT", environment)
        
        config.bronze.checkpoint_path = config.bronze.checkpoint_path.replace("PROJECT_ID", project_id).replace("ENVIRONMENT", environment)
        config.silver.checkpoint_path = config.silver.checkpoint_path.replace("PROJECT_ID", project_id).replace("ENVIRONMENT", environment)
        config.gold.checkpoint_path = config.gold.checkpoint_path.replace("PROJECT_ID", project_id).replace("ENVIRONMENT", environment)
        
        config.bigquery.project_id = project_id
        config.bigquery.dataset = config.bigquery.dataset.replace("ENVIRONMENT", environment)
        config.gcs.bucket_name = config.gcs.bucket_name.replace("PROJECT_ID", project_id).replace("ENVIRONMENT", environment)
    
    return config


def run_cdc_ingestion(config: MaritimeConfig, source_type: str, input_date: str):
    """Run CDC data ingestion."""
    logger = structlog.get_logger(__name__)
    logger.info("Starting CDC ingestion", source_type=source_type, input_date=input_date)
    
    spark = get_spark_session(config)
    orchestrator = CDCCDFOrchestrator(config, spark)
    
    try:
        # Setup tables if they don't exist
        orchestrator.setup_all_cdf_tables()
        
        if source_type == "ais":
            # Simulate AIS data ingestion (in production, would read from actual source)
            orchestrator.simulate_maritime_data(num_vessels=100, num_records=1000)
            logger.info("AIS CDC ingestion completed")
            
        elif source_type == "vessel_metadata":
            # Handle vessel metadata CDC
            logger.info("Vessel metadata CDC processing not implemented yet")
            
        else:
            raise ValueError(f"Unknown source type: {source_type}")
            
    except Exception as e:
        logger.error("CDC ingestion failed", error=str(e))
        raise
    finally:
        spark.stop()


def run_silver_processing(config: MaritimeConfig, input_date: str):
    """Run Silver layer CDF processing."""
    logger = structlog.get_logger(__name__)
    logger.info("Starting Silver layer processing", input_date=input_date)
    
    spark = get_spark_session(config)
    orchestrator = CDCCDFOrchestrator(config, spark)
    
    try:
        # Start Silver layer streaming for batch processing
        queries = orchestrator.start_silver_streams()
        
        # Process for a limited time (batch mode)
        import time
        processing_duration = 300  # 5 minutes
        
        logger.info("Processing Silver layer for batch duration", duration_seconds=processing_duration)
        time.sleep(processing_duration)
        
        # Stop streaming queries
        orchestrator.stop_all_streams()
        
        logger.info("Silver layer processing completed")
        
    except Exception as e:
        logger.error("Silver processing failed", error=str(e))
        raise
    finally:
        spark.stop()


def run_gold_analytics(config: MaritimeConfig, input_date: str, report_type: str):
    """Run Gold layer analytics generation."""
    logger = structlog.get_logger(__name__)
    logger.info("Starting Gold layer analytics", input_date=input_date, report_type=report_type)
    
    spark = get_spark_session(config)
    orchestrator = CDCCDFOrchestrator(config, spark)
    
    try:
        # Generate activity reports
        date_obj = datetime.strptime(input_date, "%Y-%m-%d")
        orchestrator.generate_activity_reports(date_obj, report_type)
        
        logger.info("Gold analytics generation completed")
        
    except Exception as e:
        logger.error("Gold analytics failed", error=str(e))
        raise
    finally:
        spark.stop()


def run_materialized_views_refresh(config: MaritimeConfig):
    """Refresh BigQuery materialized views."""
    logger = structlog.get_logger(__name__)
    logger.info("Starting materialized views refresh")
    
    try:
        from maritime_activity_reports.gold.materialized_views import GoldMaterializedViews
        
        mv_manager = GoldMaterializedViews(config)
        mv_manager.refresh_all_materialized_views()
        
        logger.info("Materialized views refresh completed")
        
    except Exception as e:
        logger.error("Materialized views refresh failed", error=str(e))
        raise


def run_maintenance(config: MaritimeConfig):
    """Run maintenance tasks (optimize, vacuum)."""
    logger = structlog.get_logger(__name__)
    logger.info("Starting maintenance tasks")
    
    spark = get_spark_session(config)
    orchestrator = CDCCDFOrchestrator(config, spark)
    
    try:
        # Optimize tables
        logger.info("Optimizing Delta tables")
        orchestrator.optimize_all_tables()
        
        # Vacuum old files
        logger.info("Vacuuming old Delta files")
        orchestrator.vacuum_all_tables(retention_hours=168)
        
        logger.info("Maintenance tasks completed")
        
    except Exception as e:
        logger.error("Maintenance tasks failed", error=str(e))
        raise
    finally:
        spark.stop()


def main():
    """Main entry point for Dataproc Serverless execution."""
    parser = argparse.ArgumentParser(description="Maritime Activity Reports CDC/CDF - Dataproc Execution")
    parser.add_argument("job_type", choices=["cdc-ingestion", "silver-processing", "gold-analytics", "materialized-views", "maintenance"],
                       help="Type of job to run")
    parser.add_argument("--source-type", choices=["ais", "vessel_metadata", "zones"], 
                       help="Source type for CDC ingestion")
    parser.add_argument("--input-date", type=str, default=datetime.now().strftime("%Y-%m-%d"),
                       help="Input date for processing (YYYY-MM-DD)")
    parser.add_argument("--report-type", type=str, default="all",
                       help="Report type for analytics generation")
    
    args = parser.parse_args()
    
    # Setup logging
    setup_gcp_logging()
    logger = structlog.get_logger(__name__)
    
    logger.info("Starting maritime reports job", 
               job_type=args.job_type,
               input_date=args.input_date,
               source_type=args.source_type,
               report_type=args.report_type)
    
    try:
        # Load configuration
        config = load_gcp_config()
        logger.info("Configuration loaded", 
                   project=config.project_name,
                   environment=config.environment)
        
        # Execute based on job type
        if args.job_type == "cdc-ingestion":
            if not args.source_type:
                raise ValueError("--source-type is required for cdc-ingestion")
            run_cdc_ingestion(config, args.source_type, args.input_date)
            
        elif args.job_type == "silver-processing":
            run_silver_processing(config, args.input_date)
            
        elif args.job_type == "gold-analytics":
            run_gold_analytics(config, args.input_date, args.report_type)
            
        elif args.job_type == "materialized-views":
            run_materialized_views_refresh(config)
            
        elif args.job_type == "maintenance":
            run_maintenance(config)
            
        else:
            raise ValueError(f"Unknown job type: {args.job_type}")
        
        logger.info("Job completed successfully", job_type=args.job_type)
        
    except Exception as e:
        logger.error("Job failed", job_type=args.job_type, error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
