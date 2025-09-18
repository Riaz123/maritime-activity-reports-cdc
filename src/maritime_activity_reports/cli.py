"""Command line interface for maritime activity reports."""

import typer
import structlog
from pathlib import Path
from typing import Optional
from datetime import datetime

from .models.config import MaritimeConfig
from .utils.logging import setup_logging
from .orchestrator.cdc_cdf_orchestrator import CDCCDFOrchestrator
from .bronze.table_setup import BronzeTableSetup
from .silver.table_setup import SilverTableSetup
from .gold.table_setup import GoldTableSetup

app = typer.Typer(
    name="maritime-reports",
    help="Maritime Activity Reports with CDC/CDF processing",
    add_completion=False
)

logger = structlog.get_logger(__name__)


@app.command()
def setup_tables(
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-c",
        help="Path to configuration file"
    ),
    layer: Optional[str] = typer.Option(
        None,
        "--layer", "-l",
        help="Specific layer to setup (bronze, silver, gold)"
    )
):
    """Setup Delta tables with CDF enabled."""
    setup_logging()
    logger.info("Setting up maritime data tables")
    
    try:
        config = MaritimeConfig.from_file(config_path)
        
        if layer is None or layer == "bronze":
            bronze_setup = BronzeTableSetup(config)
            bronze_setup.create_databases()
            bronze_setup.create_all_bronze_tables()
            
        if layer is None or layer == "silver":
            silver_setup = SilverTableSetup(config)
            silver_setup.create_all_silver_tables()
            silver_setup.create_silver_views()
            
        if layer is None or layer == "gold":
            gold_setup = GoldTableSetup(config)
            gold_setup.create_all_gold_tables()
            gold_setup.create_materialized_views()
            
        logger.info("Table setup completed successfully")
        
    except Exception as e:
        logger.error("Failed to setup tables", error=str(e))
        raise typer.Exit(1)


@app.command()
def start_streaming(
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-c",
        help="Path to configuration file"
    ),
    layer: Optional[str] = typer.Option(
        None,
        "--layer", "-l",
        help="Specific layer to start (silver, gold, all)"
    )
):
    """Start CDC/CDF streaming processors."""
    setup_logging()
    logger.info("Starting CDC/CDF streaming processors")
    
    try:
        config = MaritimeConfig.from_file(config_path)
        orchestrator = CDCCDFOrchestrator(config)
        
        if layer == "silver":
            queries = orchestrator.start_silver_streams()
        elif layer == "gold":
            queries = orchestrator.start_gold_streams()
        else:
            queries = orchestrator.start_all_streaming_queries()
        
        logger.info("Streaming processors started", num_queries=len(queries))
        
        # Monitor streams
        orchestrator.monitor_streaming_queries()
        
    except KeyboardInterrupt:
        logger.info("Stopping streaming processors")
        orchestrator.stop_all_streams()
    except Exception as e:
        logger.error("Failed to start streaming", error=str(e))
        raise typer.Exit(1)


@app.command()
def ingest_cdc(
    source: str = typer.Argument(..., help="Data source (ais, vessel_metadata, zones)"),
    file_path: Path = typer.Option(..., "--file", "-f", help="Path to CDC data file"),
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-c",
        help="Path to configuration file"
    )
):
    """Ingest CDC data into Bronze layer."""
    setup_logging()
    logger.info("Ingesting CDC data", source=source, file_path=str(file_path))
    
    try:
        config = MaritimeConfig.from_file(config_path)
        orchestrator = CDCCDFOrchestrator(config)
        
        if source == "ais":
            orchestrator.ingest_ais_cdc_from_file(file_path)
        elif source == "vessel_metadata":
            orchestrator.ingest_vessel_metadata_cdc_from_file(file_path)
        elif source == "zones":
            orchestrator.ingest_zones_from_file(file_path)
        else:
            logger.error("Unknown data source", source=source)
            raise typer.Exit(1)
            
        logger.info("CDC data ingestion completed")
        
    except Exception as e:
        logger.error("Failed to ingest CDC data", error=str(e))
        raise typer.Exit(1)


@app.command()
def generate_reports(
    date: datetime = typer.Argument(..., help="Report date (YYYY-MM-DD)"),
    report_type: str = typer.Option("all", "--type", "-t", help="Report type"),
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-c",
        help="Path to configuration file"
    )
):
    """Generate maritime activity reports."""
    setup_logging()
    logger.info("Generating maritime activity reports", 
               date=date.strftime("%Y-%m-%d"), 
               report_type=report_type)
    
    try:
        config = MaritimeConfig.from_file(config_path)
        orchestrator = CDCCDFOrchestrator(config)
        
        orchestrator.generate_activity_reports(date, report_type)
        
        logger.info("Activity reports generated successfully")
        
    except Exception as e:
        logger.error("Failed to generate reports", error=str(e))
        raise typer.Exit(1)


@app.command()
def optimize_tables(
    layer: str = typer.Option("all", "--layer", "-l", help="Layer to optimize"),
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-c",
        help="Path to configuration file"
    )
):
    """Optimize Delta tables."""
    setup_logging()
    logger.info("Optimizing Delta tables", layer=layer)
    
    try:
        config = MaritimeConfig.from_file(config_path)
        
        if layer in ["all", "bronze"]:
            bronze_setup = BronzeTableSetup(config)
            bronze_setup.optimize_tables()
            
        if layer in ["all", "silver"]:
            silver_setup = SilverTableSetup(config)
            silver_setup.optimize_silver_tables()
            
        if layer in ["all", "gold"]:
            gold_setup = GoldTableSetup(config)
            gold_setup.optimize_gold_tables()
            
        logger.info("Table optimization completed")
        
    except Exception as e:
        logger.error("Failed to optimize tables", error=str(e))
        raise typer.Exit(1)


@app.command()
def vacuum_tables(
    layer: str = typer.Option("all", "--layer", "-l", help="Layer to vacuum"),
    retention_hours: int = typer.Option(168, "--retention", "-r", help="Retention hours"),
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-c",
        help="Path to configuration file"
    )
):
    """Vacuum Delta tables to remove old files."""
    setup_logging()
    logger.info("Vacuuming Delta tables", layer=layer, retention_hours=retention_hours)
    
    try:
        config = MaritimeConfig.from_file(config_path)
        
        if layer in ["all", "bronze"]:
            bronze_setup = BronzeTableSetup(config)
            bronze_setup.vacuum_tables(retention_hours)
            
        if layer in ["all", "silver"]:
            silver_setup = SilverTableSetup(config)
            silver_setup.vacuum_silver_tables(retention_hours)
            
        if layer in ["all", "gold"]:
            gold_setup = GoldTableSetup(config)
            gold_setup.vacuum_gold_tables(retention_hours)
            
        logger.info("Table vacuum completed")
        
    except Exception as e:
        logger.error("Failed to vacuum tables", error=str(e))
        raise typer.Exit(1)


@app.command()
def health_check(
    component: str = typer.Option("all", "--component", "-c", help="Component to check"),
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-f",
        help="Path to configuration file"
    )
):
    """Check system health."""
    setup_logging()
    logger.info("Running health check", component=component)
    
    try:
        config = MaritimeConfig.from_file(config_path)
        orchestrator = CDCCDFOrchestrator(config)
        
        health_status = orchestrator.health_check(component)
        
        if health_status["overall_status"] == "healthy":
            logger.info("System health check passed", status=health_status)
        else:
            logger.warning("System health issues detected", status=health_status)
            raise typer.Exit(1)
            
    except Exception as e:
        logger.error("Health check failed", error=str(e))
        raise typer.Exit(1)


@app.command()
def simulate_data(
    num_vessels: int = typer.Option(10, "--vessels", "-v", help="Number of vessels"),
    num_records: int = typer.Option(100, "--records", "-r", help="Records per vessel"),
    config_path: Path = typer.Option(
        "config/config.yaml",
        "--config", "-c",
        help="Path to configuration file"
    )
):
    """Generate simulated maritime data for testing."""
    setup_logging()
    logger.info("Generating simulated maritime data", 
               vessels=num_vessels, 
               records=num_records)
    
    try:
        config = MaritimeConfig.from_file(config_path)
        orchestrator = CDCCDFOrchestrator(config)
        
        orchestrator.simulate_maritime_data(num_vessels, num_records)
        
        logger.info("Simulated data generation completed")
        
    except Exception as e:
        logger.error("Failed to generate simulated data", error=str(e))
        raise typer.Exit(1)


if __name__ == "__main__":
    app()
