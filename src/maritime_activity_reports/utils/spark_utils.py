"""Spark utility functions and session management."""

import structlog
from typing import Dict, Optional
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

from ..models.config import MaritimeConfig

logger = structlog.get_logger(__name__)


def configure_spark_for_delta(config: MaritimeConfig) -> SparkConf:
    """
    Configure Spark for Delta Lake operations.
    
    Args:
        config: Maritime configuration object
        
    Returns:
        SparkConf: Configured Spark configuration
    """
    spark_configs = config.to_spark_configs()
    
    # Add Delta Lake specific configurations
    spark_configs.update({
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1",
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
    })
    
    # Create Spark configuration
    conf = SparkConf()
    for key, value in spark_configs.items():
        conf.set(key, value)
    
    logger.info("Spark configuration created for Delta Lake", 
               app_name=config.spark.app_name)
    
    return conf


def get_spark_session(config: MaritimeConfig, 
                     enable_hive_support: bool = True) -> SparkSession:
    """
    Get or create a Spark session with Delta Lake support.
    
    Args:
        config: Maritime configuration object
        enable_hive_support: Whether to enable Hive support
        
    Returns:
        SparkSession: Configured Spark session
    """
    logger.info("Creating Spark session", app_name=config.spark.app_name)
    
    # Configure Spark
    spark_conf = configure_spark_for_delta(config)
    
    # Build Spark session
    builder = SparkSession.builder.config(conf=spark_conf)
    
    if enable_hive_support:
        builder = builder.enableHiveSupport()
    
    spark = builder.getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("Spark session created successfully",
               app_name=spark.conf.get("spark.app.name"),
               version=spark.version)
    
    return spark


def optimize_spark_for_streaming(spark: SparkSession, 
                                config: MaritimeConfig) -> None:
    """
    Apply streaming-specific optimizations to Spark session.
    
    Args:
        spark: Spark session to optimize
        config: Maritime configuration object
    """
    logger.info("Applying streaming optimizations to Spark session")
    
    streaming_configs = {
        "spark.sql.streaming.checkpointLocation.cleanup": "true",
        "spark.sql.streaming.minBatchesToRetain": "5",
        "spark.sql.streaming.stateStore.maintenanceInterval": "60s",
        "spark.sql.streaming.ui.enabled": "true",
        "spark.sql.streaming.ui.retainedBatches": "100",
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
    }
    
    for key, value in streaming_configs.items():
        spark.conf.set(key, value)
    
    logger.info("Streaming optimizations applied")


def get_table_statistics(spark: SparkSession, table_name: str) -> Dict:
    """
    Get comprehensive statistics for a Delta table.
    
    Args:
        spark: Spark session
        table_name: Name of the table
        
    Returns:
        Dict: Table statistics
    """
    try:
        # Get basic table details
        details_df = spark.sql(f"DESCRIBE DETAIL {table_name}")
        details = details_df.collect()[0].asDict()
        
        # Get table history
        history_df = spark.sql(f"DESCRIBE HISTORY {table_name} LIMIT 10")
        history = [row.asDict() for row in history_df.collect()]
        
        # Get column statistics
        columns_df = spark.sql(f"DESCRIBE {table_name}")
        columns = [row.asDict() for row in columns_df.collect()]
        
        return {
            "table_details": details,
            "recent_history": history,
            "columns": columns,
            "location": details.get("location"),
            "format": details.get("format"),
            "num_files": details.get("numFiles"),
            "size_in_bytes": details.get("sizeInBytes"),
            "partition_columns": details.get("partitionColumns", [])
        }
        
    except Exception as e:
        logger.error("Failed to get table statistics", 
                    table=table_name, 
                    error=str(e))
        return {"error": str(e)}


def optimize_delta_table(spark: SparkSession, table_name: str, 
                        zorder_columns: Optional[list] = None) -> None:
    """
    Optimize a Delta table with optional Z-ordering.
    
    Args:
        spark: Spark session
        table_name: Name of the table to optimize
        zorder_columns: Columns to use for Z-ordering
    """
    logger.info("Optimizing Delta table", table=table_name)
    
    try:
        if zorder_columns:
            zorder_clause = ", ".join(zorder_columns)
            spark.sql(f"OPTIMIZE {table_name} ZORDER BY ({zorder_clause})")
            logger.info("Table optimized with Z-ordering", 
                       table=table_name, 
                       zorder_columns=zorder_columns)
        else:
            spark.sql(f"OPTIMIZE {table_name}")
            logger.info("Table optimized", table=table_name)
            
    except Exception as e:
        logger.error("Failed to optimize table", 
                    table=table_name, 
                    error=str(e))
        raise


def vacuum_delta_table(spark: SparkSession, table_name: str, 
                      retention_hours: int = 168) -> None:
    """
    Vacuum a Delta table to remove old files.
    
    Args:
        spark: Spark session
        table_name: Name of the table to vacuum
        retention_hours: Retention period in hours
    """
    logger.info("Vacuuming Delta table", 
               table=table_name, 
               retention_hours=retention_hours)
    
    try:
        spark.sql(f"VACUUM {table_name} RETAIN {retention_hours} HOURS")
        logger.info("Table vacuumed successfully", table=table_name)
        
    except Exception as e:
        logger.error("Failed to vacuum table", 
                    table=table_name, 
                    error=str(e))
        raise


def create_checkpoint_directory(spark: SparkSession, 
                               checkpoint_path: str) -> None:
    """
    Create checkpoint directory for streaming queries.
    
    Args:
        spark: Spark session
        checkpoint_path: Path for checkpoint directory
    """
    try:
        # Create directory using Hadoop filesystem API
        hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
        fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
        path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(checkpoint_path)
        
        if not fs.exists(path):
            fs.mkdirs(path)
            logger.info("Checkpoint directory created", path=checkpoint_path)
        else:
            logger.info("Checkpoint directory already exists", path=checkpoint_path)
            
    except Exception as e:
        logger.error("Failed to create checkpoint directory", 
                    path=checkpoint_path, 
                    error=str(e))
        raise
