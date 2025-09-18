"""Test suite for maritime activity reports CDC/CDF system."""

import pytest
from pyspark.sql import SparkSession
from ..models.config import MaritimeConfig
from ..utils.spark_utils import get_spark_session


@pytest.fixture(scope="session")
def spark_session():
    """Create Spark session for testing."""
    config = MaritimeConfig(
        project_name="maritime-test",
        environment="test",
        bronze={"base_path": "/tmp/test/bronze", "checkpoint_path": "/tmp/test/checkpoints/bronze", "retention_days": 1},
        silver={"base_path": "/tmp/test/silver", "checkpoint_path": "/tmp/test/checkpoints/silver", "retention_days": 1},
        gold={"base_path": "/tmp/test/gold", "checkpoint_path": "/tmp/test/checkpoints/gold", "retention_days": 1},
        cdc={"enable_cdf": True, "processing_time_seconds": 1, "checkpoint_location": "/tmp/test/cdc"},
        bigquery={"project_id": "test-project", "dataset": "test_dataset"},
        gcs={"bucket_name": "test-bucket"}
    )
    
    spark = get_spark_session(config)
    spark.conf.set("spark.sql.warehouse.dir", "/tmp/test/warehouse")
    
    yield spark
    
    spark.stop()


@pytest.fixture(scope="session") 
def test_config():
    """Create test configuration."""
    return MaritimeConfig(
        project_name="maritime-test",
        environment="test",
        bronze={"base_path": "/tmp/test/bronze", "checkpoint_path": "/tmp/test/checkpoints/bronze", "retention_days": 1},
        silver={"base_path": "/tmp/test/silver", "checkpoint_path": "/tmp/test/checkpoints/silver", "retention_days": 1},
        gold={"base_path": "/tmp/test/gold", "checkpoint_path": "/tmp/test/checkpoints/gold", "retention_days": 1},
        cdc={"enable_cdf": True, "processing_time_seconds": 1, "checkpoint_location": "/tmp/test/cdc"},
        bigquery={"project_id": "test-project", "dataset": "test_dataset"},
        gcs={"bucket_name": "test-bucket"}
    )
