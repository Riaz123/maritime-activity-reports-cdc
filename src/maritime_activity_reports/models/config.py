"""Configuration models for the maritime activity reports system."""

from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from pathlib import Path


class SparkConfig(BaseModel):
    """Spark configuration settings."""
    
    app_name: str = "Maritime-Activity-Reports-CDC"
    master: Optional[str] = None
    executor_memory: str = "4g"
    executor_cores: int = 2
    executor_instances: int = 2
    driver_memory: str = "2g"
    driver_cores: int = 1
    max_result_size: str = "2g"
    
    # Delta Lake configurations
    delta_extensions: str = "io.delta.sql.DeltaSparkSessionExtension"
    delta_catalog: str = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    delta_packages: str = "io.delta:delta-spark_2.13:3.2.1"
    
    # Additional Spark configurations
    additional_configs: Dict[str, str] = Field(default_factory=dict)


class LayerConfig(BaseModel):
    """Configuration for data layer paths and settings."""
    
    base_path: str
    checkpoint_path: str
    table_properties: Dict[str, str] = Field(default_factory=dict)
    retention_days: int = 30
    vacuum_hours: int = 168  # 7 days


class CDCConfig(BaseModel):
    """Configuration for Change Data Capture settings."""
    
    enable_cdf: bool = True
    max_staleness_hours: int = 24
    processing_time_seconds: int = 30
    checkpoint_location: str
    watermark_delay: str = "10 minutes"
    

class AzureSynapseConfig(BaseModel):
    """Azure Synapse Analytics configuration settings."""
    
    workspace_name: str
    sql_pool_name: str = "default"
    resource_group: str
    subscription_id: str
    database_name: str = "maritime_reports"
    schema_name: str = "dbo"
    location: str = "West Europe"
    
    # Materialized view options for Azure Synapse
    materialized_view_options: Dict[str, str] = Field(
        default_factory=lambda: {
            "distribution": "HASH(vesselimo)",
            "clustered_columnstore_index": "true"
        }
    )


class ADLSConfig(BaseModel):
    """Azure Data Lake Storage configuration."""
    
    storage_account: str
    container_name: str = "maritime-data"
    bronze_prefix: str = "bronze"
    silver_prefix: str = "silver"
    gold_prefix: str = "gold"
    checkpoint_prefix: str = "checkpoints"
    
    # ADLS connection settings
    account_key: Optional[str] = Field(None, description="Storage account key")
    sas_token: Optional[str] = Field(None, description="SAS token for authentication")
    use_managed_identity: bool = Field(True, description="Use managed identity for authentication")


class AzureEventHubsConfig(BaseModel):
    """Azure Event Hubs configuration."""
    
    namespace: str
    connection_string: str
    consumer_group: str = "$Default"
    partition_count: int = 4
    retention_days: int = 7


class MaritimeConfig(BaseModel):
    """Main configuration class for the maritime activity reports system."""
    
    # Project settings
    project_name: str = "maritime-activity-reports-cdc"
    version: str = "1.0.0"
    environment: str = Field(default="dev", pattern="^(dev|staging|prod)$")
    
    # Spark configuration
    spark: SparkConfig = Field(default_factory=SparkConfig)
    
    # Layer configurations
    bronze: LayerConfig
    silver: LayerConfig  
    gold: LayerConfig
    
    # CDC/CDF configuration
    cdc: CDCConfig
    
    # Azure configurations
    synapse: AzureSynapseConfig
    adls: ADLSConfig
    event_hubs: AzureEventHubsConfig
    
    # Maritime domain settings
    zone_types: List[str] = Field(
        default=[
            "country_un_name", "eez", "continent", "subcontinent",
            "hrz_v2", "sanction", "seca", "inland", "port"
        ]
    )
    
    vessel_types: List[str] = Field(
        default=[
            "Bulk Carrier", "Container Ship", "Tanker", "General Cargo",
            "Fishing Vessel", "Passenger Ship", "Offshore Vessel"
        ]
    )
    
    # Data quality thresholds
    max_speed_knots: float = 50.0
    min_latitude: float = -90.0
    max_latitude: float = 90.0
    min_longitude: float = -180.0
    max_longitude: float = 180.0
    
    @classmethod
    def from_file(cls, config_path: Path) -> "MaritimeConfig":
        """Load configuration from a file."""
        import yaml
        
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)
        
        return cls(**config_data)
    
    def to_spark_configs(self) -> Dict[str, str]:
        """Convert to Spark configuration dictionary."""
        configs = {
            "spark.app.name": self.spark.app_name,
            "spark.executor.memory": self.spark.executor_memory,
            "spark.executor.cores": str(self.spark.executor_cores),
            "spark.executor.instances": str(self.spark.executor_instances),
            "spark.driver.memory": self.spark.driver_memory,
            "spark.driver.cores": str(self.spark.driver_cores),
            "spark.driver.maxResultSize": self.spark.max_result_size,
            "spark.sql.extensions": self.spark.delta_extensions,
            "spark.sql.catalog.spark_catalog": self.spark.delta_catalog,
            "spark.jars.packages": self.spark.delta_packages,
        }
        
        # Add additional configurations
        configs.update(self.spark.additional_configs)
        
        return configs
