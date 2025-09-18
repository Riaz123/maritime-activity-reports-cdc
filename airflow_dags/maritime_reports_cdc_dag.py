"""
Airflow DAG for Maritime Activity Reports with CDC/CDF processing.
"""

import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from functools import partial
from typing import Optional

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.operators.pubsub import PubSubPublishMessageOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup

# Import our custom CDC/CDF components
from maritime_activity_reports.models.config import MaritimeConfig
from maritime_activity_reports.orchestrator.cdc_cdf_orchestrator import CDCCDFOrchestrator
from maritime_activity_reports.gold.materialized_views import GoldMaterializedViews
from maritime_activity_reports.utils.logging import setup_logging

__VERSION__ = "1.0"

DAG_ID = f"Maritime-ActivityReports-CDC-Daily-v{__VERSION__}"
DAG_DESCRIPTION = "Daily maritime activity reports pipeline with CDC/CDF processing"

REGION = "europe-west2"
PROJECT_ID = "your-gcp-project-id"
SERVICE_ACCOUNT = "maritime-reports-prod@your-gcp-project-id.iam.gserviceaccount.com"

LOGGER = logging.getLogger(DAG_ID)


@dataclass
class CDCFeature:
    """Enhanced Feature class with CDC/CDF capabilities."""
    name: str
    clean_name: str = field(init=False)
    executor_instances: int = 2
    executor_cores: int = 4
    executor_memory: str = "9600m"
    driver_cores: int = 4
    driver_memory: str = "9600m"
    git_branch: str = "master"
    execution_timeout: timedelta = timedelta(hours=1)
    
    # CDC/CDF specific settings
    enable_cdf: bool = True
    streaming_enabled: bool = True
    materialized_view_refresh: bool = True
    
    def __post_init__(self):
        self.clean_name = self.name.replace("_", "-")


def setup_cdc_tables(**context):
    """Setup all CDC/CDF tables across layers."""
    setup_logging()
    
    try:
        # Load configuration
        config = MaritimeConfig.from_file("/opt/airflow/config/maritime_config.yaml")
        
        # Initialize orchestrator
        orchestrator = CDCCDFOrchestrator(config)
        
        # Setup all tables
        orchestrator.setup_all_cdf_tables()
        
        LOGGER.info("CDC/CDF tables setup completed successfully")
        
    except Exception as e:
        LOGGER.error(f"Failed to setup CDC/CDF tables: {str(e)}")
        raise


def start_streaming_processors(**context):
    """Start all CDC/CDF streaming processors."""
    setup_logging()
    
    try:
        config = MaritimeConfig.from_file("/opt/airflow/config/maritime_config.yaml")
        orchestrator = CDCCDFOrchestrator(config)
        
        # Start streaming queries
        queries = orchestrator.start_all_streaming_queries()
        
        LOGGER.info(f"Started {len(queries)} streaming queries")
        
        # Store query IDs in XCom for monitoring
        return [query.id for query in queries]
        
    except Exception as e:
        LOGGER.error(f"Failed to start streaming processors: {str(e)}")
        raise


def refresh_materialized_views(environment: str, project_id: str, **context):
    """Refresh all materialized views."""
    setup_logging()
    
    try:
        config = MaritimeConfig.from_file("/opt/airflow/config/maritime_config.yaml")
        config.bigquery.project_id = project_id
        
        mv_manager = GoldMaterializedViews(config)
        mv_manager.refresh_all_materialized_views()
        
        LOGGER.info(f"Materialized views refreshed for {environment}")
        
    except Exception as e:
        LOGGER.error(f"Failed to refresh materialized views: {str(e)}")
        raise


def monitor_streaming_health(**context):
    """Monitor streaming query health and restart if needed."""
    setup_logging()
    
    try:
        config = MaritimeConfig.from_file("/opt/airflow/config/maritime_config.yaml")
        orchestrator = CDCCDFOrchestrator(config)
        
        # Perform health check
        health_status = orchestrator.health_check("streaming")
        
        if health_status["overall_status"] != "healthy":
            LOGGER.warning("Streaming health issues detected, attempting restart")
            orchestrator.restart_failed_queries()
        
        LOGGER.info(f"Streaming health check completed: {health_status['overall_status']}")
        
    except Exception as e:
        LOGGER.error(f"Failed to monitor streaming health: {str(e)}")
        raise


def generate_data_quality_report(**context):
    """Generate comprehensive data quality report."""
    setup_logging()
    
    try:
        config = MaritimeConfig.from_file("/opt/airflow/config/maritime_config.yaml")
        orchestrator = CDCCDFOrchestrator(config)
        
        # Get comprehensive statistics
        stats = orchestrator.get_comprehensive_statistics()
        
        # Log quality metrics
        LOGGER.info("Data quality report generated", extra=stats)
        
        return stats
        
    except Exception as e:
        LOGGER.error(f"Failed to generate data quality report: {str(e)}")
        raise


def _cdc_feature_task(feature: CDCFeature, resolution: Optional[int] = None, **kwargs):
    """Enhanced feature task with CDC/CDF capabilities."""
    resolution_suffix = f"-{resolution}" if resolution is not None else ""
    
    # Enhanced Spark configuration for CDC/CDF
    enhanced_properties = {
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.jars.packages": "io.delta:delta-spark_2.13:3.2.1",
        "spark.executor.instances": f"{feature.executor_instances}",
        "spark.executor.cores": f"{feature.executor_cores}",
        "spark.executor.memory": f"{feature.executor_memory}",
        "spark.driver.cores": f"{feature.driver_cores}",
        "spark.driver.memory": f"{feature.driver_memory}",
        "spark.executor.memoryOverhead": "4g",
        "spark.driver.memoryOverhead": "4g",
        
        # CDC/CDF specific configurations
        "spark.sql.streaming.checkpointLocation.cleanup": "true",
        "spark.sql.streaming.minBatchesToRetain": "5",
        "spark.databricks.delta.retentionDurationCheck.enabled": "false",
        "spark.databricks.delta.vacuum.parallelDelete.enabled": "true",
        
        # Performance optimizations
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
    }
    
    feature_task = DataprocCreateBatchOperator(
        task_id=f"{feature.name}{resolution_suffix}",
        region=REGION,
        project_id=PROJECT_ID,
        batch_id=f"maritime-cdc-{feature.clean_name}{resolution_suffix}-{{{{ ds }}}}-{uuid.uuid4().hex[:8]}",
        batch={
            "pyspark_batch": {
                "main_python_file_uri": "file:///home/spark/run_maritime_cdc_reports.py",
                "args": [str(arg) for arg in ["maritime-cdc", feature.name, "{{ ds }}", resolution] if arg is not None]
            },
            "runtime_config": {
                "version": "2.2",
                "container_image": f"{REGION}-docker.pkg.dev/{PROJECT_ID}/maritime-reports-cdc/maritime-reports-cdc:{feature.git_branch}-latest",
                "properties": enhanced_properties
            },
            "environment_config": {
                "execution_config": {
                    "service_account": SERVICE_ACCOUNT,
                    "subnetwork_uri": "projects/your-shared-vpc-project/regions/europe-west2/subnetworks/your-dataproc-subnet",
                }
            },
        },
        execution_timeout=feature.execution_timeout,
        deferrable=True,
        **kwargs,
    )
    
    return feature_task


default_args = {
    "owner": "Data Engineering Team",
    "depends_on_past": False,
    "email": ["data-team@your-company.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    # "on_failure_callback": partial(task_alert, state="failed"),
}

with DAG(
    dag_id=DAG_ID,
    description=DAG_DESCRIPTION,
    start_date=datetime(2024, 9, 18),
    schedule="@daily",
    catchup=False,  # CDC/CDF doesn't need catchup
    max_active_runs=1,
    max_active_tasks=3,  # Allow more parallelism for CDC
    default_args=default_args,
    tags=["maritime", "cdc", "cdf", "medallion", "realtime"],
) as dag:
    
    # Start task
    start = EmptyOperator(task_id="start")
    
    # End task
    end = EmptyOperator(task_id="end")
    
    # Setup CDC/CDF infrastructure
    setup_tables_task = PythonOperator(
        task_id="setup_cdc_cdf_tables",
        python_callable=setup_cdc_tables,
        pool="setup",
    )
    
    # Start streaming processors
    start_streaming_task = PythonOperator(
        task_id="start_streaming_processors",
        python_callable=start_streaming_processors,
        pool="streaming",
    )
    
    # Monitor streaming health
    health_check_task = PythonOperator(
        task_id="monitor_streaming_health",
        python_callable=monitor_streaming_health,
        pool="monitoring",
    )
    
    # CDC Feature definitions (enhanced from original)
    cdc_feature_list = [
        CDCFeature(
            "vessel_movements_cdc",
            executor_cores=4,
            executor_memory="12g",
            executor_instances=3,
            enable_cdf=True,
            streaming_enabled=True,
        ),
        CDCFeature(
            "vessel_metadata_cdc", 
            executor_cores=2,
            executor_memory="8g",
            executor_instances=2,
            enable_cdf=True,
            streaming_enabled=True,
        ),
        CDCFeature(
            "zone_enrichment",
            executor_cores=4,
            executor_memory="10g", 
            executor_instances=2,
            enable_cdf=True,
            streaming_enabled=True,
        ),
        CDCFeature(
            "activity_reports_generation",
            executor_cores=6,
            executor_memory="14g",
            executor_instances=4,
            enable_cdf=True,
            streaming_enabled=True,
        ),
        CDCFeature(
            "compliance_scoring",
            executor_cores=4,
            executor_memory="10g",
            executor_instances=2,
            enable_cdf=True,
            streaming_enabled=True,
        ),
    ]
    
    # Create feature dictionary
    cdc_feature_dict = {feature.name: feature for feature in cdc_feature_list}
    
    # Environment configurations
    environments = {
        "dev": "your-gcp-project-id-dev",
        "prod": "your-gcp-project-id-prod",
    }
    
    # CDC/CDF Processing Task Groups
    with TaskGroup("cdc_ingestion") as cdc_ingestion_group:
        """Process CDC data ingestion into Bronze layer."""
        
        for feature_name in ["vessel_movements_cdc", "vessel_metadata_cdc", "zone_enrichment"]:
            feature = cdc_feature_dict[feature_name]
            _cdc_feature_task(feature, depends_on_past=False)
    
    with TaskGroup("silver_processing") as silver_processing_group:
        """Process Silver layer CDF transformations."""
        
        silver_feature = cdc_feature_dict["activity_reports_generation"]
        _cdc_feature_task(silver_feature, depends_on_past=False)
    
    with TaskGroup("gold_analytics") as gold_analytics_group:
        """Process Gold layer analytics and materialized views."""
        
        gold_feature = cdc_feature_dict["compliance_scoring"]
        _cdc_feature_task(gold_feature, depends_on_past=False)
    
    # Materialized Views refresh tasks
    refresh_materialized_views_tasks = [
        PythonOperator(
            task_id=f"refresh_materialized_views_{environment}",
            pool="bigquery",
            python_callable=refresh_materialized_views,
            op_kwargs={
                "environment": environment,
                "project_id": project_id,
            }
        )
        for environment, project_id in environments.items()
    ]
    
    # Data quality reporting
    quality_report_task = PythonOperator(
        task_id="generate_data_quality_report",
        python_callable=generate_data_quality_report,
        pool="monitoring",
    )
    
    # Pub/Sub notifications (enhanced with CDC metadata)
    send_cdc_notifications = [
        PubSubPublishMessageOperator(
            task_id=f"send_cdc_update_message_{environment}",
            project_id=project_id,
            topic=f"maritime-{environment}-cdc-updates",
            messages=[
                {
                    "data": json.dumps({
                        "type": "maritime_activity_reports_cdc",
                        "from": "ds_airflow_cdc",
                        "pipeline_version": __VERSION__,
                        "processing_mode": "real_time_cdc",
                        "layers_processed": ["bronze", "silver", "gold"],
                        "frequency": "continuous_streaming",
                        "start_interval": "{{ ds }}",
                        "end_interval": "{{ next_ds }}",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        "materialized_views_refreshed": True,
                        "data_quality_score": "{{ ti.xcom_pull(task_ids='generate_data_quality_report')['silver_layer']['movements']['avg_quality_score'] | default(0.0) }}",
                    }).encode("utf-8"),
                    "attributes": {
                        "pipeline": "maritime-cdc",
                        "environment": environment,
                        "layer": "orchestration",
                        "event_type": "pipeline_completion"
                    }
                },
            ],
        )
        for environment, project_id in environments.items()
    ]
    
    # Table optimization tasks
    optimize_tables_task = PythonOperator(
        task_id="optimize_all_delta_tables",
        python_callable=lambda **context: CDCCDFOrchestrator(
            MaritimeConfig.from_file("/opt/airflow/config/maritime_config.yaml")
        ).optimize_all_tables(),
        pool="optimization",
    )
    
    # Vacuum old files task
    vacuum_tables_task = PythonOperator(
        task_id="vacuum_old_delta_files",
        python_callable=lambda **context: CDCCDFOrchestrator(
            MaritimeConfig.from_file("/opt/airflow/config/maritime_config.yaml")
        ).vacuum_all_tables(retention_hours=168),
        pool="maintenance",
    )
    
    # Empty operators for flow control
    end_cdc_ingestion = EmptyOperator(task_id="end_cdc_ingestion")
    end_silver_processing = EmptyOperator(task_id="end_silver_processing") 
    end_gold_analytics = EmptyOperator(task_id="end_gold_analytics")
    end_materialized_views = EmptyOperator(task_id="end_materialized_views")
    end_notifications = EmptyOperator(task_id="end_notifications")
    end_maintenance = EmptyOperator(task_id="end_maintenance")
    
    # DAG Dependencies - CDC/CDF Flow
    (
        start 
        >> setup_tables_task 
        >> start_streaming_task 
        >> health_check_task
        >> cdc_ingestion_group 
        >> end_cdc_ingestion
    )
    
    (
        end_cdc_ingestion 
        >> silver_processing_group 
        >> end_silver_processing
    )
    
    (
        end_silver_processing 
        >> gold_analytics_group 
        >> end_gold_analytics
    )
    
    (
        end_gold_analytics 
        >> refresh_materialized_views_tasks 
        >> end_materialized_views
    )
    
    (
        end_materialized_views 
        >> quality_report_task 
        >> send_cdc_notifications 
        >> end_notifications
    )
    
    # Maintenance tasks run in parallel
    (
        end_notifications 
        >> [optimize_tables_task, vacuum_tables_task] 
        >> end_maintenance 
        >> end
    )
