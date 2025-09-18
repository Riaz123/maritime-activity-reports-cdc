# Maritime Activity Reports CDC/CDF - API Documentation

## Overview

The Maritime Activity Reports CDC/CDF system provides a comprehensive API for processing maritime vessel data using Change Data Capture (CDC) and Change Data Feed (CDF) in a medallion architecture.

## Architecture Layers

### Bronze Layer - CDC Ingestion
Raw data ingestion with Change Data Capture capabilities.

#### Classes

##### `BronzeCDCLayer`
Handles CDC data ingestion into the Bronze layer.

```python
from maritime_activity_reports.bronze import BronzeCDCLayer

cdc_layer = BronzeCDCLayer(config, spark_session)
```

**Methods:**

- `ingest_ais_cdc_data(cdc_records: DataFrame, source_system: str) -> DataFrame`
  - Ingests AIS movement CDC data
  - Applies data quality validation
  - Returns processed DataFrame

- `ingest_vessel_metadata_cdc(cdc_records: DataFrame, source_system: str) -> DataFrame`
  - Ingests vessel metadata with SCD Type 2 handling
  - Manages historical changes
  - Returns processed DataFrame

- `simulate_cdc_data(num_vessels: int, num_records_per_vessel: int) -> DataFrame`
  - Generates simulated maritime data for testing
  - Returns synthetic AIS movement data

##### `BronzeTableSetup`
Manages Bronze layer table creation and configuration.

```python
from maritime_activity_reports.bronze import BronzeTableSetup

table_setup = BronzeTableSetup(config, spark_session)
```

**Methods:**

- `create_all_bronze_tables() -> None`
  - Creates all Bronze layer Delta tables with CDF enabled
  - Sets up partitioning and optimization

- `optimize_tables() -> None`
  - Optimizes Bronze tables for performance
  - Applies Z-ordering and compaction

- `vacuum_tables(retention_hours: int) -> None`
  - Removes old Delta files
  - Manages storage costs

### Silver Layer - CDF Processing
Cleaned and enriched data with real-time change processing.

#### Classes

##### `SilverCDFProcessor`
Processes Bronze CDF changes for Silver layer transformation.

```python
from maritime_activity_reports.silver import SilverCDFProcessor

processor = SilverCDFProcessor(config, spark_session)
```

**Methods:**

- `process_ais_movements_cdf_stream() -> StreamingQuery`
  - Starts streaming query to process AIS movements
  - Applies data enrichment and quality validation
  - Returns active streaming query

- `process_vessel_metadata_cdf_stream() -> StreamingQuery`
  - Processes vessel metadata changes with SCD Type 2
  - Maintains historical vessel information
  - Returns active streaming query

##### `SilverStreamingProcessor`
Manages all Silver layer streaming queries.

```python
from maritime_activity_reports.silver import SilverStreamingProcessor

streaming_processor = SilverStreamingProcessor(config, spark_session)
```

**Methods:**

- `start_all_streaming_queries() -> List[StreamingQuery]`
  - Starts all Silver layer streaming processes
  - Returns list of active streaming queries

- `monitor_streaming_queries(interval_seconds: int) -> None`
  - Monitors streaming query health
  - Provides real-time metrics and alerts

- `health_check() -> Dict[str, Any]`
  - Performs comprehensive health check
  - Returns status of all Silver components

### Gold Layer - Business Analytics
Business-ready data marts and materialized views.

#### Classes

##### `GoldMaterializedViews`
Creates and manages BigQuery materialized views.

```python
from maritime_activity_reports.gold import GoldMaterializedViews

mv_manager = GoldMaterializedViews(config, spark_session)
```

**Methods:**

- `create_all_materialized_views() -> None`
  - Creates all Gold layer materialized views
  - Follows IHS Compliance pattern with complex CTEs

- `create_vessel_activity_summary_mv() -> None`
  - Creates vessel activity summary materialized view
  - Provides real-time vessel activity aggregations

- `create_port_performance_mv() -> None`
  - Creates port performance analytics view
  - Calculates efficiency metrics and rankings

- `refresh_all_materialized_views() -> None`
  - Refreshes all materialized views
  - Ensures data freshness

##### `GoldCDFProcessor`
Processes Silver CDF changes for Gold analytics.

```python
from maritime_activity_reports.gold import GoldCDFProcessor

gold_processor = GoldCDFProcessor(config, spark_session)
```

**Methods:**

- `process_silver_movements_to_activity_reports() -> StreamingQuery`
  - Transforms Silver movements into Gold activity reports
  - Applies business logic in real-time

- `process_activity_reports_to_analytics() -> StreamingQuery`
  - Generates derived analytics from activity reports
  - Updates materialized views automatically

## Orchestration

### `CDCCDFOrchestrator`
Main orchestrator for end-to-end CDC/CDF processing.

```python
from maritime_activity_reports.orchestrator import CDCCDFOrchestrator

orchestrator = CDCCDFOrchestrator(config, spark_session)
```

**Methods:**

- `setup_all_cdf_tables() -> None`
  - Sets up all Delta tables across Bronze, Silver, Gold layers
  - Enables Change Data Feed on all tables

- `start_all_streaming_queries() -> List[StreamingQuery]`
  - Starts streaming across all layers
  - Returns all active streaming queries

- `monitor_streaming_queries() -> None`
  - Monitors all streaming queries
  - Provides comprehensive health monitoring

- `health_check(component: str = "all") -> Dict[str, Any]`
  - Performs system-wide health check
  - Returns detailed status information

- `get_comprehensive_statistics() -> Dict[str, Any]`
  - Collects statistics across all layers
  - Provides performance and quality metrics

## Configuration

### `MaritimeConfig`
Main configuration class for the system.

```python
from maritime_activity_reports.models.config import MaritimeConfig

# Load from file
config = MaritimeConfig.from_file("config/config.yaml")

# Create programmatically
config = MaritimeConfig(
    project_name="maritime-reports",
    environment="prod",
    spark=SparkConfig(executor_memory="8g"),
    bronze=LayerConfig(base_path="gs://bucket/bronze"),
    silver=LayerConfig(base_path="gs://bucket/silver"),
    gold=LayerConfig(base_path="gs://bucket/gold"),
    bigquery=BigQueryConfig(project_id="my-project"),
    gcs=GCSConfig(bucket_name="my-bucket")
)
```

## Data Schemas

### `VesselMovement`
Schema for vessel movement data.

```python
from maritime_activity_reports.models.schemas import VesselMovement

movement = VesselMovement(
    imo="9123456",
    movementdatetime=datetime.now(),
    latitude=51.5074,
    longitude=-0.1278,
    speed_over_ground=12.5,
    navigational_status="UNDER_WAY"
)
```

### `VesselMetadata`
Schema for vessel metadata with SCD Type 2 support.

```python
from maritime_activity_reports.models.schemas import VesselMetadata

metadata = VesselMetadata(
    imo="9123456",
    vessel_name="Test Vessel",
    vessel_type="Bulk Carrier",
    gross_tonnage=50000.0,
    flag_country="Panama",
    valid_from_datetime=datetime.now(),
    is_current_record=True
)
```

## Utilities

### Data Quality Validation

```python
from maritime_activity_reports.utils.data_quality import DataQualityValidator

validator = DataQualityValidator(config)

# Validate AIS data
validated_df = validator.validate_ais_data(ais_dataframe)

# Get quality report
quality_report = validator.get_quality_report(dataframe, "table_name")
```

### Spark Utilities

```python
from maritime_activity_reports.utils.spark_utils import (
    get_spark_session, 
    optimize_delta_table,
    vacuum_delta_table
)

# Get optimized Spark session
spark = get_spark_session(config)

# Optimize Delta table
optimize_delta_table(spark, "bronze.ais_movements", ["imo", "movementdatetime"])

# Vacuum old files
vacuum_delta_table(spark, "bronze.ais_movements", retention_hours=168)
```

### BigQuery Integration

```python
from maritime_activity_reports.utils.bigquery import (
    run_bigquery_query,
    create_materialized_view,
    refresh_materialized_view
)

# Execute BigQuery query
results = run_bigquery_query("SELECT * FROM dataset.table", "project-id")

# Create materialized view
create_materialized_view(
    project_id="my-project",
    dataset_id="maritime",
    view_id="vessel_summary_mv",
    query="SELECT * FROM vessel_activity_reports",
    cluster_fields=["vesselimo"]
)
```

## Error Handling

All components include comprehensive error handling and logging:

```python
import structlog

logger = structlog.get_logger(__name__)

try:
    orchestrator.setup_all_cdf_tables()
except Exception as e:
    logger.error("Setup failed", error=str(e))
    raise
```

## Performance Considerations

### Spark Configuration
- **Dynamic allocation**: Enabled for cost optimization
- **Adaptive query execution**: Automatic skew handling
- **Delta optimizations**: Auto-compaction and Z-ordering

### Streaming Configuration
- **Checkpoint management**: Fault-tolerant processing
- **Watermark handling**: Late data tolerance
- **Batch sizing**: Optimized for throughput

### BigQuery Optimization
- **Materialized views**: Sub-second query performance
- **Clustering**: Optimized data layout
- **Partitioning**: Efficient data pruning

## Monitoring and Observability

### Health Checks
```python
# System-wide health check
health_status = orchestrator.health_check()

# Component-specific health check
streaming_health = orchestrator.health_check("streaming")
```

### Metrics Collection
```python
# Comprehensive statistics
stats = orchestrator.get_comprehensive_statistics()

# Layer-specific metrics
bronze_stats = bronze_cdc.get_ingestion_stats("bronze.ais_movements")
silver_stats = silver_processor.get_processing_statistics()
```

### Streaming Monitoring
```python
# Monitor all streaming queries
orchestrator.monitor_streaming_queries()

# Get stream metrics
for query in active_queries:
    metrics = processor.get_stream_metrics(query)
    print(f"Query {query.id}: {metrics['input_rows_per_second']} rows/sec")
```

## Best Practices

### Data Quality
- Always validate data at ingestion
- Use quality scores for filtering
- Monitor data quality trends

### Change Data Capture
- Enable CDF on all business-critical tables
- Use appropriate watermarks for late data
- Monitor CDC lag and throughput

### Performance Optimization
- Optimize tables regularly
- Vacuum old files to manage costs
- Use materialized views for frequently accessed data

### Error Recovery
- Implement automatic retry logic
- Use checkpoint recovery for streaming
- Monitor and alert on failures

## Examples

### Complete Pipeline Setup
```python
from maritime_activity_reports.models.config import MaritimeConfig
from maritime_activity_reports.orchestrator import CDCCDFOrchestrator

# Load configuration
config = MaritimeConfig.from_file("config/config.yaml")

# Initialize orchestrator
orchestrator = CDCCDFOrchestrator(config)

# Setup all tables
orchestrator.setup_all_cdf_tables()

# Start streaming
queries = orchestrator.start_all_streaming_queries()

# Monitor
orchestrator.monitor_streaming_queries()
```

### Data Ingestion
```python
# Ingest CDC data from file
orchestrator.ingest_ais_cdc_from_file("data/ais_changes.json")

# Simulate test data
orchestrator.simulate_maritime_data(num_vessels=10, num_records=100)
```

### Analytics Generation
```python
# Generate activity reports
orchestrator.generate_activity_reports(
    date=datetime(2024, 1, 15),
    report_type="port"
)

# Refresh materialized views
mv_manager.refresh_all_materialized_views()
```
