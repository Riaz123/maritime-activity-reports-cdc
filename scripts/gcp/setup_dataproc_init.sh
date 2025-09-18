#!/bin/bash
# Dataproc initialization script for Maritime Activity Reports CDC/CDF

set -e

echo "🚢 Initializing Dataproc cluster for Maritime Activity Reports CDC/CDF"
echo "=================================================================="

# Update system packages
apt-get update

# Install additional Python packages
pip3 install --upgrade pip

# Install maritime-activity-reports package and dependencies
pip3 install \
    delta-spark==3.2.1 \
    h3==3.7.6 \
    structlog==23.2.0 \
    pydantic==2.5.0 \
    google-cloud-bigquery==3.11.4 \
    google-cloud-storage==2.10.0 \
    google-cloud-pubsub==2.18.4

# Download and install the maritime reports package
gsutil cp gs://${GOOGLE_CLOUD_PROJECT}-maritime-data-${ENVIRONMENT:-dev}/packages/maritime-activity-reports-cdc-*.whl /tmp/
pip3 install /tmp/maritime-activity-reports-cdc-*.whl

# Create necessary directories
mkdir -p /home/spark
mkdir -p /tmp/spark-events
mkdir -p /tmp/checkpoints

# Download main execution script
gsutil cp gs://${GOOGLE_CLOUD_PROJECT}-maritime-data-${ENVIRONMENT:-dev}/scripts/run_maritime_cdc_reports.py /home/spark/

# Make script executable
chmod +x /home/spark/run_maritime_cdc_reports.py

# Download configuration
gsutil cp gs://${GOOGLE_CLOUD_PROJECT}-maritime-data-${ENVIRONMENT:-dev}/config/maritime_config.yaml /home/spark/

# Set permissions
chown -R spark:spark /home/spark
chown -R spark:spark /tmp/spark-events
chown -R spark:spark /tmp/checkpoints

# Configure Spark for Delta Lake
cat >> /etc/spark/conf/spark-defaults.conf << EOF

# Delta Lake configuration
spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
spark.jars.packages=io.delta:delta-spark_2.13:3.2.1

# CDC/CDF optimizations
spark.databricks.delta.retentionDurationCheck.enabled=false
spark.databricks.delta.vacuum.parallelDelete.enabled=true
spark.sql.streaming.checkpointLocation.cleanup=true
spark.sql.streaming.minBatchesToRetain=5

# Performance optimizations
spark.sql.adaptive.enabled=true
spark.sql.adaptive.coalescePartitions.enabled=true
spark.sql.adaptive.skewJoin.enabled=true
spark.sql.adaptive.skewJoin.skewedPartitionFactor=5
spark.serializer=org.apache.spark.serializer.KryoSerializer

# Memory optimizations
spark.executor.memoryOverhead=2g
spark.driver.memoryOverhead=1g

# Dynamic allocation
spark.dynamicAllocation.enabled=true
spark.dynamicAllocation.minExecutors=2
spark.dynamicAllocation.maxExecutors=10
spark.dynamicAllocation.initialExecutors=4
spark.dynamicAllocation.executorIdleTimeout=60s
spark.dynamicAllocation.cachedExecutorIdleTimeout=300s

# Event logging
spark.eventLog.enabled=true
spark.eventLog.dir=/tmp/spark-events
spark.history.fs.logDirectory=/tmp/spark-events
EOF

echo "✅ Dataproc initialization completed successfully"
echo "📊 Spark configured with Delta Lake and CDC/CDF optimizations"
echo "🔧 Maritime Activity Reports package installed and ready"
