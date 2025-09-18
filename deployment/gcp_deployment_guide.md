# GCP Deployment Guide - Maritime Activity Reports CDC/CDF

This guide walks you through deploying the Maritime Activity Reports CDC/CDF system to Google Cloud Platform.

## 🎯 Deployment Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Google Cloud Platform                    │
├─────────────────────────────────────────────────────────────────┤
│  Cloud Composer (Airflow)  │  Dataproc Serverless  │  Cloud Run │
│  ┌─────────────────────┐   │  ┌─────────────────┐   │  ┌─────────┐ │
│  │   DAG Orchestration │───┼─▶│  Spark CDC/CDF  │   │  │Streaming│ │
│  │   • Daily schedules │   │  │  Processing     │   │  │Monitor  │ │
│  │   • Task management │   │  │  • Bronze layer │   │  │Service  │ │
│  │   • Error handling  │   │  │  • Silver layer │   │  │         │ │
│  └─────────────────────┘   │  │  • Gold layer   │   │  └─────────┘ │
├─────────────────────────────┼─────────────────────┼─────────────────┤
│     Cloud Storage (GCS)     │    BigQuery         │   Pub/Sub      │
│  ┌─────────────────────┐   │  ┌─────────────────┐ │  ┌─────────────┐ │
│  │ Delta Lake Storage  │   │  │ Materialized    │ │  │ CDC Events  │ │
│  │ • Bronze (Raw)      │   │  │ Views           │ │  │ • Alerts    │ │
│  │ • Silver (Clean)    │   │  │ • Analytics     │ │  │ • Status    │ │
│  │ • Gold (Business)   │   │  │ • Dashboards    │ │  │ • Metrics   │ │
│  └─────────────────────┘   │  └─────────────────┘ │  └─────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

## 🚀 Quick Deployment

### Prerequisites

1. **GCP Account** with billing enabled
2. **gcloud CLI** installed and authenticated
3. **Terraform** installed (>= 1.0)
4. **Docker** installed
5. **Required permissions** on GCP project

### One-Command Deployment

```bash
# Set your GCP project
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export ENVIRONMENT="dev"  # or staging, prod
export SERVICE_ACCOUNT_EMAIL="maritime-reports-dev@your-gcp-project-id.iam.gserviceaccount.com"

# Run deployment
./scripts/gcp/deploy.sh
```

## 📋 Step-by-Step Deployment

### Step 1: Prepare Environment

```bash
# Clone and setup project
git clone <repository-url>
cd maritime-activity-reports-cdc

# Set environment variables
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export REGION="europe-west2"
export ENVIRONMENT="dev"
```

### Step 2: Configure Terraform

```bash
# Copy and customize Terraform variables
cp infrastructure/terraform/terraform.tfvars.example infrastructure/terraform/terraform.tfvars

# Edit the file with your specific values
vim infrastructure/terraform/terraform.tfvars
```

**Required variables:**
```hcl
project_id = "your-gcp-project-id"
region     = "europe-west2"
environment = "dev"
service_account_email = "maritime-reports-dev@your-gcp-project-id.iam.gserviceaccount.com"
```

### Step 3: Deploy Infrastructure

```bash
cd infrastructure/terraform

# Initialize Terraform
terraform init

# Plan deployment
terraform plan

# Apply deployment
terraform apply
```

### Step 4: Build and Deploy Application

```bash
# Return to project root
cd ../..

# Build Docker image
./scripts/gcp/deploy.sh build

# Deploy to Cloud Run
./scripts/gcp/deploy.sh cloud-run
```

### Step 5: Deploy Airflow DAGs

```bash
# Deploy DAGs to Cloud Composer
./scripts/gcp/deploy.sh airflow
```

### Step 6: Setup Monitoring

```bash
# Setup monitoring and alerting
./scripts/gcp/deploy.sh monitoring
```

## 🔧 Configuration

### GCP-Specific Configuration

Create `config/gcp_config.yaml` with your GCP settings:

```yaml
# Use the provided gcp_config.yaml as template
project_name: "maritime-activity-reports-cdc"
environment: "prod"

bigquery:
  project_id: "your-gcp-project-id"
  dataset: "maritime_reports_prod"
  location: "europe-west2"

gcs:
  bucket_name: "your-gcp-project-id-maritime-data-prod"

# ... other settings
```

### Environment Variables

Set these in your deployment environment:

```bash
export GOOGLE_CLOUD_PROJECT="your-gcp-project-id"
export ENVIRONMENT="prod"
export MARITIME_CONFIG_PATH="/app/config/gcp_config.yaml"
```

## 🏗️ Infrastructure Components

### 1. **Cloud Storage Buckets**
- **Data Bucket**: `${PROJECT_ID}-maritime-data-${ENVIRONMENT}`
  - Bronze, Silver, Gold layers
  - Lifecycle policies for cost optimization
- **Checkpoints Bucket**: `${PROJECT_ID}-maritime-checkpoints-${ENVIRONMENT}`
  - Streaming checkpoints
  - Fault tolerance

### 2. **BigQuery Dataset**
- **Dataset**: `maritime_reports_${ENVIRONMENT}`
- **Materialized Views**: Real-time analytics
- **Access Control**: Role-based permissions

### 3. **Dataproc Serverless**
- **Batch Jobs**: CDC/CDF processing
- **Auto-scaling**: Dynamic resource allocation
- **Delta Lake**: Optimized for CDC workloads

### 4. **Cloud Composer (Airflow)**
- **DAG Orchestration**: Daily pipeline management
- **Task Dependencies**: Bronze → Silver → Gold flow
- **Error Handling**: Automatic retries and alerts

### 5. **Cloud Run**
- **Streaming Service**: Real-time monitoring
- **Health Checks**: System status monitoring
- **Auto-scaling**: Based on demand

### 6. **Pub/Sub**
- **CDC Events**: Change notifications
- **Data Quality Alerts**: Quality monitoring
- **Dead Letter Queues**: Error handling

## 📊 Monitoring & Operations

### Health Monitoring

```bash
# Check system health
curl https://maritime-streaming-dev-<hash>-ew.a.run.app/health

# View logs
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=maritime-streaming-dev"

# Monitor Dataproc jobs
gcloud dataproc batches list --region=europe-west2
```

### Performance Monitoring

```bash
# BigQuery query performance
bq query --use_legacy_sql=false "
SELECT * FROM \`your-project.maritime_reports_dev.vessel_activity_summary_mv\`
LIMIT 10
"

# Storage usage
gsutil du -sh gs://your-project-maritime-data-dev/**

# Streaming metrics
gcloud monitoring metrics list --filter="metric.type:custom.googleapis.com/maritime"
```

### Cost Optimization

```bash
# Check storage costs
gcloud billing budgets list

# Optimize Delta tables
gcloud dataproc batches submit pyspark \
    --region=europe-west2 \
    --batch=maritime-optimize-$(date +%s) \
    gs://your-project-maritime-data-dev/scripts/run_maritime_cdc_reports.py \
    -- maintenance
```

## 🔒 Security

### IAM Roles

The deployment creates a service account with these roles:
- `roles/dataproc.worker` - For Spark job execution
- `roles/bigquery.admin` - For BigQuery operations
- `roles/storage.admin` - For GCS operations
- `roles/pubsub.editor` - For Pub/Sub messaging

### Network Security

- **VPC**: Uses default VPC (customizable)
- **Firewall**: Cloud Run allows authenticated access only
- **Encryption**: All data encrypted at rest and in transit

### Data Security

- **Column-level security** in BigQuery
- **Bucket-level IAM** for GCS
- **Audit logging** enabled

## 🚨 Troubleshooting

### Common Issues

1. **Permission Errors**
   ```bash
   # Check service account permissions
   gcloud projects get-iam-policy $GOOGLE_CLOUD_PROJECT
   
   # Add missing roles
   gcloud projects add-iam-policy-binding $GOOGLE_CLOUD_PROJECT \
       --member="serviceAccount:maritime-reports-dev@$GOOGLE_CLOUD_PROJECT.iam.gserviceaccount.com" \
       --role="roles/bigquery.admin"
   ```

2. **Dataproc Job Failures**
   ```bash
   # Check job logs
   gcloud dataproc batches describe BATCH_ID --region=europe-west2
   
   # View detailed logs
   gcloud logging read "resource.type=dataproc_batch AND resource.labels.batch_id=BATCH_ID"
   ```

3. **Cloud Run Issues**
   ```bash
   # Check service logs
   gcloud run services logs read maritime-streaming-dev --region=europe-west2
   
   # Check service status
   gcloud run services describe maritime-streaming-dev --region=europe-west2
   ```

4. **BigQuery Materialized View Issues**
   ```bash
   # Check view status
   bq show --format=prettyjson your-project:maritime_reports_dev.vessel_activity_summary_mv
   
   # Refresh manually
   bq query --use_legacy_sql=false "CALL BQ.REFRESH_MATERIALIZED_VIEW('your-project.maritime_reports_dev.vessel_activity_summary_mv')"
   ```

### Debug Mode

Enable debug mode for detailed logging:

```bash
# Set debug environment variable
export MARITIME_DEBUG=true

# Deploy with debug logging
./scripts/gcp/deploy.sh
```

## 🔄 CI/CD Pipeline

### Cloud Build Configuration

Create `.github/workflows/gcp-deploy.yml` for automated deployment:

```yaml
name: Deploy to GCP
on:
  push:
    branches: [main]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: google-github-actions/setup-gcloud@v1
        with:
          service_account_key: ${{ secrets.GCP_SA_KEY }}
          project_id: ${{ secrets.GCP_PROJECT_ID }}
      
      - name: Deploy to GCP
        run: ./scripts/gcp/deploy.sh
        env:
          GOOGLE_CLOUD_PROJECT: ${{ secrets.GCP_PROJECT_ID }}
          ENVIRONMENT: prod
```

## 📈 Scaling Considerations

### Production Scaling

For production workloads, adjust these settings:

```yaml
# config/gcp_config_prod.yaml
spark:
  executor_instances: 10
  executor_memory: "16g"
  executor_cores: 8

dataproc:
  max_instances: 50
  preemptible_percentage: 50  # Cost optimization

cloud_run:
  max_instances: 20
  cpu: "4000m"
  memory: "8Gi"
```

### Cost Optimization

- **Preemptible instances** for non-critical workloads
- **Auto-scaling** based on demand
- **Storage lifecycle policies** for data archival
- **Materialized view** refresh optimization

## 📞 Support

- **Documentation**: `docs/API.md`
- **Issues**: GitHub Issues
- **Monitoring**: Cloud Monitoring dashboards
- **Logs**: Cloud Logging

---

**🚢 Ready to sail with maritime data on GCP! ⚓**
