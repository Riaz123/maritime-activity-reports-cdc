#!/bin/bash
# GCP Deployment script for Maritime Activity Reports CDC/CDF

set -e

# Configuration
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-}"
REGION="${REGION:-europe-west2}"
ENVIRONMENT="${ENVIRONMENT:-dev}"
SERVICE_ACCOUNT_EMAIL="${SERVICE_ACCOUNT_EMAIL:-}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚢 Maritime Activity Reports CDC/CDF - GCP Deployment${NC}"
echo "=============================================================="

# Check prerequisites
check_prerequisites() {
    echo -e "${BLUE}📋 Checking prerequisites...${NC}"
    
    # Check if gcloud is installed
    if ! command -v gcloud &> /dev/null; then
        echo -e "${RED}❌ gcloud CLI is not installed${NC}"
        echo "Please install gcloud CLI: https://cloud.google.com/sdk/docs/install"
        exit 1
    fi
    
    # Check if terraform is installed
    if ! command -v terraform &> /dev/null; then
        echo -e "${RED}❌ Terraform is not installed${NC}"
        echo "Please install Terraform: https://learn.hashicorp.com/tutorials/terraform/install-cli"
        exit 1
    fi
    
    # Check if docker is installed
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}❌ Docker is not installed${NC}"
        echo "Please install Docker: https://docs.docker.com/get-docker/"
        exit 1
    fi
    
    # Check if PROJECT_ID is set
    if [ -z "$PROJECT_ID" ]; then
        echo -e "${RED}❌ GOOGLE_CLOUD_PROJECT environment variable is not set${NC}"
        echo "Please set your GCP project ID: export GOOGLE_CLOUD_PROJECT=your-project-id"
        exit 1
    fi
    
    echo -e "${GREEN}✅ All prerequisites satisfied${NC}"
}

# Authenticate with GCP
authenticate_gcp() {
    echo -e "${BLUE}🔐 Authenticating with GCP...${NC}"
    
    # Check if already authenticated
    if gcloud auth list --filter=status:ACTIVE --format="value(account)" | grep -q "@"; then
        echo -e "${GREEN}✅ Already authenticated with GCP${NC}"
        CURRENT_PROJECT=$(gcloud config get-value project)
        if [ "$CURRENT_PROJECT" != "$PROJECT_ID" ]; then
            echo -e "${YELLOW}⚠️  Setting project to $PROJECT_ID${NC}"
            gcloud config set project $PROJECT_ID
        fi
    else
        echo -e "${YELLOW}🔐 Please authenticate with GCP...${NC}"
        gcloud auth login
        gcloud config set project $PROJECT_ID
    fi
    
    # Enable required APIs
    echo -e "${BLUE}🔧 Enabling required GCP APIs...${NC}"
    gcloud services enable \
        compute.googleapis.com \
        dataproc.googleapis.com \
        bigquery.googleapis.com \
        storage.googleapis.com \
        pubsub.googleapis.com \
        composer.googleapis.com \
        run.googleapis.com \
        artifactregistry.googleapis.com \
        cloudbuild.googleapis.com \
        monitoring.googleapis.com
    
    echo -e "${GREEN}✅ GCP authentication and API enablement completed${NC}"
}

# Build and push Docker image
build_and_push_image() {
    echo -e "${BLUE}🐳 Building and pushing Docker image...${NC}"
    
    # Configure Docker for Artifact Registry
    gcloud auth configure-docker ${REGION}-docker.pkg.dev
    
    # Build image
    IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/maritime-reports-${ENVIRONMENT}/maritime-reports:latest"
    
    echo -e "${BLUE}🔨 Building Docker image: $IMAGE_TAG${NC}"
    docker build -f infrastructure/docker/Dockerfile.production -t $IMAGE_TAG .
    
    # Push image
    echo -e "${BLUE}📤 Pushing Docker image to Artifact Registry...${NC}"
    docker push $IMAGE_TAG
    
    echo -e "${GREEN}✅ Docker image built and pushed successfully${NC}"
    echo -e "${GREEN}📍 Image URL: $IMAGE_TAG${NC}"
}

# Deploy infrastructure with Terraform
deploy_infrastructure() {
    echo -e "${BLUE}🏗️  Deploying infrastructure with Terraform...${NC}"
    
    cd infrastructure/terraform
    
    # Initialize Terraform
    terraform init
    
    # Create terraform.tfvars if it doesn't exist
    if [ ! -f terraform.tfvars ]; then
        echo -e "${YELLOW}📝 Creating terraform.tfvars from example...${NC}"
        cp terraform.tfvars.example terraform.tfvars
        
        # Update with current values
        sed -i.bak "s/your-gcp-project-id/$PROJECT_ID/g" terraform.tfvars
        sed -i.bak "s/your-company.com/your-company.com/g" terraform.tfvars
        
        if [ -n "$SERVICE_ACCOUNT_EMAIL" ]; then
            sed -i.bak "s/maritime-reports-dev@your-gcp-project-id.iam.gserviceaccount.com/$SERVICE_ACCOUNT_EMAIL/g" terraform.tfvars
        fi
        
        echo -e "${YELLOW}⚠️  Please review and update terraform.tfvars before continuing${NC}"
        echo -e "${YELLOW}📝 Edit: infrastructure/terraform/terraform.tfvars${NC}"
        read -p "Press Enter to continue after updating terraform.tfvars..."
    fi
    
    # Plan deployment
    echo -e "${BLUE}📋 Planning Terraform deployment...${NC}"
    terraform plan
    
    # Ask for confirmation
    echo -e "${YELLOW}🤔 Do you want to proceed with the deployment? (y/N)${NC}"
    read -r response
    if [[ "$response" =~ ^([yY][eE][sS]|[yY])$ ]]; then
        # Apply deployment
        echo -e "${BLUE}🚀 Applying Terraform deployment...${NC}"
        terraform apply -auto-approve
        
        echo -e "${GREEN}✅ Infrastructure deployed successfully${NC}"
        
        # Show outputs
        echo -e "${BLUE}📊 Deployment outputs:${NC}"
        terraform output
    else
        echo -e "${YELLOW}⏸️  Deployment cancelled${NC}"
        cd ../..
        return 1
    fi
    
    cd ../..
}

# Deploy application to Cloud Run
deploy_cloud_run() {
    echo -e "${BLUE}☁️  Deploying to Cloud Run...${NC}"
    
    IMAGE_TAG="${REGION}-docker.pkg.dev/${PROJECT_ID}/maritime-reports-${ENVIRONMENT}/maritime-reports:latest"
    SERVICE_NAME="maritime-streaming-${ENVIRONMENT}"
    
    gcloud run deploy $SERVICE_NAME \
        --image=$IMAGE_TAG \
        --platform=managed \
        --region=$REGION \
        --service-account="${SERVICE_ACCOUNT_EMAIL}" \
        --set-env-vars="GOOGLE_CLOUD_PROJECT=${PROJECT_ID},ENVIRONMENT=${ENVIRONMENT},STARTUP_MODE=web-server" \
        --memory=4Gi \
        --cpu=2 \
        --min-instances=1 \
        --max-instances=3 \
        --timeout=3600 \
        --allow-unauthenticated
    
    echo -e "${GREEN}✅ Cloud Run deployment completed${NC}"
}

# Setup BigQuery datasets and tables
setup_bigquery() {
    echo -e "${BLUE}📊 Setting up BigQuery datasets...${NC}"
    
    DATASET_ID="maritime_reports_${ENVIRONMENT}"
    
    # Create dataset
    bq mk --dataset \
        --location=$REGION \
        --description="Maritime Activity Reports CDC/CDF - $ENVIRONMENT" \
        ${PROJECT_ID}:${DATASET_ID} || echo "Dataset might already exist"
    
    echo -e "${GREEN}✅ BigQuery dataset setup completed${NC}"
}

# Deploy Airflow DAGs to Cloud Composer
deploy_airflow_dags() {
    echo -e "${BLUE}🎼 Deploying Airflow DAGs to Cloud Composer...${NC}"
    
    COMPOSER_ENV="maritime-composer-${ENVIRONMENT}"
    
    # Get Composer bucket
    COMPOSER_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
        --location=$REGION \
        --format="value(config.dagGcsPrefix)" | sed 's|/dags||')
    
    if [ -n "$COMPOSER_BUCKET" ]; then
        # Upload DAGs
        gsutil cp airflow_dags/*.py ${COMPOSER_BUCKET}/dags/
        
        # Upload configuration
        gsutil cp config/config.yaml ${COMPOSER_BUCKET}/data/config/maritime_config.yaml
        
        echo -e "${GREEN}✅ Airflow DAGs deployed to Cloud Composer${NC}"
    else
        echo -e "${YELLOW}⚠️  Cloud Composer environment not found. Please create it first with Terraform.${NC}"
    fi
}

# Setup monitoring and alerting
setup_monitoring() {
    echo -e "${BLUE}📊 Setting up monitoring and alerting...${NC}"
    
    # Create log-based metrics
    gcloud logging metrics create maritime_cdc_errors \
        --description="Maritime CDC processing errors" \
        --log-filter='resource.type="cloud_run_revision" AND resource.labels.service_name="maritime-streaming-'${ENVIRONMENT}'" AND severity>=ERROR' || echo "Metric might already exist"
    
    # Create alerting policy
    cat > /tmp/alert_policy.json << EOF
{
  "displayName": "Maritime CDC High Error Rate",
  "conditions": [
    {
      "displayName": "Maritime CDC Error Rate",
      "conditionThreshold": {
        "filter": "metric.type=\"logging.googleapis.com/user/maritime_cdc_errors\"",
        "comparison": "COMPARISON_GREATER_THAN",
        "thresholdValue": 10,
        "duration": "300s"
      }
    }
  ],
  "notificationChannels": [],
  "alertStrategy": {
    "autoClose": "1800s"
  }
}
EOF
    
    gcloud alpha monitoring policies create --policy-from-file=/tmp/alert_policy.json || echo "Alert policy might already exist"
    
    echo -e "${GREEN}✅ Monitoring setup completed${NC}"
}

# Main deployment function
main() {
    echo -e "${BLUE}🚀 Starting GCP deployment for Maritime Activity Reports CDC/CDF${NC}"
    echo "Project: $PROJECT_ID"
    echo "Region: $REGION"
    echo "Environment: $ENVIRONMENT"
    echo ""
    
    # Check prerequisites
    check_prerequisites
    
    # Authenticate
    authenticate_gcp
    
    # Build and push Docker image
    build_and_push_image
    
    # Deploy infrastructure
    deploy_infrastructure
    
    # Setup BigQuery
    setup_bigquery
    
    # Deploy to Cloud Run
    deploy_cloud_run
    
    # Deploy Airflow DAGs
    deploy_airflow_dags
    
    # Setup monitoring
    setup_monitoring
    
    echo ""
    echo -e "${GREEN}🎉 GCP Deployment completed successfully!${NC}"
    echo ""
    echo -e "${BLUE}📋 Next steps:${NC}"
    echo "1. Check Cloud Run service: https://console.cloud.google.com/run"
    echo "2. Monitor Airflow DAGs: https://console.cloud.google.com/composer"
    echo "3. View BigQuery datasets: https://console.cloud.google.com/bigquery"
    echo "4. Check logs: https://console.cloud.google.com/logs"
    echo ""
    echo -e "${BLUE}🔧 Useful commands:${NC}"
    echo "gcloud run services list --region=$REGION"
    echo "gcloud composer environments list --locations=$REGION"
    echo "bq ls ${PROJECT_ID}:maritime_reports_${ENVIRONMENT}"
    echo ""
    echo -e "${GREEN}Happy maritime data processing! ⚓${NC}"
}

# Handle script arguments
case "${1:-deploy}" in
    "deploy")
        main
        ;;
    "build")
        check_prerequisites
        authenticate_gcp
        build_and_push_image
        ;;
    "infrastructure")
        check_prerequisites
        authenticate_gcp
        deploy_infrastructure
        ;;
    "cloud-run")
        check_prerequisites
        authenticate_gcp
        deploy_cloud_run
        ;;
    "airflow")
        check_prerequisites
        authenticate_gcp
        deploy_airflow_dags
        ;;
    "monitoring")
        check_prerequisites
        authenticate_gcp
        setup_monitoring
        ;;
    "help")
        echo "Usage: $0 [deploy|build|infrastructure|cloud-run|airflow|monitoring|help]"
        echo ""
        echo "Commands:"
        echo "  deploy         - Full deployment (default)"
        echo "  build          - Build and push Docker image only"
        echo "  infrastructure - Deploy Terraform infrastructure only"
        echo "  cloud-run      - Deploy Cloud Run service only"
        echo "  airflow        - Deploy Airflow DAGs only"
        echo "  monitoring     - Setup monitoring only"
        echo "  help           - Show this help"
        ;;
    *)
        echo -e "${RED}❌ Unknown command: $1${NC}"
        echo "Use '$0 help' for available commands"
        exit 1
        ;;
esac
