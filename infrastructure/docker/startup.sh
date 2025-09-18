#!/bin/bash
# Startup script for maritime reports container in GCP

set -e

echo "🚢 Starting Maritime Activity Reports CDC/CDF Service"
echo "=================================================="

# Set environment variables
export PYTHONPATH="/app/src:$PYTHONPATH"
export SPARK_LOCAL_IP="0.0.0.0"
export PYSPARK_PYTHON="python3"
export PYSPARK_DRIVER_PYTHON="python3"

# Check if running in Google Cloud
if [ -n "$GOOGLE_CLOUD_PROJECT" ]; then
    echo "☁️  Running in Google Cloud Project: $GOOGLE_CLOUD_PROJECT"
    
    # Authenticate using service account (if not using managed identity)
    if [ -n "$GOOGLE_APPLICATION_CREDENTIALS" ]; then
        echo "🔐 Authenticating with service account..."
        gcloud auth activate-service-account --key-file=$GOOGLE_APPLICATION_CREDENTIALS
    else
        echo "🔐 Using default service account authentication"
    fi
else
    echo "🏠 Running in local/development mode"
fi

# Check configuration file
if [ ! -f "$MARITIME_CONFIG_PATH" ]; then
    echo "❌ Configuration file not found: $MARITIME_CONFIG_PATH"
    echo "Creating default configuration..."
    
    # Create basic configuration for GCP
    cat > /app/config/maritime_config.yaml << EOF
project_name: "maritime-activity-reports-cdc"
environment: "${ENVIRONMENT:-dev}"

spark:
  app_name: "Maritime-Activity-Reports-CDC-GCP"
  executor_memory: "4g"
  executor_cores: 2
  executor_instances: 2

bronze:
  base_path: "gs://${GOOGLE_CLOUD_PROJECT}-maritime-data-${ENVIRONMENT:-dev}/bronze"
  checkpoint_path: "gs://${GOOGLE_CLOUD_PROJECT}-maritime-checkpoints-${ENVIRONMENT:-dev}/bronze"
  retention_days: 30

silver:
  base_path: "gs://${GOOGLE_CLOUD_PROJECT}-maritime-data-${ENVIRONMENT:-dev}/silver"
  checkpoint_path: "gs://${GOOGLE_CLOUD_PROJECT}-maritime-checkpoints-${ENVIRONMENT:-dev}/silver"
  retention_days: 30

gold:
  base_path: "gs://${GOOGLE_CLOUD_PROJECT}-maritime-data-${ENVIRONMENT:-dev}/gold"
  checkpoint_path: "gs://${GOOGLE_CLOUD_PROJECT}-maritime-checkpoints-${ENVIRONMENT:-dev}/gold"
  retention_days: 90

cdc:
  enable_cdf: true
  processing_time_seconds: 30
  checkpoint_location: "gs://${GOOGLE_CLOUD_PROJECT}-maritime-checkpoints-${ENVIRONMENT:-dev}/cdc"

bigquery:
  project_id: "${GOOGLE_CLOUD_PROJECT}"
  dataset: "maritime_reports_${ENVIRONMENT:-dev}"

gcs:
  bucket_name: "${GOOGLE_CLOUD_PROJECT}-maritime-data-${ENVIRONMENT:-dev}"
EOF

    export MARITIME_CONFIG_PATH="/app/config/maritime_config.yaml"
fi

echo "✅ Configuration file: $MARITIME_CONFIG_PATH"

# Determine startup mode based on environment variables
STARTUP_MODE="${STARTUP_MODE:-streaming}"

case $STARTUP_MODE in
    "setup")
        echo "🔧 Setting up tables and infrastructure..."
        pdm run maritime-reports setup-tables
        echo "✅ Setup completed"
        ;;
    
    "streaming")
        echo "🌊 Starting streaming processors..."
        # Setup tables first if they don't exist
        pdm run maritime-reports setup-tables || echo "⚠️  Tables might already exist"
        
        # Start streaming
        pdm run maritime-reports start-streaming
        ;;
    
    "batch")
        echo "📊 Running batch processing..."
        pdm run maritime-reports generate-reports --date $(date +%Y-%m-%d) --type all
        ;;
    
    "health-check")
        echo "🏥 Running health check..."
        pdm run maritime-reports health-check
        ;;
    
    "simulate")
        echo "🎲 Generating simulated data..."
        pdm run maritime-reports simulate-data --vessels ${NUM_VESSELS:-10} --records ${NUM_RECORDS:-100}
        ;;
    
    "web-server")
        echo "🌐 Starting web server for monitoring..."
        # Start a simple web server for health checks and monitoring
        python3 -c "
import http.server
import socketserver
import json
from maritime_activity_reports.orchestrator.cdc_cdf_orchestrator import CDCCDFOrchestrator
from maritime_activity_reports.models.config import MaritimeConfig

class HealthHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        if self.path == '/health':
            try:
                config = MaritimeConfig.from_file('$MARITIME_CONFIG_PATH')
                orchestrator = CDCCDFOrchestrator(config)
                health = orchestrator.health_check()
                
                self.send_response(200 if health['overall_status'] == 'healthy' else 503)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(health).encode())
            except Exception as e:
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({'error': str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

with socketserver.TCPServer(('', 8080), HealthHandler) as httpd:
    print('🌐 Health check server running on port 8080')
    httpd.serve_forever()
"
        ;;
    
    *)
        echo "❌ Unknown startup mode: $STARTUP_MODE"
        echo "Available modes: setup, streaming, batch, health-check, simulate, web-server"
        exit 1
        ;;
esac

echo "🏁 Maritime Reports service completed"
