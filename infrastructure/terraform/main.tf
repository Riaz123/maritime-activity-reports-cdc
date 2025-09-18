# Maritime Activity Reports CDC/CDF - GCP Infrastructure
terraform {
  required_version = ">= 1.0"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

# Configure the Google Cloud Provider
provider "google" {
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

# Variables
variable "project_id" {
  description = "GCP Project ID"
  type        = string
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "europe-west2"
}

variable "zone" {
  description = "GCP Zone"
  type        = string
  default     = "europe-west2-a"
}

variable "environment" {
  description = "Environment (dev, staging, prod)"
  type        = string
  default     = "dev"
}

variable "service_account_email" {
  description = "Service account email for maritime services"
  type        = string
}

# Data sources
data "google_client_config" "default" {}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "compute.googleapis.com",
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "pubsub.googleapis.com",
    "composer.googleapis.com",
    "run.googleapis.com",
    "artifactregistry.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
    "cloudbuild.googleapis.com"
  ])

  project = var.project_id
  service = each.key

  disable_dependent_services = false
}

# Cloud Storage buckets
resource "google_storage_bucket" "maritime_data" {
  name          = "${var.project_id}-maritime-data-${var.environment}"
  location      = var.region
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  labels = {
    environment = var.environment
    project     = "maritime-activity-reports"
    layer       = "data-storage"
  }
}

# Cloud Storage bucket for checkpoints
resource "google_storage_bucket" "maritime_checkpoints" {
  name          = "${var.project_id}-maritime-checkpoints-${var.environment}"
  location      = var.region
  force_destroy = true

  uniform_bucket_level_access = true

  labels = {
    environment = var.environment
    project     = "maritime-activity-reports"
    layer       = "streaming-checkpoints"
  }
}

# BigQuery dataset
resource "google_bigquery_dataset" "maritime_reports" {
  dataset_id  = "maritime_reports_${var.environment}"
  location    = var.region
  description = "Maritime Activity Reports with CDC/CDF - ${var.environment}"

  labels = {
    environment = var.environment
    project     = "maritime-activity-reports"
    layer       = "analytics"
  }

  # Access control
  access {
    role          = "OWNER"
    user_by_email = var.service_account_email
  }

  access {
    role   = "READER"
    domain = "your-company.com"  # Replace with your domain
  }
}

# Pub/Sub topics for CDC events
resource "google_pubsub_topic" "maritime_cdc_events" {
  name = "maritime-cdc-events-${var.environment}"

  labels = {
    environment = var.environment
    project     = "maritime-activity-reports"
    layer       = "messaging"
  }
}

resource "google_pubsub_topic" "maritime_data_quality_alerts" {
  name = "maritime-data-quality-alerts-${var.environment}"

  labels = {
    environment = var.environment
    project     = "maritime-activity-reports"
    layer       = "monitoring"
  }
}

# Pub/Sub subscriptions
resource "google_pubsub_subscription" "maritime_cdc_subscription" {
  name  = "maritime-cdc-subscription-${var.environment}"
  topic = google_pubsub_topic.maritime_cdc_events.name

  # Message retention
  message_retention_duration = "604800s" # 7 days

  # Dead letter policy
  dead_letter_policy {
    dead_letter_topic     = google_pubsub_topic.maritime_cdc_dead_letter.id
    max_delivery_attempts = 5
  }

  labels = {
    environment = var.environment
    project     = "maritime-activity-reports"
  }
}

resource "google_pubsub_topic" "maritime_cdc_dead_letter" {
  name = "maritime-cdc-dead-letter-${var.environment}"
}

# Artifact Registry for Docker images
resource "google_artifact_registry_repository" "maritime_reports" {
  location      = var.region
  repository_id = "maritime-reports-${var.environment}"
  description   = "Maritime Activity Reports Docker images"
  format        = "DOCKER"

  labels = {
    environment = var.environment
    project     = "maritime-activity-reports"
  }
}

# Service Account for maritime services
resource "google_service_account" "maritime_service_account" {
  account_id   = "maritime-reports-${var.environment}"
  display_name = "Maritime Reports Service Account - ${var.environment}"
  description  = "Service account for maritime activity reports CDC/CDF processing"
}

# IAM bindings for service account
resource "google_project_iam_member" "maritime_dataproc_worker" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.maritime_service_account.email}"
}

resource "google_project_iam_member" "maritime_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.admin"
  member  = "serviceAccount:${google_service_account.maritime_service_account.email}"
}

resource "google_project_iam_member" "maritime_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.maritime_service_account.email}"
}

resource "google_project_iam_member" "maritime_pubsub_editor" {
  project = var.project_id
  role    = "roles/pubsub.editor"
  member  = "serviceAccount:${google_service_account.maritime_service_account.email}"
}

# Dataproc cluster for development (optional)
resource "google_dataproc_cluster" "maritime_dev_cluster" {
  count = var.environment == "dev" ? 1 : 0
  
  name   = "maritime-dev-cluster"
  region = var.region

  cluster_config {
    staging_bucket = google_storage_bucket.maritime_data.name

    master_config {
      num_instances = 1
      machine_type  = "e2-medium"
      disk_config {
        boot_disk_type    = "pd-ssd"
        boot_disk_size_gb = 50
      }
    }

    worker_config {
      num_instances = 2
      machine_type  = "e2-standard-2"
      disk_config {
        boot_disk_type    = "pd-standard"
        boot_disk_size_gb = 50
      }
    }

    software_config {
      image_version = "2.2-debian12"
      properties = {
        "spark:spark.sql.extensions"                                      = "io.delta.sql.DeltaSparkSessionExtension"
        "spark:spark.sql.catalog.spark_catalog"                          = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        "spark:spark.jars.packages"                                       = "io.delta:delta-spark_2.13:3.2.1"
        "spark:spark.databricks.delta.retentionDurationCheck.enabled"    = "false"
        "spark:spark.sql.adaptive.enabled"                               = "true"
        "spark:spark.sql.adaptive.coalescePartitions.enabled"            = "true"
      }
    }

    initialization_action {
      script      = "gs://${google_storage_bucket.maritime_data.name}/scripts/dataproc-init.sh"
      timeout_sec = 300
    }
  }

  labels = {
    environment = var.environment
    project     = "maritime-reports"
  }
}

# Cloud Composer (Airflow) environment
resource "google_composer_environment" "maritime_airflow" {
  name   = "maritime-composer-${var.environment}"
  region = var.region

  config {
    node_count = var.environment == "prod" ? 3 : 1

    node_config {
      zone         = var.zone
      machine_type = var.environment == "prod" ? "e2-standard-4" : "e2-standard-2"
      disk_size_gb = 50

      service_account = google_service_account.maritime_service_account.email
    }

    software_config {
      image_version = "composer-2-airflow-2.8.1"
      
      pypi_packages = {
        "maritime-activity-reports-cdc" = ""
        "delta-spark"                   = ">=3.2.1"
        "google-cloud-bigquery"         = ">=3.11.4"
        "structlog"                     = ">=23.2.0"
      }

      env_variables = {
        MARITIME_CONFIG_PATH = "/home/airflow/gcs/data/config/maritime_config.yaml"
        PYTHONPATH           = "/home/airflow/gcs/dags/maritime_activity_reports"
      }
    }

    private_environment_config {
      enable_private_endpoint = true
    }
  }

  labels = {
    environment = var.environment
    project     = "maritime-reports"
  }
}

# Cloud Run service for streaming processors
resource "google_cloud_run_service" "maritime_streaming" {
  name     = "maritime-streaming-${var.environment}"
  location = var.region

  template {
    spec {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.maritime_reports.repository_id}/maritime-reports:latest"
        
        env {
          name  = "MARITIME_CONFIG_PATH"
          value = "/app/config/maritime_config.yaml"
        }
        
        env {
          name  = "GOOGLE_CLOUD_PROJECT"
          value = var.project_id
        }

        resources {
          limits = {
            cpu    = "2000m"
            memory = "4Gi"
          }
        }

        ports {
          container_port = 8080
        }
      }

      service_account_name = google_service_account.maritime_service_account.email
    }

    metadata {
      annotations = {
        "autoscaling.knative.dev/minScale" = "1"
        "autoscaling.knative.dev/maxScale" = var.environment == "prod" ? "10" : "3"
        "run.googleapis.com/execution-environment" = "gen2"
      }
    }
  }

  traffic {
    percent         = 100
    latest_revision = true
  }

  depends_on = [google_project_service.required_apis]
}

# Outputs
output "maritime_data_bucket" {
  description = "Maritime data storage bucket"
  value       = google_storage_bucket.maritime_data.name
}

output "maritime_checkpoints_bucket" {
  description = "Maritime checkpoints bucket"
  value       = google_storage_bucket.maritime_checkpoints.name
}

output "bigquery_dataset" {
  description = "BigQuery dataset for maritime reports"
  value       = google_bigquery_dataset.maritime_reports.dataset_id
}

output "service_account_email" {
  description = "Service account email for maritime services"
  value       = google_service_account.maritime_service_account.email
}

output "artifact_registry_url" {
  description = "Artifact Registry URL for Docker images"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.maritime_reports.repository_id}"
}

output "composer_environment_name" {
  description = "Cloud Composer environment name"
  value       = google_composer_environment.maritime_airflow.name
}

output "cloud_run_url" {
  description = "Cloud Run service URL"
  value       = google_cloud_run_service.maritime_streaming.status[0].url
}
