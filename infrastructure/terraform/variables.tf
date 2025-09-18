# Terraform variables for maritime activity reports deployment

variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = "europe-west2"
}

variable "zone" {
  description = "The GCP zone"
  type        = string
  default     = "europe-west2-a"
}

variable "environment" {
  description = "Environment name (dev, staging, prod)"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, staging, prod."
  }
}

variable "service_account_email" {
  description = "Service account email for maritime services"
  type        = string
}

variable "domain_name" {
  description = "Domain name for access control"
  type        = string
  default     = "your-company.com"
}

# Dataproc configuration
variable "dataproc_master_machine_type" {
  description = "Machine type for Dataproc master node"
  type        = string
  default     = "e2-standard-4"
}

variable "dataproc_worker_machine_type" {
  description = "Machine type for Dataproc worker nodes"
  type        = string
  default     = "e2-standard-4"
}

variable "dataproc_worker_count" {
  description = "Number of Dataproc worker nodes"
  type        = number
  default     = 2
}

# BigQuery configuration
variable "bigquery_location" {
  description = "BigQuery dataset location"
  type        = string
  default     = "europe-west2"
}

# Cloud Composer configuration
variable "composer_node_count" {
  description = "Number of Cloud Composer nodes"
  type        = number
  default     = 1
}

variable "composer_machine_type" {
  description = "Machine type for Cloud Composer nodes"
  type        = string
  default     = "e2-standard-2"
}

# Cloud Run configuration
variable "cloud_run_cpu" {
  description = "CPU allocation for Cloud Run service"
  type        = string
  default     = "2000m"
}

variable "cloud_run_memory" {
  description = "Memory allocation for Cloud Run service"
  type        = string
  default     = "4Gi"
}

variable "cloud_run_min_instances" {
  description = "Minimum instances for Cloud Run service"
  type        = number
  default     = 1
}

variable "cloud_run_max_instances" {
  description = "Maximum instances for Cloud Run service"
  type        = number
  default     = 10
}

# Storage configuration
variable "storage_retention_days" {
  description = "Data retention period in days"
  type        = number
  default     = 90
}

variable "storage_nearline_days" {
  description = "Days before moving to Nearline storage"
  type        = number
  default     = 30
}

# Monitoring configuration
variable "enable_monitoring" {
  description = "Enable Cloud Monitoring and alerting"
  type        = bool
  default     = true
}

variable "alert_email" {
  description = "Email for monitoring alerts"
  type        = string
  default     = "data-team@your-company.com"
}

# Network configuration
variable "network_name" {
  description = "VPC network name"
  type        = string
  default     = "default"
}

variable "subnet_name" {
  description = "Subnet name for resources"
  type        = string
  default     = "default"
}
