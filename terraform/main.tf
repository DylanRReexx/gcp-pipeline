terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# Dataset de BigQuery
resource "google_bigquery_dataset" "taxi_pipeline" {
  dataset_id  = "taxi_pipeline_tf"
  description = "Chicago Taxi Pipeline dataset managed by Terraform"
  location    = var.location

  labels = {
    env     = var.environment
    managed = "terraform"
  }
}

# Bucket de Cloud Storage
resource "google_storage_bucket" "taxi_data" {
  name                        = "taxi-pipeline-data-dylan"
  location                    = var.location
  force_destroy               = true
  uniform_bucket_level_access = true

  labels = {
    env     = var.environment
    managed = "terraform"
  }
}

# Service Account para el pipeline
resource "google_service_account" "pipeline_sa" {
  account_id   = "taxi-pipeline-sa"
  display_name = "Taxi Pipeline Service Account"
  description  = "Service account for Chicago Taxi Pipeline managed by Terraform"
}

# Permisos BigQuery
resource "google_project_iam_member" "bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Permisos Storage
resource "google_project_iam_member" "storage_editor" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# Permisos BigQuery Job (para correr queries)
resource "google_project_iam_member" "bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.pipeline_sa.email}"
}