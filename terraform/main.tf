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