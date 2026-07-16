terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = "savvy-kit-494301-m6"
  region  = "us-central1"
}

# Dataset de BigQuery
resource "google_bigquery_dataset" "taxi_pipeline" {
  dataset_id  = "taxi_pipeline_tf"
  description = "Chicago Taxi Pipeline dataset managed by Terraform"
  location    = "US"

  labels = {
    env     = "portfolio"
    managed = "terraform"
  }
}

# Bucket de Cloud Storage
resource "google_storage_bucket" "taxi_data" {
  name                        = "taxi-pipeline-data-dylan"
  location                    = "US"
  force_destroy               = true
  uniform_bucket_level_access = true

  labels = {
    env     = "portfolio"
    managed = "terraform"
  }
}