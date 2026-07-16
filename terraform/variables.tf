variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "savvy-kit-494301-m6"
}

variable "region" {
  description = "GCP Region"
  type        = string
  default     = "us-central1"
}

variable "location" {
  description = "BigQuery and Storage location"
  type        = string
  default     = "US"
}

variable "environment" {
  description = "Environment label"
  type        = string
  default     = "portfolio"
}