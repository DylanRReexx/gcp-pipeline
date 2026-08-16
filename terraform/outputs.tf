output "bigquery_dataset_id" {
  description = "BigQuery dataset ID"
  value       = google_bigquery_dataset.taxi_pipeline.dataset_id
}

output "storage_bucket_url" {
  description = "Cloud Storage bucket URL"
  value       = google_storage_bucket.taxi_data.url
}

output "service_account_email" {
  description = "Service Account email for the pipeline"
  value       = google_service_account.pipeline_sa.email
}