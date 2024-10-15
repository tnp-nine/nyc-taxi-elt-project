variable "credentials" {
  description = "My Credentials"
  default     = "/Users/tanupats/Desktop/google/credentials/google_credentials.json"
}

variable "project" {
  description = "Project"
  default     = "de-pro-436017"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "us-central1"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "de-proeject-taxi-tanu"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "nyc_taxi_dataset"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"

}