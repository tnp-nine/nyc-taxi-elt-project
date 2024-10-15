terraform {
    backend "local" {}
    required_providers {
        google = {
        source  = "hashicorp/google"
        version = "6.0.1"
        }
    }
}

provider "google" {
  credentials = file(var.credentials)
  project     = var.project
  region      = var.region
}

# Data Lake
resource "google_storage_bucket" "datalake-taxidata-bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true

  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

# Data Warehouse
resource "google_bigquery_dataset" "dataset" {
    project = var.project
    dataset_id = var.bq_dataset_name
    location   = var.location
}