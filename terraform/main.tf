terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "4.51.0"
    }
  }
}

provider "google" {
  credentials = file("../service-account/service-account.json")

  project = var.project_id
  region  = var.region
}

variable "project_id" {
  description = "GCP project ID where the storage bucket and BigQuery datasets will be created."
  type        = string
  default     = "final-project-test-393302"
}

variable "region" {
  description = "GCP region for the project resources."
  type        = string
  default     = "asia-southeast2"
}

variable "gcs_bucket_name" {
  description = "Globally unique Google Cloud Storage bucket name for the data lake."
  type        = string
  default     = "final-project-lake"
}

resource "google_storage_bucket" "data-lake-bucket" {
  name     = var.gcs_bucket_name
  location = var.region

  # Optional, but recommended settings:
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }
    condition {
      age = 30 // days
    }
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "warehouse-dataset" {
  dataset_id = "onlinetransaction_wh"
  project    = var.project_id
  location   = var.region
}

resource "google_bigquery_dataset" "stream-dataset" {
  dataset_id = "onlinetransaction_stream"
  project    = var.project_id
  location   = var.region
}
