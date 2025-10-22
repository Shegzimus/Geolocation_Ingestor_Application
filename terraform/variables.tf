locals {
  data_lake_bucket = "geolocation_ingestor_app_data_lake"
}

variable "project_id" {
  description = "geolocation ingestor app project id"
  default = "grid-seek"
  type = string
}

variable "region" {
  description = "Region for GCP resources"
  default = "europe-west3"
  type = string
}

variable "google_api_key" {
  description = "Google API key"
  type = string
}

variable "container_image" {
  description = "Container image URL in Artifact Registry or GCR"
}

variable "terra_gcp_credentials" {
  description = "Path to the terraform gcp cred json file"
  default = "../.google/grid-seek-terraform.json"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

# ------------------------------------------------------------------------------------------------------------------------------
# # Cloud Run Variables

variable "cloud_run_image" {
  description = "Path to the cloud run image"
  default = "../.google/cloud_run/cloud_run_image.json"
  type = string
}

variable "cloud_run_service_name" {
  description = "Path to the cloud run service name"
  default = "../.google/cloud_run/cloud_run_service_name.json"
  type = string
}

variable "cloud_run_service_port" {
  description = "Path to the cloud run service port"
  default = "../.google/cloud_run/cloud_run_service_port.json"
  type = string
}

variable "cloud_run_service_url" {
  description = "Path to the cloud run service url"
  default = "../.google/cloud_run/cloud_run_service_url.json"
  type = string
}