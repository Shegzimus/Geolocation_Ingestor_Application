terraform {
  required_version = ">= 1.0"
  backend "local" {}  # Can change from "local" to "gcs" (for google) or "s3" (for aws)
  required_providers {
    google = {
      source  = "hashicorp/google"
    }
  }
}

# Instantiate the google cloud provider for STORAGE
provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.gcs_credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}

# Instantiate the google cloud provider for COMPUTE
provider "google" {
  project = var.project
  region = var.region
  credentials = file(var.vm_credentials)  # Use this if you do not want to set env-var GOOGLE_APPLICATION_CREDENTIALS
}


# ------------------------------------------------------------------------------------------------------------------------------
# # Storage Configuration
# # This is where you define the storage bucket and the BigQuery datasets
resource "google_storage_bucket" "gonrel-data-lake-bucket" {
  name          = "${local.data_lake_bucket}_${var.project}" # Concatenating DL bucket & Project name for unique naming
  location      = var.region

  # Optional, but recommended settings:
  storage_class = var.storage_class
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }
  lifecycle {
    prevent_destroy = true
  }

  force_destroy = true
}

resource "google_bigquery_dataset" "restaurant_locations_wh" {
  dataset_id = var.restaurant_locations_wh
  project    = var.project
  location   = var.region
}

resource "google_bigquery_dataset" "tiktok_data_wh" {
  dataset_id = var.tiktok_data_wh
  project    = var.project
  location   = var.region
}


# ------------------------------------------------------------------------------------------------------------------------------
# # VM Configuration
# Read in script file
locals {
  script_content = file("scripts/Install_docker.sh")
  gsc_service_acct = file("../airflow/.google/compute/gontrel-d8ddd88076e6.json")
  gsc_service_acct_base64 = base64encode(file("../airflow/.google/compute/gontrel-d8ddd88076e6.json"))
}


resource "google_compute_instance" "vm_instance" {
  name         = "ubuntu-airflow"
  machine_type = "e2-medium"
  zone         = var.zone

  boot_disk {
    initialize_params {
      # Find these in gcloud sdk by running gcloud compute machine-types|grep <what you are looking for e.g. ubuntu>
      image = "ubuntu-2004-focal-v20240307b"
    }
  }

  network_interface {
    network = "default"
    # You need access_config in order to get the public IP printed
    access_config{

    }
  }

  # Use the content of the script file in the metadata_startup_script
metadata = {
  ssh-keys = "${var.user}:${file(var.ssh_key_file)}"
  user-data = <<-EOF
    #!/bin/bash
    
    echo "Updating system packages..."
    sudo apt-get update

    echo "Installing Docker..."
    echo '${local.script_content}' > /tmp/install_docker.sh
    chmod +x /tmp/install_docker.sh
    bash /tmp/install_docker.sh

    sudo mkdir -p /home/${var.user}/.google/credentials
    chmod -R 755 /home/${var.user}/.google

    echo "Cloning repository..."
    cd /home/${var.user} 
    git clone --branch mybranch --depth 1 https://github.com/Shegzimus/airflow-docker-compose2025.git

    sudo mkdir -p /home/${var.user}/airflow-docker-compose2025/.google/credentials
    chmod -R 755 /home/${var.user}/airflow-docker-compose2025/.google
    
    echo '${local.gsc_service_acct}' > /home/${var.user}/airflow-docker-compose2025/.google/credentials/google_credentials.json
    cd ./airflow-docker-compose2025

    
    echo "This is a file created on $(date)" > /home/shegz/"$(date +"%Y-%m-%d_%H-%M-%S").txt"

    sudo mkdir -p ./dags ./logs ./plugins ./config
    sudo chown -R 1001:1001 ./dags ./logs ./plugins ./config
    chmod -R 775 ./dags ./logs ./plugins ./config
    sudo usermod -aG docker ${var.user}
    newgrp docker
    docker compose up airflow-init

    sleep 30
    docker compose up

  EOF
    }
}

output "public_ip" {
  value = google_compute_instance.vm_instance.network_interface[0].access_config[0].nat_ip
}