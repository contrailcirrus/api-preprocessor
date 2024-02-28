provider "google" {
  project = "contrails-301217"
  region  = "us-east1"
  zone    = "us-east1-b"
}

provider "kubernetes" {
  config_path    = "~/.kube/config"
}

terraform {
 backend "gcs" {
   bucket  = "contrails-301217-infrastructure"
   prefix  = "terraform/state/api-preprocessor"
 }
}
