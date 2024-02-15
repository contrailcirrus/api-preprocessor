resource "google_artifact_registry_repository" "api-preprocessor-registry" {
  location      = "us-east1"
  repository_id = "api-preprocessor"
  description   = "api preprocessor docker images"
  format        = "DOCKER"
}