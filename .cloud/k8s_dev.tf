resource "kubernetes_namespace" "api_preprocessor-namespace-dev" {
  metadata {
    annotations = {
      name = "api-preprocessor-dev"
    }
    labels = {
      name = "api-preprocessor-dev"
    }
    name = "api-preprocessor-dev"
  }
}

resource "kubernetes_service_account" "api-preprocessor-k8s-default-sa-dev" {
  metadata {
    name = "api-preprocessor-k8s-default-sa"
    namespace = kubernetes_namespace.api_preprocessor-namespace-dev.id
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.api_preprocessor_sa.email
    }
  }
  depends_on = [
    google_service_account.api_preprocessor_sa,
  ]
}
