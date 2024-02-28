resource "kubernetes_namespace" "api_preprocessor" {
  metadata {
    annotations = {
      name = "api-preprocessor"
    }
    labels = {
      name = "api-preprocessor"
    }
    name = "api-preprocessor"
  }
}

resource "kubernetes_service_account" "api-preprocessor-k8s-default-sa" {
  metadata {
    name = "api-preprocessor-k8s-default-sa"
    namespace = kubernetes_namespace.api_preprocessor.id
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.api_preprocessor_sa.email
    }
  }
  depends_on = [
    google_service_account.api_preprocessor_sa,
  ]
}