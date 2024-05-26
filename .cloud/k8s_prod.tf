resource "kubernetes_namespace" "api_preprocessor-namespace-prod" {
  metadata {
    annotations = {
      name = "api-preprocessor-prod"
    }
    labels = {
      name = "api-preprocessor-prod"
    }
    name = "api-preprocessor-prod"
  }
}

resource "kubernetes_service_account" "api-preprocessor-k8s-default-sa-prod" {
  metadata {
    name = "api-preprocessor-k8s-default-sa"
    namespace = kubernetes_namespace.api_preprocessor-namespace-prod.id
    annotations = {
      "iam.gke.io/gcp-service-account" = google_service_account.api_preprocessor_sa.email
    }
  }
  depends_on = [
    google_service_account.api_preprocessor_sa,
  ]
}
