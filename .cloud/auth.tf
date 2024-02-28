# ------------------------------------
# Service account & service account->resource bindings for CloudRun instance (hosted API service)
# ------------------------------------
resource "google_service_account" "api_preprocessor_sa" {
  account_id                   = "api-preprocessor"
  create_ignore_already_exists = null
  description                  = null
  disabled                     = false
  display_name                 = "api-preprocessor-service-account"
  project                      = "contrails-301217"
  timeouts {
    create = null
  }
}

resource "google_project_iam_custom_role" "api_preprocessor_role" {
  description = "custom role for the api preprocessor cloud-run instance"
  permissions = [
    "logging.logEntries.create",
    "storage.objects.create",
    "storage.objects.get",
    "storage.objects.list",
    "pubsub.snapshots.seek",
    "pubsub.subscriptions.consume",
    "pubsub.topics.attachSubscription",
  ]
  project     = "contrails-301217"
  role_id     = "api_preprocessor_role"
  stage       = null
  title       = "api_preprocessor_role"
}

resource "google_project_iam_member" "api_preprocessor_sa_binding_A" {
  member  = "serviceAccount:${google_service_account.api_preprocessor_sa.email}"
  project = "contrails-301217"
  role    = google_project_iam_custom_role.api_preprocessor_role.id
}

resource "google_service_account_iam_binding" "k8s_sa_to_api_preprocessor_sa_binding" {
  service_account_id = google_service_account.api_preprocessor_sa.id
  role               = "roles/iam.workloadIdentityUser"

  members = [
    "serviceAccount:contrails-301217.svc.id.goog[${kubernetes_service_account.api-preprocessor-k8s-default-sa.id}]",
  ]
  depends_on = [
    kubernetes_service_account.api-preprocessor-k8s-default-sa,
  ]
}