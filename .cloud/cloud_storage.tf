# ------------------------------------
# Google Cloud Storage (GCS) resources
# ------------------------------------

resource "google_storage_bucket" "api_preprocessor_bucket_dev" {
  default_event_based_hold    = false
  enable_object_retention     = false
  force_destroy               = false
  labels                      = {}
  location                    = "US-EAST1"
  name                        = "contrails-301217-api-preprocessor-dev"
  project                     = "contrails-301217"
  public_access_prevention    = "enforced"
  requester_pays              = false
  rpo                         = null
  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  timeouts {
    create = null
    read   = null
    update = null
  }
  lifecycle_rule {
  condition {
    age = 2
  }
  action {
    type = "Delete"
  }
}
}

