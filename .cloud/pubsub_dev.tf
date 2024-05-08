# ---------------
# TOPICS
# ---------------

resource "google_pubsub_topic" "dev_api_preprocessor_cocip_regions_bigquery" {
  name = "dev-mpl-api-preprocessor-cocip-regions-bigquery"
}

resource "google_pubsub_topic" "dev_api_preprocessor_cocip_regions_bigquery_dead_letter" {
  name = "dev-mpl-api-preprocessor-cocip-regions-bigquery-dead-letter"
}

# ---------------
# SUBSCRIPTIONS
# ---------------

resource "google_pubsub_subscription" "dev_api_preprocessor_cocip_regions_bigquery_delivery" {
  name  = "dev-mpl-api-preprocessor-cocip-regions-bigquery-delivery"
  topic = google_pubsub_topic.dev_api_preprocessor_cocip_regions_bigquery.id

  bigquery_config {
    table = "contrails-301217.${google_bigquery_table.cocip_regions_dev.dataset_id}.${google_bigquery_table.cocip_regions_dev.table_id}"
    use_table_schema = true
    drop_unknown_fields = true
  }

  dead_letter_policy {
    max_delivery_attempts = 10
    dead_letter_topic = google_pubsub_topic.dev_api_preprocessor_cocip_regions_bigquery_dead_letter.id
  }

    retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }

  expiration_policy {
    ttl = ""
  }

  depends_on = [
    google_pubsub_topic.dev_api_preprocessor_cocip_regions_bigquery,
    google_pubsub_topic.dev_api_preprocessor_cocip_regions_bigquery_dead_letter,
    google_bigquery_table.cocip_regions_dev,
  ]
}

resource "google_pubsub_subscription" "dev_api_preprocessor_cocip_regions_bigquery_dead_letter" {
  name  = "dev-mpl-api-preprocessor-cocip-regions-bigquery-dead-letter"
  topic = google_pubsub_topic.dev_api_preprocessor_cocip_regions_bigquery_dead_letter.id
  message_retention_duration = "86400s"  # 1 day

  expiration_policy {
    ttl = ""
  }

  depends_on = [
    google_pubsub_topic.dev_api_preprocessor_cocip_regions_bigquery_dead_letter,
  ]
}
