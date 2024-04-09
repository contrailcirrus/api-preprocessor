resource "google_pubsub_topic" "cocip_regions_bigquery_dev" {
  name = "cocip-regions-bigquery-dev"
}

resource "google_pubsub_topic" "cocip_regions_bigquery_dead_letter_dev" {
  name = "cocip-regions-bigquery-dead-letter-dev"
}

resource "google_pubsub_subscription" "cocip_regions_bigquery_delivery_dev" {
  name  = "cocip-regions-bigquery-delivery-dev"
  topic = google_pubsub_topic.cocip_regions_bigquery_dev.id

  bigquery_config {
    table = "contrails-301217.${google_bigquery_table.cocip_regions_dev.dataset_id}.${google_bigquery_table.cocip_regions_dev.table_id}"
    use_table_schema = true
    drop_unknown_fields = true
  }

  dead_letter_policy {
    max_delivery_attempts = 10
    dead_letter_topic = google_pubsub_topic.cocip_regions_bigquery_dead_letter_dev.id
  }

    retry_policy {
    minimum_backoff = "1s"
    maximum_backoff = "60s"
  }

  expiration_policy {
    ttl = ""
  }

  depends_on = [
    google_pubsub_topic.cocip_regions_bigquery_dev,
    google_pubsub_topic.cocip_regions_bigquery_dead_letter_dev,
    google_bigquery_table.cocip_regions_dev,
  ]
}

resource "google_pubsub_subscription" "cocip_regions_bigquery_dead_letter_dev" {
  name  = "cocip-regions-bigquery-dead-letter-dev"
  topic = google_pubsub_topic.cocip_regions_bigquery_dead_letter_dev.id
  message_retention_duration = "86400s"  # 1 day

  expiration_policy {
    ttl = ""
  }

  depends_on = [
    google_pubsub_topic.cocip_regions_bigquery_dead_letter_dev,
  ]
}
