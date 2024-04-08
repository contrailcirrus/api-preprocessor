resource "google_bigquery_table" "cocip_regions_dev" {
  dataset_id = "flights_pipeline_dev"  # defined in `flights_pipeline` repo
  table_id   = "cocip_regions_dev"
  friendly_name = "[DEV] cocip regions"
  description = "region polygons generated from CoCip"
  deletion_protection = true
  time_partitioning {
    field = "timestamp"
    type = "WEEK"
  }
  schema = file("${path.module}/schemas/cocip_regions.json")
}