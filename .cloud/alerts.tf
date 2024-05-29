resource "google_monitoring_alert_policy" "k8scronjob_api_preprocessor_prod_error_in_logs" {
  display_name = "k8scronjob-api-preprocessor-prod-error-in-logs"
  combiner     = "OR"

  conditions {
    display_name = "Error in logs"
    condition_matched_log {
      filter = <<EOF
        resource.type="k8s_container"
        resource.labels.cluster_name="contrails-gke-general"
        resource.labels.namespace_name="api-preprocessor-prod"
        labels.k8s-pod/job-name:"api-preprocessor-"
        severity>=ERROR
        EOF
    }
  }

  notification_channels = [
    # Nick Masson: SMS
    "projects/contrails-301217/notificationChannels/5296843968149494052",
  ]

  alert_strategy {
    notification_rate_limit {
      period = "1800s"
    }
    auto_close = "86400s"
  }
}
