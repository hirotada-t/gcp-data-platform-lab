# __generated__ by Terraform
# Please review these resources and move them into your main configuration files.

# __generated__ by Terraform from "projects/gcp-test-504808/locations/asia-northeast1/jobs/meta-ads-job"
resource "google_cloud_run_v2_job" "meta_ads" {
  annotations           = {}
  client                = "gcloud"
  client_version        = "568.0.0"
  deletion_policy       = "DELETE"
  deletion_protection   = true
  labels                = {}
  launch_stage          = "GA"
  location              = "asia-northeast1"
  name                  = "meta-ads-job"
  project               = "gcp-test-504808"
  run_execution_token   = null
  start_execution_token = null
  tags                  = null
  template {
    annotations = {}
    labels      = {}
    parallelism = 0
    task_count  = 1
    template {
      encryption_key                = null
      execution_environment         = "EXECUTION_ENVIRONMENT_GEN2"
      gpu_zonal_redundancy_disabled = false
      max_retries                   = 0
      service_account               = "meta-ads-job@gcp-test-504808.iam.gserviceaccount.com"
      timeout                       = "1800s"
      containers {
        args        = ["--output-dir=/tmp/meta-ads-output", "export", "--account-id=1545371116355779", "--since=2026-08-06", "--until=2026-08-06", "--datasets=campaign_daily"]
        command     = []
        depends_on  = []
        image       = "asia-northeast1-docker.pkg.dev/gcp-test-504808/platform-jobs/meta-ads:0c9d6b63ff81d34d14ac4c6ba134367489d26409"
        name        = null
        working_dir = null
        env {
          name  = "BQ_DATASET"
          value = "raw_meta_ads"
        }
        env {
          name  = "FB_ACCESS_TOKEN"
          value = null
          value_source {
            secret_key_ref {
              secret  = "meta-ads-access-token"
              version = "latest"
            }
          }
        }
        env {
          name  = "FB_TOKEN_SOURCE_ID"
          value = "lab"
        }
        env {
          name  = "GCP_PROJECT_ID"
          value = "gcp-test-504808"
        }
        resources {
          limits = {
            cpu    = "1000m"
            memory = "2Gi"
          }
        }
      }
    }
  }
  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      template[0].template[0].containers[0].image,
    ]
  }
}
