resource "google_bigquery_dataset" "raw_meta_ads" {
  dataset_id = "raw_meta_ads"
  location   = "asia-northeast1"

  delete_contents_on_destroy = false
}

resource "google_service_account" "meta_ads_job" {
  account_id   = "meta-ads-job"
  display_name = "Meta Ads Cloud Run Job"
}

resource "google_service_account" "github_deployer" {
  account_id   = "github-deployer"
  display_name = "GitHub Actions Deployer"
}

resource "google_secret_manager_secret" "meta_ads_access_token" {
  secret_id = "meta-ads-access-token"

  replication {
    auto {}
  }
}

resource "google_artifact_registry_repository" "app_images" {
  location      = "asia-northeast1"
  repository_id = "platform-jobs"
  description   = "Docker images for GCP data platform jobs"
  format        = "DOCKER"
}

resource "google_service_account" "cloud_build_meta_ads" {
  account_id   = "cloud-build-meta-ads"
  display_name = "Cloud Build for Meta Ads"
}