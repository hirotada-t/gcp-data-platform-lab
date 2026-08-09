locals {
  project_id     = "gcp-test-504808"
  project_number = "18325870326"

  terraform_deployer_email = "terraform-deployer@gcp-test-504808.iam.gserviceaccount.com"
  build_service_account    = "18325870326-compute@developer.gserviceaccount.com"

  github_repository_id = "1326491907"

  github_repository_principal = "principalSet://iam.googleapis.com/projects/18325870326/locations/global/workloadIdentityPools/github-pool/attribute.repository_id/1326491907"

  cloud_build_source_bucket = "gcp-test-504808_cloudbuild"
}

# GitHub ActionsがCloud Buildへソースをアップロードするための権限
resource "google_storage_bucket_iam_member" "github_deployer_build_source" {
  for_each = toset([
    "roles/storage.bucketViewer",
    "roles/storage.objectUser",
  ])

  bucket = local.cloud_build_source_bucket
  role   = each.value
  member = "serviceAccount:${google_service_account.github_deployer.email}"
}

# GitHub Actions アプリデプロイ担当のProject権限
resource "google_project_iam_member" "github_deployer_project_roles" {
  for_each = toset([
    "roles/cloudbuild.builds.editor",
    "roles/run.developer",
    "roles/serviceusage.serviceUsageConsumer",
  ])

  project = local.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.github_deployer.email}"
}

# Artifact Registryはplatform-jobsだけ読めればよい
resource "google_artifact_registry_repository_iam_member" "github_deployer_reader" {
  project    = local.project_id
  location   = google_artifact_registry_repository.app_images.location
  repository = google_artifact_registry_repository.app_images.repository_id

  role   = "roles/artifactregistry.reader"
  member = "serviceAccount:${google_service_account.github_deployer.email}"
}

# GitHub Actions → github-deployer
resource "google_service_account_iam_member" "github_deployer_wif" {
  service_account_id = google_service_account.github_deployer.name

  role   = "roles/iam.workloadIdentityUser"
  member = local.github_repository_principal
}

# github-deployer → Meta Ads runtime SA
resource "google_service_account_iam_member" "github_deployer_act_as_meta_ads" {
  service_account_id = google_service_account.meta_ads_job.name

  role   = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.github_deployer.email}"
}

# github-deployer → Cloud Build SA
resource "google_service_account_iam_member" "github_deployer_act_as_build" {
  service_account_id = "projects/${local.project_id}/serviceAccounts/${local.build_service_account}"

  role   = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${google_service_account.github_deployer.email}"
}

# Terraform → Meta Ads runtime SA
resource "google_service_account_iam_member" "terraform_deployer_act_as_meta_ads" {
  service_account_id = google_service_account.meta_ads_job.name

  role   = "roles/iam.serviceAccountUser"
  member = "serviceAccount:${local.terraform_deployer_email}"
}

# Meta Ads JobはBigQuery Jobを実行できる
resource "google_project_iam_member" "meta_ads_bigquery_job_user" {
  project = local.project_id

  role   = "roles/bigquery.jobUser"
  member = "serviceAccount:${google_service_account.meta_ads_job.email}"
}

# データ編集はraw_meta_ads Datasetだけ
resource "google_bigquery_dataset_iam_member" "meta_ads_data_editor" {
  project    = local.project_id
  dataset_id = google_bigquery_dataset.raw_meta_ads.dataset_id

  role   = "roles/bigquery.dataEditor"
  member = "serviceAccount:${google_service_account.meta_ads_job.email}"
}