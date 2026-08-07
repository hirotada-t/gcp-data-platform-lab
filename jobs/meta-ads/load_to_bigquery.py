import os
from pathlib import Path

from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "gcp-test-504808")
DATASET_ID = os.environ.get("BQ_DATASET", "raw_meta_ads")
OUTPUT_ROOT = Path("/tmp/meta-ads-output")

files = list(OUTPUT_ROOT.rglob("campaign_daily.parquet"))

if len(files) != 1:
    raise RuntimeError(
        f"campaign_daily.parquet expected exactly once, found={len(files)}"
    )

path = files[0]
table_id = f"{PROJECT_ID}.{DATASET_ID}.campaign_daily_lab"

client = bigquery.Client(project=PROJECT_ID)

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

print(f"Loading {path} -> {table_id}")

with path.open("rb") as f:
    job = client.load_table_from_file(
        f,
        table_id,
        job_config=job_config,
    )

job.result()

table = client.get_table(table_id)

print(
    f"BigQuery load completed: "
    f"table={table_id} rows={table.num_rows}"
)
