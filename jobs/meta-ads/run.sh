#!/bin/sh
set -eu

python meta_ads_export.py "$@"

python load_to_bigquery.py
