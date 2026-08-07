FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY meta_ads_export.py .
COPY load_to_bigquery.py .
COPY run.sh .

ENTRYPOINT ["/app/run.sh"]
