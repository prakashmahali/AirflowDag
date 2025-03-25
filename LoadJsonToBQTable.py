from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
import json
import pandas as pd
from datetime import datetime
from io import StringIO

# Configuration
GCS_BUCKET = "your-gcs-bucket"
GCS_JSON_FILE = "test.json"
BQ_PROJECT = "your-gcp-project"
BQ_DATASET = "your_dataset"
BQ_TABLE = "your_table"

# Function to read, transform, and write to GCS
def transform_json_to_csv():
    gcs_hook = GCSHook()
    
    # Download JSON file from GCS
    json_data = gcs_hook.download(bucket_name=GCS_BUCKET, object_name=GCS_JSON_FILE)
    records = json.loads(json_data.decode("utf-8"))

    # Convert to Pandas DataFrame
    df = pd.DataFrame(records)
    df.reset_index(drop=True, inplace=True)
    
    # Save CSV content to GCS
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=True)
    
    gcs_hook.upload(bucket_name=GCS_BUCKET, object_name="processed/test.csv", data=csv_buffer.getvalue())

# Airflow DAG Definition
with DAG(
    dag_id="gcs_json_to_bq",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:
    
    # Step 1: Transform JSON and upload as CSV
    transform_task = PythonOperator(
        task_id="transform_json",
        python_callable=transform_json_to_csv
    )

    # Step 2: Load transformed CSV to BigQuery
    load_to_bq_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=GCS_BUCKET,
        source_objects=["processed/test.csv"],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}",
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",
        autodetect=True
    )

    transform_task >> load_to_bq_task
