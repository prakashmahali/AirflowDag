from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.gcs import GCSHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage, bigquery
import json
import os

# Define constants
GCS_BUCKET = 'your-gcs-bucket'
GCS_FOLDER = 'your/gcs/folder'
BQ_PROJECT = 'your-bq-project'
BQ_DATASET = 'your-bq-dataset'
BQ_TABLE = 'your-bq-table'

def load_json_to_bq(**kwargs):
    # Initialize GCS client
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET)
    blobs = bucket.list_blobs(prefix=GCS_FOLDER)
    
    # Initialize BigQuery client
    bigquery_client = bigquery.Client()

    # Table reference
    table_ref = bigquery_client.dataset(BQ_DATASET).table(BQ_TABLE)
    
    # List to hold JSON lines
    json_lines = []

    for blob in blobs:
        if blob.name.endswith('.json'):  # Check for JSON files
            content = blob.download_as_text()
            records = json.loads(content)
            for record in records:
                json_lines.append(json.dumps(record))
    
    # Load JSON lines into BigQuery
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("json_data", "STRING")
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )

    load_job = bigquery_client.load_table_from_json(
        json_lines, table_ref, job_config=job_config
    )
    load_job.result()  # Waits for the job to complete.

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'load_json_to_bq',
    default_args=default_args,
    description='Load JSON data from GCS to BigQuery',
    schedule_interval=None,
    catchup=False,
) as dag:

    load_json = PythonOperator(
        task_id='load_json_to_bq',
        python_callable=load_json_to_bq,
        provide_context=True,
    )

    load_json

