import json
from google.cloud import storage

def preprocess_json(bucket_name, source_blob_name, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    json_data = blob.download_as_text()
    records = json.loads(json_data)
    
    # Function to sanitize column names
    def sanitize_key(key):
        return key.replace(' ', '_').replace('-', '_').replace('.', '_')
    
    # Sanitize keys in each record
    sanitized_records = [
        {sanitize_key(key): value for key, value in record.items()}
        for record in records
    ]
    
    # Upload sanitized JSON back to GCS
    sanitized_blob = bucket.blob(destination_blob_name)
    sanitized_blob.upload_from_string(json.dumps(sanitized_records), content_type='application/json')

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

dag = DAG(
    'gcs_to_bigquery_with_preprocessing',
    default_args=default_args,
    description='Preprocess JSON data and load into BigQuery',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
)

bucket_name = 'your-gcs-bucket-name'
source_blob_name = 'path/to/your/source_file.json'
destination_blob_name = 'path/to/your/sanitized_file.json'
project_id = 'your-gcp-project-id'
dataset_id = 'your_bq_dataset_id'
table_id = 'your_bq_table_id'

# Task to preprocess JSON data
preprocess_task = PythonOperator(
    task_id='preprocess_json',
    python_callable=preprocess_json,
    op_kwargs={
        'bucket_name': bucket_name,
        'source_blob_name': source_blob_name,
        'destination_blob_name': destination_blob_name,
    },
    dag=dag,
)

# Task to load data into BigQuery
load_to_bq_task = GCSToBigQueryOperator(
    task_id='load_to_bq',
    bucket=bucket_name,
    source_objects=[destination_blob_name],
    destination_project_dataset_table=f'{project_id}.{dataset_id}.{table_id}',
    source_format='NEWLINE_DELIMITED_JSON',
    write_disposition='WRITE_APPEND',
    create_disposition='CREATE_IF_NEEDED',
    schema_fields=[
        {'name': 'column1', 'type': 'STRING', 'mode': 'NULLABLE'},
        # Add other schema fields as needed
    ],
    dag=dag,
)

preprocess_task >> load_to_bq_task
