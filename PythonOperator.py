import json
from google.cloud import storage
from google.cloud import bigquery

def load_json_to_bq(gcs_bucket, gcs_folder, bq_table):
    # Initialize the GCS and BigQuery clients
    storage_client = storage.Client()
    bq_client = bigquery.Client()

    # Get the GCS bucket
    bucket = storage_client.bucket(gcs_bucket)

    # List objects in the specified folder
    blobs = bucket.list_blobs(prefix=gcs_folder)

    # Process each JSON file in the folder
    for blob in blobs:
        # Read the JSON file content
        content = blob.download_as_string()
        json_data = json.loads(content)

        # Transform the JSON data to handle special characters in column names
        transformed_data = json.dumps({"data": json_data})

        # Load the transformed data into BigQuery
        bq_table_ref = bq_client.dataset(bq_table.split('.')[0]).table(bq_table.split('.')[1])
        job_config = bigquery.LoadJobConfig(
            schema=[
                bigquery.SchemaField("data", "STRING")
            ],
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        )
        load_job = bq_client.load_table_from_json([{"data": transformed_data}], bq_table_ref, job_config=job_config)
        load_job.result()  # Wait for the job to complete

    print("JSON data loaded into BigQuery table:", bq_table)

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Import the custom function
from my_custom_module import load_json_to_bq

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'load_json_to_bq',
    default_args=default_args,
    description='Load JSON files from GCS to BigQuery',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Define the PythonOperator
    load_task = PythonOperator(
        task_id='load_json_to_bq_task',
        python_callable=load_json_to_bq,
        op_kwargs={
            'gcs_bucket': 'your-gcs-bucket',
            'gcs_folder': 'your-gcs-folder',
            'bq_table': 'your-dataset.your-table'
        },
    )

    load_task
