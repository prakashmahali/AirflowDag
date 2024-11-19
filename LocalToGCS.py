from airflow import DAG
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='local_csv_to_gcs_to_bq',
    default_args=default_args,
    description='Copy a local CSV to GCS and load it into BigQuery',
    schedule_interval='@once',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Step 1: Upload CSV from local file system to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',
        src='/home/airflow/gcs/data/flattened-file.csv',  # Local path in Composer
        dst='data/flattened-file.csv',  # Path in GCS bucket
        bucket_name='your-gcs-bucket-name',  # Replace with your GCS bucket name
    )

    # Step 2: Load CSV from GCS to BigQuery
    load_csv_to_bq = BigQueryInsertJobOperator(
        task_id='load_csv_to_bq',
        configuration={
            "load": {
                "sourceUris": ["gs://your-gcs-bucket-name/data/flattened-file.csv"],  # GCS URI
                "destinationTable": {
                    "projectId": "your-gcp-project-id",  # Replace with your GCP project ID
                    "datasetId": "your_dataset_id",      # Replace with your BigQuery dataset ID
                    "tableId": "your_table_name",        # Replace with your BigQuery table name
                },
                "sourceFormat": "CSV",
                "writeDisposition": "WRITE_TRUNCATE",  # Overwrites the table; change if needed
                "autodetect": True,                   # Auto-detect schema
            }
        },
    )

    # Define task dependencies
    upload_to_gcs >> load_csv_to_bq
