from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
import logging

def list_and_check_csv_files(bucket_name, prefix):
    # Create a GCSHook instance
    gcs_hook = GCSHook()
    
    # List blobs in the specified bucket and prefix
    blobs = gcs_hook.list(bucket_name=bucket_name, prefix=prefix)
    
    # Log the blobs found
    logging.info(f"Blobs found: {blobs}")
    
    # Filter for CSV files
    csv_files = [blob for blob in blobs if blob.endswith('.csv')]
    
    # Log the CSV files found
    logging.info(f"CSV files found: {csv_files}")
    
    # Check if any CSV files are found and return success or failure
    if csv_files:
        logging.info("CSV files found. Task succeeded.")
        return "success"
    else:
        logging.info("No CSV files found. Task failed.")
        return "failure"

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'gcs_list_and_check_csv',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    check_gcs_for_csv_files = PythonOperator(
        task_id='check_gcs_for_csv_files',
        python_callable=list_and_check_csv_files,
        op_kwargs={
            'bucket_name': 'your-gcs-bucket',  # Replace with your bucket name
            'prefix': 'your-folder/'  # Replace with your folder prefix
        },
    )

check_gcs_for_csv_files
