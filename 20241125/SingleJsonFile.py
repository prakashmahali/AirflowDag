from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Variables
GCS_BUCKET = 'your-gcs-bucket-name'
GCS_PREFIX = 'your-folder-path/'  # Folder path in GCS
LOCAL_FILE_PATH = '/home/airflow/dags/data/event.json'  # Local path for renamed file

# Python function to find the first .json file
def find_and_rename_file(**context):
    files = context['task_instance'].xcom_pull(task_ids='list_files')
    json_files = [file for file in files if file.endswith('.json')]

    if not json_files:
        raise ValueError("No JSON files found in the specified GCS folder.")

    # Select the first JSON file (you can modify logic if needed)
    selected_file = json_files[0]
    context['task_instance'].xcom_push(key='selected_file', value=selected_file)

    print(f"Selected JSON file: {selected_file}")

# Define the DAG
default_args = {
    'start_date': datetime(2024, 11, 23),
    'catchup': False,
}

with DAG(
    dag_id='gcs_check_and_download_json',
    default_args=default_args,
    schedule_interval=None,  # Trigger manually
    description='Check for .json in GCS and download as event.json',
    tags=['gcs', 'json', 'download'],
) as dag:

    # Task 1: List all files in the GCS folder
    list_files = GCSListObjectsOperator(
        task_id='list_files',
        bucket_name=GCS_BUCKET,
        prefix=GCS_PREFIX,
    )

    # Task 2: Check and find the first .json file
    find_json_file = PythonOperator(
        task_id='find_json_file',
        python_callable=find_and_rename_file,
        provide_context=True,
    )

    # Task 3: Download the selected JSON file and rename it
    download_and_rename = GCSToLocalFilesystemOperator(
        task_id='download_json',
        bucket_name=GCS_BUCKET,
        object_name="{{ task_instance.xcom_pull(task_ids='find_json_file', key='selected_file') }}",
        filename=LOCAL_FILE_PATH,
    )

    # Task dependencies
    list_files >> find_json_file >> download_and_rename
