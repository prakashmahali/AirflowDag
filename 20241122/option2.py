from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSFileToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Function to create download tasks dynamically
def download_all_files(**kwargs):
    file_list = kwargs['ti'].xcom_pull(task_ids='list_files_in_gcs')  # Retrieve file list from XCom
    local_folder = "/tmp/gcs_files/"
    os.makedirs(local_folder, exist_ok=True)

    download_tasks = []
    for file_name in file_list:
        if file_name.endswith(".json"):  # Filter JSON files
            task = GCSFileToLocalFilesystemOperator(
                task_id=f"download_{file_name.replace('/', '_')}",
                bucket_name="your-source-bucket",  # Replace with your bucket name
                object_name=file_name,
                filename=os.path.join(local_folder, os.path.basename(file_name)),
                dag=kwargs['dag'],  # Reference the DAG
            )
            download_tasks.append(task)

    return download_tasks

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 22),
    "retries": 1,
}

# DAG definition
with DAG(
    "copy_all_json_files_from_gcs",
    default_args=default_args,
    schedule_interval=None,  # Adjust schedule as needed
    catchup=False,
) as dag:

    # Task 1: List JSON files in the GCS folder
    list_files_task = GCSListObjectsOperator(
        task_id="list_files_in_gcs",
        bucket_name="your-source-bucket",  # Replace with your bucket name
        prefix="source/path/",  # Replace with the folder containing JSON files
    )

    # Task 2: Download all files listed
    download_files_task = PythonOperator(
        task_id="download_files_from_gcs",
        python_callable=download_all_files,
        provide_context=True,
    )

    list_files_task >> download_files_task
