from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator, GCSFileToLocalFilesystemOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json

# Function to merge JSON files
def merge_json_files(local_folder: str, output_file: str):
    combined_data = []

    # Iterate through files in the folder
    for file_name in os.listdir(local_folder):
        file_path = os.path.join(local_folder, file_name)
        with open(file_path, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                combined_data.extend(data)
            else:
                combined_data.append(data)

    # Write the combined data to the output file
    with open(output_file, 'w') as f:
        json.dump(combined_data, f, indent=2)

# Default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 22),
    "retries": 1,
}

# DAG definition
with DAG(
    "merge_gcs_json_to_single_file",
    default_args=default_args,
    schedule_interval=None,  # Adjust schedule as needed
    catchup=False,
) as dag:

    # Task 1: List JSON files in the GCS bucket
    list_files_task = GCSListObjectsOperator(
        task_id="list_json_files",
        bucket_name="your-source-bucket",  # Replace with your GCS bucket name
        prefix="source/path/",  # Replace with the folder containing JSON files
    )

    # Task 2: Download JSON files locally
    def download_files(**kwargs):
        file_list = kwargs['ti'].xcom_pull(task_ids='list_json_files')
        local_folder = "/tmp/json_files/"
        os.makedirs(local_folder, exist_ok=True)

        tasks = []
        for file_name in file_list:
            if file_name.endswith(".json"):  # Ensure only JSON files are downloaded
                tasks.append(
                    GCSFileToLocalFilesystemOperator(
                        task_id=f"download_{file_name.replace('/', '_')}",
                        bucket_name="your-source-bucket",
                        object_name=file_name,
                        filename=os.path.join(local_folder, os.path.basename(file_name)),
                    )
                )
        return tasks

    download_files_task = PythonOperator(
        task_id="download_files",
        python_callable=download_files,
        provide_context=True,
    )

    # Task 3: Merge JSON files into a single file
    merge_task = PythonOperator(
        task_id="merge_json_files",
        python_callable=merge_json_files,
        op_kwargs={
            "local_folder": "/tmp/json_files/",
            "output_file": "/tmp/merged.json",
        },
    )

    # Task dependencies
    list_files_task >> download_files_task >> merge_task
