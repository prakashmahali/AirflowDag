from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import (
    GoogleCloudStorageListOperator,
    GCSToLocalFilesystemOperator,
)
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

# Define constants
GCS_BUCKET_NAME = "your-gcs-bucket-name"
GCS_PREFIX = "your-folder-path/"  # Folder path in GCS
LOCAL_DEST_FOLDER = "/home/airflow/gcs/data/json_files"  # Temporary folder in Composer
MERGED_FILE_PATH = "/home/airflow/gcs/data/merged_file.json"  # Output merged file

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 11, 20),
    "retries": 1,
}

# Python function to merge JSON files
def merge_json_files(local_folder, output_file):
    import json

    all_data = []

    # Iterate through all files in the folder
    for filename in os.listdir(local_folder):
        if filename.endswith(".json"):  # Ensure it's a JSON file
            with open(os.path.join(local_folder, filename), "r") as f:
                data = json.load(f)
                all_data.append(data)

    # Write merged data to the output file
    with open(output_file, "w") as out_f:
        json.dump(all_data, out_f)

    print(f"Merged JSON written to {output_file}")

# Define the DAG
with DAG(
    dag_id="download_and_merge_gcs_files",
    default_args=default_args,
    schedule_interval=None,  # Set your desired schedule
    catchup=False,
    tags=["gcs", "composer"],
) as dag:

    # Step 1: List all files in the GCS folder
    list_files = GoogleCloudStorageListOperator(
        task_id="list_gcs_files",
        bucket=GCS_BUCKET_NAME,
        prefix=GCS_PREFIX,
    )

    # Step 2: Download files dynamically
    def create_download_tasks(**context):
        files = context["ti"].xcom_pull(task_ids="list_gcs_files")
        tasks = []

        for file in files:
            if file.endswith(".json"):  # Ensure it's a JSON file
                task = GCSToLocalFilesystemOperator(
                    task_id=f"download_{file.replace('/', '_')}",
                    bucket=GCS_BUCKET_NAME,
                    object_name=file,
                    filename=f"{LOCAL_DEST_FOLDER}/{os.path.basename(file)}",
                )
                tasks.append(task)

        return tasks

    download_files = create_download_tasks

    # Step 3: Merge all downloaded files into one
    merge_files = PythonOperator(
        task_id="merge_json_files",
        python_callable=merge_json_files,
        op_kwargs={
            "local_folder": LOCAL_DEST_FOLDER,
            "output_file": MERGED_FILE_PATH,
        },
    )

    # Task dependencies
    list_files >> download_files >> merge_files
