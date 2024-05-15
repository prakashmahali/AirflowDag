from datetime import datetime, timedelta
from airflow import DAG
from airflow.contrib.operators.gcs_to_gcs import GCSToGCSOperator

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'gcs_copy_files',
    default_args=default_args,
    description='Copy files from GCS bucket to another folder using wildcard',
    schedule_interval=timedelta(days=1),
)

# Define the GCSToGCSOperator to copy files
copy_files_task = GCSToGCSOperator(
    task_id='copy_files_to_folder',
    source_bucket='your-source-bucket',
    destination_bucket='your-destination-bucket',
    source_objects=['your-source-folder/*.csv'],  # Modify this wildcard pattern as needed
    destination_object='your-destination-folder/',
    move_object=False,  # Set this to True if you want to move instead of copy
    dag=dag,
)

# Set task dependencies
copy_files_task
