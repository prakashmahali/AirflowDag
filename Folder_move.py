from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.dates import days_ago
import logging

def rename_gcs_folder(source_bucket, source_prefix, dest_bucket, dest_prefix, **kwargs):
    hook = GCSHook()
    
    # List all objects in the source folder
    source_objects = hook.list(source_bucket, prefix=source_prefix)
    
    if not source_objects:
        logging.info(f"No objects found in {source_bucket}/{source_prefix}. Nothing to rename.")
        return

    for obj in source_objects:
        # Define the destination object path
        dest_object = obj.replace(source_prefix, dest_prefix, 1)
        
        # Copy the object to the new location
        hook.copy(source_bucket, obj, dest_bucket, dest_object)
        logging.info(f"Copied {source_bucket}/{obj} to {dest_bucket}/{dest_object}")
        
        # Delete the original object
        hook.delete(source_bucket, obj)
        logging.info(f"Deleted {source_bucket}/{obj}")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'rename_gcs_folder',
    default_args=default_args,
    description='Rename a GCS folder by moving its contents',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['example'],
) as dag:

    # Define the task using PythonOperator
    rename_gcs_folder_task = PythonOperator(
        task_id='rename_gcs_folder',
        python_callable=rename_gcs_folder,
        op_kwargs={
            'source_bucket': 'your-source-bucket',
            'source_prefix': 'your-source-folder/',
            'dest_bucket': 'your-source-bucket',
            'dest_prefix': 'your-destination-folder/',
        },
    )

    rename_gcs_folder_task
