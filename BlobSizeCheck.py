from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.hooks.gcs import GCSHook

def check_gcs_file_size(bucket_name, object_name):
    hook = GCSHook(gcp_conn_id='google_cloud_default')  # or your custom conn_id
    blob = hook.get_blob(bucket_name=bucket_name, object_name=object_name)

    if blob:
        print(f"File size: {blob.size} bytes")
        return blob.size
    else:
        raise ValueError("Blob not found")

with DAG(
    dag_id='check_gcs_file_size_dag',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["gcs", "file-size"]
) as dag:

    check_file = PythonOperator(
        task_id='check_gcs_file_size',
        python_callable=check_gcs_file_size,
        op_kwargs={
            'bucket_name': 'your-bucket-name',
            'object_name': 'path/to/your/file.json'
        }
    )

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import storage

def get_gcs_file_size(bucket_name, blob_name):
    client = storage.Client()  # Assumes Composer environment is authenticated
    blob = storage.Blob(bucket=bucket_name, name=blob_name, client=client)
    blob.reload()  # This only requires storage.objects.get

    print(f"GCS file size: {blob.size} bytes")
    return blob.size

with DAG(
    dag_id="gcs_file_size_check_dag",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["gcs", "size-check"]
) as dag:

    check_file_size = PythonOperator(
        task_id="check_file_size",
        python_callable=get_gcs_file_size,
        op_kwargs={
            "bucket_name": "your-bucket-name",
            "blob_name": "path/to/your/file.json"
        }
    )
