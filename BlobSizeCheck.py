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
