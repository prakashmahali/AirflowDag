from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GoogleCloudStorageListOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

def check_file_exists(**kwargs):
    bucket_name = Variable.get('gcs_bucket_name')
    file_name = "test.csv"  # File to check for existence

    gcs_hook = kwargs['ti'].xcom_pull(task_ids='list_gcs_files')
    file_list = gcs_hook['blobs']

    for file in file_list:
        if file.name == file_name:
            return 'file_exists'
    return 'file_not_found'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
}

with DAG(
    dag_id='check_gcs_file_existence',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # Task to list files in GCS bucket
    list_gcs_files = GoogleCloudStorageListOperator(
        task_id='list_gcs_files',
        bucket="{{ var.value.gcs_bucket_name }}",
    )

    # Task to check if file exists
    check_file_task = PythonOperator(
        task_id='check_file_task',
        python_callable=check_file_exists,
        provide_context=True,
    )

    # Dummy tasks for conditional execution
    file_exists_task = DummyOperator(task_id='file_exists_task')
    file_not_found_task = DummyOperator(task_id='file_not_found_task')

    # Define DAG structure
    list_gcs_files >> check_file_task
    check_file_task >> [file_exists_task, file_not_found_task]

========================================================
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Define a Python function that returns some data
def my_python_function():
    return {'key': 'value'}

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2022, 1, 1),
    'retries': 1,
}
with DAG('my_dag', default_args=default_args, schedule_interval=None) as dag:

    # Task 1: Execute the Python function and push the result to XCom
    task1 = PythonOperator(
        task_id='task1',
        python_callable=my_python_function
    )

    # Task 2: Pull the result from XCom and print it
    def print_result(**kwargs):
        ti = kwargs['ti']
        result = ti.xcom_pull(task_ids='task1')
        print(result)

    task2 = PythonOperator(
        task_id='task2',
        python_callable=print_result
    )

    # Define task dependencies
    task1 >> task2

