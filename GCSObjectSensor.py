from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'gcs_file_existence_dag',
    default_args=default_args,
    description='DAG to check GCS file existence and branch tasks accordingly',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

# Check if the file exists
check_gcs_file = GCSObjectExistenceSensor(
    task_id='check_gcs_file',
    bucket='your-gcs-bucket-name',
    object='path/to/your/file.txt',
    google_cloud_conn_id='your_gcp_connection',
    timeout=20,
    poke_interval=10,
    dag=dag,
)

# Branching logic
def decide_which_task(**context):
    file_exists = context['task_instance'].xcom_pull(task_ids='check_gcs_file')
    if file_exists:
        return 'task1'
    else:
        return 'task2'

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=decide_which_task,
    provide_context=True,
    dag=dag,
)

# Define tasks
task1 = DummyOperator(
    task_id='task1',
    dag=dag,
)

task2 = DummyOperator(
    task_id='task2',
    dag=dag,
)

# Define the task dependencies
check_gcs_file >> branching
branching >> [task1, task2]
