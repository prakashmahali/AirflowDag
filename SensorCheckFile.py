from airflow import DAG
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

BUCKET_NAME = 'your-bucket-name'
FILE_NAME = 'path/to/your/file.txt'

def choose_task(**kwargs):
    ti = kwargs['ti']
    file_exists = ti.xcom_pull(task_ids='check_file')
    if file_exists:
        return 'task1'
    else:
        return 'task2'

with DAG(
    dag_id='example_gcs_file_check_dag',
    default_args=default_args,
    schedule_interval=None,
) as dag:

    start = DummyOperator(task_id='start')

    check_file = GCSObjectExistenceSensor(
        task_id='check_file',
        bucket=BUCKET_NAME,
        object=FILE_NAME,
    )

    branch = BranchPythonOperator(
        task_id='branch',
        provide_context=True,
        python_callable=choose_task,
    )

    task1 = DummyOperator(task_id='task1')
    task2 = DummyOperator(task_id='task2')
    end = DummyOperator(task_id='end', trigger_rule='none_failed_or_skipped')

    start >> check_file >> branch
    branch >> [task1, task2] >> end

Step 1 : 
check_file = GCSObjectExistenceSensor(
    task_id='check_file',
    bucket=BUCKET_NAME,
    object=FILE_NAME,
)
 Step2 
BranchPythonOperator: Uses the choose_task function to decide which task to execute next based on the file's existence.
def choose_task(**kwargs):
    ti = kwargs['ti']
    file_exists = ti.xcom_pull(task_ids='check_file')
    if file_exists:
        return 'task1'
    else:
        return 'task2'
Task Flow:

start: Initial dummy task.
check_file: Sensor task to check for the file's existence.
branch: Branching based on the sensor result.
task1: Dummy task executed if the file exists.
task2: Dummy task executed if the file does not exist.
end: Final task to ensure the DAG completes successfully.
