from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_local import GCSToLocalFilesystemOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import json
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 11, 15),
    'retries': 1,
}

def flatten_json(json_data):
    # Flatten JSON structure
    def flatten(x, name=''):
        result = {}
        if type(x) is dict:
            for a in x:
                result.update(flatten(x[a], name + a + '_'))
        elif type(x) is list:
            i = 0
            for a in x:
                result.update(flatten(a, name + str(i) + '_'))
                i += 1
        else:
            result[name[:-1]] = x
        return result

    flat_json = [flatten(record) for record in json_data]
    return flat_json

def load_json_to_bq(**kwargs):
    ti = kwargs['ti']
    file_path = ti.xcom_pull(task_ids='download_from_gcs')
    
    with open(file_path, 'r') as f:
        json_data = json.load(f)
    
    flattened_data = flatten_json(json_data)
    
    df = pd.DataFrame(flattened_data)
    
    # Upload the flattened DataFrame to BigQuery
    client = bigquery.Client()
    table_id = 'your_project.your_dataset.your_table'
    client.load_table_from_dataframe(df, table_id)

with DAG('flatten_json_gcs_to_bq', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    download_from_gcs = GCSToLocalFilesystemOperator(
        task_id='download_from_gcs',
        bucket='your_gcs_bucket',
        object_name='path/to/your/file.json',
        filename='/tmp/file.json',
    )
    
    load_to_bq = PythonOperator(
        task_id='load_json_to_bq',
        provide_context=True,
        python_callable=load_json_to_bq,
    )
    
    download_from_gcs >> load_to_bq
