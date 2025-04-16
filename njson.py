from google.cloud import storage
import json
import os

def combine_json_to_ndjson(bucket_name, source_prefix, target_blob_name, local_tmp_file='/tmp/combined.ndjson'):
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = list(bucket.list_blobs(prefix=source_prefix))

    with open(local_tmp_file, 'w') as outfile:
        for blob in blobs:
            if blob.name.endswith('.json'):
                data = blob.download_as_text()
                try:
                    # Handle if it's a single JSON object or a list
                    records = json.loads(data)
                    if isinstance(records, dict):
                        outfile.write(json.dumps(records) + '\n')
                    elif isinstance(records, list):
                        for record in records:
                            outfile.write(json.dumps(record) + '\n')
                except json.JSONDecodeError:
                    print(f"Skipping invalid JSON in {blob.name}")

    # Upload back to GCS as NDJSON
    blob = bucket.blob(target_blob_name)
    blob.upload_from_filename(local_tmp_file)
    print(f"Combined NDJSON uploaded to gs://{bucket_name}/{target_blob_name}")
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('combine_json_to_ndjson_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    combine_task = PythonOperator(
        task_id='combine_json_to_ndjson',
        python_callable=combine_json_to_ndjson,
        op_kwargs={
            'bucket_name': 'your-bucket-name',
            'source_prefix': 'your/source/folder/',
            'target_blob_name': 'combined/ndjson/output.ndjson'
        }
    )
