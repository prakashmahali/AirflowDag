from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json

# Define the directory and output file
BASE_DIR = '/home/airflow/gcs/data'
OUTPUT_FILE = 'event.json'

# Python function to combine JSON files
def combine_json_files():
    combined_data = []
    for filename in os.listdir(BASE_DIR):
        file_path = os.path.join(BASE_DIR, filename)
        if filename.endswith('.json') and filename != OUTPUT_FILE:
            with open(file_path, 'r') as file:
                try:
                    data = json.load(file)
                    if isinstance(data, list):  # If the file contains a list of JSON objects
                        combined_data.extend(data)
                    else:  # If the file contains a single JSON object
                        combined_data.append(data)
                except json.JSONDecodeError as e:
                    print(f"Error reading {filename}: {e}")

    # Write the combined data to the output file
    output_path = os.path.join(BASE_DIR, OUTPUT_FILE)
    with open(output_path, 'w') as outfile:
        json.dump(combined_data, outfile, indent=4)

# Define the DAG
default_args = {
    'start_date': datetime(2024, 11, 23),
    'catchup': False
}

with DAG(
    dag_id='combine_json_files',
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    description='Combine all JSON files into a single file',
    tags=['json', 'combine', 'example']
) as dag:

    # Task to combine JSON files
    combine_files_task = PythonOperator(
        task_id='combine_json_files',
        python_callable=combine_json_files
    )
