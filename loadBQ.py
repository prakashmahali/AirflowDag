def load_json_to_bq(**kwargs):
    # Initialize GCS and BigQuery hooks
    gcs_hook = GCSHook(gcp_conn_id='google_cloud_default')
    bq_hook = BigQueryHook(bigquery_conn_id='google_cloud_default', use_legacy_sql=False)
    
    # Define GCS and BigQuery details
    bucket_name = 'your-gcs-bucket'
    gcs_key = 'path/to/your/jsonfile.json'
    dataset_id = 'your_bq_dataset'
    table_id = 'your_bq_table'
    
    # Download JSON file from GCS
    json_data = gcs_hook.download(bucket_name, gcs_key)
    
    # Load JSON data into a DataFrame
    data = json.loads(json_data)
    
    # Convert the DataFrame to a JSON string with one column
    df = pd.DataFrame([json.dumps(data)], columns=['json_data'])
    
    # Convert DataFrame to CSV format
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    # Load the CSV data into BigQuery
    bq_hook.run_load(
        destination_project_dataset_table=f'{dataset_id}.{table_id}',
        source_objects=[csv_buffer.getvalue()],
        source_format='CSV',
        field_delimiter=',',
        write_disposition='WRITE_TRUNCATE',
        autodetect=True,
        skip_leading_rows=1,
    )

load_json_task = PythonOperator(
    task_id='load_json_to_bq',
    python_callable=load_json_to_bq,
    dag=dag,
)
