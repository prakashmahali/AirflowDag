from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to print the formatted logical_date
def print_logical_date(**kwargs):
    # Getting the logical_date from kwargs (Airflow's context)
    logical_date = kwargs["logical_date"]  
    formatted_date = logical_date.strftime("%Y%m%d%H%M")  # Format as needed
    print(f"Formatted Logical Date: {formatted_date}")  # Prints the formatted date

# Define Airflow DAG
with DAG(
    dag_id="test_logical_date_template",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task that prints the formatted logical date
    print_task = PythonOperator(
        task_id="print_formatted_logical_date",
        python_callable=print_logical_date,
        provide_context=True,  # Ensures kwargs has access to context variables
    )

    # Task Order
    print_task
