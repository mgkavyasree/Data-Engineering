from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Function to write data into a shared location
def write_file():
    file_path = '/opt/airflow/dags/data.txt'  # Shared folder between container & host
    with open(file_path, 'w') as f:
        f.write("This is the dataset written by Producer DAG.")
    print(f"File written to: {file_path}")

# Define the DAG
with DAG(
    dag_id='producer_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=['example']
) as dag:

    write_task = PythonOperator(
        task_id='write_file_task',
        python_callable=write_file
    )
