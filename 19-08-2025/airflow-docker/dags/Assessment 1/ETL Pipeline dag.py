from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Shared file path to simulate intermediate data
EXTRACT_PATH = "/opt/airflow/dags/raw_data.txt"
TRANSFORM_PATH = "/opt/airflow/dags/transformed_data.txt"

# Extract Task
def extract_data():
    raw_data = "100,200,300,400,500"
    with open(EXTRACT_PATH, "w") as f:
        f.write(raw_data)
    print(f"[Extract] Raw data saved to {EXTRACT_PATH}: {raw_data}")

# Load Task
def load_data():
    if os.path.exists(TRANSFORM_PATH):
        with open(TRANSFORM_PATH, "r") as f:
            data = f.read()
        print(f"[Load] Final transformed data loaded: {data}")
    else:
        raise FileNotFoundError("[Load] No transformed data found. Did transform step run?")

# Define DAG
with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["etl", "example"],
) as dag:

    # Extract
    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data
    )

    # Transform (BashOperator to simulate transformation)
    transform_task = BashOperator(
        task_id="transform_data",
        bash_command=f"cat {EXTRACT_PATH} | tr ',' '\n' | awk '{{sum+=$1}} END {{print sum}}' > {TRANSFORM_PATH}"
    )

    # Load
    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data
    )

    # Task chaining
    extract_task >> transform_task >> load_task