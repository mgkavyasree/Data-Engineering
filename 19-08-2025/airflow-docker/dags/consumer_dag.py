from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def read_file():
    path = '/opt/airflow/dags/data.txt'  # same path where Producer wrote the file
    if os.path.exists(path):
        with open(path, 'r') as f:
            content = f.read()
            print("Read from file:", content)
    else:
        raise FileNotFoundError("File not found. Please run Producer DAG first.")

with DAG(
    dag_id='consumer_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # run manually
    catchup=False,
    tags=['example']
) as dag:

    read_task = PythonOperator(
        task_id='read_file_task',
        python_callable=read_file
    )
