from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import os

# Better to use shared dags folder so file is visible on host too
MESSAGE_PATH = "/opt/airflow/dags/airflow_message.txt"

def generate_message():
    message = "Hello, this message flows through tasks!"
    with open(MESSAGE_PATH, 'w') as f:
        f.write(message)

def read_message():
    if os.path.exists(MESSAGE_PATH):
        with open(MESSAGE_PATH, 'r') as f:
            print("Message from file:", f.read())
    else:
        raise FileNotFoundError("Message file does not exist.")

with DAG(
    dag_id="chained_tasks_dag",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,  # Run manually
    catchup=False,
    tags=["example"]
) as dag:

    generate = PythonOperator(
        task_id="generate_message",
        python_callable=generate_message
    )

    simulate_save = BashOperator(
        task_id="simulate_file_configuration",
        bash_command=f"echo 'Message saved at {MESSAGE_PATH}'"
    )

    read = PythonOperator(
        task_id="read_message",
        python_callable=read_message
    )

    # Set task order: generate → simulate_save → read
    generate >> simulate_save >> read
