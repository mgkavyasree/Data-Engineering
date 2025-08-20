from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import json
import random
import os

# Default args with owner, retries, email
default_args = {
    "owner": "rajpriya",
    "depends_on_past": False,
    "email": ["youremail@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

AUDIT_FILE = "/tmp/audit_result.json"

# 1. Data Pull (simulate external API or DB call)
def data_pull():
    # Simulating API/database data
    data = {"user_id": 101, "balance": random.randint(100, 2000), "timestamp": str(datetime.now())}
    print("[Pull] Pulled Data:", data)
    return data

# 2. Validation Rule
def audit_rule(**context):
    data = context['ti'].xcom_pull(task_ids="data_pull_task")
    if not data:
        raise ValueError("No data received for audit.")

    # Business rule: balance must be >= 500
    result = {"data": data, "valid": data["balance"] >= 500}
    with open(AUDIT_FILE, "w") as f:
        json.dump(result, f)

    print("[Audit] Validation result saved:", result)
    if not result["valid"]:
        raise ValueError("Audit failed: balance below threshold!")

# 3. Log Audit Results
def log_audit():
    if os.path.exists(AUDIT_FILE):
        with open(AUDIT_FILE, "r") as f:
            result = json.load(f)
        print("[Log] Audit Result:", result)
    else:
        raise FileNotFoundError("Audit result file missing!")

# 4. Final Status Update
def final_status():
    with open(AUDIT_FILE, "r") as f:
        result = json.load(f)

    if result["valid"]:
        print("[Final]  Audit Success for user:", result["data"]["user_id"])
    else:
        print("[Final]  Audit Failed for user:", result["data"]["user_id"])

# DAG Definition
with DAG(
    dag_id="data_audit_dag",
    default_args=default_args,
    description="Event-driven Data Audit DAG",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["audit", "example"],
) as dag:

    # Task 1: Pull data
    pull = PythonOperator(
        task_id="data_pull_task",
        python_callable=data_pull,
    )

    # Task 2: Validate data
    validate = PythonOperator(
        task_id="validate_audit_task",
        python_callable=audit_rule,
        provide_context=True,
    )

    # Task 3: Log results
    log_results = PythonOperator(
        task_id="log_results_task",
        python_callable=log_audit,
    )

    # Task 4: Final status update
    final = PythonOperator(
        task_id="final_status_task",
        python_callable=final_status,
    )

    # Extra: Bash operator for logging to shell
    bash_notify = BashOperator(
        task_id="bash_notify_task",
        bash_command="echo 'Audit DAG run completed!'"
    )

    # Task chaining
    pull >> validate >> log_results >> final>>bash_notify