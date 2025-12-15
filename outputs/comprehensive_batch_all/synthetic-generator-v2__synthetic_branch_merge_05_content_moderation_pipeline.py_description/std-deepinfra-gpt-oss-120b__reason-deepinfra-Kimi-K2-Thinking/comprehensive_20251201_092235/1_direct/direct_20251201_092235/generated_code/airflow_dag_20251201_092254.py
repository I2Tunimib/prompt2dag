import random
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator


def scan_csv(**kwargs):
    """
    Scan a CSV file containing user‑generated content and push metadata to XCom.
    """
    file_path = "/path/to/user_content.csv"  # Adjust to actual location
    try:
        df = pd.read_csv(file_path)
        total_items = len(df)
    except Exception as e:
        total_items = 0
        print(f"Failed to read CSV: {e}")

    kwargs["ti"].xcom_push(key="scan_metadata", value={"total_items": total_items})
    return total_items


def decide_branch(**kwargs):
    """
    Mock toxicity evaluation.
    Returns the task_id of the downstream branch based on a random score.
    """
    score = random.random()  # Simulated toxicity score between 0 and 1
    kwargs["ti"].xcom_push(key="toxicity_score", value=score)

    if score >= 0.7:
        return "remove_and_flag_content"
    return "publish_content"


def remove_and_flag_content(**kwargs):
    """
    Remove toxic content and flag the user account.
    """
    ti = kwargs["ti"]
    score = ti.xcom_pull(key="toxicity_score", task_ids="toxicity_check")
    print(f"Removing content with toxicity score {score:.2f}")
    ti.xcom_push(key="action", value="removed")


def publish_content(**kwargs):
    """
    Publish safe content.
    """
    ti = kwargs["ti"]
    score = ti.xcom_pull(key="toxicity_score", task_ids="toxicity_check")
    print(f"Publishing content with toxicity score {score:.2f}")
    ti.xcom_push(key="action", value="published")


def audit_log(**kwargs):
    """
    Consolidate results from the branch paths and create an audit log entry.
    """
    ti = kwargs["ti"]
    metadata = ti.xcom_pull(key="scan_metadata", task_ids="scan_csv")
    # Pull the action from whichever branch executed; the other will be None
    action = ti.xcom_pull(key="action", task_ids=["remove_and_flag_content", "publish_content"])
    action = next((a for a in action if a), None)

    log_entry = {
        "execution_date": kwargs["execution_date"].isoformat(),
        "metadata": metadata,
        "action": action,
    }
    print("Audit Log:", log_entry)


default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="content_moderation_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["moderation", "example"],
) as dag:
    scan_task = PythonOperator(
        task_id="scan_csv",
        python_callable=scan_csv,
    )

    branch_task = BranchPythonOperator(
        task_id="toxicity_check",
        python_callable=decide_branch,
    )

    remove_task = PythonOperator(
        task_id="remove_and_flag_content",
        python_callable=remove_and_flag_content,
    )

    publish_task = PythonOperator(
        task_id="publish_content",
        python_callable=publish_content,
    )

    audit_task = PythonOperator(
        task_id="audit_log",
        python_callable=audit_log,
    )

    # Define dependencies
    scan_task >> branch_task
    branch_task >> [remove_task, publish_task]
    [remove_task, publish_task] >> audit_task