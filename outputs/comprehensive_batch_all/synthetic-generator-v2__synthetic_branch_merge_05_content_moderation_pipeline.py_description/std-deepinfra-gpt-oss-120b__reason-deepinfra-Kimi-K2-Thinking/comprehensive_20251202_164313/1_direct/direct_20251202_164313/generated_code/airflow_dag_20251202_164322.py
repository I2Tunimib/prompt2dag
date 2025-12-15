import logging
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator

# Path to the CSV file containing user‑generated content.
# Adjust this path to point to the actual location in your environment.
CSV_PATH = "/path/to/user_content.csv"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2024, 1, 1),
}


def scan_csv(**context):
    """
    Scan the CSV file and push metadata to XCom.
    Metadata includes the total number of rows (items) found.
    """
    if not os.path.isfile(CSV_PATH):
        logging.warning("CSV file not found at %s. Using empty dataset.", CSV_PATH)
        total_items = 0
    else:
        df = pd.read_csv(CSV_PATH)
        total_items = len(df)
        logging.info("Scanned %d items from %s.", total_items, CSV_PATH)

    # Push metadata for downstream tasks.
    context["ti"].xcom_push(key="metadata", value={"total_items": total_items})


def decide_branch(**context):
    """
    Branching logic based on a mock toxicity score.
    For demonstration, we compute a pseudo toxicity score as
    total_items / (total_items + 10). If the score exceeds 0.7,
    route to the removal path; otherwise, route to publishing.
    """
    metadata = context["ti"].xcom_pull(key="metadata", task_ids="scan_csv")
    total_items = metadata.get("total_items", 0)

    # Mock toxicity calculation.
    toxicity_score = total_items / (total_items + 10) if total_items > 0 else 0.0
    logging.info("Calculated mock toxicity score: %.3f", toxicity_score)

    if toxicity_score >= 0.7:
        chosen_task = "remove_and_flag_content"
    else:
        chosen_task = "publish_content"

    logging.info("Branching to task: %s", chosen_task)
    return chosen_task


def remove_and_flag_content(**context):
    """
    Mock removal of toxic content and flagging of the user.
    """
    logging.info("Removing toxic content and flagging associated user accounts.")
    # Simulate result payload.
    result = {"action": "removed", "status": "success"}
    context["ti"].xcom_push(key="remove_result", value=result)


def publish_content(**context):
    """
    Mock publishing of safe content.
    """
    logging.info("Publishing safe content to the platform.")
    # Simulate result payload.
    result = {"action": "published", "status": "success"}
    context["ti"].xcom_push(key="publish_result", value=result)


def audit_log(**context):
    """
    Consolidate results from both possible branches and create an audit log entry.
    """
    remove_res = context["ti"].xcom_pull(key="remove_result", task_ids="remove_and_flag_content")
    publish_res = context["ti"].xcom_pull(key="publish_result", task_ids="publish_content")

    # Determine which branch produced a result.
    if remove_res:
        outcome = remove_res
    elif publish_res:
        outcome = publish_res
    else:
        outcome = {"action": "none", "status": "skipped"}

    logging.info("Audit Log Entry: %s", outcome)


with DAG(
    dag_id="content_moderation_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["moderation", "example"],
) as dag:
    scan_csv_task = PythonOperator(
        task_id="scan_csv",
        python_callable=scan_csv,
        provide_context=True,
    )

    toxicity_check_task = BranchPythonOperator(
        task_id="toxicity_check",
        python_callable=decide_branch,
        provide_context=True,
    )

    remove_task = PythonOperator(
        task_id="remove_and_flag_content",
        python_callable=remove_and_flag_content,
        provide_context=True,
    )

    publish_task = PythonOperator(
        task_id="publish_content",
        python_callable=publish_content,
        provide_context=True,
    )

    audit_log_task = PythonOperator(
        task_id="audit_log",
        python_callable=audit_log,
        provide_context=True,
        trigger_rule="none_failed_min_one_success",
    )

    # Define task dependencies.
    scan_csv_task >> toxicity_check_task
    toxicity_check_task >> [remove_task, publish_task]
    [remove_task, publish_task] >> audit_log_task