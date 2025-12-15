"""Data quality gate DAG.

Ingests customer CSV data, evaluates quality, and routes the data to
production or quarantine based on a quality score threshold.
"""

from datetime import datetime, timedelta
import logging
import os
import random

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.email.operators.email import EmailOperator

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False,
}


def ingest_csv(**context):
    """Simulate ingestion of a CSV file and push metadata via XCom."""
    source_path = "/tmp/customer_data.csv"
    # In a real scenario, you would copy or move the file to a landing zone.
    if not os.path.exists(source_path):
        # Create a dummy file for demonstration purposes.
        with open(source_path, "w", encoding="utf-8") as f:
            f.write("id,name,email\n1,John Doe,john@example.com\n")
    file_size = os.path.getsize(source_path)
    metadata = {"path": source_path, "size": file_size}
    logging.info("Ingested CSV file: %s (size=%d bytes)", source_path, file_size)
    # Push metadata to XCom
    return metadata


def quality_check(**context):
    """Calculate a dummy quality score and push it via XCom."""
    # Retrieve metadata from the previous task
    metadata = context["ti"].xcom_pull(task_ids="ingest_csv")
    # Dummy calculation: random score between 80 and 100
    score = random.uniform(80, 100)
    logging.info(
        "Calculated quality score %.2f for file %s", score, metadata.get("path")
    )
    # Push the score to XCom
    return round(score, 2)


def decide_path(**context):
    """Branch based on the quality score."""
    score = context["ti"].xcom_pull(task_ids="quality_check")
    threshold = 95.0
    if score >= threshold:
        logging.info("Score %.2f >= %.2f – routing to production", score, threshold)
        return "production_load"
    else:
        logging.info("Score %.2f < %.2f – routing to quarantine", score, threshold)
        return "quarantine_and_alert"


def production_load(**context):
    """Load high‑quality data into the production database."""
    metadata = context["ti"].xcom_pull(task_ids="ingest_csv")
    logging.info(
        "Loading file %s into production database (simulated)", metadata.get("path")
    )
    # Placeholder for actual DB load logic


def quarantine_and_alert(**context):
    """Move low‑quality data to quarantine storage."""
    metadata = context["ti"].xcom_pull(task_ids="ingest_csv")
    quarantine_path = "/tmp/quarantine/customer_data.csv"
    os.makedirs(os.path.dirname(quarantine_path), exist_ok=True)
    os.replace(metadata.get("path"), quarantine_path)
    logging.info(
        "Moved file to quarantine location: %s", quarantine_path
    )
    # The alert email will be sent in the downstream task


def cleanup(**context):
    """Perform cleanup of temporary resources."""
    temp_path = "/tmp/customer_data.csv"
    if os.path.exists(temp_path):
        os.remove(temp_path)
        logging.info("Removed temporary file: %s", temp_path)
    else:
        logging.info("No temporary file to clean up.")
    # Additional cleanup steps could be added here.


with DAG(
    dag_id="data_quality_gate",
    description="Ingest CSV, assess quality, and route to production or quarantine.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["data_quality", "example"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv,
        provide_context=True,
    )

    quality_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
        provide_context=True,
    )

    branch_task = BranchPythonOperator(
        task_id="decide_path",
        python_callable=decide_path,
        provide_context=True,
    )

    production_task = PythonOperator(
        task_id="production_load",
        python_callable=production_load,
        provide_context=True,
    )

    quarantine_task = PythonOperator(
        task_id="quarantine_and_alert",
        python_callable=quarantine_and_alert,
        provide_context=True,
    )

    alert_task = EmailOperator(
        task_id="send_alert_email",
        to="data.steward@example.com",
        subject="Data Quality Alert: Low‑Quality CSV Detected",
        html_content="The ingested CSV file failed the quality check and was moved to quarantine.",
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        provide_context=True,
    )

    # Define task dependencies
    ingest_task >> quality_task >> branch_task
    branch_task >> production_task >> cleanup_task
    branch_task >> quarantine_task >> alert_task >> cleanup_task