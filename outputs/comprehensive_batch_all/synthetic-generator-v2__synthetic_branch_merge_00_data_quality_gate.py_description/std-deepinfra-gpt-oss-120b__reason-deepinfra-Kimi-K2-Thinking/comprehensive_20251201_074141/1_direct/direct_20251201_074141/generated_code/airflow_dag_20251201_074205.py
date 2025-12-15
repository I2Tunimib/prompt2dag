from datetime import datetime, timedelta
import os
import random
import logging
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago


def ingest_csv(**kwargs):
    """
    Ingest raw customer CSV data from a local source directory.
    Returns file metadata as XCom.
    """
    source_dir = Path("/tmp/data/source")
    source_dir.mkdir(parents=True, exist_ok=True)
    # For demonstration, create a dummy CSV file if none exists
    csv_path = source_dir / "customers.csv"
    if not csv_path.exists():
        csv_path.write_text("id,name,email\n1,John Doe,john@example.com\n")
    file_meta = {
        "file_path": str(csv_path),
        "file_size": csv_path.stat().st_size,
        "ingest_timestamp": datetime.utcnow().isoformat(),
    }
    logging.info("Ingested CSV file: %s", file_meta)
    return file_meta


def quality_check(**kwargs):
    """
    Calculate a dummy data quality score based on file size.
    Pushes the score to XCom.
    """
    ti = kwargs["ti"]
    file_meta = ti.xcom_pull(task_ids="ingest_csv")
    if not file_meta:
        raise ValueError("No file metadata found from ingest_csv")
    file_size = file_meta.get("file_size", 0)
    # Dummy quality score: larger files get higher score up to 100%
    score = min(100, max(0, int((file_size % 200) * 0.5)))  # deterministic pseudo-score
    # Ensure a reasonable score for demo purposes
    if score < 50:
        score = 94
    else:
        score = 96
    logging.info("Calculated quality score: %s%%", score)
    ti.xcom_push(key="quality_score", value=score)


def decide_path(**kwargs):
    """
    Branching decision based on quality score.
    Returns the task_id of the next path.
    """
    ti = kwargs["ti"]
    score = ti.xcom_pull(task_ids="quality_check", key="quality_score")
    if score is None:
        raise ValueError("Quality score not found in XCom")
    if score >= 95:
        logging.info("Score %s >= 95, routing to production_load", score)
        return "production_load"
    else:
        logging.info("Score %s < 95, routing to quarantine_and_alert", score)
        return "quarantine_and_alert"


def production_load(**kwargs):
    """
    Load high-quality data into the production database.
    """
    ti = kwargs["ti"]
    file_meta = ti.xcom_pull(task_ids="ingest_csv")
    logging.info("Loading file %s into production database", file_meta["file_path"])
    # Placeholder for actual DB load logic


def quarantine_and_alert(**kwargs):
    """
    Move low-quality data to quarantine storage.
    """
    ti = kwargs["ti"]
    file_meta = ti.xcom_pull(task_ids="ingest_csv")
    quarantine_dir = Path("/tmp/data/quarantine")
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    dest_path = quarantine_dir / Path(file_meta["file_path"]).name
    os.replace(file_meta["file_path"], dest_path)
    logging.info("Moved file to quarantine: %s", dest_path)
    # Store quarantine path for downstream tasks
    ti.xcom_push(key="quarantine_path", value=str(dest_path))


def send_alert_email(**kwargs):
    """
    Send an alert email to data stewards about low-quality data.
    """
    ti = kwargs["ti"]
    quarantine_path = ti.xcom_pull(task_ids="quarantine_and_alert", key="quarantine_path")
    logging.info("Sending alert email for quarantined file: %s", quarantine_path)
    # Placeholder for actual email sending logic


def cleanup(**kwargs):
    """
    Perform final cleanup of temporary resources.
    """
    temp_dirs = ["/tmp/data/source", "/tmp/data/quarantine"]
    for d in temp_dirs:
        path = Path(d)
        if path.is_dir():
            for item in path.iterdir():
                try:
                    if item.is_file():
                        item.unlink()
                    elif item.is_dir():
                        item.rmdir()
                except Exception as e:
                    logging.warning("Failed to delete %s: %s", item, e)
    logging.info("Cleanup completed.")


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "start_date": days_ago(1),
}

with DAG(
    dag_id="customer_data_quality_gate",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["data_quality", "customer"],
) as dag:
    ingest_task = PythonOperator(
        task_id="ingest_csv",
        python_callable=ingest_csv,
    )

    quality_task = PythonOperator(
        task_id="quality_check",
        python_callable=quality_check,
    )

    branch_task = BranchPythonOperator(
        task_id="decide_path",
        python_callable=decide_path,
    )

    production_task = PythonOperator(
        task_id="production_load",
        python_callable=production_load,
    )

    quarantine_task = PythonOperator(
        task_id="quarantine_and_alert",
        python_callable=quarantine_and_alert,
    )

    alert_task = PythonOperator(
        task_id="send_alert_email",
        python_callable=send_alert_email,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )

    # Define dependencies
    ingest_task >> quality_task >> branch_task
    branch_task >> production_task >> cleanup_task
    branch_task >> quarantine_task >> alert_task >> cleanup_task