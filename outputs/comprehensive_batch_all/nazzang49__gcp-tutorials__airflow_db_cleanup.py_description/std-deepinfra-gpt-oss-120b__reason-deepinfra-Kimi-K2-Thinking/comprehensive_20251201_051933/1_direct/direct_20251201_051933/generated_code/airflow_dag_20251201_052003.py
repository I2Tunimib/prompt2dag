"""Airflow DAG for periodic cleanup of Airflow metadata tables.

The DAG runs daily at midnight UTC and performs two sequential steps:
1. ``print_configuration`` – loads retention settings from Airflow Variables,
   computes the cutoff datetime and pushes it to XCom.
2. ``cleanup_airflow_db`` – deletes records older than the cutoff from a set
   of Airflow core models. Deletion can be disabled via the ``enable_delete``
   variable.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.timezone import utcnow
from airflow.operators.python import PythonOperator
from airflow.settings import Session

# Airflow core models that may contain historic data
from airflow.models import (
    DagRun,
    TaskInstance,
    Log,
    XCom,
    SlaMiss,
    DagModel,
    TaskReschedule,
    TaskFail,
    RenderedTaskInstanceFields,
    ImportError,
    Job,
)

DEFAULT_RETENTION_DAYS = 30
DEFAULT_ENABLE_DELETE = False

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email": ["admin@example.com"],
}


def print_configuration(**context) -> None:
    """Load configuration, compute cutoff datetime and push to XCom."""
    # Retention period (in days) – can be overridden via Airflow Variable
    retention_str = Variable.get(
        "max_db_entry_age_in_days", default_var=str(DEFAULT_RETENTION_DAYS)
    )
    try:
        retention_days = int(retention_str)
    except ValueError:
        logging.warning(
            "Invalid retention value '%s'; falling back to %d days.",
            retention_str,
            DEFAULT_RETENTION_DAYS,
        )
        retention_days = DEFAULT_RETENTION_DAYS

    # Whether actual deletion should be performed
    enable_delete_str = Variable.get(
        "enable_delete", default_var=str(DEFAULT_ENABLE_DELETE)
    )
    enable_delete = enable_delete_str.lower() in ("true", "1", "yes")

    # Compute the maximum datetime for records to keep
    max_date = utcnow() - timedelta(days=retention_days)

    logging.info("Retention days: %d", retention_days)
    logging.info("Enable delete: %s", enable_delete)
    logging.info("Cutoff datetime (max_date): %s", max_date.isoformat())

    ti = context["ti"]
    ti.xcom_push(key="max_date", value=max_date.isoformat())
    ti.xcom_push(key="enable_delete", value=enable_delete)


def _delete_records(
    session,
    model,
    max_date: datetime,
    enable_delete: bool,
) -> None:
    """Delete rows older than ``max_date`` from ``model`` if possible."""
    # Identify a datetime column commonly used for filtering
    date_column = None
    for col_name in ("execution_date", "start_date", "timestamp", "created_at"):
        if hasattr(model, col_name):
            date_column = getattr(model, col_name)
            break

    if date_column is None:
        logging.info(
            "Model %s does not have a recognizable date column; skipping.",
            model.__name__,
        )
        return

    query = session.query(model).filter(date_column < max_date)
    count = query.count()

    if enable_delete:
        query.delete(synchronize_session=False)
        session.commit()
        logging.info("Deleted %d rows from %s.", count, model.__name__)
    else:
        logging.info(
            "Would delete %d rows from %s (deletion disabled).",
            count,
            model.__name__,
        )


def cleanup_airflow_db(**context) -> None:
    """Perform cleanup of Airflow metadata tables based on configuration."""
    ti = context["ti"]
    max_date_str = ti.xcom_pull(key="max_date", task_ids="print_configuration")
    enable_delete = ti.xcom_pull(key="enable_delete", task_ids="print_configuration")

    if not max_date_str:
        raise ValueError("max_date not found in XCom; aborting cleanup.")
    max_date = datetime.fromisoformat(max_date_str)

    logging.info("Starting cleanup with cutoff datetime: %s", max_date.isoformat())
    logging.info("Deletion enabled: %s", enable_delete)

    models_to_clean = [
        DagRun,
        TaskInstance,
        Log,
        XCom,
        SlaMiss,
        DagModel,
        TaskReschedule,
        TaskFail,
        RenderedTaskInstanceFields,
        ImportError,
        Job,
    ]

    session = Session()
    try:
        for model in models_to_clean:
            _delete_records(session, model, max_date, enable_delete)
    finally:
        session.close()
        logging.info("Cleanup task completed.")


with DAG(
    dag_id="airflow_metadata_cleanup",
    default_args=default_args,
    description="Daily cleanup of Airflow metadata tables.",
    schedule_interval="0 0 * * *",  # daily at midnight UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["maintenance", "cleanup"],
) as dag:
    print_config_task = PythonOperator(
        task_id="print_configuration",
        python_callable=print_configuration,
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_airflow_db",
        python_callable=cleanup_airflow_db,
    )

    print_config_task >> cleanup_task