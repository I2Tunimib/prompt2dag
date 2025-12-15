"""Airflow DAG for periodic cleanup of Airflow metadata tables.

The DAG runs daily at midnight UTC and removes records older than a configurable
retention period. The retention period and a flag to enable/disable actual
deletions are read from Airflow Variables.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG, settings
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import delete

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": True,
    "email": ["admin@example.com"],
}

# DAG definition
with DAG(
    dag_id="airflow_metadata_cleanup",
    default_args=default_args,
    description="Remove old records from Airflow meta database tables",
    schedule_interval="0 0 * * *",  # daily at midnight UTC
    start_date=days_ago(1),
    catchup=False,
    tags=["maintenance", "cleanup"],
) as dag:

    def print_configuration(**context):
        """Load configuration and push max_date to XCom."""
        # Load retention period (in days) from Variable, default to 30 days
        retention_days = int(
            Variable.get("max_db_entry_age_in_days", default_var="30")
        )
        # Load delete flag, default to True
        enable_delete = Variable.get("enable_delete", default_var="True").lower()
        enable_delete = enable_delete in ("true", "1", "yes")

        max_date = datetime.utcnow() - timedelta(days=retention_days)

        logging.info(
            "Configuration - retention_days: %s, enable_delete: %s, max_date: %s",
            retention_days,
            enable_delete,
            max_date.isoformat(),
        )

        # Push values to XCom for downstream tasks
        ti = context["ti"]
        ti.xcom_push(key="max_date", value=max_date.isoformat())
        ti.xcom_push(key="enable_delete", value=enable_delete)

    print_configuration_task = PythonOperator(
        task_id="print_configuration",
        python_callable=print_configuration,
        provide_context=True,
    )

    def cleanup_airflow_db(**context):
        """Delete records older than max_date from selected Airflow tables."""
        ti = context["ti"]
        max_date_iso = ti.xcom_pull(key="max_date", task_ids="print_configuration")
        enable_delete = ti.xcom_pull(key="enable_delete", task_ids="print_configuration")

        if not max_date_iso:
            raise ValueError("max_date not found in XCom")
        max_date = datetime.fromisoformat(max_date_iso)

        logging.info(
            "Starting cleanup. max_date=%s, enable_delete=%s", max_date, enable_delete
        )

        # Import models lazily to avoid circular imports
        from airflow.models import (
            DagRun,
            DagModel,
            Log,
            TaskInstance,
            XCom,
            SlaMiss,
            TaskReschedule,
            TaskFail,
            RenderedTaskInstanceFields,
            ImportError,
            Job,
        )

        # Mapping of model to filter condition
        model_filters = {
            DagRun: DagRun.start_date < max_date,
            TaskInstance: TaskInstance.start_date < max_date,
            Log: Log.dttm < max_date,
            XCom: XCom.timestamp < max_date,
            SlaMiss: SlaMiss.execution_date < max_date,
            DagModel: DagModel.last_expired < max_date,
            TaskReschedule: TaskReschedule.reschedule_date < max_date,
            TaskFail: TaskFail.start_date < max_date,
            RenderedTaskInstanceFields: RenderedTaskInstanceFields.timestamp < max_date,
            ImportError: ImportError.timestamp < max_date,
            Job: Job.start_date < max_date,
        }

        session = settings.Session()
        try:
            for model, condition in model_filters.items():
                query = session.query(model).filter(condition)
                count = query.count()
                logging.info("Found %s rows to delete from %s", count, model.__name__)

                if count == 0:
                    continue

                if enable_delete:
                    delete_stmt = delete(model).where(condition)
                    session.execute(delete_stmt)
                    logging.info("Deleted %s rows from %s", count, model.__name__)
                else:
                    logging.info(
                        "Deletion disabled; would have removed %s rows from %s",
                        count,
                        model.__name__,
                    )
            session.commit()
            logging.info("Cleanup completed successfully.")
        except Exception as exc:
            session.rollback()
            logging.error("Error during cleanup: %s", exc)
            raise
        finally:
            session.close()

    cleanup_airflow_db_task = PythonOperator(
        task_id="cleanup_airflow_db",
        python_callable=cleanup_airflow_db,
        provide_context=True,
    )

    # Define linear dependencies
    print_configuration_task >> cleanup_airflow_db_task