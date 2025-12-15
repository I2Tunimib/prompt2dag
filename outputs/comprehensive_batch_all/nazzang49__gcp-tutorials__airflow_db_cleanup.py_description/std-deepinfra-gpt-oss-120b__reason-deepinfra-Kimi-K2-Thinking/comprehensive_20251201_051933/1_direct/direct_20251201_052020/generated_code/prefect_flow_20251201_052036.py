from datetime import datetime, timedelta
import os
import logging

from prefect import flow, task
from prefect.tasks import task_input_hash
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(retries=1, retry_delay_seconds=60)
def print_configuration() -> datetime:
    """
    Load cleanup configuration and calculate the maximum date threshold.

    Returns:
        datetime: The cutoff datetime; records older than this will be removed.
    """
    # Retrieve retention period from environment variable (simulating Airflow Variable)
    retention_days_str = os.getenv("MAX_DB_ENTRY_AGE_IN_DAYS", "30")
    try:
        retention_days = int(retention_days_str)
    except ValueError:
        logger.warning(
            "Invalid retention period '%s'; falling back to default of 30 days.",
            retention_days_str,
        )
        retention_days = 30

    max_date = datetime.utcnow() - timedelta(days=retention_days)
    logger.info("Retention period: %d days", retention_days)
    logger.info("Maximum date threshold for deletion: %s", max_date.isoformat())
    return max_date


@task(retries=1, retry_delay_seconds=60)
def cleanup_airflow_db(max_date: datetime) -> None:
    """
    Delete old records from Airflow metadata tables based on the provided cutoff date.

    Args:
        max_date (datetime): Records older than this datetime will be removed.
    """
    # Database connection URL (simulating Airflow metadata DB)
    db_url = os.getenv("AIRFLOW_DB_URL", "sqlite:///airflow_meta.db")
    engine = create_engine(db_url)

    # Mapping of table names to the column used for age filtering.
    # Adjust column names according to actual Airflow schema.
    table_column_map = {
        "dag_run": "execution_date",
        "task_instance": "execution_date",
        "log": "execution_date",
        "xcom": "timestamp",
        "sla_miss": "execution_date",
        "dag_model": "created_date",
        # Optional / version‑specific tables can be added here.
    }

    # Whether deletions should actually be performed.
    enable_delete = os.getenv("ENABLE_DELETE", "false").lower() == "true"

    with engine.begin() as connection:
        for table, column in table_column_map.items():
            delete_sql = text(
                f"DELETE FROM {table} WHERE {column} < :max_date"
            )
            try:
                result = connection.execute(delete_sql, {"max_date": max_date})
                rows_deleted = result.rowcount or 0
                logger.info(
                    "Table %s: %d rows %s.",
                    table,
                    rows_deleted,
                    "deleted" if enable_delete else "matched (deletion disabled)",
                )
                if not enable_delete:
                    # If deletions are disabled, roll back the transaction for this table.
                    connection.rollback()
            except SQLAlchemyError as exc:
                logger.error("Error cleaning table %s: %s", table, exc)
                raise


@flow
def airflow_db_cleanup_flow() -> None:
    """
    Orchestrates the Airflow metadata database cleanup.

    This flow runs two sequential tasks:
    1. ``print_configuration`` – determines the date threshold.
    2. ``cleanup_airflow_db`` – removes old records based on the threshold.
    """
    max_date = print_configuration()
    cleanup_airflow_db(max_date)


if __name__ == "__main__":
    # Local execution entry point.
    # In production, this flow would be deployed with a daily schedule at midnight UTC.
    airflow_db_cleanup_flow()