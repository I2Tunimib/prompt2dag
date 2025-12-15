import os
from datetime import datetime, timedelta
from typing import Dict, List

import sqlalchemy
from sqlalchemy import text
from prefect import flow, task


DEFAULT_RETENTION_DAYS = 30
AIRFLOW_DB_URL = os.getenv("AIRFLOW_DB_URL", "sqlite:///airflow.db")  # Adjust as needed


@task(retries=1, retry_delay_seconds=60, name="Print Configuration")
def print_configuration() -> datetime:
    """
    Load cleanup configuration and calculate the maximum date threshold.

    Returns
    -------
    datetime
        The cutoff datetime; records older than this will be considered for deletion.
    """
    retention_days_str = os.getenv("MAX_DB_ENTRY_AGE_IN_DAYS")
    try:
        retention_days = int(retention_days_str) if retention_days_str else DEFAULT_RETENTION_DAYS
    except ValueError:
        retention_days = DEFAULT_RETENTION_DAYS

    max_date = datetime.utcnow() - timedelta(days=retention_days)
    print(f"Retention period: {retention_days} days")
    print(f"Maximum date threshold for deletion: {max_date.isoformat()} UTC")
    return max_date


@task(retries=1, retry_delay_seconds=60, name="Cleanup Airflow DB")
def cleanup_airflow_db(max_date: datetime) -> Dict[str, int]:
    """
    Delete old records from Airflow metadata tables.

    Parameters
    ----------
    max_date : datetime
        Cut‑off datetime; rows older than this will be deleted.

    Returns
    -------
    dict
        Mapping of table name to number of rows deleted.
    """
    enable_delete = os.getenv("ENABLE_DELETE", "false").lower() == "true"
    engine = sqlalchemy.create_engine(AIRFLOW_DB_URL)

    # Mapping of table name to the column used for age filtering.
    # Adjust column names according to your Airflow version/schema.
    tables: List[Dict[str, str]] = [
        {"name": "dag_run", "column": "execution_date"},
        {"name": "task_instance", "column": "execution_date"},
        {"name": "log", "column": "event_timestamp"},
        {"name": "xcom", "column": "timestamp"},
        {"name": "sla_miss", "column": "execution_date"},
        {"name": "dag", "column": "last_parsed_time"},
        # Add additional tables/models as needed.
    ]

    results: Dict[str, int] = {}

    with engine.begin() as conn:
        for tbl in tables:
            table_name = tbl["name"]
            column_name = tbl["column"]
            delete_stmt = text(
                f"DELETE FROM {table_name} WHERE {column_name} < :max_date"
            )
            select_stmt = text(
                f"SELECT COUNT(*) FROM {table_name} WHERE {column_name} < :max_date"
            )
            count = conn.execute(select_stmt, {"max_date": max_date}).scalar()
            results[table_name] = int(count)

            if count and enable_delete:
                conn.execute(delete_stmt, {"max_date": max_date})
                print(f"Deleted {count} rows from {table_name}.")
            else:
                print(
                    f"Skipping deletion for {table_name}. "
                    f"{'No rows to delete.' if count == 0 else 'Deletion disabled.'}"
                )

    return results


@flow(name="Airflow Metadata Cleanup")
def airflow_cleanup_flow() -> None:
    """
    Orchestrates the Airflow metadata cleanup pipeline.
    """
    max_date = print_configuration()
    cleanup_airflow_db(max_date)


# Deployment note:
# This flow is intended to run daily at midnight UTC.
# Configure a Prefect deployment with a CronSchedule("0 0 * * *") and disable catchup.

if __name__ == "__main__":
    airflow_cleanup_flow()