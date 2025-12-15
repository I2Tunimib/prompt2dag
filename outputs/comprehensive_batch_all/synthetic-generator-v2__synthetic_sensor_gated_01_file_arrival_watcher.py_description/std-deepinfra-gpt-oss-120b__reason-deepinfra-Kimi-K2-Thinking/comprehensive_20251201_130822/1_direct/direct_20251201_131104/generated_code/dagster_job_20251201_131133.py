from __future__ import annotations

import csv
import datetime
import pathlib
import time
from typing import Any

from dagster import (
    Definitions,
    DefaultScheduleStatus,
    JobDefinition,
    OpExecutionContext,
    RetryPolicy,
    ResourceDefinition,
    ScheduleDefinition,
    job,
    op,
    resource,
    schedule,
)


class SimplePostgresResource:
    """Minimal PostgreSQL resource providing a raw connection."""

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        dbname: str = "mydb",
        user: str = "postgres",
        password: str = "postgres",
    ) -> None:
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.password = password

    def get_connection(self):
        """Create a new psycopg2 connection."""
        try:
            import psycopg2  # type: ignore
        except ImportError as exc:
            raise ImportError(
                "psycopg2 is required for PostgreSQL connections. Install it via "
                "`pip install psycopg2-binary`."
            ) from exc

        return psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password,
        )


@resource
def postgres_resource() -> SimplePostgresResource:
    """Provides a SimplePostgresResource with default connection parameters."""
    return SimplePostgresResource()


@op(
    config_schema={"directory": str, "pattern": str},
    retry_policy=RetryPolicy(max_retries=2, delay=5 * 60),
    description="Polls a directory for a CSV file matching a date‑based pattern.",
)
def wait_for_file(context: OpExecutionContext) -> str:
    directory = pathlib.Path(context.op_config["directory"])
    pattern = context.op_config["pattern"]
    timeout_seconds = 24 * 60 * 60  # 24 hours
    poll_interval = 30  # seconds
    deadline = datetime.datetime.now() + datetime.timedelta(seconds=timeout_seconds)

    context.log.info(
        f"Waiting for file in '{directory}' matching pattern '{pattern}' "
        f"for up to {timeout_seconds // 3600} hours."
    )
    while datetime.datetime.now() < deadline:
        matches = list(directory.glob(pattern))
        if matches:
            file_path = matches[0]
            context.log.info(f"Found file: {file_path}")
            return str(file_path)
        context.log.debug(f"No matching file yet. Sleeping {poll_interval}s.")
        time.sleep(poll_interval)

    raise RuntimeError(f"File not found within {timeout_seconds} seconds.")


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=5 * 60),
    description="Validates that the CSV file contains the expected schema.",
)
def validate_schema(context: OpExecutionContext, file_path: str) -> str:
    expected_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    context.log.info(f"Validating schema of file: {file_path}")

    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("CSV file is empty; cannot validate schema.")

    missing = set(expected_columns) - set(header)
    if missing:
        raise ValueError(f"Missing expected columns: {sorted(missing)}")

    context.log.info("Schema validation passed.")
    return file_path


@op(
    required_resource_keys={"postgres"},
    retry_policy=RetryPolicy(max_retries=2, delay=5 * 60),
    description="Loads the validated CSV data into the PostgreSQL table public.transactions.",
)
def load_db(context: OpExecutionContext, file_path: str) -> None:
    context.log.info(f"Loading data from {file_path} into PostgreSQL.")
    conn = context.resources.postgres.get_connection()
    try:
        with conn.cursor() as cur, open(file_path, "r") as f:
            copy_sql = (
                "COPY public.transactions "
                "(transaction_id, customer_id, amount, transaction_date) "
                "FROM STDIN WITH CSV HEADER"
            )
            cur.copy_expert(sql=copy_sql, file=f)
        conn.commit()
        context.log.info("Data load completed successfully.")
    finally:
        conn.close()


@job(resource_defs={"postgres": postgres_resource})
def transaction_job() -> None:
    """Linear pipeline: wait → validate → load."""
    file_path = wait_for_file()
    validated_path = validate_schema(file_path)
    load_db(validated_path)


@schedule(
    cron_schedule="0 0 * * *",  # daily at midnight UTC
    job=transaction_job,
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily schedule that triggers the transaction pipeline.",
)
def daily_transaction_schedule(context) -> dict[str, Any]:
    """Provides run configuration for the daily schedule."""
    return {
        "ops": {
            "wait_for_file": {
                "config": {
                    "directory": "/data/incoming",
                    "pattern": "transactions_*.csv",
                }
            }
        }
    }


defs = Definitions(
    jobs=[transaction_job],
    schedules=[daily_transaction_schedule],
    resources={"postgres": postgres_resource},
)


if __name__ == "__main__":
    result = transaction_job.execute_in_process(
        run_config={
            "ops": {
                "wait_for_file": {
                    "config": {
                        "directory": "/data/incoming",
                        "pattern": "transactions_*.csv",
                    }
                }
            }
        }
    )
    if not result.success:
        raise RuntimeError("Job execution failed.")