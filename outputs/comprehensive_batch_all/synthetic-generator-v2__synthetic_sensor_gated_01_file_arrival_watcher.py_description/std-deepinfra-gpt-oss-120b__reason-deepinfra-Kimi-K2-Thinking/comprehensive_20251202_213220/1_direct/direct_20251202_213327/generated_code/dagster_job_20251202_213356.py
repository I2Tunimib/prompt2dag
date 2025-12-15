import os
import time
import datetime
import csv
from typing import List, Optional

import dagster
from dagster import (
    op,
    job,
    RetryPolicy,
    Config,
    ConfigurableResource,
    resource,
    ScheduleDefinition,
    sensor,
    RunRequest,
    SkipReason,
    Definitions,
)


class PostgresResource(ConfigurableResource):
    """Minimal PostgreSQL resource stub.

    In a real deployment, you would use a library such as `psycopg2` or `sqlalchemy`
    to manage connections and execute statements.
    """

    connection_string: str = "postgresql://postgres:postgres@localhost:5432/postgres"

    def get_connection(self):
        """Return a connection object.

        This stub returns the connection string; replace with actual connection logic.
        """
        return self.connection_string


@resource
def postgres_resource(init_context) -> PostgresResource:
    return PostgresResource()


class WaitForFileConfig(Config):
    """Configuration for the ``wait_for_file`` op."""

    directory: str = "/data/incoming"
    pattern: str = "transactions_{date}.csv"
    timeout_seconds: int = 24 * 60 * 60  # 24 hours
    poll_interval_seconds: int = 30


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    config_schema=WaitForFileConfig,
    description="Polls the filesystem until a file matching the pattern appears.",
    out={"file_path": dagster.Out(str)},
)
def wait_for_file(context) -> str:
    cfg: WaitForFileConfig = context.op_config
    deadline = datetime.datetime.utcnow() + datetime.timedelta(seconds=cfg.timeout_seconds)

    while datetime.datetime.utcnow() < deadline:
        today_str = datetime.datetime.utcnow().strftime("%Y%m%d")
        expected_name = cfg.pattern.format(date=today_str)
        candidate_path = os.path.join(cfg.directory, expected_name)

        if os.path.isfile(candidate_path):
            context.log.info(f"Found file: {candidate_path}")
            return candidate_path

        context.log.info(
            f"File not found yet ({expected_name}). Sleeping {cfg.poll_interval_seconds}s..."
        )
        time.sleep(cfg.poll_interval_seconds)

    raise dagster.DagsterError(
        f"Timeout after {cfg.timeout_seconds} seconds waiting for file matching pattern "
        f"{cfg.pattern} in {cfg.directory}"
    )


class ValidateSchemaConfig(Config):
    """Configuration for the ``validate_schema`` op."""

    expected_columns: List[str] = ["transaction_id", "customer_id", "amount", "transaction_date"]
    # Simple type expectations; real validation would be more robust.
    column_types: List[str] = ["string", "string", "decimal", "date"]


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    config_schema=ValidateSchemaConfig,
    description="Validates CSV schema against expected columns and basic types.",
    ins={"file_path": dagster.In(str)},
    out={"validated_path": dagster.Out(str)},
)
def validate_schema(context, file_path: str) -> str:
    cfg: ValidateSchemaConfig = context.op_config

    with open(file_path, newline="") as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader, None)

    if header is None:
        raise dagster.DagsterError(f"The file {file_path} is empty or missing a header row.")

    missing = set(cfg.expected_columns) - set(header)
    extra = set(header) - set(cfg.expected_columns)

    if missing:
        raise dagster.DagsterError(f"Missing expected columns: {missing}")
    if extra:
        context.log.warning(f"Extra columns present and will be ignored: {extra}")

    # Basic type checks could be added here; omitted for brevity.
    context.log.info(f"Schema validation passed for file {file_path}")
    return file_path


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    description="Loads validated CSV data into PostgreSQL.",
    required_resource_keys={"postgres"},
    ins={"validated_path": dagster.In(str)},
)
def load_db(context, validated_path: str):
    postgres: PostgresResource = context.resources.postgres
    conn_str = postgres.get_connection()
    table_name = "public.transactions"

    # Placeholder implementation: log the intended action.
    # Replace with actual loading logic using pandas, sqlalchemy, or COPY.
    context.log.info(
        f"Would load data from {validated_path} into table {table_name} using connection {conn_str}"
    )
    # Example with pandas (commented out):
    # import pandas as pd
    # df = pd.read_csv(validated_path)
    # from sqlalchemy import create_engine
    # engine = create_engine(conn_str)
    # df.to_sql(name='transactions', con=engine, schema='public', if_exists='append', index=False)


@job(
    description="Daily pipeline that waits for a transaction file, validates it, and loads it into PostgreSQL.",
    resource_defs={"postgres": postgres_resource},
)
def daily_transaction_job():
    file_path = wait_for_file()
    validated_path = validate_schema(file_path)
    load_db(validated_path)


# Schedule: runs daily at midnight UTC, starting Jan 1, 2024, without catchup.
daily_schedule = ScheduleDefinition(
    job=daily_transaction_job,
    cron_schedule="0 0 * * *",  # daily at 00:00 UTC
    execution_timezone="UTC",
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
)


# Sensor: watches the incoming directory for today's file and triggers the job.
@sensor(job=daily_transaction_job, minimum_interval_seconds=60)
def transaction_file_sensor(context):
    directory = "/data/incoming"
    pattern = "transactions_{date}.csv"
    today_str = datetime.datetime.utcnow().strftime("%Y%m%d")
    expected_name = pattern.format(date=today_str)
    candidate_path = os.path.join(directory, expected_name)

    if os.path.isfile(candidate_path):
        context.log.info(f"Sensor detected file {candidate_path}, triggering job.")
        run_config = {
            "ops": {
                "wait_for_file": {
                    "config": {
                        "directory": directory,
                        "pattern": pattern,
                        "timeout_seconds": 0,  # No waiting needed; file already present
                        "poll_interval_seconds": 0,
                    }
                }
            }
        }
        return RunRequest(run_key=candidate_path, run_config=run_config)
    else:
        return SkipReason(f"No file {expected_name} found in {directory}.")


defs = Definitions(
    jobs=[daily_transaction_job],
    schedules=[daily_schedule],
    sensors=[transaction_file_sensor],
    resources={"postgres": postgres_resource},
)


if __name__ == "__main__":
    # Execute the job immediately for testing purposes.
    result = daily_transaction_job.execute_in_process()
    if result.success:
        print("Job completed successfully.")
    else:
        print("Job failed.")