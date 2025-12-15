import os
import datetime
from typing import List

import pandas as pd
from sqlalchemy import create_engine
from dagster import (
    op,
    job,
    RetryPolicy,
    Config,
    ConfigurableResource,
    In,
    Out,
    Nothing,
    SensorDefinition,
    RunRequest,
    ScheduleDefinition,
    Definitions,
    get_dagster_logger,
)


class PostgresResource(ConfigurableResource):
    """Simple PostgreSQL resource using SQLAlchemy."""

    host: str = "localhost"
    port: int = 5432
    database: str = "postgres"
    username: str = "postgres"
    password: str = "postgres"
    schema: str = "public"
    table: str = "transactions"

    def get_engine(self):
        url = (
            f"postgresql://{self.username}:{self.password}"
            f"@{self.host}:{self.port}/{self.database}"
        )
        return create_engine(url)


class ValidateSchemaConfig(Config):
    file_path: str


class LoadDbConfig(Config):
    file_path: str  # kept for parity; actual data passed via upstream output


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=datetime.timedelta(minutes=5)),
    out=Out(pd.DataFrame),
    description="Validate CSV schema and data types.",
)
def validate_schema(context, config: ValidateSchemaConfig) -> pd.DataFrame:
    logger = get_dagster_logger()
    logger.info(f"Validating schema for file: {config.file_path}")

    required_columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    dtype_map = {
        "transaction_id": "object",
        "customer_id": "object",
        "amount": "float64",
        "transaction_date": "datetime64[ns]",
    }

    df = pd.read_csv(config.file_path)

    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Enforce dtypes
    for col, dtype in dtype_map.items():
        try:
            df[col] = df[col].astype(dtype)
        except Exception as exc:
            raise ValueError(f"Column '{col}' cannot be cast to {dtype}: {exc}")

    logger.info("Schema validation passed")
    return df


@op(
    retry_policy=RetryPolicy(max_retries=2, delay=datetime.timedelta(minutes=5)),
    description="Load validated data into PostgreSQL.",
    required_resource_keys={"postgres"},
)
def load_db(context, df: pd.DataFrame, config: LoadDbConfig) -> Nothing:
    logger = get_dagster_logger()
    logger.info(f"Loading data from {config.file_path} into PostgreSQL")

    engine = context.resources.postgres.get_engine()
    table_name = f"{context.resources.postgres.schema}.{context.resources.postgres.table}"

    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )
    logger.info("Data load complete")


@job(
    description="Daily transaction ingestion pipeline.",
    resource_defs={"postgres": PostgresResource()},
)
def transaction_ingestion_job():
    df = validate_schema()
    load_db(df)


def _today_file_path() -> str:
    """Construct expected file path for today's date."""
    today = datetime.date.today()
    filename = f"transactions_{today.strftime('%Y%m%d')}.csv"
    return os.path.join("/data/incoming", filename)


def _file_exists(path: str) -> bool:
    return os.path.isfile(path)


def file_sensor(context) -> RunRequest | None:
    """Sensor that triggers the job when today's transaction file appears."""
    file_path = _today_file_path()
    if _file_exists(file_path):
        run_config = {
            "ops": {
                "validate_schema": {"config": {"file_path": file_path}},
                "load_db": {"config": {"file_path": file_path}},
            }
        }
        return RunRequest(run_key=file_path, run_config=run_config)
    return None


file_sensor_def = SensorDefinition(
    name="transaction_file_sensor",
    evaluate_interval=datetime.timedelta(seconds=30),
    minimum_interval=datetime.timedelta(seconds=30),
    target=transaction_ingestion_job,
    evaluation_fn=file_sensor,
)


daily_schedule = ScheduleDefinition(
    name="daily_transaction_schedule",
    cron_schedule="@daily",
    job=transaction_ingestion_job,
    start_date=datetime.datetime(2024, 1, 1, tzinfo=datetime.timezone.utc),
    catchup=False,
    default_status="RUNNING",
    run_config={
        "ops": {
            "validate_schema": {"config": {"file_path": _today_file_path()}},
            "load_db": {"config": {"file_path": _today_file_path()}},
        }
    },
)


defs = Definitions(
    jobs=[transaction_ingestion_job],
    sensors=[file_sensor_def],
    schedules=[daily_schedule],
    resources={"postgres": PostgresResource()},
)


if __name__ == "__main__":
    result = transaction_ingestion_job.execute_in_process(
        run_config={
            "ops": {
                "validate_schema": {"config": {"file_path": _today_file_path()}},
                "load_db": {"config": {"file_path": _today_file_path()}},
            }
        }
    )
    assert result.success