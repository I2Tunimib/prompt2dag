from datetime import datetime, timedelta
import os
import subprocess

from dagster import (
    op,
    job,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    DefaultScheduleStatus,
    schedule,
    Definitions,
    get_dagster_logger,
)


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    tags={"owner": "data_engineering", "category": "replication, database, fanout"},
    description="Creates a CSV snapshot from the production database.",
)
def dump_prod_csv(context) -> str:
    """Generate a dated CSV file in the temporary directory."""
    logger = get_dagster_logger()
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    filename = f"prod_snapshot_{date_str}.csv"
    tmp_dir = "/tmp"
    os.makedirs(tmp_dir, exist_ok=True)
    file_path = os.path.join(tmp_dir, filename)

    # Placeholder for actual dump command; replace with real command as needed.
    dump_command = f"echo 'id,name,value' > {file_path}"
    logger.info(f"Running dump command: {dump_command}")
    try:
        subprocess.run(dump_command, shell=True, check=True)
    except subprocess.CalledProcessError as exc:
        logger.error(f"Failed to dump production CSV: {exc}")
        raise

    logger.info(f"Production CSV snapshot created at {file_path}")
    return file_path


def _load_csv_to_env(env_name: str, csv_path: str, logger) -> None:
    """Helper to simulate loading CSV into a target environment."""
    # Placeholder for actual load command; replace with real command as needed.
    load_command = f"echo 'Loading {csv_path} into {env_name} database...'"
    logger.info(f"Running load command for {env_name}: {load_command}")
    try:
        subprocess.run(load_command, shell=True, check=True)
    except subprocess.CalledProcessError as exc:
        logger.error(f"Failed to load CSV into {env_name}: {exc}")
        raise
    logger.info(f"Successfully loaded CSV into {env_name} environment.")


@op(
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    tags={"owner": "data_engineering", "environment": "dev"},
    description="Loads the CSV snapshot into the Development database environment.",
)
def copy_dev(context, csv_path: str) -> None:
    _load_csv_to_env("Development", csv_path, get_dagster_logger())


@op(
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    tags={"owner": "data_engineering", "environment": "staging"},
    description="Loads the CSV snapshot into the Staging database environment.",
)
def copy_staging(context, csv_path: str) -> None:
    _load_csv_to_env("Staging", csv_path, get_dagster_logger())


@op(
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
    tags={"owner": "data_engineering", "environment": "qa"},
    description="Loads the CSV snapshot into the QA database environment.",
)
def copy_qa(context, csv_path: str) -> None:
    _load_csv_to_env("QA", csv_path, get_dagster_logger())


@job(
    tags={"owner": "data_engineering", "category": "replication, database, fanout"},
    description="Daily replication pipeline that creates a production CSV snapshot and distributes it to Dev, Staging, and QA environments.",
)
def replication_job():
    csv_path = dump_prod_csv()
    copy_dev(csv_path)
    copy_staging(csv_path)
    copy_qa(csv_path)


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=replication_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    catchup=False,
    tags={"owner": "data_engineering", "category": "replication, database, fanout"},
)
def daily_replication_schedule():
    """Schedule that triggers the replication_job daily."""
    return {}


defs = Definitions(
    jobs=[replication_job],
    schedules=[daily_replication_schedule],
)


if __name__ == "__main__":
    result = replication_job.execute_in_process()
    if result.success:
        print("Replication job completed successfully.")
    else:
        print("Replication job failed.")