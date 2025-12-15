from datetime import datetime
from pathlib import Path
import subprocess

from dagster import (
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    DefaultScheduleStatus,
    Definitions,
    job,
    op,
)


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "data_engineering", "category": "replication, database, fanout"},
    description="Creates a CSV snapshot from the production database.",
)
def dump_prod_csv(context) -> str:
    """Dump production data to a dated CSV file in /tmp."""
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    file_path = Path("/tmp") / f"prod_snapshot_{date_str}.csv"

    # Placeholder command – replace with actual dump logic.
    cmd = f"echo 'id,name' > {file_path}"
    context.log.info(f"Running dump command: {cmd}")
    subprocess.run(cmd, shell=True, check=True)

    context.log.info(f"Created snapshot at {file_path}")
    return str(file_path)


@op(
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "data_engineering", "environment": "dev"},
    description="Loads the CSV snapshot into the Development database.",
)
def copy_dev(context, csv_path: str):
    """Copy CSV snapshot to Development environment."""
    cmd = f"echo 'Loading {csv_path} into Development DB'"
    context.log.info(f"Running copy_dev command: {cmd}")
    subprocess.run(cmd, shell=True, check=True)


@op(
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "data_engineering", "environment": "staging"},
    description="Loads the CSV snapshot into the Staging database.",
)
def copy_staging(context, csv_path: str):
    """Copy CSV snapshot to Staging environment."""
    cmd = f"echo 'Loading {csv_path} into Staging DB'"
    context.log.info(f"Running copy_staging command: {cmd}")
    subprocess.run(cmd, shell=True, check=True)


@op(
    ins={"csv_path": In(str)},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"owner": "data_engineering", "environment": "qa"},
    description="Loads the CSV snapshot into the QA database.",
)
def copy_qa(context, csv_path: str):
    """Copy CSV snapshot to QA environment."""
    cmd = f"echo 'Loading {csv_path} into QA DB'"
    context.log.info(f"Running copy_qa command: {cmd}")
    subprocess.run(cmd, shell=True, check=True)


@job(
    description="Database replication pipeline with fan‑out to dev, staging, and QA.",
    tags={"owner": "data_engineering", "category": "replication, database, fanout"},
)
def replication_job():
    """Orchestrates the dump and parallel copy operations."""
    csv_path = dump_prod_csv()
    copy_dev(csv_path)
    copy_staging(csv_path)
    copy_qa(csv_path)


daily_replication_schedule = ScheduleDefinition(
    job=replication_job,
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"owner": "data_engineering"},
    catchup=False,
)


defs = Definitions(
    jobs=[replication_job],
    schedules=[daily_replication_schedule],
)


if __name__ == "__main__":
    result = replication_job.execute_in_process()
    assert result.success, "Job execution failed"