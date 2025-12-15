import os
import subprocess
from datetime import datetime, timedelta

from dagster import (
    op,
    job,
    RetryPolicy,
    resource,
    ConfigurableResource,
    Definitions,
    ScheduleDefinition,
    DefaultScheduleStatus,
)


@resource
def temp_dir_resource() -> str:
    """Simple resource that provides a temporary directory path."""
    return "/tmp"


@op(
    name="dump_prod_csv",
    description="Creates a CSV snapshot from the production database.",
    required_resource_keys={"temp_dir"},
    tags={"owner": "data_engineering", "category": "replication"},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def dump_prod_csv(context) -> str:
    temp_dir = context.resources.temp_dir
    date_str = datetime.now().strftime("%Y-%m-%d")
    csv_path = os.path.join(temp_dir, f"prod_snapshot_{date_str}.csv")

    # Placeholder for the real bash command that extracts data.
    # Here we simply create a dummy CSV file.
    cmd = f"echo 'id,name\\n1,example' > {csv_path}"
    subprocess.run(cmd, shell=True, check=True)

    context.log.info(f"Created CSV snapshot at {csv_path}")
    return csv_path


def _load_csv_to_env(env_name: str, csv_path: str, context) -> None:
    """Helper that simulates loading a CSV into a target environment."""
    # Placeholder for the real bash command that loads the CSV.
    cmd = f"echo 'Loading {csv_path} into {env_name} database...'"
    subprocess.run(cmd, shell=True, check=True)
    context.log.info(f"Loaded CSV into {env_name} environment.")


@op(
    name="copy_dev",
    description="Loads the CSV snapshot into the Development database environment.",
    tags={"owner": "data_engineering", "category": "replication"},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def copy_dev(context, csv_path: str) -> None:
    _load_csv_to_env("development", csv_path, context)


@op(
    name="copy_staging",
    description="Loads the CSV snapshot into the Staging database environment.",
    tags={"owner": "data_engineering", "category": "replication"},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def copy_staging(context, csv_path: str) -> None:
    _load_csv_to_env("staging", csv_path, context)


@op(
    name="copy_qa",
    description="Loads the CSV snapshot into the QA database environment.",
    tags={"owner": "data_engineering", "category": "replication"},
    retry_policy=RetryPolicy(max_retries=2, delay=timedelta(minutes=5)),
)
def copy_qa(context, csv_path: str) -> None:
    _load_csv_to_env("qa", csv_path, context)


@job(
    name="db_replication_job",
    description="Daily database replication job with fan‑out to dev, staging, and QA.",
    tags={"owner": "data_engineering", "category": "replication", "type": "fanout"},
    resource_defs={"temp_dir": temp_dir_resource},
)
def db_replication_job():
    csv_path = dump_prod_csv()
    copy_dev(csv_path)
    copy_staging(csv_path)
    copy_qa(csv_path)


daily_schedule = ScheduleDefinition(
    job=db_replication_job,
    cron_schedule="0 0 * * *",  # Runs daily at midnight UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    tags={"owner": "data_engineering"},
    description="Daily schedule for the DB replication job.",
    catchup=False,
)


defs = Definitions(
    jobs=[db_replication_job],
    schedules=[daily_schedule],
    resources={"temp_dir": temp_dir_resource},
)


if __name__ == "__main__":
    result = db_replication_job.execute_in_process()
    if result.success:
        print("DB replication job completed successfully.")
    else:
        print("DB replication job failed.")