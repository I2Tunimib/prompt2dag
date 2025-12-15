from __future__ import annotations

import datetime
import pathlib
import subprocess

from prefect import flow, task
from prefect.tasks import task_input_hash


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "dump"],
    cache_key_fn=task_input_hash,
    cache_expiration=None,
)
def dump_prod_csv() -> str:
    """
    Create a CSV snapshot of the production database.

    Returns
    -------
    str
        Path to the generated CSV file.
    """
    date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    tmp_dir = pathlib.Path("/tmp")
    csv_path = tmp_dir / f"prod_snapshot_{date_str}.csv"

    # Placeholder command – replace with actual dump logic (e.g., pg_dump)
    cmd = f"echo 'id,name\\n1,example' > {csv_path}"
    subprocess.run(cmd, shell=True, check=True)

    return str(csv_path)


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "dev"],
)
def copy_dev(csv_path: str) -> None:
    """
    Load the CSV snapshot into the Development database environment.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file produced by ``dump_prod_csv``.
    """
    cmd = f"echo 'Loading {csv_path} into Development DB'"
    subprocess.run(cmd, shell=True, check=True)


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "staging"],
)
def copy_staging(csv_path: str) -> None:
    """
    Load the CSV snapshot into the Staging database environment.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file produced by ``dump_prod_csv``.
    """
    cmd = f"echo 'Loading {csv_path} into Staging DB'"
    subprocess.run(cmd, shell=True, check=True)


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "qa"],
)
def copy_qa(csv_path: str) -> None:
    """
    Load the CSV snapshot into the QA database environment.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file produced by ``dump_prod_csv``.
    """
    cmd = f"echo 'Loading {csv_path} into QA DB'"
    subprocess.run(cmd, shell=True, check=True)


@flow(
    name="daily_db_replication",
    tags=["replication", "database", "fanout"],
)
def db_replication_flow() -> None:
    """
    Orchestrates the daily database replication pipeline.

    The flow performs the following steps:
    1. Dump the production database to a CSV file.
    2. Fan‑out to copy the CSV into Development, Staging, and QA environments
       concurrently.
    """
    csv_path = dump_prod_csv()

    # Fan‑out: run copy tasks in parallel
    dev_future = copy_dev.submit(csv_path)
    staging_future = copy_staging.submit(csv_path)
    qa_future = copy_qa.submit(csv_path)

    # Wait for all parallel tasks to finish before completing the flow
    dev_future.result()
    staging_future.result()
    qa_future.result()


# Note: In a real deployment, configure a daily schedule with no catch‑up,
# assign the owner to the data_engineering team, and set appropriate tags.

if __name__ == "__main__":
    db_replication_flow()