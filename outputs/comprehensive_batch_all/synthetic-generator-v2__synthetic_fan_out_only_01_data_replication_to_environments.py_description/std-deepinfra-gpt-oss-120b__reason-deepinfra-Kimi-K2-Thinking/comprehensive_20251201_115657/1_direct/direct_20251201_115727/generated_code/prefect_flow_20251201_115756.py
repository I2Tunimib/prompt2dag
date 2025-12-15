from __future__ import annotations

import datetime
import os
import subprocess
import tempfile

from prefect import flow, task


@task(retries=2, retry_delay_seconds=300)
def dump_prod_csv() -> str:
    """Create a CSV snapshot of the production database.

    Returns:
        The absolute path to the generated CSV file.
    """
    date_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
    tmp_dir = tempfile.gettempdir()
    csv_path = os.path.join(tmp_dir, f"prod_snapshot_{date_str}.csv")

    # Placeholder command – replace with actual dump logic.
    cmd = f"echo 'id,name,value' > {csv_path}"
    subprocess.run(cmd, shell=True, check=True)

    return csv_path


@task(retries=2, retry_delay_seconds=300)
def copy_dev(csv_path: str) -> None:
    """Load the CSV snapshot into the Development database."""
    # Placeholder command – replace with actual load logic.
    cmd = f"echo 'Loading {csv_path} into Development DB'"
    subprocess.run(cmd, shell=True, check=True)


@task(retries=2, retry_delay_seconds=300)
def copy_staging(csv_path: str) -> None:
    """Load the CSV snapshot into the Staging database."""
    # Placeholder command – replace with actual load logic.
    cmd = f"echo 'Loading {csv_path} into Staging DB'"
    subprocess.run(cmd, shell=True, check=True)


@task(retries=2, retry_delay_seconds=300)
def copy_qa(csv_path: str) -> None:
    """Load the CSV snapshot into the QA database."""
    # Placeholder command – replace with actual load logic.
    cmd = f"echo 'Loading {csv_path} into QA DB'"
    subprocess.run(cmd, shell=True, check=True)


@flow(
    name="daily_db_replication",
    tags=["replication", "database", "fanout"],
)
def db_replication_flow() -> None:
    """Orchestrate the daily database replication pipeline."""
    csv_path = dump_prod_csv()

    # Fan‑out: copy to each environment concurrently.
    dev_future = copy_dev.submit(csv_path)
    staging_future = copy_staging.submit(csv_path)
    qa_future = copy_qa.submit(csv_path)

    # Wait for all copies to finish before completing the flow.
    dev_future.result()
    staging_future.result()
    qa_future.result()


# Deployment note:
# This flow is intended to run daily with no catch‑up.
# Configure the schedule (e.g., using Prefect deployments) to trigger once per day
# and assign the owner "data_engineering" with appropriate tags.

if __name__ == "__main__":
    db_replication_flow()