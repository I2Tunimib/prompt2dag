from datetime import datetime
import subprocess
from pathlib import Path

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import MaxConcurrency


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "fanout"],
    description="Create a dated CSV snapshot from the production database.",
)
def dump_prod_csv() -> Path:
    """Dump the production database to a CSV file in a temporary directory.

    Returns:
        Path: The absolute path to the generated CSV file.
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    output_dir = Path("/tmp")
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"prod_snapshot_{timestamp}.csv"

    # Example bash command; replace with the actual dump command.
    command = (
        f"psql -d production_db -c \"\\copy (SELECT * FROM my_table) TO '{csv_path}' "
        f"WITH CSV HEADER\""
    )
    subprocess.run(command, shell=True, check=True)

    return csv_path


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "fanout"],
    description="Load the CSV snapshot into the Development environment.",
    # Limit concurrency to avoid overloading the dev DB if needed.
    concurrency_limit=MaxConcurrency(1),
)
def copy_dev(csv_path: Path) -> None:
    """Copy the CSV snapshot into the Development database."""
    command = (
        f"psql -d dev_db -c \"\\copy my_table FROM '{csv_path}' WITH CSV HEADER\""
    )
    subprocess.run(command, shell=True, check=True)


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "fanout"],
    description="Load the CSV snapshot into the Staging environment.",
    concurrency_limit=MaxConcurrency(1),
)
def copy_staging(csv_path: Path) -> None:
    """Copy the CSV snapshot into the Staging database."""
    command = (
        f"psql -d staging_db -c \"\\copy my_table FROM '{csv_path}' WITH CSV HEADER\""
    )
    subprocess.run(command, shell=True, check=True)


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "fanout"],
    description="Load the CSV snapshot into the QA environment.",
    concurrency_limit=MaxConcurrency(1),
)
def copy_qa(csv_path: Path) -> None:
    """Copy the CSV snapshot into the QA database."""
    command = (
        f"psql -d qa_db -c \"\\copy my_table FROM '{csv_path}' WITH CSV HEADER\""
    )
    subprocess.run(command, shell=True, check=True)


@flow(
    name="daily_database_replication",
    tags=["replication", "database", "fanout"],
    description="Daily pipeline that creates a production CSV snapshot and distributes it to Dev, Staging, and QA.",
)
def replication_flow() -> None:
    """Orchestrate the replication pipeline with a fan‑out pattern."""
    # Step 1: Create the CSV snapshot.
    csv_path = dump_prod_csv()

    # Step 2: Fan‑out to three parallel copy tasks.
    dev_future = copy_dev.submit(csv_path)
    staging_future = copy_staging.submit(csv_path)
    qa_future = copy_qa.submit(csv_path)

    # No explicit fan‑in; the flow will wait for all submitted tasks to finish before exiting.
    dev_future.result()
    staging_future.result()
    qa_future.result()


if __name__ == "__main__":
    # For local execution. In production, configure a Prefect deployment with a daily schedule,
    # no catchup, and assign the owner to the data_engineering team.
    replication_flow()