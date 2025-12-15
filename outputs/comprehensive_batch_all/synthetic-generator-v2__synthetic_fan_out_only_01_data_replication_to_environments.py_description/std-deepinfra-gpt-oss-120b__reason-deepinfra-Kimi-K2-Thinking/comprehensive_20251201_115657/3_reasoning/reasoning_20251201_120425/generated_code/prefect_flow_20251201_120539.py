from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import subprocess
from datetime import datetime
from pathlib import Path
import os

# Configuration via environment variables
TEMP_DIR = os.getenv("TEMP_DIR", "/tmp")
PROD_DB_URL = os.getenv("PROD_DB_URL", "postgresql://prod_user:prod_pass@prod-host/prod_db")
DEV_DB_URL = os.getenv("DEV_DB_URL", "postgresql://dev_user:dev_pass@dev-host/dev_db")
STAGING_DB_URL = os.getenv("STAGING_DB_URL", "postgresql://staging_user:staging_pass@staging-host/staging_db")
QA_DB_URL = os.getenv("QA_DB_URL", "postgresql://qa_user:qa_pass@qa-host/qa_db")
TABLE_NAME = os.getenv("TABLE_NAME", "replication_table")


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "extract"]
)
def dump_prod_csv() -> str:
    """Creates a dated CSV snapshot from the production database."""
    timestamp = datetime.utcnow().strftime("%Y%m%d")
    csv_path = Path(TEMP_DIR) / f"prod_snapshot_{timestamp}.csv"
    
    # PostgreSQL example: COPY command to export data
    # Adjust for your specific database system
    cmd = (
        f"psql {PROD_DB_URL} -c "
        f"\"COPY (SELECT * FROM {TABLE_NAME}) TO STDOUT WITH CSV HEADER\" > {csv_path}"
    )
    
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
        return str(csv_path)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Production dump failed: {e.stderr}")


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "fanout"]
)
def copy_dev(csv_path: str):
    """Loads CSV snapshot into the Development database environment."""
    cmd = f"psql {DEV_DB_URL} -c \"\\copy {TABLE_NAME} FROM '{csv_path}' WITH CSV HEADER\""
    
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Dev copy failed: {e.stderr}")


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "fanout"]
)
def copy_staging(csv_path: str):
    """Loads CSV snapshot into the Staging database environment."""
    cmd = f"psql {STAGING_DB_URL} -c \"\\copy {TABLE_NAME} FROM '{csv_path}' WITH CSV HEADER\""
    
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Staging copy failed: {e.stderr}")


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "fanout"]
)
def copy_qa(csv_path: str):
    """Loads CSV snapshot into the QA database environment."""
    cmd = f"psql {QA_DB_URL} -c \"\\copy {TABLE_NAME} FROM '{csv_path}' WITH CSV HEADER\""
    
    try:
        subprocess.run(cmd, shell=True, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"QA copy failed: {e.stderr}")


@flow(
    name="database-replication-pipeline",
    description="Daily production database snapshot distributed to dev, staging, and QA environments",
    task_runner=ConcurrentTaskRunner(),
    tags=["replication", "database", "fanout"]
    # Deployment configuration:
    # - Schedule: Daily (e.g., Cron('0 2 * * *'))
    # - Catchup: False
    # - Owner: data_engineering team
)
def database_replication_pipeline():
    """Orchestrates the fan-out replication pipeline.
    
    1. Dumps production database to CSV
    2. Concurrently copies CSV to dev, staging, and QA environments
    """
    # Step 1: Sequential dump from production
    csv_path = dump_prod_csv()
    
    # Step 2: Fan-out to three parallel copy tasks
    # Tasks execute concurrently with no synchronization
    copy_dev.submit(csv_path)
    copy_staging.submit(csv_path)
    copy_qa.submit(csv_path)


if __name__ == "__main__":
    # Execute the flow locally
    database_replication_pipeline()