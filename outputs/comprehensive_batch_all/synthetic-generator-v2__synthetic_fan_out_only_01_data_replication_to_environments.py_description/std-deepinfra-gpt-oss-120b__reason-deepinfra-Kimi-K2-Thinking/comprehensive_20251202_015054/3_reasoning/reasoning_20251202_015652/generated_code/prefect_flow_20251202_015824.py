import subprocess
from datetime import datetime
from pathlib import Path

from prefect import flow, task


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "extract"]
)
def dump_prod_csv() -> str:
    """Creates a CSV snapshot from the production database."""
    snapshot_date = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    snapshot_path = Path(f"/tmp/prod_snapshot_{snapshot_date}.csv")
    
    cmd = [
        "psql",
        "-h", "prod-db.example.com",
        "-U", "prod_user",
        "-d", "production",
        "-c",
        f"\\COPY (SELECT * FROM important_table) TO '{snapshot_path}' CSV HEADER"
    ]
    
    env = {"PGPASSWORD": "your_prod_password"}
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        return str(snapshot_path)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Production dump failed: {e.stderr}")


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "load", "dev"]
)
def copy_dev(snapshot_path: str) -> None:
    """Loads the CSV snapshot into the Development database environment."""
    cmd = [
        "psql",
        "-h", "dev-db.example.com",
        "-U", "dev_user",
        "-d", "development",
        "-c",
        f"\\COPY important_table FROM '{snapshot_path}' CSV HEADER"
    ]
    
    env = {"PGPASSWORD": "your_dev_password"}
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Dev copy failed: {e.stderr}")


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "load", "staging"]
)
def copy_staging(snapshot_path: str) -> None:
    """Loads the CSV snapshot into the Staging database environment."""
    cmd = [
        "psql",
        "-h", "staging-db.example.com",
        "-U", "staging_user",
        "-d", "staging",
        "-c",
        f"\\COPY important_table FROM '{snapshot_path}' CSV HEADER"
    ]
    
    env = {"PGPASSWORD": "your_staging_password"}
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"Staging copy failed: {e.stderr}")


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "load", "qa"]
)
def copy_qa(snapshot_path: str) -> None:
    """Loads the CSV snapshot into the QA database environment."""
    cmd = [
        "psql",
        "-h", "qa-db.example.com",
        "-U", "qa_user",
        "-d", "qa",
        "-c",
        f"\\COPY important_table FROM '{snapshot_path}' CSV HEADER"
    ]
    
    env = {"PGPASSWORD": "your_qa_password"}
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
    except subprocess.CalledProcessError as e:
        raise RuntimeError(f"QA copy failed: {e.stderr}")


@flow(
    name="database-replication-pipeline",
    description="Daily snapshot of production database distributed to three downstream environments",
    tags=["replication", "database", "fanout"]
)
def database_replication_pipeline():
    """
    Daily database replication pipeline with fan-out pattern.
    
    1. Dumps production database to CSV (sequential)
    2. Concurrently loads CSV into Dev, Staging, and QA (parallel)
    """
    snapshot_path = dump_prod_csv()
    
    copy_dev.submit(snapshot_path)
    copy_staging.submit(snapshot_path)
    copy_qa.submit(snapshot_path)


if __name__ == "__main__":
    # Local execution for testing
    # For production deployment with daily schedule:
    # prefect deployment build database_replication_pipeline:database_replication_pipeline \
    #   --name "daily-db-replication" \
    #   --cron "0 2 * * *" \
    #   --timezone UTC \
    #   --tags replication,database,fanout \
    #   --description "Daily DB replication to dev/staging/qa" \
    #   --owner data_engineering
    database_replication_pipeline()