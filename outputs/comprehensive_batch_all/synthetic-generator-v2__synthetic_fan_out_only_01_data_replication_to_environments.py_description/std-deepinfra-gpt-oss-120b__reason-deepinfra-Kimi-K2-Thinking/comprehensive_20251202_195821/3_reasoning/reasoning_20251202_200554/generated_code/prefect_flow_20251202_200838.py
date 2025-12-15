import os
import subprocess
from datetime import datetime

from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "extract"]
)
def dump_prod_csv() -> str:
    """
    Creates a CSV snapshot from the production database.
    
    Returns:
        str: Path to the generated CSV file
    """
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    csv_path = f"/tmp/prod_snapshot_{timestamp}.csv"
    
    prod_host = os.getenv("PROD_DB_HOST", "prod.db.example.com")
    prod_user = os.getenv("PROD_DB_USER", "prod_user")
    prod_password = os.getenv("PROD_DB_PASSWORD", "prod_pass")
    prod_database = os.getenv("PROD_DB_NAME", "production")
    prod_table = os.getenv("PROD_DB_TABLE", "public.users")
    
    cmd = [
        "psql",
        f"--host={prod_host}",
        f"--username={prod_user}",
        f"--dbname={prod_database}",
        "--command",
        f"\\copy (SELECT * FROM {prod_table}) TO '{csv_path}' WITH CSV HEADER"
    ]
    
    env = os.environ.copy()
    env["PGPASSWORD"] = prod_password
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        print(f"Successfully created production CSV snapshot: {csv_path}")
        return csv_path
    except subprocess.CalledProcessError as e:
        print(f"Failed to create CSV snapshot: {e.stderr}")
        raise


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "load", "dev"]
)
def copy_dev(csv_path: str):
    """
    Loads the CSV snapshot into the Development database environment.
    """
    dev_host = os.getenv("DEV_DB_HOST", "dev.db.example.com")
    dev_user = os.getenv("DEV_DB_USER", "dev_user")
    dev_password = os.getenv("DEV_DB_PASSWORD", "dev_pass")
    dev_database = os.getenv("DEV_DB_NAME", "development")
    dev_table = os.getenv("DEV_DB_TABLE", "public.users")
    
    cmd = [
        "psql",
        f"--host={dev_host}",
        f"--username={dev_user}",
        f"--dbname={dev_database}",
        "--command",
        f"TRUNCATE {dev_table}; \\copy {dev_table} FROM '{csv_path}' WITH CSV HEADER"
    ]
    
    env = os.environ.copy()
    env["PGPASSWORD"] = dev_password
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        print(f"Successfully loaded CSV into Development database from {csv_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to load CSV into Development: {e.stderr}")
        raise


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "load", "staging"]
)
def copy_staging(csv_path: str):
    """
    Loads the CSV snapshot into the Staging database environment.
    """
    staging_host = os.getenv("STAGING_DB_HOST", "staging.db.example.com")
    staging_user = os.getenv("STAGING_DB_USER", "staging_user")
    staging_password = os.getenv("STAGING_DB_PASSWORD", "staging_pass")
    staging_database = os.getenv("STAGING_DB_NAME", "staging")
    staging_table = os.getenv("STAGING_DB_TABLE", "public.users")
    
    cmd = [
        "psql",
        f"--host={staging_host}",
        f"--username={staging_user}",
        f"--dbname={staging_database}",
        "--command",
        f"TRUNCATE {staging_table}; \\copy {staging_table} FROM '{csv_path}' WITH CSV HEADER"
    ]
    
    env = os.environ.copy()
    env["PGPASSWORD"] = staging_password
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        print(f"Successfully loaded CSV into Staging database from {csv_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to load CSV into Staging: {e.stderr}")
        raise


@task(
    retries=2,
    retry_delay_seconds=300,
    tags=["replication", "database", "load", "qa"]
)
def copy_qa(csv_path: str):
    """
    Loads the CSV snapshot into the QA database environment.
    """
    qa_host = os.getenv("QA_DB_HOST", "qa.db.example.com")
    qa_user = os.getenv("QA_DB_USER", "qa_user")
    qa_password = os.getenv("QA_DB_PASSWORD", "qa_pass")
    qa_database = os.getenv("QA_DB_NAME", "qa")
    qa_table = os.getenv("QA_DB_TABLE", "public.users")
    
    cmd = [
        "psql",
        f"--host={qa_host}",
        f"--username={qa_user}",
        f"--dbname={qa_database}",
        "--command",
        f"TRUNCATE {qa_table}; \\copy {qa_table} FROM '{csv_path}' WITH CSV HEADER"
    ]
    
    env = os.environ.copy()
    env["PGPASSWORD"] = qa_password
    
    try:
        subprocess.run(cmd, env=env, check=True, capture_output=True, text=True)
        print(f"Successfully loaded CSV into QA database from {csv_path}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to load CSV into QA: {e.stderr}")
        raise


@flow(
    name="database-replication-pipeline",
    description="Daily snapshot of production database distributed to three downstream environments",
    owner="data_engineering",
    tags=["replication", "database", "fanout"],
    task_runner=ConcurrentTaskRunner(),
    # Deployment configuration (uncomment when deploying):
    # schedule={"interval": 86400},  # Daily interval in seconds
    # catchup=False,
)
def database_replication_pipeline():
    """
    Orchestrates the database replication pipeline.
    Follows a fan-out pattern: dump -> parallel copy to dev/staging/qa.
    """
    csv_path = dump_prod_csv()
    
    dev_future = copy_dev.submit(csv_path)
    staging_future = copy_staging.submit(csv_path)
    qa_future = copy_qa.submit(csv_path)


if __name__ == "__main__":
    database_replication_pipeline()