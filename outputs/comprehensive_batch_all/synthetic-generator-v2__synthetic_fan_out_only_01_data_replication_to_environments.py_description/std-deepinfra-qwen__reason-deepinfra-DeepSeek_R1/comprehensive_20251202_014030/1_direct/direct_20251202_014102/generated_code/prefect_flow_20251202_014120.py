from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def dump_prod_csv():
    """Creates a CSV snapshot from the production database, generating a dated file in the temporary directory."""
    # Example shell command to create a CSV dump
    os.system("pg_dump -d production_db -F c -b -v -f /tmp/prod_db_dump_$(date +%Y%m%d).csv")
    return "/tmp/prod_db_dump_$(date +%Y%m%d).csv"

@task(retries=2, retry_delay_seconds=300)
def copy_dev(csv_file_path):
    """Loads the CSV snapshot into the Development database environment."""
    # Example shell command to load CSV into Development database
    os.system(f"psql -d dev_db -c \"COPY dev_table FROM '{csv_file_path}' CSV HEADER;\"")

@task(retries=2, retry_delay_seconds=300)
def copy_staging(csv_file_path):
    """Loads the CSV snapshot into the Staging database environment."""
    # Example shell command to load CSV into Staging database
    os.system(f"psql -d staging_db -c \"COPY staging_table FROM '{csv_file_path}' CSV HEADER;\"")

@task(retries=2, retry_delay_seconds=300)
def copy_qa(csv_file_path):
    """Loads the CSV snapshot into the QA database environment."""
    # Example shell command to load CSV into QA database
    os.system(f"psql -d qa_db -c \"COPY qa_table FROM '{csv_file_path}' CSV HEADER;\"")

@flow(name="Database Replication Pipeline", tags=["replication", "database", "fanout"])
def database_replication_pipeline():
    """Orchestrates the database replication pipeline with a fan-out pattern."""
    csv_file_path = dump_prod_csv()
    dev_future = copy_dev.submit(csv_file_path)
    staging_future = copy_staging.submit(csv_file_path)
    qa_future = copy_qa.submit(csv_file_path)
    # No fan-in or synchronization after the parallel environment copies
    dev_future.result()
    staging_future.result()
    qa_future.result()

if __name__ == "__main__":
    database_replication_pipeline()

# Deployment/schedule configuration (optional):
# Schedule: Daily execution schedule
# No catchup enabled to prevent historical backfills
# Owner: data_engineering team
# Tags: replication, database, fanout