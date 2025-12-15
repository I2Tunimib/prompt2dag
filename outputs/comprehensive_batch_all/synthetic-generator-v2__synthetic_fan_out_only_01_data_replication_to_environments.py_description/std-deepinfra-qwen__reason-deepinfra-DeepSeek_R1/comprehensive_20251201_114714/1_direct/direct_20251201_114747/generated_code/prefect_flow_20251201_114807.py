from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
import os

@task(retries=2, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def dump_prod_csv():
    """Creates a CSV snapshot from the production database, generating a dated file in the temporary directory."""
    date_str = os.popen("date +%Y%m%d").read().strip()
    csv_file = f"/tmp/prod_snapshot_{date_str}.csv"
    os.system(f"pg_dump -d production_db -F c -f {csv_file}")
    return csv_file

@task(retries=2, retry_delay_seconds=300)
def copy_dev(csv_file):
    """Loads the CSV snapshot into the Development database environment."""
    os.system(f"pg_restore -d dev_db {csv_file}")

@task(retries=2, retry_delay_seconds=300)
def copy_staging(csv_file):
    """Loads the CSV snapshot into the Staging database environment."""
    os.system(f"pg_restore -d staging_db {csv_file}")

@task(retries=2, retry_delay_seconds=300)
def copy_qa(csv_file):
    """Loads the CSV snapshot into the QA database environment."""
    os.system(f"pg_restore -d qa_db {csv_file}")

@flow(name="Database Replication Pipeline", tags=["replication", "database", "fanout"])
def database_replication_pipeline():
    """Orchestrates the database replication pipeline with a fan-out pattern."""
    csv_file = dump_prod_csv()
    copy_dev.submit(csv_file)
    copy_staging.submit(csv_file)
    copy_qa.submit(csv_file)

if __name__ == '__main__':
    database_replication_pipeline()