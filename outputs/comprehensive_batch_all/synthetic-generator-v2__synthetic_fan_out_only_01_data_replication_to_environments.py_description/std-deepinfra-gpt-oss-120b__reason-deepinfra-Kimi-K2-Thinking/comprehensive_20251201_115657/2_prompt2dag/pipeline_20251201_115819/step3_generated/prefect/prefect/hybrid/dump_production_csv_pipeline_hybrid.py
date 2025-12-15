from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='dump_production_csv', retries=2)
def dump_production_csv():
    """Task: Dump Production Database to CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_dev_database', retries=2)
def load_dev_database():
    """Task: Load CSV Snapshot into Development Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_qa_database', retries=2)
def load_qa_database():
    """Task: Load CSV Snapshot into QA Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_staging_database', retries=2)
def load_staging_database():
    """Task: Load CSV Snapshot into Staging Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="dump_production_csv_pipeline",
    task_runner=ConcurrentTaskRunner(),
)
def dump_production_csv_pipeline():
    """Main pipeline flow implementing a fan‑out pattern."""
    # Entry task
    dump = dump_production_csv()

    # Fan‑out: load into multiple environments in parallel, each waiting for the dump
    load_dev_database(wait_for=[dump])
    load_qa_database(wait_for=[dump])
    load_staging_database(wait_for=[dump])


if __name__ == "__main__":
    dump_production_csv_pipeline()