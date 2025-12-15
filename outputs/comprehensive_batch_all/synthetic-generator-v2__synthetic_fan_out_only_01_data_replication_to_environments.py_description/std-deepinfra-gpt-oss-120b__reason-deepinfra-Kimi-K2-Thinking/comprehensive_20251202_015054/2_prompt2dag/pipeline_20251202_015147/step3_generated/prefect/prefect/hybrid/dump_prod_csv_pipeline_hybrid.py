from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name="dump_prod_csv", retries=2)
def dump_prod_csv():
    """Task: Dump Production Database to CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="copy_dev", retries=2)
def copy_dev():
    """Task: Load CSV Snapshot into Development Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="copy_qa", retries=2)
def copy_qa():
    """Task: Load CSV Snapshot into QA Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="copy_staging", retries=2)
def copy_staging():
    """Task: Load CSV Snapshot into Staging Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="dump_prod_csv_pipeline", task_runner=ConcurrentTaskRunner())
def dump_prod_csv_pipeline():
    """Main flow for dumping production CSV and copying to environments."""
    dump = dump_prod_csv()
    copy_dev(wait_for=[dump])
    copy_qa(wait_for=[dump])
    copy_staging(wait_for=[dump])


if __name__ == "__main__":
    dump_prod_csv_pipeline()