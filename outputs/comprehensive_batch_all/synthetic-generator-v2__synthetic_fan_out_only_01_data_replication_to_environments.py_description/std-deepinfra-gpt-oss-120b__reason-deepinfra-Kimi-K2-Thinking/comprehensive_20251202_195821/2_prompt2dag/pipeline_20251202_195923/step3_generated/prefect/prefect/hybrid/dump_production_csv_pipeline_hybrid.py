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
    """Task: Load CSV into Development Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_qa_database', retries=2)
def load_qa_database():
    """Task: Load CSV into QA Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_staging_database', retries=2)
def load_staging_database():
    """Task: Load CSV into Staging Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="dump_production_csv_pipeline",
    task_runner=ConcurrentTaskRunner(),
)
def dump_production_csv_pipeline():
    """
    Pipeline that dumps the production database to CSV and loads it into
    development, QA, and staging databases using a fan‑out pattern.
    """
    # Entry point: dump production data
    dump_future = dump_production_csv.submit()

    # Fan‑out: load the CSV into the three target environments concurrently
    dev_future = load_dev_database.submit(wait_for=[dump_future])
    qa_future = load_qa_database.submit(wait_for=[dump_future])
    staging_future = load_staging_database.submit(wait_for=[dump_future])

    # Optionally wait for all downstream tasks to complete before finishing the flow
    dev_future.result()
    qa_future.result()
    staging_future.result()


if __name__ == "__main__":
    dump_production_csv_pipeline()