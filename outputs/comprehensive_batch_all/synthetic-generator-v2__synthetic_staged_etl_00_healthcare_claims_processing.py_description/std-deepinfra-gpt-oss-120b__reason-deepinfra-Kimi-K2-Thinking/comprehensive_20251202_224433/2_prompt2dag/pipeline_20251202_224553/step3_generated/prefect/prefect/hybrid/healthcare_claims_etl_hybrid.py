from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='extract_claims', retries=2)
def extract_claims():
    """Task: Extract Claims CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='extract_providers', retries=2)
def extract_providers():
    """Task: Extract Providers CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='transform_join', retries=2)
def transform_join():
    """Task: Join and Anonymize Claims with Providers"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='load_warehouse', retries=2)
def load_warehouse():
    """Task: Load Transformed Data to Data Warehouse"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='refresh_bi', retries=2)
def refresh_bi():
    """Task: Refresh BI Dashboards"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="healthcare_claims_etl", task_runner=ConcurrentTaskRunner())
def healthcare_claims_etl():
    # Fan‑out: extract claims and providers in parallel
    claims_future = extract_claims()
    providers_future = extract_providers()

    # Fan‑in: wait for both extracts before joining
    join_future = transform_join(wait_for=[claims_future, providers_future])

    # Fan‑out after join: load warehouse and refresh BI in parallel
    load_warehouse(wait_for=[join_future])
    refresh_bi(wait_for=[join_future])


if __name__ == "__main__":
    healthcare_claims_etl()