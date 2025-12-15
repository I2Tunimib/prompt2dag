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
    """Task: Join Claims and Providers, Anonymize PII, Calculate Risk Scores"""
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


@flow(name="extract_claims_pipeline", task_runner=ConcurrentTaskRunner())
def extract_claims_pipeline():
    """
    Orchestrates the ETL pipeline using a fan‑out/fan‑in pattern:
    1. Parallel extraction of claims and providers.
    2. Join/transform step that depends on both extracts.
    3. Parallel loading to the warehouse and BI refresh, both depending on the join.
    """
    # Fan‑out: run extracts concurrently
    claims_future = extract_claims.submit()
    providers_future = extract_providers.submit()

    # Ensure both extractions complete before proceeding
    claims_future.result()
    providers_future.result()

    # Fan‑in: join after both extracts are ready
    transform_join()

    # Fan‑out again: load and refresh in parallel
    load_warehouse.submit()
    refresh_bi.submit()


if __name__ == "__main__":
    extract_claims_pipeline()