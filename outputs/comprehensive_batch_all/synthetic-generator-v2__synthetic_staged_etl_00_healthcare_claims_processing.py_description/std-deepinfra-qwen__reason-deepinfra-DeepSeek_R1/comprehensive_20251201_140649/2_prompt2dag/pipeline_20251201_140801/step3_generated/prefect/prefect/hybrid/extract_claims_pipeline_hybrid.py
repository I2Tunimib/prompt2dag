from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

@task(name='extract_claims', retries=2)
def extract_claims():
    """Task: Extract Claims Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_providers', retries=2)
def extract_providers():
    """Task: Extract Providers Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='transform_join', retries=2)
def transform_join():
    """Task: Transform and Join Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_warehouse', retries=2)
def load_warehouse():
    """Task: Load Data to Warehouse"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='refresh_bi', retries=2)
def refresh_bi():
    """Task: Refresh BI Tools"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="extract_claims_pipeline", task_runner=ConcurrentTaskRunner)
def extract_claims_pipeline():
    logger = get_run_logger()
    logger.info("Starting extract_claims_pipeline")

    claims_data = extract_claims.submit()
    providers_data = extract_providers.submit()

    joined_data = transform_join.submit(wait_for=[claims_data, providers_data])

    load_warehouse.submit(wait_for=[joined_data])
    refresh_bi.submit(wait_for=[joined_data])

    logger.info("extract_claims_pipeline completed")

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=extract_claims_pipeline,
        name="extract_claims_pipeline_deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 0 * * *"),  # @daily
    )
    deployment.apply()