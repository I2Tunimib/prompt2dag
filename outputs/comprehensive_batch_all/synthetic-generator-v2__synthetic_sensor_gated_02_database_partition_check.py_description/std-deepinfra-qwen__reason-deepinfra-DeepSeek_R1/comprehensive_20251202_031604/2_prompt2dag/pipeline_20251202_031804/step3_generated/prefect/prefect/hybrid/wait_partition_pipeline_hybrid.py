from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import SequentialTaskRunner

@task(name='wait_partition', retries=0)
def wait_partition():
    """Task: Wait for Partition"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_incremental', retries=2)
def extract_incremental():
    """Task: Extract Incremental Orders"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='transform', retries=2)
def transform():
    """Task: Transform Orders Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load', retries=2)
def load():
    """Task: Load Orders Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="wait_partition_pipeline", task_runner=SequentialTaskRunner)
def wait_partition_pipeline():
    logger = get_run_logger()
    logger.info("Starting wait_partition_pipeline")

    partition_wait = wait_partition()
    incremental_data = extract_incremental(wait_for=[partition_wait])
    transformed_data = transform(wait_for=[incremental_data])
    load(wait_for=[transformed_data])

    logger.info("wait_partition_pipeline completed successfully")

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=wait_partition_pipeline,
        name="wait_partition_pipeline_deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 0 * * *"),  # @daily
    )
    deployment.apply()