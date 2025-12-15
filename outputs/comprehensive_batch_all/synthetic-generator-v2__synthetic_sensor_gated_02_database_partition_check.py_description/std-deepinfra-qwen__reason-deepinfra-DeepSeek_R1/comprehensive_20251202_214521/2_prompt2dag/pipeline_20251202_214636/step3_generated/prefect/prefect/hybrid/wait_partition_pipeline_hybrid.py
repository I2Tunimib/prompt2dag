from prefect import flow, task, SequentialTaskRunner
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

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
    wait_partition_result = wait_partition()
    extract_incremental_result = extract_incremental(wait=wait_partition_result)
    transform_result = transform(wait=extract_incremental_result)
    load_result = load(wait=transform_result)

deployment = Deployment.build_from_flow(
    flow=wait_partition_pipeline,
    name="wait_partition_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),
)

if __name__ == "__main__":
    deployment.apply()