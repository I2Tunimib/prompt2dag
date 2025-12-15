from prefect import flow, task, SequentialTaskRunner
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule

@task(name='wait_for_file', retries=0)
def wait_for_file():
    """Task: Wait for File"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='validate_schema', retries=2)
def validate_schema():
    """Task: Validate Schema"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_db', retries=2)
def load_db():
    """Task: Load Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="wait_for_file_pipeline", task_runner=SequentialTaskRunner)
def wait_for_file_pipeline():
    wait_for_file_result = wait_for_file()
    validate_schema_result = validate_schema(wait_for_file_result)
    load_db_result = load_db(validate_schema_result)

deployment = Deployment.build_from_flow(
    flow=wait_for_file_pipeline,
    name="wait_for_file_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),
)

if __name__ == "__main__":
    deployment.apply()