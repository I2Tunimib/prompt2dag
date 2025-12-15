from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

@task(name='dump_prod_csv', retries=2)
def dump_prod_csv():
    """Task: Dump Production CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='copy_dev', retries=2)
def copy_dev():
    """Task: Copy to Development"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='copy_qa', retries=2)
def copy_qa():
    """Task: Copy to QA"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='copy_staging', retries=2)
def copy_staging():
    """Task: Copy to Staging"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="dump_prod_csv_pipeline", task_runner=ConcurrentTaskRunner)
def dump_prod_csv_pipeline():
    logger = get_run_logger()
    logger.info("Starting dump_prod_csv_pipeline")

    # Run the initial task
    dump_prod_csv_result = dump_prod_csv()

    # Fanout to dependent tasks
    copy_dev(dump_prod_csv_result)
    copy_qa(dump_prod_csv_result)
    copy_staging(dump_prod_csv_result)

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=dump_prod_csv_pipeline,
    name="dump_prod_csv_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()