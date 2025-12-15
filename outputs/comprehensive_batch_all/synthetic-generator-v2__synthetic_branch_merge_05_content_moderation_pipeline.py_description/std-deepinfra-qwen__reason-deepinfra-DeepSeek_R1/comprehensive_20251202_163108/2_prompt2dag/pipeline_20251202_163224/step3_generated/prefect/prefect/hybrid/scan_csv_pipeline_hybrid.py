from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import SequentialTaskRunner

# Task definitions
@task(name='scan_csv', retries=2)
def scan_csv():
    """Task: Scan CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='toxicity_check', retries=2)
def toxicity_check():
    """Task: Toxicity Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='publish_content', retries=2)
def publish_content():
    """Task: Publish Content"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='remove_and_flag_content', retries=2)
def remove_and_flag_content():
    """Task: Remove and Flag Content"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='audit_log', retries=2)
def audit_log():
    """Task: Audit Log"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

# Flow definition
@flow(name="scan_csv_pipeline", task_runner=SequentialTaskRunner)
def scan_csv_pipeline():
    logger = get_run_logger()
    logger.info("Starting scan_csv_pipeline")

    # Task execution with dependencies
    scan_csv_result = scan_csv()
    toxicity_check_result = toxicity_check(wait_for=[scan_csv_result])
    remove_and_flag_content_result = remove_and_flag_content(wait_for=[toxicity_check_result])
    publish_content_result = publish_content(wait_for=[toxicity_check_result])
    audit_log_result = audit_log(wait_for=[remove_and_flag_content_result, publish_content_result])

    logger.info("scan_csv_pipeline completed successfully")

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=scan_csv_pipeline,
    name="scan_csv_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()