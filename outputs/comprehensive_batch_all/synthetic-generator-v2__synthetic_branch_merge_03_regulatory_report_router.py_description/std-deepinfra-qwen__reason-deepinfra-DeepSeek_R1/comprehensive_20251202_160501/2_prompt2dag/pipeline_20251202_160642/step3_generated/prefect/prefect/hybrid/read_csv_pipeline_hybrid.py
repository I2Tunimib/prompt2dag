from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

@task(name='read_csv', retries=2)
def read_csv():
    """Task: Read CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='account_check', retries=2)
def account_check():
    """Task: Account Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='route_to_fatca', retries=2)
def route_to_fatca():
    """Task: Route to FATCA"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='route_to_irs', retries=2)
def route_to_irs():
    """Task: Route to IRS"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='archive_reports', retries=2)
def archive_reports():
    """Task: Archive Reports"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="read_csv_pipeline", task_runner=ConcurrentTaskRunner)
def read_csv_pipeline():
    logger = get_run_logger()
    logger.info("Starting read_csv_pipeline")

    # Entry point
    csv_data = read_csv()

    # Fan out
    account_check_result = account_check(wait_for=[csv_data])
    fatca_result = route_to_fatca(wait_for=[account_check_result])
    irs_result = route_to_irs(wait_for=[account_check_result])

    # Fan in
    archive_reports(wait_for=[fatca_result, irs_result])

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=read_csv_pipeline,
    name="read_csv_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()