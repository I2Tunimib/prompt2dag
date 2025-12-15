from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

@task(name='ingest_csv', retries=1)
def ingest_csv():
    """Task: Ingest CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='quality_check', retries=1)
def quality_check():
    """Task: Quality Check"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='production_load', retries=1)
def production_load():
    """Task: Production Load"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='quarantine_and_alert', retries=1)
def quarantine_and_alert():
    """Task: Quarantine and Alert"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='send_alert_email', retries=1)
def send_alert_email():
    """Task: Send Alert Email"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='cleanup', retries=1)
def cleanup():
    """Task: Cleanup"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="ingest_csv_pipeline", task_runner=ConcurrentTaskRunner)
def ingest_csv_pipeline():
    logger = get_run_logger()
    logger.info("Starting ingest_csv_pipeline")

    # Ingest CSV
    ingest_result = ingest_csv()

    # Quality Check
    quality_result = quality_check.submit(ingest_result)

    # Fanout
    production_load_future = production_load.submit(quality_result)
    quarantine_and_alert_future = quarantine_and_alert.submit(quality_result)

    # Fanin
    send_alert_email_future = send_alert_email.submit(quarantine_and_alert_future)

    # Cleanup
    cleanup.submit(wait_for=[production_load_future, send_alert_email_future])

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=ingest_csv_pipeline,
    name="ingest_csv_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()