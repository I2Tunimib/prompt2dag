from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

# Task definitions
@task(name='ingest_vendor_a', retries=2)
def ingest_vendor_a():
    """Task: Ingest Vendor A Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='ingest_vendor_b', retries=2)
def ingest_vendor_b():
    """Task: Ingest Vendor B Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='ingest_vendor_c', retries=2)
def ingest_vendor_c():
    """Task: Ingest Vendor C Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='cleanse_data', retries=2)
def cleanse_data():
    """Task: Cleanse and Normalize Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_to_db', retries=2)
def load_to_db():
    """Task: Load to Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='send_summary_email', retries=2)
def send_summary_email():
    """Task: Send Summary Email"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

# Flow definition
@flow(name="ingest_vendor_a_pipeline", task_runner=ConcurrentTaskRunner)
def ingest_vendor_a_pipeline():
    logger = get_run_logger()
    logger.info("Starting ingest_vendor_a_pipeline")

    # Fan-in pattern
    vendor_a_data = ingest_vendor_a.submit()
    vendor_b_data = ingest_vendor_b.submit()
    vendor_c_data = ingest_vendor_c.submit()

    cleansed_data = cleanse_data.submit(wait_for=[vendor_a_data, vendor_b_data, vendor_c_data])
    loaded_data = load_to_db.submit(wait_for=[cleansed_data])
    send_summary_email.submit(wait_for=[loaded_data])

    logger.info("Completed ingest_vendor_a_pipeline")

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=ingest_vendor_a_pipeline,
    name="ingest_vendor_a_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()