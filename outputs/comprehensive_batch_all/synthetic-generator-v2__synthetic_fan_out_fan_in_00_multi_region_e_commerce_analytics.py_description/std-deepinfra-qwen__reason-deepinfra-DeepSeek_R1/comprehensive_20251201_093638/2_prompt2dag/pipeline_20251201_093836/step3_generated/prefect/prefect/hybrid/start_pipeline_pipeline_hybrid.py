from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

@task(name='start_pipeline', retries=0)
def start_pipeline():
    """Task: Start Pipeline"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='ingest_apac', retries=2)
def ingest_apac():
    """Task: Ingest APAC Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='ingest_eu', retries=2)
def ingest_eu():
    """Task: Ingest EU Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='ingest_us_east', retries=2)
def ingest_us_east():
    """Task: Ingest US-East Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='ingest_us_west', retries=2)
def ingest_us_west():
    """Task: Ingest US-West Sales Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='convert_currency_apac', retries=2)
def convert_currency_apac():
    """Task: Convert APAC Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='convert_currency_eu', retries=2)
def convert_currency_eu():
    """Task: Convert EU Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='convert_currency_us_east', retries=2)
def convert_currency_us_east():
    """Task: Convert US-East Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='convert_currency_us_west', retries=2)
def convert_currency_us_west():
    """Task: Convert US-West Currency to USD"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='aggregate_global_revenue', retries=2)
def aggregate_global_revenue():
    """Task: Aggregate Global Revenue"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='end_pipeline', retries=0)
def end_pipeline():
    """Task: End Pipeline"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="start_pipeline_pipeline", task_runner=ConcurrentTaskRunner)
def start_pipeline_pipeline():
    logger = get_run_logger()
    logger.info("Starting the pipeline")

    start = start_pipeline.submit()

    ingest_us_east_task = ingest_us_east.submit(wait_for=[start])
    ingest_us_west_task = ingest_us_west.submit(wait_for=[start])
    ingest_eu_task = ingest_eu.submit(wait_for=[start])
    ingest_apac_task = ingest_apac.submit(wait_for=[start])

    convert_currency_us_east_task = convert_currency_us_east.submit(wait_for=[ingest_us_east_task])
    convert_currency_us_west_task = convert_currency_us_west.submit(wait_for=[ingest_us_west_task])
    convert_currency_eu_task = convert_currency_eu.submit(wait_for=[ingest_eu_task])
    convert_currency_apac_task = convert_currency_apac.submit(wait_for=[ingest_apac_task])

    aggregate_global_revenue_task = aggregate_global_revenue.submit(
        wait_for=[convert_currency_us_east_task, convert_currency_us_west_task, convert_currency_eu_task, convert_currency_apac_task]
    )

    end_pipeline.submit(wait_for=[aggregate_global_revenue_task])

    logger.info("Pipeline completed")

if __name__ == "__main__":
    deployment = Deployment.build_from_flow(
        flow=start_pipeline_pipeline,
        name="start_pipeline_pipeline_deployment",
        work_pool_name="default-agent-pool",
        schedule=CronSchedule(cron="0 0 * * *"),
    )
    deployment.apply()