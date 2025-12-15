from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
from prefect.task_runners import ConcurrentTaskRunner

@task(name='download_bom', retries=3)
def download_bom():
    """Task: Download BOM Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_ecmwf', retries=3)
def download_ecmwf():
    """Task: Download ECMWF Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_jma', retries=3)
def download_jma():
    """Task: Download JMA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_metoffice', retries=3)
def download_metoffice():
    """Task: Download MetOffice Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='download_noaa', retries=3)
def download_noaa():
    """Task: Download NOAA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_bom', retries=3)
def normalize_bom():
    """Task: Normalize BOM Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_ecmwf', retries=3)
def normalize_ecmwf():
    """Task: Normalize ECMWF Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_jma', retries=3)
def normalize_jma():
    """Task: Normalize JMA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_metoffice', retries=3)
def normalize_metoffice():
    """Task: Normalize MetOffice Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_noaa', retries=3)
def normalize_noaa():
    """Task: Normalize NOAA Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='merge_climate_data', retries=3)
def merge_climate_data():
    """Task: Merge Climate Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="download_noaa_pipeline", task_runner=ConcurrentTaskRunner)
def download_noaa_pipeline():
    logger = get_run_logger()
    logger.info("Starting download_noaa_pipeline")

    # Entry points
    noaa_data = download_noaa.submit()
    ecmwf_data = download_ecmwf.submit()
    jma_data = download_jma.submit()
    metoffice_data = download_metoffice.submit()
    bom_data = download_bom.submit()

    # Normalization tasks
    normalized_noaa = normalize_noaa.submit(wait_for=[noaa_data])
    normalized_ecmwf = normalize_ecmwf.submit(wait_for=[ecmwf_data])
    normalized_jma = normalize_jma.submit(wait_for=[jma_data])
    normalized_metoffice = normalize_metoffice.submit(wait_for=[metoffice_data])
    normalized_bom = normalize_bom.submit(wait_for=[bom_data])

    # Merge task
    merge_climate_data.submit(wait_for=[normalized_noaa, normalized_ecmwf, normalized_jma, normalized_metoffice, normalized_bom])

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=download_noaa_pipeline,
    name="download_noaa_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()