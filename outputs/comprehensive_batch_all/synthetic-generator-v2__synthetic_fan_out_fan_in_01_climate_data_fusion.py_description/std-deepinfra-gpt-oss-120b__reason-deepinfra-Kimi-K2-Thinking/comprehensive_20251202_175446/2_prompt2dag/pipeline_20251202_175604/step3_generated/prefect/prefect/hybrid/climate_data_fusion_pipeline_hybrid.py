from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name='download_bom', retries=3)
def download_bom():
    """Task: Download BOM Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='download_ecmwf', retries=3)
def download_ecmwf():
    """Task: Download ECMWF Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='download_jma', retries=3)
def download_jma():
    """Task: Download JMA Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='download_metoffice', retries=3)
def download_metoffice():
    """Task: Download MetOffice Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='download_noaa', retries=3)
def download_noaa():
    """Task: Download NOAA Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='normalize_bom', retries=3)
def normalize_bom():
    """Task: Normalize BOM Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='normalize_ecmwf', retries=3)
def normalize_ecmwf():
    """Task: Normalize ECMWF Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='normalize_jma', retries=3)
def normalize_jma():
    """Task: Normalize JMA Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='normalize_metoffice', retries=3)
def normalize_metoffice():
    """Task: Normalize MetOffice Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='normalize_noaa', retries=3)
def normalize_noaa():
    """Task: Normalize NOAA Weather Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name='merge_climate_data', retries=3)
def merge_climate_data():
    """Task: Merge Climate Datasets"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(name="climate_data_fusion_pipeline", task_runner=ConcurrentTaskRunner())
def climate_data_fusion_pipeline():
    # Entry point download tasks
    bom_raw = download_bom()
    ecmwf_raw = download_ecmwf()
    jma_raw = download_jma()
    metoffice_raw = download_metoffice()
    noaa_raw = download_noaa()

    # Normalization tasks, each waiting on its respective download
    bom_norm = normalize_bom(wait_for=[bom_raw])
    ecmwf_norm = normalize_ecmwf(wait_for=[ecmwf_raw])
    jma_norm = normalize_jma(wait_for=[jma_raw])
    metoffice_norm = normalize_metoffice(wait_for=[metoffice_raw])
    noaa_norm = normalize_noaa(wait_for=[noaa_raw])

    # Fan‑in merge task
    merge = merge_climate_data(
        wait_for=[bom_norm, ecmwf_norm, jma_norm, metoffice_norm, noaa_norm]
    )
    return merge


if __name__ == "__main__":
    climate_data_fusion_pipeline()