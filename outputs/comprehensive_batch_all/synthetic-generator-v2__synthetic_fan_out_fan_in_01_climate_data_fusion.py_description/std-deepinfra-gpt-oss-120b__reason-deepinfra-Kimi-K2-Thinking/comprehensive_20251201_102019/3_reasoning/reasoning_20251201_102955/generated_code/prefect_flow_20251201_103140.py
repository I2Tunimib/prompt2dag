from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from typing import List
import time
import random


@task(retries=3, retry_delay_seconds=300)
def download_noaa() -> str:
    """Fetches NOAA weather station CSV data from FTP server."""
    time.sleep(random.uniform(1, 3))
    return "/tmp/noaa_data.csv"


@task(retries=3, retry_delay_seconds=300)
def download_ecmwf() -> str:
    """Fetches ECMWF weather station CSV data from HTTPS endpoint."""
    time.sleep(random.uniform(1, 3))
    return "/tmp/ecmwf_data.csv"


@task(retries=3, retry_delay_seconds=300)
def download_jma() -> str:
    """Fetches JMA weather station CSV data from HTTPS endpoint."""
    time.sleep(random.uniform(1, 3))
    return "/tmp/jma_data.csv"


@task(retries=3, retry_delay_seconds=300)
def download_metoffice() -> str:
    """Fetches MetOffice weather station CSV data from HTTPS endpoint."""
    time.sleep(random.uniform(1, 3))
    return "/tmp/metoffice_data.csv"


@task(retries=3, retry_delay_seconds=300)
def download_bom() -> str:
    """Fetches BOM weather station CSV data from HTTPS endpoint."""
    time.sleep(random.uniform(1, 3))
    return "/tmp/bom_data.csv"


@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(data_path: str) -> str:
    """Converts NOAA data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(random.uniform(1, 2))
    return f"{data_path}.normalized"


@task(retries=3, retry_delay_seconds=300)
def normalize_ecmwf(data_path: str) -> str:
    """Converts ECMWF data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(random.uniform(1, 2))
    return f"{data_path}.normalized"


@task(retries=3, retry_delay_seconds=300)
def normalize_jma(data_path: str) -> str:
    """Converts JMA data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(random.uniform(1, 2))
    return f"{data_path}.normalized"


@task(retries=3, retry_delay_seconds=300)
def normalize_metoffice(data_path: str) -> str:
    """Converts MetOffice data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(random.uniform(1, 2))
    return f"{data_path}.normalized"


@task(retries=3, retry_delay_seconds=300)
def normalize_bom(data_path: str) -> str:
    """Converts BOM data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(random.uniform(1, 2))
    return f"{data_path}.normalized"


@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(normalized_paths: List[str]) -> str:
    """Combines all five normalized datasets into unified Parquet dataset."""
    time.sleep(random.uniform(2, 4))
    return "/tmp/unified_climate_data.parquet"


@flow(
    name="climate-data-fusion-pipeline",
    task_runner=ConcurrentTaskRunner(),
    # Deployment configuration:
    # - Schedule: daily with no catch-up (e.g., cron: "0 6 * * *")
    # - Notifications: configure email notification on task failure
    #   using prefect-email integration or deployment settings
    # - Maximum parallel width: 5 (handled naturally by task runner)
)
def climate_data_fusion_pipeline():
    """Climate data fusion pipeline with fan-out/fan-in pattern."""
    
    # Fan-out: Parallel download phase
    noaa_future = download_noaa.submit()
    ecmwf_future = download_ecmwf.submit()
    jma_future = download_jma.submit()
    metoffice_future = download_metoffice.submit()
    bom_future = download_bom.submit()
    
    # Fan-out: Parallel normalization phase (depends on respective downloads)
    normalized_noaa = normalize_noaa.submit(noaa_future)
    normalized_ecmwf = normalize_ecmwf.submit(ecmwf_future)
    normalized_jma = normalize_jma.submit(jma_future)
    normalized_metoffice = normalize_metoffice.submit(metoffice_future)
    normalized_bom = normalize_bom.submit(bom_future)
    
    # Fan-in: Merge phase (depends on all normalizations)
    merge_climate_data.submit([
        normalized_noaa,
        normalized_ecmwf,
        normalized_jma,
        normalized_metoffice,
        normalized_bom
    ])


if __name__ == "__main__":
    # Local execution for testing and development
    climate_data_fusion_pipeline()