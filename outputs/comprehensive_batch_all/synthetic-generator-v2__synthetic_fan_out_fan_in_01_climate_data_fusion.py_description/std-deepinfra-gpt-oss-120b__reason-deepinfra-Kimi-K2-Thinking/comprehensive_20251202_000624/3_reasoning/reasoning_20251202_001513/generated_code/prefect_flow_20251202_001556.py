from prefect import flow, task
from typing import List
import time
import os


@task(retries=3, retry_delay_seconds=300)
def download_noaa() -> str:
    """Fetches NOAA weather station CSV data from NOAA FTP server."""
    time.sleep(2)
    return "/tmp/noaa_raw.csv"


@task(retries=3, retry_delay_seconds=300)
def download_ecmwf() -> str:
    """Fetches ECMWF weather station CSV data from HTTPS endpoint."""
    time.sleep(2)
    return "/tmp/ecmwf_raw.csv"


@task(retries=3, retry_delay_seconds=300)
def download_jma() -> str:
    """Fetches JMA weather station CSV data from HTTPS endpoint."""
    time.sleep(2)
    return "/tmp/jma_raw.csv"


@task(retries=3, retry_delay_seconds=300)
def download_metoffice() -> str:
    """Fetches MetOffice weather station CSV data from HTTPS endpoint."""
    time.sleep(2)
    return "/tmp/metoffice_raw.csv"


@task(retries=3, retry_delay_seconds=300)
def download_bom() -> str:
    """Fetches BOM weather station CSV data from HTTPS endpoint."""
    time.sleep(2)
    return "/tmp/bom_raw.csv"


@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(raw_file: str) -> str:
    """Converts NOAA data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(1)
    return raw_file.replace("_raw.csv", "_normalized.parquet")


@task(retries=3, retry_delay_seconds=300)
def normalize_ecmwf(raw_file: str) -> str:
    """Converts ECMWF data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(1)
    return raw_file.replace("_raw.csv", "_normalized.parquet")


@task(retries=3, retry_delay_seconds=300)
def normalize_jma(raw_file: str) -> str:
    """Converts JMA data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(1)
    return raw_file.replace("_raw.csv", "_normalized.parquet")


@task(retries=3, retry_delay_seconds=300)
def normalize_metoffice(raw_file: str) -> str:
    """Converts MetOffice data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(1)
    return raw_file.replace("_raw.csv", "_normalized.parquet")


@task(retries=3, retry_delay_seconds=300)
def normalize_bom(raw_file: str) -> str:
    """Converts BOM data to standard format (ISO timestamp, Celsius, meters)."""
    time.sleep(1)
    return raw_file.replace("_raw.csv", "_normalized.parquet")


@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(normalized_files: List[str]) -> str:
    """Combines all five normalized datasets into unified Parquet dataset."""
    time.sleep(3)
    return "/tmp/unified_climate_dataset.parquet"


@flow(name="climate-data-fusion-pipeline")
def climate_data_fusion_pipeline():
    """
    Climate data fusion pipeline with fan-out/fan-in pattern.
    Downloads, normalizes, and merges weather data from five agencies.
    """
    # Fan-out: Parallel download tasks
    noaa_raw = download_noaa.submit()
    ecmwf_raw = download_ecmwf.submit()
    jma_raw = download_jma.submit()
    metoffice_raw = download_metoffice.submit()
    bom_raw = download_bom.submit()

    # Fan-out: Parallel normalization tasks (dependent on respective downloads)
    noaa_normalized = normalize_noaa.submit(noaa_raw)
    ecmwf_normalized = normalize_ecmwf.submit(ecmwf_raw)
    jma_normalized = normalize_jma.submit(jma_raw)
    metoffice_normalized = normalize_metoffice.submit(metoffice_raw)
    bom_normalized = normalize_bom.submit(bom_raw)

    # Fan-in: Merge task waits for all normalizations
    normalized_files = [
        noaa_normalized,
        ecmwf_normalized,
        jma_normalized,
        metoffice_normalized,
        bom_normalized
    ]
    
    unified_dataset = merge_climate_data(normalized_files)
    
    return unified_dataset


if __name__ == "__main__":
    # Local execution entry point
    # For deployment with daily schedule (no catch-up) and email notifications:
    # prefect deployment build climate_data_fusion_pipeline:climate_data_fusion_pipeline \
    #   --name "daily-climate-pipeline" \
    #   --schedule "0 6 * * *" \
    #   --timezone "UTC" \
    #   --apply
    # Configure email notifications via Prefect Cloud/Server UI or blocks
    climate_data_fusion_pipeline()