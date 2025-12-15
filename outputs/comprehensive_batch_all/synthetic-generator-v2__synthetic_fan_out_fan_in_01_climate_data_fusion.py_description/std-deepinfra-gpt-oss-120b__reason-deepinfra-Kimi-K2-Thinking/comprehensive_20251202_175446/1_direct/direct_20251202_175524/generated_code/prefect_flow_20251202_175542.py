from pathlib import Path
from typing import List

from prefect import flow, task, get_run_logger


@task(retries=3, retry_delay_seconds=300)
def download_noaa() -> str:
    """Fetch NOAA weather station CSV data."""
    logger = get_run_logger()
    logger.info("Downloading NOAA data...")
    # Placeholder implementation: simulate download and return file path
    raw_path = Path("noaa_raw.csv")
    raw_path.touch()
    return str(raw_path)


@task(retries=3, retry_delay_seconds=300)
def download_ecmwf() -> str:
    """Fetch ECMWF weather station CSV data."""
    logger = get_run_logger()
    logger.info("Downloading ECMWF data...")
    raw_path = Path("ecmwf_raw.csv")
    raw_path.touch()
    return str(raw_path)


@task(retries=3, retry_delay_seconds=300)
def download_jma() -> str:
    """Fetch JMA weather station CSV data."""
    logger = get_run_logger()
    logger.info("Downloading JMA data...")
    raw_path = Path("jma_raw.csv")
    raw_path.touch()
    return str(raw_path)


@task(retries=3, retry_delay_seconds=300)
def download_metoffice() -> str:
    """Fetch MetOffice weather station CSV data."""
    logger = get_run_logger()
    logger.info("Downloading MetOffice data...")
    raw_path = Path("metoffice_raw.csv")
    raw_path.touch()
    return str(raw_path)


@task(retries=3, retry_delay_seconds=300)
def download_bom() -> str:
    """Fetch BOM weather station CSV data."""
    logger = get_run_logger()
    logger.info("Downloading BOM data...")
    raw_path = Path("bom_raw.csv")
    raw_path.touch()
    return str(raw_path)


@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(raw_path: str) -> str:
    """Normalize NOAA data to the standard format."""
    logger = get_run_logger()
    logger.info(f"Normalizing NOAA data from {raw_path}...")
    normalized_path = Path("noaa_normalized.parquet")
    normalized_path.touch()
    return str(normalized_path)


@task(retries=3, retry_delay_seconds=300)
def normalize_ecmwf(raw_path: str) -> str:
    """Normalize ECMWF data to the standard format."""
    logger = get_run_logger()
    logger.info(f"Normalizing ECMWF data from {raw_path}...")
    normalized_path = Path("ecmwf_normalized.parquet")
    normalized_path.touch()
    return str(normalized_path)


@task(retries=3, retry_delay_seconds=300)
def normalize_jma(raw_path: str) -> str:
    """Normalize JMA data to the standard format."""
    logger = get_run_logger()
    logger.info(f"Normalizing JMA data from {raw_path}...")
    normalized_path = Path("jma_normalized.parquet")
    normalized_path.touch()
    return str(normalized_path)


@task(retries=3, retry_delay_seconds=300)
def normalize_metoffice(raw_path: str) -> str:
    """Normalize MetOffice data to the standard format."""
    logger = get_run_logger()
    logger.info(f"Normalizing MetOffice data from {raw_path}...")
    normalized_path = Path("metoffice_normalized.parquet")
    normalized_path.touch()
    return str(normalized_path)


@task(retries=3, retry_delay_seconds=300)
def normalize_bom(raw_path: str) -> str:
    """Normalize BOM data to the standard format."""
    logger = get_run_logger()
    logger.info(f"Normalizing BOM data from {raw_path}...")
    normalized_path = Path("bom_normalized.parquet")
    normalized_path.touch()
    return str(normalized_path)


@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(normalized_paths: List[str]) -> str:
    """Merge all normalized datasets into a unified climate dataset."""
    logger = get_run_logger()
    logger.info(f"Merging datasets: {normalized_paths}")
    merged_path = Path("climate_merged.parquet")
    merged_path.touch()
    return str(merged_path)


@flow
def climate_data_fusion_flow() -> str:
    """
    Orchestrates the climate data fusion pipeline.

    The flow runs download and normalization steps in parallel (fan‑out) and
    merges the results after all normalizations complete (fan‑in).
    """
    # Fan‑out: parallel downloads
    download_futures = {
        "noaa": download_noaa.submit(),
        "ecmwf": download_ecmwf.submit(),
        "jma": download_jma.submit(),
        "metoffice": download_metoffice.submit(),
        "bom": download_bom.submit(),
    }

    # Fan‑out: parallel normalizations, each depends on its download
    normalize_futures = {
        "noaa": normalize_noaa.submit(download_futures["noaa"].result()),
        "ecmwf": normalize_ecmwf.submit(download_futures["ecmwf"].result()),
        "jma": normalize_jma.submit(download_futures["jma"].result()),
        "metoffice": normalize_metoffice.submit(download_futures["metoffice"].result()),
        "bom": normalize_bom.submit(download_futures["bom"].result()),
    }

    # Fan‑in: wait for all normalizations to finish
    normalized_paths = [future.result() for future in normalize_futures.values()]

    # Merge step
    merged_path = merge_climate_data(normalized_paths)

    return merged_path


if __name__ == "__main__":
    # Local execution; in production, configure a deployment with a daily schedule,
    # no catch‑up, and email notifications on task failures.
    result = climate_data_fusion_flow()
    print(f"Pipeline completed. Merged dataset at: {result}")