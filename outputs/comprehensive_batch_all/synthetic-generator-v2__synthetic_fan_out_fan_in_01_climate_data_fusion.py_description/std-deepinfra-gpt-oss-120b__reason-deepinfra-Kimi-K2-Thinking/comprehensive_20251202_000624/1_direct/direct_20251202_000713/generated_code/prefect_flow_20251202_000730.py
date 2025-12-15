from pathlib import Path
import logging
import time
from typing import List

from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect.utilities.annotations import quote

logger = logging.getLogger(__name__)


def _notify_failure(task_name: str, exc: Exception) -> None:
    """Placeholder for failure notification (e.g., email)."""
    logger.error("Task %s failed with exception: %s", task_name, exc)
    # TODO: integrate email service here


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Fetch NOAA weather station CSV data.",
)
def download_noaa() -> Path:
    """Simulate downloading NOAA data."""
    time.sleep(1)  # simulate network latency
    path = Path("/tmp/noaa_raw.csv")
    path.write_text("noaa,data\n")
    logger.info("NOAA data downloaded to %s", path)
    return path


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Fetch ECMWF weather station CSV data.",
)
def download_ecmwf() -> Path:
    """Simulate downloading ECMWF data."""
    time.sleep(1)
    path = Path("/tmp/ecmwf_raw.csv")
    path.write_text("ecmwf,data\n")
    logger.info("ECMWF data downloaded to %s", path)
    return path


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Fetch JMA weather station CSV data.",
)
def download_jma() -> Path:
    """Simulate downloading JMA data."""
    time.sleep(1)
    path = Path("/tmp/jma_raw.csv")
    path.write_text("jma,data\n")
    logger.info("JMA data downloaded to %s", path)
    return path


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Fetch MetOffice weather station CSV data.",
)
def download_metoffice() -> Path:
    """Simulate downloading MetOffice data."""
    time.sleep(1)
    path = Path("/tmp/metoffice_raw.csv")
    path.write_text("metoffice,data\n")
    logger.info("MetOffice data downloaded to %s", path)
    return path


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Fetch BOM weather station CSV data.",
)
def download_bom() -> Path:
    """Simulate downloading BOM data."""
    time.sleep(1)
    path = Path("/tmp/bom_raw.csv")
    path.write_text("bom,data\n")
    logger.info("BOM data downloaded to %s", path)
    return path


def _normalize(input_path: Path, source: str) -> Path:
    """Common normalization logic placeholder."""
    time.sleep(0.5)  # simulate processing time
    normalized_path = input_path.with_name(f"{source}_normalized.parquet")
    normalized_path.write_text(f"{source} normalized data")
    logger.info("%s data normalized to %s", source.upper(), normalized_path)
    return normalized_path


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Normalize NOAA data to standard format.",
)
def normalize_noaa(raw_path: Path) -> Path:
    return _normalize(raw_path, "noaa")


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Normalize ECMWF data to standard format.",
)
def normalize_ecmwf(raw_path: Path) -> Path:
    return _normalize(raw_path, "ecmwf")


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Normalize JMA data to standard format.",
)
def normalize_jma(raw_path: Path) -> Path:
    return _normalize(raw_path, "jma")


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Normalize MetOffice data to standard format.",
)
def normalize_metoffice(raw_path: Path) -> Path:
    return _normalize(raw_path, "metoffice")


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Normalize BOM data to standard format.",
)
def normalize_bom(raw_path: Path) -> Path:
    return _normalize(raw_path, "bom")


@task(
    retries=3,
    retry_delay_seconds=300,
    on_failure=lambda task, exc: _notify_failure(task.name, exc),
    description="Merge all normalized climate datasets into a unified Parquet file.",
)
def merge_climate_data(normalized_paths: List[Path]) -> Path:
    """Combine normalized datasets into a single Parquet file."""
    time.sleep(1)  # simulate merge time
    merged_path = Path("/tmp/unified_climate_dataset.parquet")
    merged_path.write_text(
        "\n".join(str(p) for p in normalized_paths) + "\nmerged data"
    )
    logger.info("Merged climate data written to %s", merged_path)
    return merged_path


@flow
def climate_data_fusion_flow() -> Path:
    """Orchestrates the climate data fusion pipeline."""
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
        "metoffice": normalize_metoffice.submit(
            download_futures["metoffice"].result()
        ),
        "bom": normalize_bom.submit(download_futures["bom"].result()),
    }

    # Fan‑in: merge after all normalizations complete
    normalized_paths = [
        normalize_futures["noaa"].result(),
        normalize_futures["ecmwf"].result(),
        normalize_futures["jma"].result(),
        normalize_futures["metoffice"].result(),
        normalize_futures["bom"].result(),
    ]

    merged_path = merge_climate_data(normalized_paths)
    return merged_path


# Note: In a production deployment, configure a daily schedule with no catch‑up,
# and set up email notifications for task failures via Prefect server or
# orchestration platform.

if __name__ == "__main__":
    result_path = climate_data_fusion_flow()
    logger.info("Pipeline completed. Unified dataset at %s", result_path)