from datetime import datetime
from pathlib import Path
from typing import List

from dagster import (
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    job,
    op,
)


def _dummy_download(source_name: str) -> str:
    """Simulate downloading a CSV file and return its local path."""
    timestamp = datetime.utcnow().strftime("%Y%m%d%H%M%S")
    filename = f"{source_name}_raw_{timestamp}.csv"
    path = Path("/tmp") / filename
    path.touch()
    return str(path)


def _dummy_normalize(raw_path: str, source_name: str) -> str:
    """Simulate normalizing a raw CSV file and return the normalized file path."""
    normalized_path = Path(raw_path).with_name(f"{source_name}_norm.parquet")
    normalized_path.touch()
    return str(normalized_path)


def _dummy_merge(normalized_paths: List[str]) -> str:
    """Simulate merging normalized Parquet files into a unified dataset."""
    merged_path = Path("/tmp") / f"climate_merged_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.parquet"
    merged_path.touch()
    return str(merged_path)


# -------------------- Download Ops --------------------


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch NOAA weather station CSV data.",
)
def download_noaa() -> str:
    return _dummy_download("noaa")


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch ECMWF weather station CSV data.",
)
def download_ecmwf() -> str:
    return _dummy_download("ecmwf")


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch JMA weather station CSV data.",
)
def download_jma() -> str:
    return _dummy_download("jma")


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch MetOffice weather station CSV data.",
)
def download_metoffice() -> str:
    return _dummy_download("metoffice")


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch BOM weather station CSV data.",
)
def download_bom() -> str:
    return _dummy_download("bom")


# -------------------- Normalization Ops --------------------


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize NOAA data to standard format.",
)
def normalize_noaa(raw_path: str) -> str:
    return _dummy_normalize(raw_path, "noaa")


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize ECMWF data to standard format.",
)
def normalize_ecmwf(raw_path: str) -> str:
    return _dummy_normalize(raw_path, "ecmwf")


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize JMA data to standard format.",
)
def normalize_jma(raw_path: str) -> str:
    return _dummy_normalize(raw_path, "jma")


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize MetOffice data to standard format.",
)
def normalize_metoffice(raw_path: str) -> str:
    return _dummy_normalize(raw_path, "metoffice")


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize BOM data to standard format.",
)
def normalize_bom(raw_path: str) -> str:
    return _dummy_normalize(raw_path, "bom")


# -------------------- Merge Op --------------------


@op(
    ins={
        "noaa": In(str),
        "ecmwf": In(str),
        "jma": In(str),
        "metoffice": In(str),
        "bom": In(str),
    },
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Merge all normalized climate datasets into a unified Parquet file.",
)
def merge_climate_data(noaa: str, ecmwf: str, jma: str, metoffice: str, bom: str) -> str:
    return _dummy_merge([noaa, ecmwf, jma, metoffice, bom])


# -------------------- Job Definition --------------------


@job(description="Daily climate data fusion pipeline.")
def climate_job():
    # Fan‑out: parallel downloads
    noaa_raw = download_noaa()
    ecmwf_raw = download_ecmwf()
    jma_raw = download_jma()
    metoffice_raw = download_metoffice()
    bom_raw = download_bom()

    # Fan‑out: parallel normalizations (each depends on its download)
    noaa_norm = normalize_noaa(noaa_raw)
    ecmwf_norm = normalize_ecmwf(ecmwf_raw)
    jma_norm = normalize_jma(jma_raw)
    metoffice_norm = normalize_metoffice(metoffice_raw)
    bom_norm = normalize_bom(bom_raw)

    # Fan‑in: merge after all normalizations complete
    merge_climate_data(
        noaa=noaa_norm,
        ecmwf=ecmwf_norm,
        jma=jma_norm,
        metoffice=metoffice_norm,
        bom=bom_norm,
    )


# -------------------- Schedule --------------------


daily_climate_schedule = ScheduleDefinition(
    job=climate_job,
    cron_schedule="0 0 * * *",  # Every day at midnight UTC
    execution_timezone="UTC",
    default_status="RUNNING",
    description="Runs the climate data fusion pipeline once per day.",
)


# -------------------- Entry Point --------------------


if __name__ == "__main__":
    result = climate_job.execute_in_process()
    if result.success:
        print("Climate data fusion pipeline completed successfully.")
    else:
        print("Pipeline failed.")