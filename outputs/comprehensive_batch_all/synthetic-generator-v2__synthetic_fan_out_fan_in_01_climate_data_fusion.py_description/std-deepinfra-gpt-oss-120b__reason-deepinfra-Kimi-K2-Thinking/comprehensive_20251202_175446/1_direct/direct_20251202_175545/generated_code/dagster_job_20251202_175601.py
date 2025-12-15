from pathlib import Path
from typing import List

from dagster import (
    DefaultScheduleStatus,
    Definitions,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    graph,
    job,
    op,
    schedule,
)


def _dummy_download(agency: str) -> str:
    """Simulate downloading a CSV file for the given agency."""
    path = Path(f"/tmp/{agency}_raw.csv")
    path.write_text(f"dummy data for {agency}")
    return str(path)


def _dummy_normalize(agency: str, raw_path: str) -> str:
    """Simulate normalizing raw CSV data to a Parquet file."""
    norm_path = Path(f"/tmp/{agency}_norm.parquet")
    norm_path.write_text(f"normalized data from {raw_path}")
    return str(norm_path)


def _dummy_merge(normalized_paths: List[str]) -> str:
    """Simulate merging normalized Parquet files into a unified dataset."""
    merged_path = Path("/tmp/unified_climate.parquet")
    merged_path.write_text("\n".join(normalized_paths))
    return str(merged_path)


@op(
    name="download_noaa",
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetches NOAA weather station CSV data.",
)
def download_noaa() -> str:
    return _dummy_download("noaa")


@op(
    name="download_ecmwf",
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetches ECMWF weather station CSV data.",
)
def download_ecmwf() -> str:
    return _dummy_download("ecmwf")


@op(
    name="download_jma",
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetches JMA weather station CSV data.",
)
def download_jma() -> str:
    return _dummy_download("jma")


@op(
    name="download_metoffice",
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetches MetOffice weather station CSV data.",
)
def download_metoffice() -> str:
    return _dummy_download("metoffice")


@op(
    name="download_bom",
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetches BOM weather station CSV data.",
)
def download_bom() -> str:
    return _dummy_download("bom")


@op(
    name="normalize_noaa",
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Converts NOAA data to standard format.",
)
def normalize_noaa(raw_path: str) -> str:
    return _dummy_normalize("noaa", raw_path)


@op(
    name="normalize_ecmwf",
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Converts ECMWF data to standard format.",
)
def normalize_ecmwf(raw_path: str) -> str:
    return _dummy_normalize("ecmwf", raw_path)


@op(
    name="normalize_jma",
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Converts JMA data to standard format.",
)
def normalize_jma(raw_path: str) -> str:
    return _dummy_normalize("jma", raw_path)


@op(
    name="normalize_metoffice",
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Converts MetOffice data to standard format.",
)
def normalize_metoffice(raw_path: str) -> str:
    return _dummy_normalize("metoffice", raw_path)


@op(
    name="normalize_bom",
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Converts BOM data to standard format.",
)
def normalize_bom(raw_path: str) -> str:
    return _dummy_normalize("bom", raw_path)


@op(
    name="merge_climate_data",
    ins={
        "noaa": In(str),
        "ecmwf": In(str),
        "jma": In(str),
        "metoffice": In(str),
        "bom": In(str),
    },
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Combines all normalized datasets into a unified climate dataset.",
)
def merge_climate_data(noaa: str, ecmwf: str, jma: str, metoffice: str, bom: str) -> str:
    return _dummy_merge([noaa, ecmwf, jma, metoffice, bom])


@graph
def climate_fusion_graph() -> str:
    """Orchestrates the climate data fusion pipeline."""
    noaa_raw = download_noaa()
    ecmwf_raw = download_ecmwf()
    jma_raw = download_jma()
    metoffice_raw = download_metoffice()
    bom_raw = download_bom()

    noaa_norm = normalize_noaa(noaa_raw)
    ecmwf_norm = normalize_ecmwf(ecmwf_raw)
    jma_norm = normalize_jma(jma_raw)
    metoffice_norm = normalize_metoffice(metoffice_raw)
    bom_norm = normalize_bom(bom_raw)

    merged = merge_climate_data(
        noaa=noaa_norm,
        ecmwf=ecmwf_norm,
        jma=jma_norm,
        metoffice=metoffice_norm,
        bom=bom_norm,
    )
    return merged


climate_fusion_job = climate_fusion_graph.to_job(name="climate_fusion_job")


@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight UTC
    job=climate_fusion_job,
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily execution of the climate data fusion pipeline.",
)
def daily_climate_fusion_schedule():
    """Schedule that triggers the climate fusion job daily."""
    return {}


defs = Definitions(
    jobs=[climate_fusion_job],
    schedules=[daily_climate_fusion_schedule],
)


if __name__ == "__main__":
    result = climate_fusion_job.execute_in_process()
    print(f"Pipeline succeeded: {result.success}")