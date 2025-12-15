from pathlib import Path
import tempfile
import pandas as pd
from dagster import (
    op,
    job,
    In,
    Out,
    RetryPolicy,
    ScheduleDefinition,
    DefaultScheduleStatus,
    Definitions,
    get_dagster_logger,
)


def _write_dummy_csv(file_path: Path, source_name: str) -> None:
    """Create a small dummy CSV file to simulate downloaded data."""
    df = pd.DataFrame(
        {
            "timestamp": ["2024-01-01 00:00", "2024-01-01 01:00"],
            "temperature_f": [68, 70],
            "elevation_ft": [1000, 1010],
        }
    )
    df.to_csv(file_path, index=False)
    get_dagster_logger().info(f"Dummy data for {source_name} written to {file_path}")


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert to ISO timestamp, Celsius, meters."""
    df = df.copy()
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%S")
    df["temperature_c"] = (df["temperature_f"] - 32) * 5.0 / 9.0
    df["elevation_m"] = df["elevation_ft"] * 0.3048
    # Drop original columns
    df = df.drop(columns=["temperature_f", "elevation_ft"])
    return df


def _write_parquet(df: pd.DataFrame, file_path: Path) -> None:
    df.to_parquet(file_path, index=False)
    get_dagster_logger().info(f"Parquet file written to {file_path}")


# ----------------------------------------------------------------------
# Download ops (fan‑out)
# ----------------------------------------------------------------------


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch NOAA weather station CSV data.",
)
def download_noaa() -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "noaa_raw.csv"
        _write_dummy_csv(path, "NOAA")
        return str(path)


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch ECMWF weather station CSV data.",
)
def download_ecmwf() -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "ecmwf_raw.csv"
        _write_dummy_csv(path, "ECMWF")
        return str(path)


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch JMA weather station CSV data.",
)
def download_jma() -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "jma_raw.csv"
        _write_dummy_csv(path, "JMA")
        return str(path)


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch MetOffice weather station CSV data.",
)
def download_metoffice() -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "metoffice_raw.csv"
        _write_dummy_csv(path, "MetOffice")
        return str(path)


@op(
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Fetch BOM weather station CSV data.",
)
def download_bom() -> str:
    with tempfile.TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "bom_raw.csv"
        _write_dummy_csv(path, "BOM")
        return str(path)


# ----------------------------------------------------------------------
# Normalization ops (fan‑out)
# ----------------------------------------------------------------------


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize NOAA data to the standard format.",
)
def normalize_noaa(raw_path: str) -> str:
    df = pd.read_csv(raw_path)
    norm_df = _normalize_dataframe(df)
    out_path = Path(raw_path).with_name("noaa_normalized.parquet")
    _write_parquet(norm_df, out_path)
    return str(out_path)


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize ECMWF data to the standard format.",
)
def normalize_ecmwf(raw_path: str) -> str:
    df = pd.read_csv(raw_path)
    norm_df = _normalize_dataframe(df)
    out_path = Path(raw_path).with_name("ecmwf_normalized.parquet")
    _write_parquet(norm_df, out_path)
    return str(out_path)


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize JMA data to the standard format.",
)
def normalize_jma(raw_path: str) -> str:
    df = pd.read_csv(raw_path)
    norm_df = _normalize_dataframe(df)
    out_path = Path(raw_path).with_name("jma_normalized.parquet")
    _write_parquet(norm_df, out_path)
    return str(out_path)


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize MetOffice data to the standard format.",
)
def normalize_metoffice(raw_path: str) -> str:
    df = pd.read_csv(raw_path)
    norm_df = _normalize_dataframe(df)
    out_path = Path(raw_path).with_name("metoffice_normalized.parquet")
    _write_parquet(norm_df, out_path)
    return str(out_path)


@op(
    ins={"raw_path": In(str)},
    out=Out(str),
    retry_policy=RetryPolicy(max_retries=3, delay=300),
    description="Normalize BOM data to the standard format.",
)
def normalize_bom(raw_path: str) -> str:
    df = pd.read_csv(raw_path)
    norm_df = _normalize_dataframe(df)
    out_path = Path(raw_path).with_name("bom_normalized.parquet")
    _write_parquet(norm_df, out_path)
    return str(out_path)


# ----------------------------------------------------------------------
# Merge op (fan‑in)
# ----------------------------------------------------------------------


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
    dfs = []
    for path in (noaa, ecmwf, jma, metoffice, bom):
        dfs.append(pd.read_parquet(path))
    merged_df = pd.concat(dfs, ignore_index=True)
    output_path = Path(tempfile.gettempdir()) / "unified_climate_dataset.parquet"
    _write_parquet(merged_df, output_path)
    return str(output_path)


# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------


@job
def climate_fusion_job():
    # Fan‑out: downloads
    noaa_raw = download_noaa()
    ecmwf_raw = download_ecmwf()
    jma_raw = download_jma()
    metoffice_raw = download_metoffice()
    bom_raw = download_bom()

    # Fan‑out: normalizations (each depends on its download)
    noaa_norm = normalize_noaa(noaa_raw)
    ecmwf_norm = normalize_ecmwf(ecmwf_raw)
    jma_norm = normalize_jma(jma_raw)
    metoffice_norm = normalize_metoffice(metoffice_raw)
    bom_norm = normalize_bom(bom_raw)

    # Fan‑in: merge
    merge_climate_data(
        noaa=noaa_norm,
        ecmwf=ecmwf_norm,
        jma=jma_norm,
        metoffice=metoffice_norm,
        bom=bom_norm,
    )


# ----------------------------------------------------------------------
# Schedule (daily execution, no catch‑up)
# ----------------------------------------------------------------------


daily_schedule = ScheduleDefinition(
    job=climate_fusion_job,
    cron_schedule="0 0 * * *",  # Every day at 00:00 UTC
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING,
    description="Daily execution of the climate data fusion pipeline.",
)


# ----------------------------------------------------------------------
# Definitions (optional, useful for Dagster UI)
# ----------------------------------------------------------------------


defs = Definitions(
    jobs=[climate_fusion_job],
    schedules=[daily_schedule],
)


# ----------------------------------------------------------------------
# Simple entry point for local testing
# ----------------------------------------------------------------------


if __name__ == "__main__":
    result = climate_fusion_job.execute_in_process()
    if result.success:
        print("Pipeline completed successfully.")
    else:
        print("Pipeline failed.")