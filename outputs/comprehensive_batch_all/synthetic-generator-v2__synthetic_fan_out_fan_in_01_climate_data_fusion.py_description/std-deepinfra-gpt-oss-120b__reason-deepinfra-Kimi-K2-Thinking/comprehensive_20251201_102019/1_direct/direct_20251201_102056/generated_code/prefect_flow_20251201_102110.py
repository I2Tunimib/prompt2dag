from pathlib import Path
import random
import string
import pandas as pd
from datetime import datetime, timedelta

from prefect import flow, task


DATA_DIR = Path("data")
RAW_DIR = DATA_DIR / "raw"
NORMALIZED_DIR = DATA_DIR / "normalized"
OUTPUT_DIR = DATA_DIR / "output"


def _ensure_directories() -> None:
    """Create required directories if they do not exist."""
    for directory in (RAW_DIR, NORMALIZED_DIR, OUTPUT_DIR):
        directory.mkdir(parents=True, exist_ok=True)


def _random_string(length: int = 8) -> str:
    """Generate a random alphanumeric string."""
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=length))


def _generate_dummy_csv(path: Path) -> None:
    """Create a small dummy CSV file with weather station data."""
    records = []
    base_time = datetime.utcnow() - timedelta(days=1)
    for i in range(5):
        record = {
            "station_id": f"{_random_string(4)}_{i}",
            "timestamp": (base_time + timedelta(hours=i)).strftime("%Y-%m-%d %H:%M:%S"),
            "temperature_f": round(random.uniform(30, 100), 2),
            "elevation_ft": round(random.uniform(0, 5000), 2),
        }
        records.append(record)
    df = pd.DataFrame(records)
    df.to_csv(path, index=False)


@task(retries=3, retry_delay_seconds=300)
def download_noaa() -> Path:
    """Fetch NOAA weather station CSV data (simulated)."""
    _ensure_directories()
    path = RAW_DIR / "noaa_raw.csv"
    _generate_dummy_csv(path)
    return path


@task(retries=3, retry_delay_seconds=300)
def download_ecmwf() -> Path:
    """Fetch ECMWF weather station CSV data (simulated)."""
    _ensure_directories()
    path = RAW_DIR / "ecmwf_raw.csv"
    _generate_dummy_csv(path)
    return path


@task(retries=3, retry_delay_seconds=300)
def download_jma() -> Path:
    """Fetch JMA weather station CSV data (simulated)."""
    _ensure_directories()
    path = RAW_DIR / "jma_raw.csv"
    _generate_dummy_csv(path)
    return path


@task(retries=3, retry_delay_seconds=300)
def download_metoffice() -> Path:
    """Fetch MetOffice weather station CSV data (simulated)."""
    _ensure_directories()
    path = RAW_DIR / "metoffice_raw.csv"
    _generate_dummy_csv(path)
    return path


@task(retries=3, retry_delay_seconds=300)
def download_bom() -> Path:
    """Fetch BOM weather station CSV data (simulated)."""
    _ensure_directories()
    path = RAW_DIR / "bom_raw.csv"
    _generate_dummy_csv(path)
    return path


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Convert raw columns to the standard schema."""
    # Convert timestamp to ISO 8601 format
    df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    # Convert Fahrenheit to Celsius
    df["temperature_c"] = (df["temperature_f"] - 32) * 5.0 / 9.0
    # Convert feet to meters
    df["elevation_m"] = df["elevation_ft"] * 0.3048
    # Keep only standardized columns
    return df[["station_id", "timestamp", "temperature_c", "elevation_m"]]


@task(retries=3, retry_delay_seconds=300)
def normalize_noaa(raw_path: Path) -> Path:
    """Normalize NOAA data to the standard format."""
    df = pd.read_csv(raw_path)
    normalized_df = _normalize_dataframe(df)
    out_path = NORMALIZED_DIR / "noaa_normalized.csv"
    normalized_df.to_csv(out_path, index=False)
    return out_path


@task(retries=3, retry_delay_seconds=300)
def normalize_ecmwf(raw_path: Path) -> Path:
    """Normalize ECMWF data to the standard format."""
    df = pd.read_csv(raw_path)
    normalized_df = _normalize_dataframe(df)
    out_path = NORMALIZED_DIR / "ecmwf_normalized.csv"
    normalized_df.to_csv(out_path, index=False)
    return out_path


@task(retries=3, retry_delay_seconds=300)
def normalize_jma(raw_path: Path) -> Path:
    """Normalize JMA data to the standard format."""
    df = pd.read_csv(raw_path)
    normalized_df = _normalize_dataframe(df)
    out_path = NORMALIZED_DIR / "jma_normalized.csv"
    normalized_df.to_csv(out_path, index=False)
    return out_path


@task(retries=3, retry_delay_seconds=300)
def normalize_metoffice(raw_path: Path) -> Path:
    """Normalize MetOffice data to the standard format."""
    df = pd.read_csv(raw_path)
    normalized_df = _normalize_dataframe(df)
    out_path = NORMALIZED_DIR / "metoffice_normalized.csv"
    normalized_df.to_csv(out_path, index=False)
    return out_path


@task(retries=3, retry_delay_seconds=300)
def normalize_bom(raw_path: Path) -> Path:
    """Normalize BOM data to the standard format."""
    df = pd.read_csv(raw_path)
    normalized_df = _normalize_dataframe(df)
    out_path = NORMALIZED_DIR / "bom_normalized.csv"
    normalized_df.to_csv(out_path, index=False)
    return out_path


@task(retries=3, retry_delay_seconds=300)
def merge_climate_data(normalized_paths: list[Path]) -> Path:
    """Merge all normalized datasets into a unified Parquet file."""
    data_frames = [pd.read_csv(p) for p in normalized_paths]
    merged_df = pd.concat(data_frames, ignore_index=True)
    out_path = OUTPUT_DIR / "unified_climate_data.parquet"
    merged_df.to_parquet(out_path, index=False)
    return out_path


@flow
def climate_data_fusion_flow() -> Path:
    """
    Orchestrates the climate data fusion pipeline:
    - Parallel download of raw datasets.
    - Parallel normalization of each dataset.
    - Fan‑in merge into a unified Parquet file.
    """
    # Fan‑out: download tasks
    download_futures = {
        "noaa": download_noaa.submit(),
        "ecmwf": download_ecmwf.submit(),
        "jma": download_jma.submit(),
        "metoffice": download_metoffice.submit(),
        "bom": download_bom.submit(),
    }

    # Resolve download results
    raw_paths = {key: future.result() for key, future in download_futures.items()}

    # Fan‑out: normalization tasks (each depends on its download)
    normalize_futures = {
        "noaa": normalize_noaa.submit(raw_paths["noaa"]),
        "ecmwf": normalize_ecmwf.submit(raw_paths["ecmwf"]),
        "jma": normalize_jma.submit(raw_paths["jma"]),
        "metoffice": normalize_metoffice.submit(raw_paths["metoffice"]),
        "bom": normalize_bom.submit(raw_paths["bom"]),
    }

    # Resolve normalization results
    normalized_paths = [future.result() for future in normalize_futures.values()]

    # Fan‑in: merge task
    merged_path = merge_climate_data(normalized_paths)

    return merged_path


if __name__ == "__main__":
    # Local execution; in production, configure a deployment with a daily schedule,
    # no catch‑up, and email notifications on task failures.
    result_path = climate_data_fusion_flow()
    print(f"Unified climate dataset written to: {result_path}")