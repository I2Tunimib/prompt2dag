"""Climate data fusion DAG.

Downloads weather station data from multiple agencies, normalizes each dataset,
and merges them into a unified Parquet file. Execution is scheduled daily
without catch‑up. Failures trigger email notifications.
"""

from __future__ import annotations

import os
import datetime as dt
from datetime import timedelta
from pathlib import Path
from typing import List

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# ----------------------------------------------------------------------
# Default arguments for the DAG
# ----------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
DATA_DIR = Path("/tmp/climate_data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def _download_file(url: str, dest_path: Path) -> None:
    """Download a file from a URL to a local destination."""
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    dest_path.write_bytes(response.content)


def download_noaa(**kwargs):
    """Fetch NOAA weather station CSV data."""
    dest = DATA_DIR / "noaa_raw.csv"
    _download_file("ftp://example.com/noaa/stations.csv", dest)
    kwargs["ti"].xcom_push(key="noaa_path", value=str(dest))


def download_ecmwf(**kwargs):
    """Fetch ECMWF weather station CSV data."""
    dest = DATA_DIR / "ecmwf_raw.csv"
    _download_file("https://example.com/ecmwf/stations.csv", dest)
    kwargs["ti"].xcom_push(key="ecmwf_path", value=str(dest))


def download_jma(**kwargs):
    """Fetch JMA weather station CSV data."""
    dest = DATA_DIR / "jma_raw.csv"
    _download_file("https://example.com/jma/stations.csv", dest)
    kwargs["ti"].xcom_push(key="jma_path", value=str(dest))


def download_metoffice(**kwargs):
    """Fetch MetOffice weather station CSV data."""
    dest = DATA_DIR / "metoffice_raw.csv"
    _download_file("https://example.com/metoffice/stations.csv", dest)
    kwargs["ti"].xcom_push(key="metoffice_path", value=str(dest))


def download_bom(**kwargs):
    """Fetch BOM weather station CSV data."""
    dest = DATA_DIR / "bom_raw.csv"
    _download_file("https://example.com/bom/stations.csv", dest)
    kwargs["ti"].xcom_push(key="bom_path", value=str(dest))


def _normalize(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize columns: ISO timestamp, Celsius temperature, meters."""
    # Placeholder normalization logic
    df = df.copy()
    if "timestamp" in df.columns:
        df["timestamp"] = pd.to_datetime(df["timestamp"]).dt.isoformat()
    if "temp_f" in df.columns:
        df["temperature_c"] = (df["temp_f"] - 32) * 5.0 / 9.0
        df.drop(columns=["temp_f"], inplace=True)
    if "elevation_ft" in df.columns:
        df["elevation_m"] = df["elevation_ft"] * 0.3048
        df.drop(columns=["elevation_ft"], inplace=True)
    return df


def normalize_noaa(**kwargs):
    """Normalize NOAA dataset."""
    ti = kwargs["ti"]
    raw_path = Path(ti.xcom_pull(key="noaa_path", task_ids="download_noaa"))
    df = pd.read_csv(raw_path)
    norm_df = _normalize(df)
    out_path = DATA_DIR / "noaa_normalized.parquet"
    norm_df.to_parquet(out_path, index=False)
    ti.xcom_push(key="noaa_norm_path", value=str(out_path))


def normalize_ecmwf(**kwargs):
    """Normalize ECMWF dataset."""
    ti = kwargs["ti"]
    raw_path = Path(ti.xcom_pull(key="ecmwf_path", task_ids="download_ecmwf"))
    df = pd.read_csv(raw_path)
    norm_df = _normalize(df)
    out_path = DATA_DIR / "ecmwf_normalized.parquet"
    norm_df.to_parquet(out_path, index=False)
    ti.xcom_push(key="ecmwf_norm_path", value=str(out_path))


def normalize_jma(**kwargs):
    """Normalize JMA dataset."""
    ti = kwargs["ti"]
    raw_path = Path(ti.xcom_pull(key="jma_path", task_ids="download_jma"))
    df = pd.read_csv(raw_path)
    norm_df = _normalize(df)
    out_path = DATA_DIR / "jma_normalized.parquet"
    norm_df.to_parquet(out_path, index=False)
    ti.xcom_push(key="jma_norm_path", value=str(out_path))


def normalize_metoffice(**kwargs):
    """Normalize MetOffice dataset."""
    ti = kwargs["ti"]
    raw_path = Path(ti.xcom_pull(key="metoffice_path", task_ids="download_metoffice"))
    df = pd.read_csv(raw_path)
    norm_df = _normalize(df)
    out_path = DATA_DIR / "metoffice_normalized.parquet"
    norm_df.to_parquet(out_path, index=False)
    ti.xcom_push(key="metoffice_norm_path", value=str(out_path))


def normalize_bom(**kwargs):
    """Normalize BOM dataset."""
    ti = kwargs["ti"]
    raw_path = Path(ti.xcom_pull(key="bom_path", task_ids="download_bom"))
    df = pd.read_csv(raw_path)
    norm_df = _normalize(df)
    out_path = DATA_DIR / "bom_normalized.parquet"
    norm_df.to_parquet(out_path, index=False)
    ti.xcom_push(key="bom_norm_path", value=str(out_path))


def merge_climate_data(**kwargs):
    """Merge all normalized datasets into a unified Parquet file."""
    ti = kwargs["ti"]
    paths: List[Path] = [
        Path(ti.xcom_pull(key="noaa_norm_path", task_ids="normalize_noaa")),
        Path(ti.xcom_pull(key="ecmwf_norm_path", task_ids="normalize_ecmwf")),
        Path(ti.xcom_pull(key="jma_norm_path", task_ids="normalize_jma")),
        Path(ti.xcom_pull(key="metoffice_norm_path", task_ids="normalize_metoffice")),
        Path(ti.xcom_pull(key="bom_norm_path", task_ids="normalize_bom")),
    ]

    dfs = [pd.read_parquet(p) for p in paths if p.exists()]
    merged_df = pd.concat(dfs, ignore_index=True)
    out_path = DATA_DIR / "climate_unified.parquet"
    merged_df.to_parquet(out_path, index=False)
    ti.xcom_push(key="merged_path", value=str(out_path))


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="climate_data_fusion",
    default_args=DEFAULT_ARGS,
    description="Download, normalize, and merge climate data from multiple agencies",
    schedule_interval="@daily",
    start_date=dt.datetime(2024, 1, 1),
    catchup=False,
    tags=["climate", "data-fusion"],
) as dag:
    # Download tasks (fan‑out)
    download_noaa_task = PythonOperator(
        task_id="download_noaa", python_callable=download_noaa
    )
    download_ecmwf_task = PythonOperator(
        task_id="download_ecmwf", python_callable=download_ecmwf
    )
    download_jma_task = PythonOperator(
        task_id="download_jma", python_callable=download_jma
    )
    download_metoffice_task = PythonOperator(
        task_id="download_metoffice", python_callable=download_metoffice
    )
    download_bom_task = PythonOperator(
        task_id="download_bom", python_callable=download_bom
    )

    # Normalization tasks (fan‑out)
    normalize_noaa_task = PythonOperator(
        task_id="normalize_noaa", python_callable=normalize_noaa
    )
    normalize_ecmwf_task = PythonOperator(
        task_id="normalize_ecmwf", python_callable=normalize_ecmwf
    )
    normalize_jma_task = PythonOperator(
        task_id="normalize_jma", python_callable=normalize_jma
    )
    normalize_metoffice_task = PythonOperator(
        task_id="normalize_metoffice", python_callable=normalize_metoffice
    )
    normalize_bom_task = PythonOperator(
        task_id="normalize_bom", python_callable=normalize_bom
    )

    # Merge task (fan‑in)
    merge_task = PythonOperator(
        task_id="merge_climate_data", python_callable=merge_climate_data
    )

    # ------------------------------------------------------------------
    # Set dependencies
    # ------------------------------------------------------------------
    download_noaa_task >> normalize_noaa_task
    download_ecmwf_task >> normalize_ecmwf_task
    download_jma_task >> normalize_jma_task
    download_metoffice_task >> normalize_metoffice_task
    download_bom_task >> normalize_bom_task

    (
        normalize_noaa_task
        >> normalize_ecmwf_task
        >> normalize_jma_task
        >> normalize_metoffice_task
        >> normalize_bom_task
        >> merge_task
    )
    # The above chain ensures merge runs after all normalizations.
    # Explicit parallel dependencies are also expressed:
    normalize_noaa_task >> merge_task
    normalize_ecmwf_task >> merge_task
    normalize_jma_task >> merge_task
    normalize_metoffice_task >> merge_task
    normalize_bom_task >> merge_task