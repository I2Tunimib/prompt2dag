"""Climate data fusion DAG.

Downloads weather station data from multiple agencies, normalizes each dataset,
and merges them into a unified climate dataset.
"""

from datetime import datetime, timedelta
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments applied to all tasks
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}


def _write_dummy_file(path: str, content: str = "dummy"):
    """Utility to create a dummy file for demonstration purposes."""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(content)


def download_noaa(**kwargs):
    """Fetch NOAA weather station CSV data (simulated)."""
    file_path = "/tmp/climate/noaa_raw.csv"
    _write_dummy_file(file_path, "noaa raw data")
    kwargs["ti"].xcom_push(key="file_path", value=file_path)


def download_ecmwf(**kwargs):
    """Fetch ECMWF weather station CSV data (simulated)."""
    file_path = "/tmp/climate/ecmwf_raw.csv"
    _write_dummy_file(file_path, "ecmwf raw data")
    kwargs["ti"].xcom_push(key="file_path", value=file_path)


def download_jma(**kwargs):
    """Fetch JMA weather station CSV data (simulated)."""
    file_path = "/tmp/climate/jma_raw.csv"
    _write_dummy_file(file_path, "jma raw data")
    kwargs["ti"].xcom_push(key="file_path", value=file_path)


def download_metoffice(**kwargs):
    """Fetch MetOffice weather station CSV data (simulated)."""
    file_path = "/tmp/climate/metoffice_raw.csv"
    _write_dummy_file(file_path, "metoffice raw data")
    kwargs["ti"].xcom_push(key="file_path", value=file_path)


def download_bom(**kwargs):
    """Fetch BOM weather station CSV data (simulated)."""
    file_path = "/tmp/climate/bom_raw.csv"
    _write_dummy_file(file_path, "bom raw data")
    kwargs["ti"].xcom_push(key="file_path", value=file_path)


def _normalize(source_task_id: str, target_name: str, **kwargs):
    """Generic normalization logic."""
    ti = kwargs["ti"]
    raw_path = ti.xcom_pull(key="file_path", task_ids=source_task_id)
    normalized_path = f"/tmp/climate/{target_name}_normalized.parquet"
    # Simulate normalization by creating a dummy parquet file
    _write_dummy_file(normalized_path, f"{target_name} normalized data from {raw_path}")
    ti.xcom_push(key="file_path", value=normalized_path)


def normalize_noaa(**kwargs):
    """Normalize NOAA dataset."""
    _normalize("download_noaa", "noaa", **kwargs)


def normalize_ecmwf(**kwargs):
    """Normalize ECMWF dataset."""
    _normalize("download_ecmwf", "ecmwf", **kwargs)


def normalize_jma(**kwargs):
    """Normalize JMA dataset."""
    _normalize("download_jma", "jma", **kwargs)


def normalize_metoffice(**kwargs):
    """Normalize MetOffice dataset."""
    _normalize("download_metoffice", "metoffice", **kwargs)


def normalize_bom(**kwargs):
    """Normalize BOM dataset."""
    _normalize("download_bom", "bom", **kwargs)


def merge_climate_data(**kwargs):
    """Merge all normalized datasets into a unified Parquet file."""
    ti = kwargs["ti"]
    normalized_tasks = [
        "normalize_noaa",
        "normalize_ecmwf",
        "normalize_jma",
        "normalize_metoffice",
        "normalize_bom",
    ]
    normalized_paths = [
        ti.xcom_pull(key="file_path", task_ids=task_id) for task_id in normalized_tasks
    ]
    merged_path = "/tmp/climate/unified_climate_dataset.parquet"
    # Simulate merging by concatenating file references
    content = "\n".join(f"Merged from {p}" for p in normalized_paths if p)
    _write_dummy_file(merged_path, content)
    ti.xcom_push(key="merged_path", value=merged_path)


with DAG(
    dag_id="climate_data_fusion",
    default_args=default_args,
    description="Download, normalize, and merge climate data from multiple agencies",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["climate", "data-fusion"],
) as dag:
    # Download tasks (fan‑out)
    download_tasks = {
        "download_noaa": PythonOperator(task_id="download_noaa", python_callable=download_noaa),
        "download_ecmwf": PythonOperator(task_id="download_ecmwf", python_callable=download_ecmwf),
        "download_jma": PythonOperator(task_id="download_jma", python_callable=download_jma),
        "download_metoffice": PythonOperator(task_id="download_metoffice", python_callable=download_metoffice),
        "download_bom": PythonOperator(task_id="download_bom", python_callable=download_bom),
    }

    # Normalization tasks (fan‑out after each download)
    normalize_tasks = {
        "normalize_noaa": PythonOperator(task_id="normalize_noaa", python_callable=normalize_noaa),
        "normalize_ecmwf": PythonOperator(task_id="normalize_ecmwf", python_callable=normalize_ecmwf),
        "normalize_jma": PythonOperator(task_id="normalize_jma", python_callable=normalize_jma),
        "normalize_metoffice": PythonOperator(task_id="normalize_metoffice", python_callable=normalize_metoffice),
        "normalize_bom": PythonOperator(task_id="normalize_bom", python_callable=normalize_bom),
    }

    # Set dependencies: each download >> its normalization
    download_tasks["download_noaa"] >> normalize_tasks["normalize_noaa"]
    download_tasks["download_ecmwf"] >> normalize_tasks["normalize_ecmwf"]
    download_tasks["download_jma"] >> normalize_tasks["normalize_jma"]
    download_tasks["download_metoffice"] >> normalize_tasks["normalize_metoffice"]
    download_tasks["download_bom"] >> normalize_tasks["normalize_bom"]

    # Merge task (fan‑in)
    merge_task = PythonOperator(
        task_id="merge_climate_data", python_callable=merge_climate_data
    )

    # All normalization tasks must complete before merging
    (
        normalize_tasks["normalize_noaa"]
        >> normalize_tasks["normalize_ecmwf"]
        >> normalize_tasks["normalize_jma"]
        >> normalize_tasks["normalize_metoffice"]
        >> normalize_tasks["normalize_bom"]
        >> merge_task
    )

    # Alternative explicit fan‑in using list of upstreams
    (
        [
            normalize_tasks["normalize_noaa"],
            normalize_tasks["normalize_ecmwf"],
            normalize_tasks["normalize_jma"],
            normalize_tasks["normalize_metoffice"],
            normalize_tasks["normalize_bom"],
        ]
        >> merge_task
    )