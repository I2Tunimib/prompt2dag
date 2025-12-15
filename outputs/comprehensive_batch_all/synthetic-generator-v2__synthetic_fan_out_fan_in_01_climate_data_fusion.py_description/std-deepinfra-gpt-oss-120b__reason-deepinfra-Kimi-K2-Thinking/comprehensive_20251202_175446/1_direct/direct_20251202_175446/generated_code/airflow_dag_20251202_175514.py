from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def download_noaa(**context):
    """Simulate downloading NOAA weather station CSV data."""
    file_path = "/tmp/noaa_raw.csv"
    with open(file_path, "w") as f:
        f.write("station_id,temp_f,alt_ft\n")
    context["ti"].xcom_push(key="file_path", value=file_path)


def download_ecmwf(**context):
    """Simulate downloading ECMWF weather station CSV data."""
    file_path = "/tmp/ecmwf_raw.csv"
    with open(file_path, "w") as f:
        f.write("station_id,temp_f,alt_ft\n")
    context["ti"].xcom_push(key="file_path", value=file_path)


def download_jma(**context):
    """Simulate downloading JMA weather station CSV data."""
    file_path = "/tmp/jma_raw.csv"
    with open(file_path, "w") as f:
        f.write("station_id,temp_f,alt_ft\n")
    context["ti"].xcom_push(key="file_path", value=file_path)


def download_metoffice(**context):
    """Simulate downloading MetOffice weather station CSV data."""
    file_path = "/tmp/metoffice_raw.csv"
    with open(file_path, "w") as f:
        f.write("station_id,temp_f,alt_ft\n")
    context["ti"].xcom_push(key="file_path", value=file_path)


def download_bom(**context):
    """Simulate downloading BOM weather station CSV data."""
    file_path = "/tmp/bom_raw.csv"
    with open(file_path, "w") as f:
        f.write("station_id,temp_f,alt_ft\n")
    context["ti"].xcom_push(key="file_path", value=file_path)


def normalize_noaa(**context):
    """Convert NOAA data to standard format."""
    raw_path = context["ti"].xcom_pull(key="file_path", task_ids="download_noaa")
    norm_path = "/tmp/noaa_normalized.parquet"
    with open(norm_path, "w") as f:
        f.write("station_id,timestamp,temp_c,alt_m\n")
    context["ti"].xcom_push(key="norm_path", value=norm_path)


def normalize_ecmwf(**context):
    """Convert ECMWF data to standard format."""
    raw_path = context["ti"].xcom_pull(key="file_path", task_ids="download_ecmwf")
    norm_path = "/tmp/ecmwf_normalized.parquet"
    with open(norm_path, "w") as f:
        f.write("station_id,timestamp,temp_c,alt_m\n")
    context["ti"].xcom_push(key="norm_path", value=norm_path)


def normalize_jma(**context):
    """Convert JMA data to standard format."""
    raw_path = context["ti"].xcom_pull(key="file_path", task_ids="download_jma")
    norm_path = "/tmp/jma_normalized.parquet"
    with open(norm_path, "w") as f:
        f.write("station_id,timestamp,temp_c,alt_m\n")
    context["ti"].xcom_push(key="norm_path", value=norm_path)


def normalize_metoffice(**context):
    """Convert MetOffice data to standard format."""
    raw_path = context["ti"].xcom_pull(key="file_path", task_ids="download_metoffice")
    norm_path = "/tmp/metoffice_normalized.parquet"
    with open(norm_path, "w") as f:
        f.write("station_id,timestamp,temp_c,alt_m\n")
    context["ti"].xcom_push(key="norm_path", value=norm_path)


def normalize_bom(**context):
    """Convert BOM data to standard format."""
    raw_path = context["ti"].xcom_pull(key="file_path", task_ids="download_bom")
    norm_path = "/tmp/bom_normalized.parquet"
    with open(norm_path, "w") as f:
        f.write("station_id,timestamp,temp_c,alt_m\n")
    context["ti"].xcom_push(key="norm_path", value=norm_path)


def merge_climate_data(**context):
    """Combine all normalized datasets into a unified climate dataset."""
    norm_tasks = [
        "normalize_noaa",
        "normalize_ecmwf",
        "normalize_jma",
        "normalize_metoffice",
        "normalize_bom",
    ]
    normalized_paths = [
        context["ti"].xcom_pull(key="norm_path", task_ids=task) for task in norm_tasks
    ]
    merged_path = "/tmp/climate_merged.parquet"
    with open(merged_path, "w") as f:
        f.write("merged climate data\n")
    print(f"Merged dataset written to {merged_path}")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["alerts@example.com"],
}

with DAG(
    dag_id="climate_data_fusion",
    default_args=default_args,
    description="Daily pipeline to download, normalize, and merge climate data from multiple agencies.",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
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

    # Normalization tasks (fan‑out after each download)
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

    # Define dependencies
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

    # Alternatively, explicit fan‑in dependencies
    normalize_noaa_task >> merge_task
    normalize_ecmwf_task >> merge_task
    normalize_jma_task >> merge_task
    normalize_metoffice_task >> merge_task
    normalize_bom_task >> merge_task