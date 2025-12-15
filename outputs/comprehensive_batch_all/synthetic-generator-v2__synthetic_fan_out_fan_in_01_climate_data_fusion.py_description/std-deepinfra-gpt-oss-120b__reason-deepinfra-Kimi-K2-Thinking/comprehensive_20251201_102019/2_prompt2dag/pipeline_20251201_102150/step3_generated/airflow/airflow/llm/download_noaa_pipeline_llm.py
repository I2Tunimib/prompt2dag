# -*- coding: utf-8 -*-
"""
Generated Airflow DAG: download_noaa_pipeline
Author: Auto-generated
Date: 2024-06-13
Description: No description provided.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from airflow.utils.dates import days_ago

# ----------------------------------------------------------------------
# Default arguments applied to all tasks
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
with DAG(
    dag_id="download_noaa_pipeline",
    description="No description provided.",
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
    default_args=default_args,
    tags=["climate", "download"],
    max_active_runs=1,
) as dag:

    # ------------------------------------------------------------------
    # Helper functions
    # ------------------------------------------------------------------
    def _write_dummy_file(source_name: str) -> str:
        """
        Simulate a download or normalization step by writing a dummy file.
        Returns the absolute path to the created file.
        """
        try:
            output_dir = Path("/tmp/airflow_climate")
            output_dir.mkdir(parents=True, exist_ok=True)
            file_path = output_dir / f"{source_name}_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.txt"
            file_path.write_text(f"{source_name} data generated at {datetime.utcnow().isoformat()}\n")
            return str(file_path)
        except Exception as exc:
            raise AirflowException(f"Failed to write dummy file for {source_name}: {exc}")

    # ------------------------------------------------------------------
    # Download tasks
    # ------------------------------------------------------------------
    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="download_bom")
    def download_bom() -> str:
        """Download BOM data from the HTTPS endpoint."""
        # Placeholder for real download logic using the `https_bom` connection.
        return _write_dummy_file("bom")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="download_ecmwf")
    def download_ecmwf() -> str:
        """Download ECMWF data from the HTTPS endpoint."""
        # Placeholder for real download logic using the `https_ecmwf` connection.
        return _write_dummy_file("ecmwf")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="download_jma")
    def download_jma() -> str:
        """Download JMA data from the HTTPS endpoint."""
        # Placeholder for real download logic using the `https_jma` connection.
        return _write_dummy_file("jma")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="download_metoffice")
    def download_metoffice() -> str:
        """Download MetOffice data from the HTTPS endpoint."""
        # Placeholder for real download logic using the `https_metoffice` connection.
        return _write_dummy_file("metoffice")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="download_noaa")
    def download_noaa() -> str:
        """Download NOAA data from the FTP server."""
        # Placeholder for real download logic using the `ftp_noaa` connection.
        return _write_dummy_file("noaa")

    # ------------------------------------------------------------------
    # Normalization tasks
    # ------------------------------------------------------------------
    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="normalize_bom")
    def normalize_bom(raw_path: str) -> str:
        """Normalize BOM data."""
        # Placeholder for real normalization logic.
        return _write_dummy_file("bom_normalized")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="normalize_ecmwf")
    def normalize_ecmwf(raw_path: str) -> str:
        """Normalize ECMWF data."""
        # Placeholder for real normalization logic.
        return _write_dummy_file("ecmwf_normalized")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="normalize_jma")
    def normalize_jma(raw_path: str) -> str:
        """Normalize JMA data."""
        # Placeholder for real normalization logic.
        return _write_dummy_file("jma_normalized")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="normalize_metoffice")
    def normalize_metoffice(raw_path: str) -> str:
        """Normalize MetOffice data."""
        # Placeholder for real normalization logic.
        return _write_dummy_file("metoffice_normalized")

    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="normalize_noaa")
    def normalize_noaa(raw_path: str) -> str:
        """Normalize NOAA data."""
        # Placeholder for real normalization logic.
        return _write_dummy_file("noaa_normalized")

    # ------------------------------------------------------------------
    # Merge task
    # ------------------------------------------------------------------
    @task(retries=3, retry_delay=timedelta(minutes=5), task_id="merge_climate_data")
    def merge_climate_data(
        bom_path: str,
        ecmwf_path: str,
        jma_path: str,
        metoffice_path: str,
        noaa_path: str,
    ) -> str:
        """
        Merge all normalized climate datasets into a single file.
        Returns the path to the merged file.
        """
        try:
            merged_dir = Path("/tmp/airflow_climate")
            merged_dir.mkdir(parents=True, exist_ok=True)
            merged_file = merged_dir / f"merged_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.txt"

            with merged_file.open("w") as outfile:
                for src_path in [bom_path, ecmwf_path, jma_path, metoffice_path, noaa_path]:
                    outfile.write(f"--- Content from {src_path} ---\n")
                    outfile.write(Path(src_path).read_text())
                    outfile.write("\n")
            return str(merged_file)
        except Exception as exc:
            raise AirflowException(f"Failed to merge climate data: {exc}")

    # ------------------------------------------------------------------
    # Task orchestration
    # ------------------------------------------------------------------
    # Download stage
    bom_raw = download_bom()
    ecmwf_raw = download_ecmwf()
    jma_raw = download_jma()
    metoffice_raw = download_metoffice()
    noaa_raw = download_noaa()

    # Normalization stage (fan‑in)
    bom_norm = normalize_bom(bom_raw)
    ecmwf_norm = normalize_ecmwf(ecmwf_raw)
    jma_norm = normalize_jma(jma_raw)
    metoffice_norm = normalize_metoffice(metoffice_raw)
    noaa_norm = normalize_noaa(noaa_raw)

    # Merge stage (fan‑in)
    merged_file = merge_climate_data(
        bom_path=bom_norm,
        ecmwf_path=ecmwf_norm,
        jma_path=jma_norm,
        metoffice_path=metoffice_norm,
        noaa_path=noaa_norm,
    )

    # Explicit dependencies (optional, as XCom chaining already defines them)
    (
        bom_raw
        >> bom_norm
        >> merged_file
    )
    (
        ecmwf_raw
        >> ecmwf_norm
        >> merged_file
    )
    (
        jma_raw
        >> jma_norm
        >> merged_file
    )
    (
        metoffice_raw
        >> metoffice_norm
        >> merged_file
    )
    (
        noaa_raw
        >> noaa_norm
        >> merged_file
    )