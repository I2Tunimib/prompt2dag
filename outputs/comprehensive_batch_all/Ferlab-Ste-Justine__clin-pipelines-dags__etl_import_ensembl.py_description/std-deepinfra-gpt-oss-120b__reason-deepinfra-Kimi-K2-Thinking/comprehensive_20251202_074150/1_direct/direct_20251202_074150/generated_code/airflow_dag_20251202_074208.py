import os
import hashlib
import logging
import ftplib
from datetime import datetime, timedelta

import boto3
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ----------------------------------------------------------------------
# Configuration (could also be loaded from external config files)
# ----------------------------------------------------------------------
FTP_HOST = "ftp.ensembl.org"
FTP_BASE_DIR = "/pub/current_release/tsv/homo_sapiens"
DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]
S3_BUCKET = os.getenv("S3_BUCKET", "my-data-lake")
S3_LANDING_PREFIX = "raw/landing/ensembl/"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# ----------------------------------------------------------------------
# Helper functions
# ----------------------------------------------------------------------
def send_slack_message(text: str) -> None:
    """Send a simple message to Slack via webhook."""
    if not SLACK_WEBHOOK_URL:
        logging.warning("SLACK_WEBHOOK_URL not set; skipping Slack notification.")
        return
    payload = {"text": text}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover
        logging.error("Failed to send Slack notification: %s", exc)


def on_success(context):
    """Callback executed on successful DAG run."""
    dag_id = context.get("dag").dag_id
    run_id = context.get("run_id")
    send_slack_message(f":white_check_mark: DAG *{dag_id}* succeeded (run `{run_id}`).")


def on_failure(context):
    """Callback executed on failed DAG run."""
    dag_id = context.get("dag").dag_id
    run_id = context.get("run_id")
    task_id = context.get("task_instance").task_id
    send_slack_message(
        f":x: Task *{task_id}* in DAG *{dag_id}* failed (run `{run_id}`)."
    )


def compute_md5(file_path: str) -> str:
    """Calculate MD5 checksum of a local file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def list_ftp_files(ftp: ftplib.FTP, data_type: str) -> list:
    """Return a list of file names for a given data type on the FTP server."""
    pattern = f"{data_type}.*.tsv.gz"
    files = []
    try:
        ftp.cwd(FTP_BASE_DIR)
        for entry in ftp.nlst():
            if entry.startswith(data_type) and entry.endswith(".tsv.gz"):
                files.append(entry)
    except ftplib.error_perm as exc:  # pragma: no cover
        logging.error("FTP permission error while listing %s: %s", data_type, exc)
    return files


def get_latest_ftp_file(ftp: ftplib.FTP, data_type: str) -> str | None:
    """Determine the latest file for a data type based on filename sorting."""
    files = list_ftp_files(ftp, data_type)
    if not files:
        return None
    # Assuming filenames contain version info that sorts correctly
    return sorted(files)[-1]


def get_s3_latest_key(s3_client, data_type: str) -> str | None:
    """Retrieve the most recent S3 object key for a data type."""
    prefix = f"{S3_LANDING_PREFIX}{data_type}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    latest_key = None
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".tsv.gz"):
                if not latest_key or key > latest_key:
                    latest_key = key
    return latest_key


def download_ftp_file(ftp: ftplib.FTP, remote_path: str, local_path: str) -> None:
    """Download a file from FTP to a local path."""
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)


def upload_to_s3(s3_client, local_path: str, s3_key: str) -> None:
    """Upload a local file to S3."""
    s3_client.upload_file(local_path, S3_BUCKET, s3_key)


def check_and_sync_files(**context):
    """
    Checks for newer Ensembl files on the FTP server.
    Downloads, validates MD5, and uploads to S3 if newer.
    Skips downstream tasks when no updates are found.
    """
    ftp = ftplib.FTP(FTP_HOST)
    ftp.login()
    s3 = boto3.client("s3")
    updates_found = False

    for data_type in DATA_TYPES:
        logging.info("Processing data type: %s", data_type)
        latest_ftp_file = get_latest_ftp_file(ftp, data_type)
        if not latest_ftp_file:
            logging.warning("No FTP files found for %s; skipping.", data_type)
            continue

        s3_latest_key = get_s3_latest_key(s3, data_type)
        ftp_version = latest_ftp_file
        s3_version = os.path.basename(s3_latest_key) if s3_latest_key else None

        if ftp_version == s3_version:
            logging.info("No new version for %s (latest: %s).", data_type, ftp_version)
            continue

        logging.info(
            "New version detected for %s: FTP %s vs S3 %s",
            data_type,
            ftp_version,
            s3_version or "none",
        )
        remote_path = f"{FTP_BASE_DIR}/{latest_ftp_file}"
        local_path = f"/tmp/{latest_ftp_file}"
        download_ftp_file(ftp, remote_path, local_path)

        md5_checksum = compute_md5(local_path)
        logging.info("Computed MD5 for %s: %s", latest_ftp_file, md5_checksum)

        s3_key = f"{S3_LANDING_PREFIX}{data_type}/{latest_ftp_file}"
        upload_to_s3(s3, local_path, s3_key)
        logging.info("Uploaded %s to s3://%s/%s", local_path, S3_BUCKET, s3_key)

        updates_found = True
        os.remove(local_path)

    ftp.quit()

    if not updates_found:
        logging.info("No updates found for any data type; skipping downstream tasks.")
        raise AirflowSkipException("No new Ensembl data available.")


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="ensembl_etl_pipeline",
    default_args=default_args,
    description="ETL pipeline for Ensembl mapping data",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "ensembl", "spark"],
) as dag:
    file_task = PythonOperator(
        task_id="file",
        python_callable=check_and_sync_files,
        provide_context=True,
    )

    table_task = SparkSubmitOperator(
        task_id="table",
        application="/opt/spark/jobs/ensembl_mapping.py",
        name="ensembl_mapping_job",
        conn_id="spark_default",
        conf={
            "spark.master": "k8s://https://kubernetes.default.svc",
            "spark.kubernetes.namespace": "spark-jobs",
            "spark.kubernetes.container.image": "myrepo/spark-ensembl:latest",
        },
        application_args=[
            f"s3://{S3_BUCKET}/{S3_LANDING_PREFIX}",
            f"s3://{S3_BUCKET}/processed/ensembl_mapping/",
        ],
        packages="org.apache.hadoop:hadoop-aws:3.3.2",
        driver_memory="2g",
        executor_memory="4g",
        num_executors=2,
    )

    file_task >> table_task