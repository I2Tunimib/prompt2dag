import os
import json
import logging
import ftplib
import hashlib
import boto3
import requests
from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.trigger_rule import TriggerRule

# ----------------------------------------------------------------------
# Slack notification helpers
# ----------------------------------------------------------------------
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


def _post_to_slack(message: str) -> None:
    """Send a simple text message to Slack via webhook."""
    if not SLACK_WEBHOOK_URL:
        logging.warning("SLACK_WEBHOOK_URL not set; skipping Slack notification.")
        return
    payload = {"text": message}
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL, data=json.dumps(payload), headers={"Content-Type": "application/json"}
        )
        response.raise_for_status()
    except Exception as exc:  # pragma: no cover
        logging.error("Failed to send Slack notification: %s", exc)


def notify_start(context):
    """Callback executed when DAG run starts."""
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    _post_to_slack(f":rocket: DAG *{dag_id}* started (run id: `{run_id}`).")


def notify_success(context):
    """Callback executed when DAG run succeeds."""
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    _post_to_slack(f":white_check_mark: DAG *{dag_id}* succeeded (run id: `{run_id}`).")


def notify_failure(context):
    """Callback executed when DAG run fails."""
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    task_id = context["task_instance"].task_id
    _post_to_slack(
        f":x: DAG *{dag_id}* failed in task *{task_id}* (run id: `{run_id}`)."
    )


# ----------------------------------------------------------------------
# Core ETL logic
# ----------------------------------------------------------------------
FTP_HOST = "ftp.ensembl.org"
FTP_BASE_DIR = "/pub/current_fasta/homo_sapiens"
DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]
S3_BUCKET = os.getenv("S3_BUCKET", "my-data-lake")
S3_LANDING_PREFIX = "raw/landing/ensembl/"

s3_client = boto3.client("s3")


def _list_ftp_files() -> dict:
    """Return a mapping of data_type -> (filename, md5) from the FTP server."""
    result = {}
    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login()
        for data_type in DATA_TYPES:
            # Example path: /pub/current_fasta/homo_sapiens/canonical/...
            dir_path = f"{FTP_BASE_DIR}/{data_type}"
            try:
                ftp.cwd(dir_path)
                files = ftp.nlst()
                # Assume the latest file ends with .tsv and there is a .md5 file
                tsv_files = [f for f in files if f.endswith(".tsv")]
                md5_files = [f for f in files if f.endswith(".md5")]
                if not tsv_files or not md5_files:
                    continue
                latest_tsv = sorted(tsv_files)[-1]
                latest_md5 = sorted(md5_files)[-1]
                result[data_type] = (latest_tsv, latest_md5)
            except ftplib.error_perm:
                logging.warning("Directory %s not found on FTP.", dir_path)
    return result


def _s3_key_for_type(data_type: str, filename: str) -> str:
    """Construct the S3 key for a given data type and filename."""
    return f"{S3_LANDING_PREFIX}{data_type}/{filename}"


def _s3_object_exists(key: str) -> bool:
    """Check if an object exists in the landing bucket."""
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except s3_client.exceptions.ClientError:
        return False


def _download_ftp_file(ftp_path: str, local_path: str) -> None:
    """Download a file from the FTP server to a local path."""
    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login()
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {ftp_path}", f.write)


def _calculate_md5(file_path: str) -> str:
    """Calculate MD5 checksum of a local file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def check_and_download(**context):
    """
    Checks for newer versions of Ensembl mapping files.
    Downloads, validates, and uploads them to S3 if newer.
    Raises AirflowSkipException if no updates are found.
    """
    ftp_files = _list_ftp_files()
    updates_found = False

    for data_type, (tsv_name, md5_name) in ftp_files.items():
        s3_key = _s3_key_for_type(data_type, tsv_name)
        if _s3_object_exists(s3_key):
            logging.info("File %s already present in S3; skipping.", s3_key)
            continue

        # Download TSV and its MD5 file
        local_tsv = f"/tmp/{tsv_name}"
        local_md5 = f"/tmp/{md5_name}"
        _download_ftp_file(f"{FTP_BASE_DIR}/{data_type}/{tsv_name}", local_tsv)
        _download_ftp_file(f"{FTP_BASE_DIR}/{data_type}/{md5_name}", local_md5)

        # Validate MD5
        with open(local_md5, "r") as f:
            expected_md5 = f.read().strip().split()[0]
        actual_md5 = _calculate_md5(local_tsv)
        if actual_md5 != expected_md5:
            raise ValueError(
                f"MD5 mismatch for {tsv_name}: expected {expected_md5}, got {actual_md5}"
            )

        # Upload to S3
        s3_client.upload_file(local_tsv, S3_BUCKET, s3_key)
        logging.info("Uploaded %s to s3://%s/%s", local_tsv, S3_BUCKET, s3_key)
        updates_found = True

    if not updates_found:
        logging.info("No new Ensembl mapping files detected; skipping downstream tasks.")
        raise AirflowSkipException("No updates found.")


# ----------------------------------------------------------------------
# DAG definition
# ----------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ensembl_mapping_etl",
    description="ETL pipeline that loads Ensembl mapping data into S3 and processes it with Spark.",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["ensembl", "etl", "spark"],
    on_success_callback=notify_success,
    on_failure_callback=notify_failure,
    on_start_callback=notify_start,
) as dag:
    file_task = PythonOperator(
        task_id="check_and_download_files",
        python_callable=check_and_download,
        provide_context=True,
    )

    table_task = SparkSubmitOperator(
        task_id="process_ensembl_mapping",
        application="/opt/airflow/dags/spark/ensembl_mapping.py",
        name="ensembl_mapping_spark_job",
        conn_id="spark_default",
        conf={"spark.master": "k8s://https://kubernetes.default.svc"},
        application_args=[
            "--s3-bucket",
            S3_BUCKET,
            "--landing-prefix",
            S3_LANDING_PREFIX,
            "--output-table",
            "ensembl_mapping",
        ],
        execution_timeout=timedelta(hours=3),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    file_task >> table_task