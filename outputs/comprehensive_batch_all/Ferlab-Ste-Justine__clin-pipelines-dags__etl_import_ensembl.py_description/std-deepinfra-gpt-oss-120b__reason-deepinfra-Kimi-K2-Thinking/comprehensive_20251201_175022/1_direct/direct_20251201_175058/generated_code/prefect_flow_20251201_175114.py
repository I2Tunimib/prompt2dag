import ftplib
import hashlib
import logging
import os
import subprocess
import tempfile
from pathlib import Path
from typing import List

import boto3
import requests
from prefect import flow, task

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
FTP_HOST = "ftp.ensembl.org"
FTP_BASE_DIR = "/pub/current_release/tsv/homo_sapiens"
DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]
S3_BUCKET = os.getenv("S3_BUCKET", "my-data-lake")
S3_LANDING_PREFIX = "raw/landing/ensembl/"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


@task
def slack_notify(message: str) -> None:
    """Send a simple Slack notification via webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL not configured; skipping notification.")
        return
    payload = {"text": message}
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Slack notification sent.")
    except Exception as exc:
        logger.error("Failed to send Slack notification: %s", exc)


def _list_ftp_files(ftp: ftplib.FTP, data_type: str) -> List[str]:
    """Return a list of file names for a given data type on the FTP server."""
    pattern = f"{data_type}.*.tsv.gz"
    files = []
    try:
        ftp.cwd(FTP_BASE_DIR)
        for entry in ftp.nlst():
            if entry.startswith(data_type) and entry.endswith(".tsv.gz"):
                files.append(entry)
    except ftplib.error_perm as exc:
        logger.error("FTP permission error while listing %s: %s", data_type, exc)
    return files


def _download_ftp_file(ftp: ftplib.FTP, filename: str, dest_path: Path) -> None:
    """Download a single file from the FTP server to a local path."""
    with dest_path.open("wb") as f:
        ftp.retrbinary(f"RETR {filename}", f.write)
    logger.info("Downloaded %s to %s", filename, dest_path)


def _calculate_md5(file_path: Path) -> str:
    """Calculate the MD5 checksum of a file."""
    hash_md5 = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def _s3_key_for_file(filename: str) -> str:
    """Construct the S3 key for a given filename."""
    return f"{S3_LANDING_PREFIX}{filename}"


def _s3_object_exists(s3_client, key: str) -> bool:
    """Check whether an object exists in S3."""
    try:
        s3_client.head_object(Bucket=S3_BUCKET, Key=key)
        return True
    except s3_client.exceptions.NoSuchKey:
        return False
    except Exception:
        return False


def _upload_to_s3(s3_client, file_path: Path, key: str) -> None:
    """Upload a local file to S3."""
    s3_client.upload_file(str(file_path), S3_BUCKET, key)
    logger.info("Uploaded %s to s3://%s/%s", file_path, S3_BUCKET, key)


@task
def check_and_load_files() -> List[str]:
    """
    Check the Ensembl FTP server for newer versions of the defined data types.
    Download, validate, and upload new files to S3.
    Returns a list of S3 keys that were uploaded (empty if no new data).
    """
    uploaded_keys: List[str] = []
    s3_client = boto3.client("s3")
    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login()
        logger.info("Connected to FTP %s", FTP_HOST)

        for data_type in DATA_TYPES:
            ftp_files = _list_ftp_files(ftp, data_type)
            if not ftp_files:
                logger.info("No files found for data type %s", data_type)
                continue

            # Assume the latest file is the one with the highest lexical order
            latest_file = sorted(ftp_files)[-1]
            s3_key = _s3_key_for_file(latest_file)

            if _s3_object_exists(s3_client, s3_key):
                logger.info("File %s already present in S3; skipping.", latest_file)
                continue

            with tempfile.TemporaryDirectory() as tmp_dir:
                local_path = Path(tmp_dir) / latest_file
                _download_ftp_file(ftp, latest_file, local_path)

                # Simple MD5 validation (placeholder: compare against a .md5 file if available)
                md5_checksum = _calculate_md5(local_path)
                logger.info("MD5 checksum for %s: %s", latest_file, md5_checksum)

                _upload_to_s3(s3_client, local_path, s3_key)
                uploaded_keys.append(s3_key)

    if not uploaded_keys:
        logger.info("No new files were uploaded; pipeline will be skipped.")
    else:
        logger.info("Uploaded %d new file(s) to S3.", len(uploaded_keys))
    return uploaded_keys


@task
def run_spark_job(s3_keys: List[str]) -> None:
    """
    Execute a Spark job that processes the newly uploaded Ensembl files.
    The Spark application reads the raw TSV files from S3 and writes to the
    'ensembl_mapping' table in the data lake.
    """
    if not s3_keys:
        logger.info("No new files to process; skipping Spark job.")
        return

    spark_submit_cmd = [
        "spark-submit",
        "--master", "k8s://https://kubernetes.default.svc",
        "--deploy-mode", "cluster",
        "--conf", "spark.kubernetes.namespace=etl",
        "--conf", "spark.kubernetes.container.image=my-spark-image:latest",
        "spark_jobs/ensembl_mapping.py",
        "--input-keys",
        ",".join(s3_keys),
        "--output-table",
        "ensembl_mapping",
    ]

    logger.info("Running Spark job with command: %s", " ".join(spark_submit_cmd))
    try:
        result = subprocess.run(
            spark_submit_cmd,
            check=True,
            capture_output=True,
            text=True,
        )
        logger.info("Spark job completed successfully.")
        logger.debug("Spark stdout: %s", result.stdout)
        logger.debug("Spark stderr: %s", result.stderr)
    except subprocess.CalledProcessError as exc:
        logger.error("Spark job failed with return code %s", exc.returncode)
        logger.error("Stdout: %s", exc.stdout)
        logger.error("Stderr: %s", exc.stderr)
        raise


@flow
def ensembl_etl_flow() -> None:
    """
    Prefect flow orchestrating the Ensembl ETL pipeline.
    It checks for new data, uploads to S3, and runs a Spark transformation.
    """
    slack_notify.submit("Ensembl ETL pipeline started.")
    try:
        uploaded_keys = check_and_load_files()
        if uploaded_keys:
            run_spark_job(uploaded_keys)
            slack_notify.submit("Ensembl ETL pipeline completed successfully.")
        else:
            slack_notify.submit("Ensembl ETL pipeline skipped: no new data.")
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        slack_notify.submit(f"Ensembl ETL pipeline failed: {exc}")
        raise


if __name__ == "__main__":
    ensembl_etl_flow()