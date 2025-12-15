import os
import ftplib
import hashlib
import logging
import subprocess
from pathlib import Path
from typing import List

import boto3
import requests
from prefect import flow, task

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants (could be moved to a config file or environment variables)
FTP_HOST = "ftp.ensembl.org"
FTP_ROOT = "/pub/current_variation/tsv"
DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]
S3_BUCKET = os.getenv("S3_BUCKET", "my-data-lake")
S3_RAW_PREFIX = "raw/landing/ensembl"
SPARK_APP_PATH = "/opt/spark/apps/ensembl_mapping.py"
SPARK_SUBMIT_CMD = ["spark-submit", "--master", "k8s://https://kubernetes.default.svc"]
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


@task
def send_slack_message(message: str) -> None:
    """Send a simple message to Slack via webhook."""
    if not SLACK_WEBHOOK_URL:
        logger.warning("Slack webhook URL not configured; skipping notification.")
        return
    try:
        response = requests.post(
            SLACK_WEBHOOK_URL, json={"text": message}, timeout=10
        )
        response.raise_for_status()
        logger.info("Sent Slack notification.")
    except Exception as exc:  # pragma: no cover
        logger.error("Failed to send Slack notification: %s", exc)


def _list_ftp_files(ftp: ftplib.FTP, directory: str) -> List[str]:
    """Return a list of filenames in the given FTP directory."""
    files = []

    def _collector(line: str) -> None:
        parts = line.split()
        if len(parts) >= 9:
            files.append(parts[-1])

    ftp.cwd(directory)
    ftp.retrlines("LIST", _collector)
    return files


def _extract_version(filename: str, data_type: str) -> str:
    """
    Extract version string from a filename.
    Expected pattern: <data_type>_v<version>.tsv or similar.
    """
    base = Path(filename).stem
    if base.startswith(f"{data_type}_v"):
        return base.split("_v")[-1]
    return ""


def _latest_ftp_version(ftp: ftplib.FTP, data_type: str) -> str:
    """Determine the latest version for a data type on the FTP server."""
    files = _list_ftp_files(ftp, FTP_ROOT)
    versions = [
        _extract_version(f, data_type)
        for f in files
        if f.startswith(data_type) and f.endswith(".tsv")
    ]
    versions = [v for v in versions if v]
    return max(versions, default="")


def _current_s3_version(s3_client, data_type: str) -> str:
    """Determine the latest version stored in S3 for a data type."""
    prefix = f"{S3_RAW_PREFIX}/{data_type}_v"
    paginator = s3_client.get_paginator("list_objects_v2")
    versions = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            version = _extract_version(Path(key).name, data_type)
            if version:
                versions.append(version)
    return max(versions, default="")


def _download_ftp_file(ftp: ftplib.FTP, remote_path: str, local_path: Path) -> None:
    """Download a file from FTP to a local path."""
    with open(local_path, "wb") as f:
        ftp.retrbinary(f"RETR {remote_path}", f.write)


def _compute_md5(file_path: Path) -> str:
    """Compute MD5 checksum of a file."""
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


@task
def check_and_load_files() -> List[str]:
    """
    Check FTP for newer versions of each data type, download, validate,
    and upload to S3. Returns a list of data types that were updated.
    """
    updated_types = []
    s3_client = boto3.client("s3")
    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login()
        for data_type in DATA_TYPES:
            logger.info("Processing data type: %s", data_type)
            latest_version = _latest_ftp_version(ftp, data_type)
            if not latest_version:
                logger.warning("No version found on FTP for %s", data_type)
                continue

            current_version = _current_s3_version(s3_client, data_type)
            logger.info(
                "Latest FTP version: %s, current S3 version: %s",
                latest_version,
                current_version,
            )
            if latest_version <= current_version:
                logger.info("No new version for %s; skipping.", data_type)
                continue

            remote_filename = f"{data_type}_v{latest_version}.tsv"
            remote_path = f"{FTP_ROOT}/{remote_filename}"
            local_path = Path("/tmp") / remote_filename

            logger.info("Downloading %s from FTP.", remote_filename)
            _download_ftp_file(ftp, remote_path, local_path)

            # Simple MD5 validation: assume a .md5 file exists alongside the TSV
            md5_remote_path = f"{remote_path}.md5"
            md5_local_path = Path("/tmp") / f"{remote_filename}.md5"
            try:
                _download_ftp_file(ftp, md5_remote_path, md5_local_path)
                with open(md5_local_path, "r") as f:
                    expected_md5 = f.read().strip().split()[0]
                actual_md5 = _compute_md5(local_path)
                if expected_md5 != actual_md5:
                    raise ValueError(
                        f"MD5 mismatch for {remote_filename}: expected {expected_md5}, got {actual_md5}"
                    )
                logger.info("MD5 checksum validated for %s.", remote_filename)
            except ftplib.error_perm:
                logger.warning(
                    "MD5 file not found for %s; skipping checksum validation.", remote_filename
                )

            s3_key = f"{S3_RAW_PREFIX}/{remote_filename}"
            logger.info("Uploading %s to s3://%s/%s", local_path, S3_BUCKET, s3_key)
            s3_client.upload_file(str(local_path), S3_BUCKET, s3_key)

            updated_types.append(data_type)

            # Cleanup local files
            local_path.unlink(missing_ok=True)
            md5_local_path.unlink(missing_ok=True)

    return updated_types


@task
def run_spark_job(updated_types: List[str]) -> None:
    """
    Execute the Spark application to process newly landed Ensembl files.
    The Spark job reads from the S3 raw landing zone and writes to the
    structured table `ensembl_mapping`.
    """
    if not updated_types:
        logger.info("No new data to process; skipping Spark job.")
        return

    s3_input_path = f"s3a://{S3_BUCKET}/{S3_RAW_PREFIX}/"
    spark_cmd = SPARK_SUBMIT_CMD + [
        SPARK_APP_PATH,
        "--input_path",
        s3_input_path,
        "--output_table",
        "ensembl_mapping",
    ]

    logger.info("Running Spark job: %s", " ".join(spark_cmd))
    try:
        subprocess.run(spark_cmd, check=True)
        logger.info("Spark job completed successfully.")
    except subprocess.CalledProcessError as exc:  # pragma: no cover
        logger.error("Spark job failed with exit code %s", exc.returncode)
        raise


@flow
def ensembl_etl_flow() -> None:
    """
    Prefect flow that orchestrates the Ensembl ETL pipeline:
    1. Checks for new data versions on the Ensembl FTP server.
    2. Downloads, validates, and uploads new files to S3.
    3. Runs a Spark job to transform the raw files into a structured table.
    """
    send_slack_message.submit("🚀 Ensembl ETL pipeline started.")
    try:
        updated = check_and_load_files()
        run_spark_job(updated)
        send_slack_message.submit("✅ Ensembl ETL pipeline completed successfully.")
    except Exception as exc:  # pragma: no cover
        logger.exception("Pipeline failed: %s", exc)
        send_slack_message.submit(f"❌ Ensembl ETL pipeline failed: {exc}")
        raise


if __name__ == "__main__":
    # Manual execution; in production this flow would be deployed with a schedule if needed.
    ensembl_etl_flow()