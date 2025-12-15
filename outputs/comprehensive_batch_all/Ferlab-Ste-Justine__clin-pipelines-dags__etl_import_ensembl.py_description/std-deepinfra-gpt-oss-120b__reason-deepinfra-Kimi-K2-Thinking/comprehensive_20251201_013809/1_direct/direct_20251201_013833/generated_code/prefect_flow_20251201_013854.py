import os
import ftplib
import hashlib
import logging
import subprocess
import tempfile
from pathlib import Path
from typing import List

import boto3
from prefect import flow, task
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants – can be overridden by environment variables
FTP_HOST = os.getenv("ENSEMBL_FTP_HOST", "ftp.ensembl.org")
FTP_DIR = os.getenv(
    "ENSEMBL_FTP_DIR", "/pub/current_tsv/homo_sapiens"
)  # Adjust as needed
DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]

S3_BUCKET = os.getenv("S3_BUCKET", "my-data-lake")
S3_RAW_PREFIX = os.getenv("S3_RAW_PREFIX", "raw/landing/ensembl/")

SLACK_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL", "#etl-notifications")

SPARK_SCRIPT = os.getenv(
    "SPARK_SCRIPT", "/opt/spark/jobs/ensembl_mapping.py"
)  # Path to Spark application
SPARK_SUBMIT = os.getenv("SPARK_SUBMIT", "spark-submit")  # Spark submit executable


def _md5_checksum(file_path: Path) -> str:
    """Calculate the MD5 checksum of a file."""
    hash_md5 = hashlib.md5()
    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()


def _slack_client() -> WebClient:
    """Create a Slack WebClient if credentials are provided."""
    if not SLACK_TOKEN:
        logger.warning("Slack token not configured; notifications will be disabled.")
        return None
    return WebClient(token=SLACK_TOKEN)


@task(retries=2, retry_delay_seconds=30)
def send_slack_message(message: str) -> None:
    """Send a simple message to a Slack channel."""
    client = _slack_client()
    if client is None:
        return
    try:
        client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
    except SlackApiError as exc:
        logger.error("Failed to send Slack message: %s", exc.response["error"])


@task
def check_and_sync_files() -> List[str]:
    """
    Check Ensembl FTP for newer files, download, validate MD5, and upload to S3.

    Returns:
        List of S3 object keys that were uploaded. Empty list means no new data.
    """
    s3 = boto3.client("s3")
    uploaded_keys: List[str] = []

    with ftplib.FTP(FTP_HOST) as ftp:
        ftp.login()
        ftp.cwd(FTP_DIR)
        logger.info("Connected to FTP %s:%s", FTP_HOST, FTP_DIR)

        for data_type in DATA_TYPES:
            # Assume files are named like <data_type>.tsv.gz
            filename = f"{data_type}.tsv.gz"
            try:
                ftp.size(filename)  # Verify file exists
            except ftplib.error_perm:
                logger.warning("File %s not found on FTP; skipping.", filename)
                continue

            s3_key = f"{S3_RAW_PREFIX}{filename}"
            # Check if the file already exists in S3
            try:
                s3.head_object(Bucket=S3_BUCKET, Key=s3_key)
                logger.info("File %s already present in S3; skipping download.", filename)
                continue
            except s3.exceptions.NoSuchKey:
                logger.info("New file detected: %s", filename)

            # Download to a temporary location
            with tempfile.TemporaryDirectory() as tmp_dir:
                local_path = Path(tmp_dir) / filename
                with local_path.open("wb") as f:
                    ftp.retrbinary(f"RETR {filename}", f.write)
                logger.info("Downloaded %s to %s", filename, local_path)

                # Compute MD5 and compare with any stored checksum (optional)
                md5_local = _md5_checksum(local_path)
                logger.info("MD5 checksum for %s: %s", filename, md5_local)

                # Upload to S3 with MD5 as metadata
                with local_path.open("rb") as data:
                    s3.upload_fileobj(
                        Fileobj=data,
                        Bucket=S3_BUCKET,
                        Key=s3_key,
                        ExtraArgs={"Metadata": {"md5": md5_local}},
                    )
                logger.info("Uploaded %s to s3://%s/%s", filename, S3_BUCKET, s3_key)
                uploaded_keys.append(s3_key)

    return uploaded_keys


@task
def process_with_spark(uploaded_keys: List[str]) -> None:
    """
    Run the Spark job to transform the newly landed files.

    Args:
        uploaded_keys: List of S3 keys that were uploaded in the previous step.
    """
    if not uploaded_keys:
        logger.info("No new files to process; skipping Spark job.")
        return

    # Build Spark submit command
    cmd = [
        SPARK_SUBMIT,
        "--master",
        "k8s://https://kubernetes.default.svc",
        "--conf",
        f"spark.kubernetes.namespace={os.getenv('K8S_NAMESPACE', 'default')}",
        SPARK_SCRIPT,
        "--input-prefix",
        f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}",
        "--output-table",
        "ensembl_mapping",
    ]

    logger.info("Executing Spark job: %s", " ".join(cmd))
    try:
        result = subprocess.run(
            cmd, check=True, capture_output=True, text=True, timeout=3600
        )
        logger.info("Spark job completed successfully.")
        logger.debug("Spark stdout: %s", result.stdout)
        logger.debug("Spark stderr: %s", result.stderr)
    except subprocess.CalledProcessError as exc:
        logger.error(
            "Spark job failed with return code %s. Stderr: %s",
            exc.returncode,
            exc.stderr,
        )
        raise
    except subprocess.TimeoutExpired:
        logger.error("Spark job timed out.")
        raise


@flow
def ensembl_etl_flow() -> None:
    """
    Prefect flow orchestrating the Ensembl ETL pipeline.
    """
    send_slack_message.submit("🚀 Ensembl ETL pipeline started.")
    uploaded = check_and_sync_files()
    if uploaded:
        logger.info("New files detected: %s", uploaded)
        process_with_spark(uploaded)
        send_slack_message.submit(
            f"✅ Ensembl ETL pipeline completed successfully. Processed {len(uploaded)} file(s)."
        )
    else:
        logger.info("No new data found; pipeline will not run further.")
        send_slack_message.submit("ℹ️ Ensembl ETL pipeline skipped – no new data available.")


if __name__ == "__main__":
    ensembl_etl_flow()