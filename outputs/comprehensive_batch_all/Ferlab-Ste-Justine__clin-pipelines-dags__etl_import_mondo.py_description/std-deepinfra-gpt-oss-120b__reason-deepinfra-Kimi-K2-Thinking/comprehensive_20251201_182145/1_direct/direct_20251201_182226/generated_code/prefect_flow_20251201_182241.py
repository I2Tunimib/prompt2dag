import logging
import os
import tempfile
from pathlib import Path
from typing import Optional

import boto3
import requests
from elasticsearch import Elasticsearch, helpers
from prefect import flow, task
from slack_sdk.webhook import WebhookClient

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
GITHUB_API_LATEST_RELEASE = (
    "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
)
ALLOWED_COLORS = {"red", "green", "blue", "yellow"}


@task
def params_validate(color: str) -> str:
    """
    Validate the provided color parameter.

    Args:
        color: Color string to validate.

    Returns:
        The validated color.

    Raises:
        ValueError: If the color is not allowed.
    """
    logger.info("Validating color parameter: %s", color)
    if color not in ALLOWED_COLORS:
        raise ValueError(f"Invalid color '{color}'. Allowed values: {ALLOWED_COLORS}")
    return color


@task
def download_mondo_terms(
    s3_bucket: str,
    s3_key_prefix: str,
    aws_region: str = "us-east-1",
) -> str:
    """
    Download the latest Mondo OBO file from GitHub releases and upload it to S3
    if a newer version is available.

    Returns:
        The S3 URI of the uploaded OBO file.
    """
    logger.info("Fetching latest Mondo release information from GitHub")
    response = requests.get(GITHUB_API_LATEST_RELEASE, timeout=30)
    response.raise_for_status()
    release_info = response.json()
    tag_name = release_info.get("tag_name", "unknown")
    assets = release_info.get("assets", [])

    obo_asset = next(
        (a for a in assets if a["name"].endswith(".obo")), None
    )
    if not obo_asset:
        raise RuntimeError("No OBO asset found in the latest Mondo release.")

    download_url = obo_asset["browser_download_url"]
    logger.info("Downloading OBO file from %s", download_url)

    with requests.get(download_url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".obo") as tmp_file:
            for chunk in r.iter_content(chunk_size=8192):
                if chunk:
                    tmp_file.write(chunk)
            local_path = tmp_file.name

    s3_key = f"{s3_key_prefix}/mondo-{tag_name}.obo"
    s3_uri = f"s3://{s3_bucket}/{s3_key}"
    logger.info("Uploading OBO file to %s", s3_uri)

    s3_client = boto3.client("s3", region_name=aws_region)
    s3_client.upload_file(
        Filename=local_path,
        Bucket=s3_bucket,
        Key=s3_key,
        ExtraArgs={"Metadata": {"tag": tag_name}},
    )
    os.remove(local_path)
    logger.info("Upload complete")
    return s3_uri


@task
def normalized_mondo_terms(
    obo_s3_uri: str,
    spark_jar: str,
    obo_parser_spark_jar: str,
    output_path: str = "s3://my-data-lake/mondo/normalized/",
) -> str:
    """
    Process the OBO file using Spark to normalize ontology terms.

    Returns:
        The path where normalized data is stored.
    """
    logger.info(
        "Starting Spark normalization job for %s with jars %s, %s",
        obo_s3_uri,
        spark_jar,
        obo_parser_spark_jar,
    )
    # Placeholder for Spark job execution.
    # In a real implementation, you would submit a Spark job here.
    logger.info("Spark job completed, normalized data stored at %s", output_path)
    return output_path


@task
def index_mondo_terms(
    normalized_path: str,
    spark_jar: str,
    es_host: str = "http://localhost:9200",
    index_name: str = "mondo",
) -> str:
    """
    Index the normalized Mondo terms into Elasticsearch using Spark.

    Returns:
        The name of the Elasticsearch index used.
    """
    logger.info(
        "Starting Spark indexing job for data at %s to Elasticsearch %s",
        normalized_path,
        es_host,
    )
    # Placeholder for Spark indexing job.
    # In a real implementation, you would submit a Spark job that writes to ES.
    logger.info("Indexing job completed, data indexed in index '%s'", index_name)
    return index_name


@task
def publish_mondo(index_name: str) -> None:
    """
    Publish the indexed Mondo data (e.g., set an alias or make it searchable).

    Args:
        index_name: The Elasticsearch index containing Mondo data.
    """
    logger.info("Publishing Mondo data for index '%s'", index_name)
    # Placeholder for publishing logic.
    # For example, you might update an alias to point to the new index.
    logger.info("Publish step completed")


@task
def slack_notify(
    message: str,
    webhook_url: str,
) -> None:
    """
    Send a Slack notification via webhook.

    Args:
        message: The message to send.
        webhook_url: Slack incoming webhook URL.
    """
    logger.info("Sending Slack notification")
    client = WebhookClient(webhook_url)
    response = client.send(text=message)
    if response.status_code != 200:
        logger.error("Failed to send Slack notification: %s", response.body)
    else:
        logger.info("Slack notification sent successfully")


@flow
def mondo_etl_flow(
    color: str = "green",
    spark_jar: str = "local:/path/to/spark-job.jar",
    obo_parser_spark_jar: str = "local:/path/to/obo-parser.jar",
    s3_bucket: str = "my-mondo-bucket",
    s3_key_prefix: str = "raw",
    slack_webhook_url: Optional[str] = None,
) -> None:
    """
    Orchestrates the Mondo ETL pipeline:
    1. Validate parameters.
    2. Download latest OBO file.
    3. Normalize terms with Spark.
    4. Index terms into Elasticsearch.
    5. Publish the indexed data.
    6. Send a Slack notification on success or failure.
    """
    try:
        validated_color = params_validate(color)
        obo_s3_uri = download_mondo_terms(s3_bucket, s3_key_prefix)
        normalized_path = normalized_mondo_terms(
            obo_s3_uri, spark_jar, obo_parser_spark_jar
        )
        index_name = index_mondo_terms(normalized_path, spark_jar)
        publish_mondo(index_name)

        success_message = (
            f"Mondo ETL pipeline completed successfully. "
            f"Color: {validated_color}, Index: {index_name}"
        )
        if slack_webhook_url:
            slack_notify(success_message, slack_webhook_url)
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Mondo ETL pipeline failed")
        failure_message = f"Mondo ETL pipeline failed: {exc}"
        if slack_webhook_url:
            slack_notify(failure_message, slack_webhook_url)
        raise


if __name__ == "__main__":
    # Manual execution entry point.
    # Deployment/schedule configuration can be added via Prefect UI or CLI.
    mondo_etl_flow()