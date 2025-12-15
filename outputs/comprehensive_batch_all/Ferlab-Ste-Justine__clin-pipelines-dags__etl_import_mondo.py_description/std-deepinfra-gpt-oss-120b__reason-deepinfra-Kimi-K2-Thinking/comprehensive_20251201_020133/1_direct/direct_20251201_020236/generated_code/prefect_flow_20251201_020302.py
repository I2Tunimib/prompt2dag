import os
import json
import logging
from datetime import datetime
from typing import Optional

import boto3
import requests
from prefect import flow, task, get_run_logger

# Constants – adjust as needed
S3_BUCKET = os.getenv("S3_BUCKET", "my-mondo-bucket")
RAW_S3_KEY = "mondo/raw/mondo.obo"
VERSION_S3_KEY = "mondo/version.txt"
NORMALIZED_S3_PREFIX = "mondo/normalized/"
ELASTICSEARCH_INDEX = "mondo"
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


@task
def params_validate(color: str) -> None:
    """Validate pipeline parameters."""
    logger = get_run_logger()
    allowed_colors = {"red", "green", "blue"}
    if color not in allowed_colors:
        raise ValueError(f"Invalid color '{color}'. Allowed values: {allowed_colors}")
    logger.info("Parameter validation passed for color='%s'.", color)


@task
def _get_latest_release_info() -> dict:
    """Fetch latest release metadata from GitHub."""
    logger = get_run_logger()
    url = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    data = response.json()
    logger.info("Fetched latest release: %s", data.get("tag_name"))
    return data


@task
def _get_stored_version(s3_client) -> Optional[str]:
    """Retrieve the previously stored Mondo version from S3."""
    logger = get_run_logger()
    try:
        obj = s3_client.get_object(Bucket=S3_BUCKET, Key=VERSION_S3_KEY)
        version = obj["Body"].read().decode().strip()
        logger.info("Stored version retrieved: %s", version)
        return version
    except s3_client.exceptions.NoSuchKey:
        logger.info("No stored version found in S3.")
        return None
    except Exception as exc:
        logger.error("Error retrieving stored version: %s", exc)
        raise


@task
def download_mondo_terms(s3_client) -> str:
    """
    Download the latest Mondo OBO file if newer than stored version,
    and upload it to S3.
    Returns the version tag that was processed.
    """
    logger = get_run_logger()
    release_info = _get_latest_release_info()
    latest_version = release_info.get("tag_name")
    if not latest_version:
        raise RuntimeError("Could not determine latest version from GitHub.")

    stored_version = _get_stored_version(s3_client)

    if stored_version == latest_version:
        logger.info("Mondo version %s is already up‑to‑date. Skipping download.", latest_version)
        return latest_version

    # Find the .obo asset
    obo_asset = next(
        (a for a in release_info.get("assets", []) if a["name"].endswith(".obo")),
        None,
    )
    if not obo_asset:
        raise RuntimeError("No .obo asset found in the latest release.")

    download_url = obo_asset["browser_download_url"]
    logger.info("Downloading OBO file from %s", download_url)
    response = requests.get(download_url, timeout=60)
    response.raise_for_status()
    obo_content = response.content

    # Upload to S3
    logger.info("Uploading OBO file to s3://%s/%s", S3_BUCKET, RAW_S3_KEY)
    s3_client.put_object(Bucket=S3_BUCKET, Key=RAW_S3_KEY, Body=obo_content)

    # Store the new version tag
    logger.info("Updating stored version to %s", latest_version)
    s3_client.put_object(Bucket=S3_BUCKET, Key=VERSION_S3_KEY, Body=latest_version.encode())

    return latest_version


@task
def normalized_mondo_terms(s3_client, spark_jar: str, obo_parser_spark_jar: str) -> str:
    """
    Process the raw OBO file with Spark to produce normalized terms.
    Returns the S3 prefix where normalized data is stored.
    """
    logger = get_run_logger()
    # Placeholder for Spark job – in a real implementation you would
    # submit a Spark application using spark-submit or a Spark client.
    logger.info(
        "Running Spark normalization job with spark_jar=%s and obo_parser_spark_jar=%s",
        spark_jar,
        obo_parser_spark_jar,
    )
    # Simulate processing time
    import time

    time.sleep(2)

    normalized_prefix = f"{NORMALIZED_S3_PREFIX}{datetime.utcnow().strftime('%Y%m%d%H%M%S')}/"
    logger.info("Normalized data would be written to s3://%s/%s", S3_BUCKET, normalized_prefix)

    # In a real job, you would write output to this prefix.
    return normalized_prefix


@task
def index_mondo_terms(s3_client, normalized_prefix: str, spark_jar: str) -> str:
    """
    Index normalized terms into Elasticsearch using Spark.
    Returns the name of the Elasticsearch index used.
    """
    logger = get_run_logger()
    logger.info(
        "Running Spark indexing job for data at s3://%s/%s using spark_jar=%s",
        S3_BUCKET,
        normalized_prefix,
        spark_jar,
    )
    # Simulate processing time
    import time

    time.sleep(2)

    logger.info("Data indexed into Elasticsearch index '%s'.", ELASTICSEARCH_INDEX)
    return ELASTICSEARCH_INDEX


@task
def publish_mondo(index_name: str) -> None:
    """
    Publish the indexed data (e.g., make it searchable or update an alias).
    """
    logger = get_run_logger()
    # Placeholder for publishing logic – could involve updating an alias,
    # refreshing index settings, etc.
    logger.info("Publishing Elasticsearch index '%s'.", index_name)
    # Simulate work
    import time

    time.sleep(1)
    logger.info("Publish step completed.")


@task
def slack_notify(message: str, success: bool = True) -> None:
    """
    Send a Slack notification via webhook.
    """
    logger = get_run_logger()
    if not SLACK_WEBHOOK_URL:
        logger.warning("SLACK_WEBHOOK_URL not set; skipping Slack notification.")
        return

    payload = {
        "text": f"{'✅' if success else '❌'} {message}",
        "mrkdwn": True,
    }
    try:
        response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Slack notification sent.")
    except Exception as exc:
        logger.error("Failed to send Slack notification: %s", exc)


@flow
def mondo_etl_flow(
    color: str = "green",
    spark_jar: str = "s3://my-bucket/jars/spark-job.jar",
    obo_parser_spark_jar: str = "s3://my-bucket/jars/obo-parser.jar",
) -> None:
    """
    Orchestrates the Mondo ETL pipeline.
    """
    logger = get_run_logger()
    logger.info("Starting Mondo ETL flow with color=%s", color)

    # Initialize AWS client once
    s3_client = boto3.client("s3")

    try:
        params_validate(color)
        version = download_mondo_terms(s3_client)
        normalized_prefix = normalized_mondo_terms(
            s3_client, spark_jar=spark_jar, obo_parser_spark_jar=obo_parser_spark_jar
        )
        index_name = index_mondo_terms(s3_client, normalized_prefix, spark_jar=spark_jar)
        publish_mondo(index_name)
        slack_notify(f"Mondo ETL pipeline completed successfully for version {version}.")
    except Exception as exc:
        logger.exception("Pipeline failed: %s", exc)
        slack_notify(f"Mondo ETL pipeline failed: {exc}", success=False)
        raise


if __name__ == "__main__":
    # Manual execution entry point.
    # Deployment configuration (e.g., schedule=None, concurrency limits) should be
    # defined in the Prefect UI or via infrastructure-as-code.
    mondo_etl_flow()