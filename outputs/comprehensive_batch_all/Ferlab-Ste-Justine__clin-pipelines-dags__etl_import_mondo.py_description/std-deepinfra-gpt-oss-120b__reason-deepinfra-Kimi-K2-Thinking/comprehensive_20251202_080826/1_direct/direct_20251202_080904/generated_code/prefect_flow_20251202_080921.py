import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, Optional

import boto3
import requests
from elasticsearch import Elasticsearch, helpers
from prefect import flow, task

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


@task
def params_validate(color: str) -> None:
    """Validate pipeline parameters.

    Args:
        color: A string representing a color parameter.

    Raises:
        ValueError: If the color is not a non‑empty string.
    """
    if not isinstance(color, str) or not color.strip():
        raise ValueError("Parameter 'color' must be a non‑empty string.")
    logger.info("Parameter validation passed (color=%s).", color)


@task
def download_mondo_terms(
    s3_bucket: str,
    s3_key: str,
    github_repo: str = "monarch-initiative/mondo",
    raw_url: str = "https://raw.githubusercontent.com/monarch-initiative/mondo/master/mondo.obo",
) -> str:
    """Download the latest Mondo OBO file and upload to S3 if newer.

    Args:
        s3_bucket: Target S3 bucket name.
        s3_key: Target SBO key (path) for the raw OBO file.
        github_repo: Repository identifier (unused in this simplified version).
        raw_url: Direct URL to the raw OBO file.

    Returns:
        The local path to the downloaded OBO file.
    """
    logger.info("Downloading Mondo OBO from %s", raw_url)
    response = requests.get(raw_url, timeout=30)
    response.raise_for_status()

    local_path = Path("/tmp") / "mondo.obo"
    local_path.write_bytes(response.content)
    logger.info("Downloaded OBO file to %s", local_path)

    s3 = boto3.client("s3")
    try:
        s3.head_object(Bucket=s3_bucket, Key=s3_key)
        logger.info("File already exists in s3://%s/%s – checking for updates.", s3_bucket, s3_key)
        # In a real implementation we would compare ETag or LastModified.
        # Here we always overwrite for simplicity.
    except s3.exceptions.NoSuchKey:
        logger.info("File does not exist in S3; will upload.")

    s3.upload_file(str(local_path), s3_bucket, s3_key)
    logger.info("Uploaded OBO file to s3://%s/%s", s3_bucket, s3_key)
    return str(local_path)


@task
def normalized_mondo_terms(
    raw_local_path: str,
    s3_bucket: str,
    processed_key: str,
    spark_jar: Optional[str] = None,
    obo_parser_spark_jar: Optional[str] = None,
) -> str:
    """Normalize Mondo terms using Spark (placeholder implementation).

    Args:
        raw_local_path: Path to the raw OBO file.
        s3_bucket: S3 bucket for the processed output.
        processed_key: S3 key for the normalized data.
        spark_jar: Optional path to a Spark JAR.
        obo_parser_spark_jar: Optional path to an OBO parser JAR.

    Returns:
        The S3 path of the processed data.
    """
    logger.info(
        "Starting Spark normalization (spark_jar=%s, obo_parser_spark_jar=%s).",
        spark_jar,
        obo_parser_spark_jar,
    )
    # Placeholder: simply copy the raw file to a "processed" location.
    processed_local = Path("/tmp") / "mondo_normalized.json"
    with open(raw_local_path, "r", encoding="utf-8") as src, open(
        processed_local, "w", encoding="utf-8"
    ) as dst:
        # Very naive conversion: each line becomes a JSON object.
        for line in src:
            if line.strip():
                json.dump({"term": line.strip()}, dst)
                dst.write("\n")
    logger.info("Normalization complete, written to %s", processed_local)

    s3 = boto3.client("s3")
    s3.upload_file(str(processed_local), s3_bucket, processed_key)
    logger.info("Uploaded normalized data to s3://%s/%s", s3_bucket, processed_key)
    return f"s3://{s3_bucket}/{processed_key}"


@task
def index_mondo_terms(
    processed_s3_path: str,
    es_host: str = "http://localhost:9200",
    es_index: str = "mondo",
) -> None:
    """Index normalized Mondo terms into Elasticsearch (placeholder).

    Args:
        processed_s3_path: S3 URI of the processed data.
        es_host: Elasticsearch host URL.
        es_index: Target Elasticsearch index name.
    """
    logger.info("Indexing data from %s into Elasticsearch index %s", processed_s3_path, es_index)
    # Download the processed file locally.
    s3 = boto3.client("s3")
    bucket, key = processed_s3_path.replace("s3://", "").split("/", 1)
    local_path = Path("/tmp") / "mondo_normalized.json"
    s3.download_file(bucket, key, str(local_path))

    es = Elasticsearch(es_host)
    if not es.indices.exists(index=es_index):
        es.indices.create(index=es_index)
        logger.info("Created Elasticsearch index %s", es_index)

    actions = []
    with open(local_path, "r", encoding="utf-8") as f:
        for line in f:
            doc = json.loads(line)
            actions.append(
                {
                    "_index": es_index,
                    "_source": doc,
                }
            )
    helpers.bulk(es, actions)
    logger.info("Indexed %d documents into %s", len(actions), es_index)


@task
def publish_mondo(es_index: str, es_host: str = "http://localhost:9200") -> None:
    """Publish the indexed Mondo data (e.g., refresh the index).

    Args:
        es_index: Elasticsearch index to publish.
        es_host: Elasticsearch host URL.
    """
    logger.info("Publishing Elasticsearch index %s", es_index)
    es = Elasticsearch(es_host)
    es.indices.refresh(index=es_index)
    logger.info("Index %s refreshed and ready for consumption.", es_index)


@task
def slack_notify(
    message: str,
    webhook_url: str = os.getenv("SLACK_WEBHOOK_URL", ""),
) -> None:
    """Send a Slack notification via Incoming Webhook.

    Args:
        message: Text message to send.
        webhook_url: Slack Incoming Webhook URL.
    """
    if not webhook_url:
        logger.warning("Slack webhook URL not configured; skipping notification.")
        return

    payload = {"text": message}
    response = requests.post(webhook_url, json=payload, timeout=10)
    if response.status_code != 200:
        logger.error("Failed to send Slack notification: %s", response.text)
        response.raise_for_status()
    else:
        logger.info("Slack notification sent successfully.")


@flow
def mondo_etl_flow(
    color: str = "blue",
    s3_bucket: str = "my-mondo-bucket",
    raw_s3_key: str = "raw/mondo.obo",
    processed_s3_key: str = "processed/mondo_normalized.json",
    spark_jar: Optional[str] = None,
    obo_parser_spark_jar: Optional[str] = None,
    es_host: str = "http://localhost:9200",
    es_index: str = "mondo",
) -> None:
    """Orchestrate the Mondo ETL pipeline."""
    try:
        params_validate(color)

        raw_path = download_mondo_terms(
            s3_bucket=s3_bucket,
            s3_key=raw_s3_key,
        )

        processed_path = normalized_mondo_terms(
            raw_local_path=raw_path,
            s3_bucket=s3_bucket,
            processed_key=processed_s3_key,
            spark_jar=spark_jar,
            obo_parser_spark_jar=obo_parser_spark_jar,
        )

        index_mondo_terms(
            processed_s3_path=processed_path,
            es_host=es_host,
            es_index=es_index,
        )

        publish_mondo(es_index=es_index, es_host=es_host)

        slack_notify(message="✅ Mondo ETL pipeline completed successfully.")
    except Exception as exc:  # pylint: disable=broad-except
        logger.exception("Pipeline failed: %s", exc)
        slack_notify(message=f"❌ Mondo ETL pipeline failed: {exc}")
        raise


if __name__ == "__main__":
    # Manual execution; in production this flow would be deployed with a schedule if needed.
    mondo_etl_flow()