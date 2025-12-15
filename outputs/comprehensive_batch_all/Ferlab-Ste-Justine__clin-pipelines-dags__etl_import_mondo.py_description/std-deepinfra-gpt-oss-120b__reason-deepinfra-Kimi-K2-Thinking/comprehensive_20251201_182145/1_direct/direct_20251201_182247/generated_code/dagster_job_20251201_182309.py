from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict, Optional

import requests
from dagster import (
    Field,
    InitResourceContext,
    InputDefinition,
    Nothing,
    Output,
    OutputDefinition,
    ResourceDefinition,
    String,
    op,
    job,
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)


# ----------------------------------------------------------------------
# Resource stubs
# ----------------------------------------------------------------------


class S3Resource:
    """Minimal stub for AWS S3 interactions."""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def upload_file(self, file_path: str, key: str) -> None:
        logger.info("Uploading %s to s3://%s/%s", file_path, self.bucket_name, key)
        # In a real implementation, use boto3.client('s3').upload_file(...)
        # Here we just simulate success.


def s3_resource_factory(init_context: InitResourceContext) -> S3Resource:
    bucket = init_context.resource_config.get("bucket", "default-bucket")
    return S3Resource(bucket_name=bucket)


class SparkResource:
    """Minimal stub for Spark session."""

    def __init__(self, jar_path: str):
        self.jar_path = jar_path

    def submit_job(self, job_name: str, args: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        logger.info("Submitting Spark job %s with args %s", job_name, args)
        # Simulate Spark job execution and return a result dict.
        return {"status": "success", "job_name": job_name, "output_path": "/tmp/spark_output"}


def spark_resource_factory(init_context: InitResourceContext) -> SparkResource:
    jar = init_context.resource_config.get("spark_jar", "/path/to/default.jar")
    return SparkResource(jar_path=jar)


class ElasticsearchResource:
    """Minimal stub for Elasticsearch client."""

    def __init__(self, host: str):
        self.host = host

    def index_documents(self, index_name: str, docs: Any) -> None:
        logger.info("Indexing documents into %s at %s", index_name, self.host)
        # Simulate indexing.


def elasticsearch_resource_factory(init_context: InitResourceContext) -> ElasticsearchResource:
    host = init_context.resource_config.get("host", "http://localhost:9200")
    return ElasticsearchResource(host=host)


class SlackResource:
    """Minimal stub for Slack webhook."""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_message(self, text: str) -> None:
        logger.info("Sending Slack message: %s", text)
        payload = {"text": text}
        try:
            requests.post(self.webhook_url, json=payload, timeout=10)
        except Exception as exc:  # pragma: no cover
            logger.error("Failed to send Slack message: %s", exc)


def slack_resource_factory(init_context: InitResourceContext) -> SlackResource:
    url = init_context.resource_config.get("webhook_url", "")
    return SlackResource(webhook_url=url)


# ----------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------


@op(
    config_schema={"color": Field(String, is_required=True, description="Color parameter")},
    out=OutputDefinition(Nothing),
)
def params_validate(context) -> Nothing:
    """Validate the provided color parameter."""
    color = context.op_config["color"]
    allowed = {"red", "green", "blue"}
    if color not in allowed:
        raise ValueError(f"Invalid color '{color}'. Allowed values are {allowed}.")
    logger.info("Parameter validation passed for color: %s", color)
    return Nothing


@op(
    required_resource_keys={"s3"},
    out=OutputDefinition(dict),
)
def download_mondo_terms(context) -> Dict[str, Any]:
    """
    Download the latest Mondo OBO file from GitHub releases.
    If a newer version exists, upload it to S3.
    Returns a dict with version information.
    """
    # Simplified download logic.
    github_api = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
    try:
        response = requests.get(github_api, timeout=10)
        response.raise_for_status()
        release = response.json()
        version = release.get("tag_name", "unknown")
        asset_url = None
        for asset in release.get("assets", []):
            if asset.get("name", "").endswith(".obo"):
                asset_url = asset.get("browser_download_url")
                break
        if not asset_url:
            raise RuntimeError("No OBO asset found in the latest release.")
        logger.info("Latest Mondo version: %s", version)
        # Simulate download to a temporary file.
        local_path = f"/tmp/mondo-{version}.obo"
        with open(local_path, "w") as f:
            f.write("# Simulated OBO content")
        # Upload to S3.
        s3_key = f"mondo/{version}/mondo.obo"
        context.resources.s3.upload_file(local_path, s3_key)
        return {"version": version, "s3_key": s3_key}
    except Exception as exc:
        logger.error("Failed to download Mondo terms: %s", exc)
        raise


@op(
    required_resource_keys={"spark"},
    ins={"download_info": InputDefinition(dict)},
    out=OutputDefinition(dict),
)
def normalized_mondo_terms(context, download_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Process the downloaded OBO file using Spark to normalize ontology terms.
    Returns metadata about the normalized output.
    """
    version = download_info.get("version", "unknown")
    job_name = f"normalize_mondo_{version}"
    args = {"input_s3_key": download_info.get("s3_key"), "output_path": f"/data/normalized/{version}"}
    result = context.resources.spark.submit_job(job_name, args)
    if result.get("status") != "success":
        raise RuntimeError(f"Spark job {job_name} failed.")
    logger.info("Normalization completed for version %s", version)
    return {"version": version, "normalized_path": args["output_path"]}


@op(
    required_resource_keys={"spark", "elasticsearch"},
    ins={"norm_info": InputDefinition(dict)},
    out=OutputDefinition(dict),
)
def index_mondo_terms(context, norm_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Index the normalized Mondo terms into Elasticsearch using Spark.
    Returns indexing metadata.
    """
    version = norm_info.get("version", "unknown")
    job_name = f"index_mondo_{version}"
    args = {
        "input_path": norm_info.get("normalized_path"),
        "es_index": f"mondo-{version}",
    }
    result = context.resources.spark.submit_job(job_name, args)
    if result.get("status") != "success":
        raise RuntimeError(f"Spark indexing job {job_name} failed.")
    # Simulate sending documents to Elasticsearch.
    docs = []  # In real case, load from Spark output.
    context.resources.elasticsearch.index_documents(args["es_index"], docs)
    logger.info("Indexing completed for version %s into index %s", version, args["es_index"])
    return {"version": version, "es_index": args["es_index"]}


@op(
    ins={"index_info": InputDefinition(dict)},
    out=OutputDefinition(Nothing),
)
def publish_mondo(context, index_info: Dict[str, Any]) -> Nothing:
    """
    Publish the indexed Mondo data to make it available for consumption.
    This is a placeholder for any post‑indexing actions.
    """
    version = index_info.get("version", "unknown")
    es_index = index_info.get("es_index", "unknown")
    logger.info("Publishing Mondo data version %s from index %s", version, es_index)
    # Placeholder: could update a catalog, send a message, etc.
    return Nothing


@op(
    required_resource_keys={"slack"},
    out=OutputDefinition(Nothing),
)
def slack(context) -> Nothing:
    """
    Send a Slack notification indicating pipeline completion.
    """
    message = "Mondo ETL pipeline completed successfully."
    context.resources.slack.send_message(message)
    logger.info("Slack notification sent.")
    return Nothing


# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------


@job(
    resource_defs={
        "s3": ResourceDefinition.resource_fn(s3_resource_factory, config_schema={"bucket": String}),
        "spark": ResourceDefinition.resource_fn(
            spark_resource_factory,
            config_schema={"spark_jar": String},
        ),
        "elasticsearch": ResourceDefinition.resource_fn(
            elasticsearch_resource_factory,
            config_schema={"host": String},
        ),
        "slack": ResourceDefinition.resource_fn(
            slack_resource_factory,
            config_schema={"webhook_url": String},
        ),
    },
    config={
        "ops": {
            "params_validate": {"config": {"color": "green"}},
            "download_mondo_terms": {"config": {}},
            "normalized_mondo_terms": {"config": {}},
            "index_mondo_terms": {"config": {}},
            "publish_mondo": {"config": {}},
            "slack": {"config": {}},
        },
        "resources": {
            "s3": {"config": {"bucket": "my-mondo-bucket"}},
            "spark": {"config": {"spark_jar": "/opt/spark/jars/mondo.jar"}},
            "elasticsearch": {"config": {"host": "http://es.example.com:9200"}},
            "slack": {"config": {"webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX"}},
        },
    },
)
def mondo_etl_job():
    """Linear DAG for the Mondo ontology ETL pipeline."""
    validation = params_validate()
    download = download_mondo_terms()
    normalized = normalized_mondo_terms(download)
    indexed = index_mondo_terms(normalized)
    publish = publish_mondo(indexed)
    slack(publish)


# ----------------------------------------------------------------------
# Entry point for local execution
# ----------------------------------------------------------------------


if __name__ == "__main__":
    result = mondo_etl_job.execute_in_process()
    if result.success:
        logger.info("Pipeline executed successfully.")
        sys.exit(0)
    else:
        logger.error("Pipeline execution failed.")
        sys.exit(1)