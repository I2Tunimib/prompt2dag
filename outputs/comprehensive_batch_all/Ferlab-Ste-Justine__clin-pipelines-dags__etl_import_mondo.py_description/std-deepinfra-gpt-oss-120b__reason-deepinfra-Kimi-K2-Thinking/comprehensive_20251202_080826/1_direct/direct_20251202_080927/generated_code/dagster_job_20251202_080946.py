from __future__ import annotations

import json
import logging
import os
import sys
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dagster import (
    Config,
    InitResourceContext,
    InputDefinition,
    OutputDefinition,
    ResourceDefinition,
    String,
    op,
    job,
    Field,
    Nothing,
)

logger = logging.getLogger(__name__)
logging.basicConfig(stream=sys.stdout, level=logging.INFO)


# ----------------------------------------------------------------------
# Resource stubs
# ----------------------------------------------------------------------
class S3Resource:
    """Minimal stub for AWS S3 interactions."""

    def __init__(self, bucket_name: str):
        self.bucket_name = bucket_name

    def upload_file(self, local_path: str, s3_key: str) -> str:
        """Pretend to upload a file to S3 and return the S3 URI."""
        logger.info("Uploading %s to s3://%s/%s", local_path, self.bucket_name, s3_key)
        # In a real implementation, boto3 would be used here.
        return f"s3://{self.bucket_name}/{s3_key}"


class SparkResource:
    """Minimal stub for submitting Spark jobs."""

    def __init__(self, spark_jar: str, obo_parser_spark_jar: str):
        self.spark_jar = spark_jar
        self.obo_parser_spark_jar = obo_parser_spark_jar

    def submit_job(self, job_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """Pretend to run a Spark job and return a result dict."""
        logger.info("Submitting Spark job %s with args %s", job_name, args)
        # Real implementation would invoke spark-submit.
        return {"status": "success", "output_path": args.get("output_path", "/tmp/output")}


class ElasticsearchResource:
    """Minimal stub for Elasticsearch indexing."""

    def __init__(self, host: str):
        self.host = host

    def index_data(self, index_name: str, data_path: str) -> str:
        """Pretend to index data and return the index name."""
        logger.info("Indexing data from %s into Elasticsearch index %s", data_path, index_name)
        # Real implementation would use elasticsearch-py.
        return index_name


class SlackResource:
    """Minimal stub for sending Slack messages."""

    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url

    def send_message(self, text: str) -> None:
        """Pretend to send a Slack message."""
        logger.info("Sending Slack message: %s", text)
        # Real implementation would POST to the webhook URL.


# ----------------------------------------------------------------------
# Config schemas
# ----------------------------------------------------------------------
@dataclass
class PipelineConfig(Config):
    color: str = Field(
        default="blue",
        description="A color parameter used for validation; allowed values are blue, green, red.",
    )
    spark_jar: str = Field(default="/opt/spark/jars/spark-job.jar")
    obo_parser_spark_jar: str = Field(default="/opt/spark/jars/obo-parser.jar")
    s3_bucket: str = Field(default="my-mondo-bucket")
    elasticsearch_host: str = Field(default="http://localhost:9200")
    slack_webhook_url: str = Field(default="https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX")


# ----------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------
@op(
    config_schema={"color": String},
    out={"valid": OutputDefinition(Nothing)},
    description="Validates the color parameter provided to the pipeline.",
)
def params_validate(context: OpExecutionContext) -> Nothing:
    allowed = {"blue", "green", "red"}
    color = context.op_config["color"]
    if color not in allowed:
        raise ValueError(f"Invalid color '{color}'. Allowed values are {allowed}.")
    logger.info("Parameter validation succeeded for color: %s", color)
    return Nothing


@op(
    required_resource_keys={"s3"},
    out={"s3_path": OutputDefinition(String)},
    description="Downloads the latest Mondo OBO file and uploads it to S3 if newer.",
)
def download_mondo_terms(context: OpExecutionContext) -> str:
    # Stub implementation: pretend we downloaded a file.
    local_path = "/tmp/mondo.obo"
    logger.info("Downloading latest Mondo OBO file to %s (stub)", local_path)

    # Determine a fake version check; always upload for demo.
    s3_key = "raw/mondo.obo"
    s3_path = context.resources.s3.upload_file(local_path, s3_key)
    logger.info("Uploaded OBO file to %s", s3_path)
    return s3_path


@op(
    required_resource_keys={"spark"},
    ins={"obo_path": InputDefinition(String)},
    out={"normalized_path": OutputDefinition(String)},
    description="Processes the OBO file with Spark to normalize ontology terms.",
)
def normalized_mondo_terms(context: OpExecutionContext, obo_path: str) -> str:
    job_name = "normalize_mondo_terms"
    output_path = "/tmp/normalized_mondo.json"
    args = {
        "input_path": obo_path,
        "output_path": output_path,
        "spark_jar": context.resources.spark.spark_jar,
        "obo_parser_jar": context.resources.spark.obo_parser_spark_jar,
    }
    result = context.resources.spark.submit_job(job_name, args)
    if result.get("status") != "success":
        raise RuntimeError("Spark normalization job failed")
    logger.info("Normalization completed, output at %s", output_path)
    return output_path


@op(
    required_resource_keys={"spark", "elasticsearch"},
    ins={"norm_path": InputDefinition(String)},
    out={"index_name": OutputDefinition(String)},
    description="Indexes the normalized Mondo terms into Elasticsearch using Spark.",
)
def index_mondo_terms(context: OpExecutionContext, norm_path: str) -> str:
    job_name = "index_mondo_terms"
    index_name = "mondo"
    args = {
        "input_path": norm_path,
        "index_name": index_name,
        "spark_jar": context.resources.spark.spark_jar,
    }
    result = context.resources.spark.submit_job(job_name, args)
    if result.get("status") != "success":
        raise RuntimeError("Spark indexing job failed")
    indexed = context.resources.elasticsearch.index_data(index_name, norm_path)
    logger.info("Data indexed into Elasticsearch index %s", indexed)
    return indexed


@op(
    ins={"index_name": InputDefinition(String)},
    out={"published": OutputDefinition(Nothing)},
    description="Publishes the indexed Mondo data for consumption.",
)
def publish_mondo(context: OpExecutionContext, index_name: str) -> Nothing:
    # Stub: In a real system this might update a catalog or set a flag.
    logger.info("Publishing Elasticsearch index %s (stub)", index_name)
    return Nothing


@op(
    required_resource_keys={"slack"},
    ins={"_": InputDefinition(Nothing)},
    out={"slack_sent": OutputDefinition(Nothing)},
    description="Sends a Slack notification indicating pipeline completion.",
)
def slack(context: OpExecutionContext, _: Nothing) -> Nothing:
    message = "Mondo ETL pipeline completed successfully."
    context.resources.slack.send_message(message)
    logger.info("Slack notification sent.")
    return Nothing


# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------
@job(
    config=PipelineConfig,
    resource_defs={
        "s3": ResourceDefinition(
            resource_fn=lambda init_context: S3Resource(bucket_name=init_context.resource_config["s3_bucket"]),
            config_schema={"s3_bucket": String},
        ),
        "spark": ResourceDefinition(
            resource_fn=lambda init_context: SparkResource(
                spark_jar=init_context.resource_config["spark_jar"],
                obo_parser_spark_jar=init_context.resource_config["obo_parser_spark_jar"],
            ),
            config_schema={"spark_jar": String, "obo_parser_spark_jar": String},
        ),
        "elasticsearch": ResourceDefinition(
            resource_fn=lambda init_context: ElasticsearchResource(host=init_context.resource_config["elasticsearch_host"]),
            config_schema={"elasticsearch_host": String},
        ),
        "slack": ResourceDefinition(
            resource_fn=lambda init_context: SlackResource(webhook_url=init_context.resource_config["slack_webhook_url"]),
            config_schema={"slack_webhook_url": String},
        ),
    },
)
def mondo_etl_job():
    """Linear ETL pipeline that downloads, normalizes, indexes, publishes, and notifies."""
    validated = params_validate()
    obo_s3_path = download_mondo_terms()
    normalized_path = normalized_mondo_terms(obo_s3_path)
    index_name = index_mondo_terms(normalized_path)
    publish_mondo(index_name)
    slack(Nothing)


if __name__ == "__main__":
    result = mondo_etl_job.execute_in_process(
        run_config={
            "ops": {
                "params_validate": {"config": {"color": "blue"}},
            },
            "resources": {
                "s3": {"config": {"s3_bucket": "my-mondo-bucket"}},
                "spark": {
                    "config": {
                        "spark_jar": "/opt/spark/jars/spark-job.jar",
                        "obo_parser_spark_jar": "/opt/spark/jars/obo-parser.jar",
                    }
                },
                "elasticsearch": {"config": {"elasticsearch_host": "http://localhost:9200"}},
                "slack": {"config": {"slack_webhook_url": "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"}},
            },
        }
    )
    if result.success:
        logger.info("Pipeline executed successfully.")
    else:
        logger.error("Pipeline failed.")