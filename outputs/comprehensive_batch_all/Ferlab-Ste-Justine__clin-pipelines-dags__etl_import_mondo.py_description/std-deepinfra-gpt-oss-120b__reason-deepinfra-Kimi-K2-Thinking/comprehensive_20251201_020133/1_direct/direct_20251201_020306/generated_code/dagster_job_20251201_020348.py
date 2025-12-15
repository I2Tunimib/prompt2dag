from pathlib import Path
import json
import logging
import tempfile
import requests

from dagster import (
    op,
    job,
    In,
    Out,
    Nothing,
    ConfigurableResource,
    ResourceDefinition,
    get_dagster_logger,
)


class S3Resource(ConfigurableResource):
    """Minimal stub for S3 interactions."""

    bucket: str

    def upload_file(self, file_path: str, key: str) -> None:
        logger = get_dagster_logger()
        logger.info(f"Uploading {file_path} to s3://{self.bucket}/{key}")

    def download_file(self, key: str, dest_path: str) -> None:
        logger = get_dagster_logger()
        logger.info(f"Downloading s3://{self.bucket}/{key} to {dest_path}")


class SlackResource(ConfigurableResource):
    """Minimal stub for Slack webhook notifications."""

    webhook_url: str

    def send_message(self, text: str) -> None:
        logger = get_dagster_logger()
        logger.info(f"Sending Slack message: {text}")
        # In a real implementation you would POST to the webhook URL:
        # requests.post(self.webhook_url, json={"text": text})


class SparkResource(ConfigurableResource):
    """Minimal stub for Spark job execution."""

    spark_jar: str
    obo_parser_spark_jar: str

    def run_job(self, job_name: str, args: dict) -> None:
        logger = get_dagster_logger()
        logger.info(f"Running Spark job '{job_name}' with args: {json.dumps(args)}")
        # Real implementation would submit a Spark job via spark-submit or a cluster API.


@op(
    out=Out(str),
    config_schema={"color": str},
    description="Validates the color parameter provided to the pipeline.",
)
def params_validate(context) -> str:
    color = context.op_config["color"]
    if not color:
        raise ValueError("Color parameter must be a non‑empty string.")
    context.log.info(f"Validated color parameter: {color}")
    return color


@op(
    ins={"color": In(str)},
    out=Out(str),
    config_schema={
        "s3_bucket": str,
        "s3_key": str,
        "github_repo": str,
        "github_api_url": str,
    },
    description="Downloads the latest Mondo OBO file and uploads it to S3 if newer.",
)
def download_mondo_terms(context, color: str) -> str:
    cfg = context.op_config
    bucket = cfg["s3_bucket"]
    key = cfg["s3_key"]
    repo = cfg.get("github_repo", "monarch-initiative/mondo")
    api_url = cfg.get(
        "github_api_url", f"https://api.github.com/repos/{repo}/releases/latest"
    )

    logger = context.log
    logger.info(f"Fetching latest release info from {api_url}")

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()
    release_info = response.json()
    asset = next(
        (a for a in release_info["assets"] if a["name"].endswith(".obo")), None
    )
    if not asset:
        raise RuntimeError("No OBO asset found in the latest release.")

    download_url = asset["browser_download_url"]
    logger.info(f"Downloading OBO file from {download_url}")

    with tempfile.NamedTemporaryFile(delete=False, suffix=".obo") as tmp_file:
        download_resp = requests.get(download_url, timeout=60)
        download_resp.raise_for_status()
        tmp_file.write(download_resp.content)
        local_path = tmp_file.name

    # Upload to S3 (stub)
    s3: S3Resource = context.resources.s3
    s3.upload_file(local_path, key)

    logger.info(f"Downloaded and uploaded OBO file to s3://{bucket}/{key}")
    return local_path


@op(
    ins={"obo_path": In(str)},
    out=Out(str),
    description="Processes the OBO file with Spark to normalize ontology terms.",
)
def normalized_mondo_terms(context, obo_path: str) -> str:
    spark: SparkResource = context.resources.spark
    output_path = str(Path(tempfile.gettempdir()) / "normalized_mondo.json")
    args = {
        "input_path": obo_path,
        "output_path": output_path,
        "spark_jar": spark.spark_jar,
        "obo_parser_spark_jar": spark.obo_parser_spark_jar,
    }
    spark.run_job("normalize_mondo", args)
    context.log.info(f"Normalized data written to {output_path}")
    return output_path


@op(
    ins={"normalized_path": In(str)},
    out=Out(str),
    description="Indexes the normalized Mondo terms to Elasticsearch using Spark.",
)
def index_mondo_terms(context, normalized_path: str) -> str:
    spark: SparkResource = context.resources.spark
    index_name = "mondo"
    args = {
        "input_path": normalized_path,
        "es_index": index_name,
        "spark_jar": spark.spark_jar,
    }
    spark.run_job("index_mondo", args)
    context.log.info(f"Indexed data to Elasticsearch index '{index_name}'")
    return index_name


@op(
    ins={"index_name": In(str)},
    out=Out(Nothing),
    description="Publishes the indexed Mondo data for consumption.",
)
def publish_mondo(context, index_name: str) -> Nothing:
    # Placeholder for any publishing steps (e.g., updating a catalog)
    context.log.info(f"Publishing Elasticsearch index '{index_name}'")
    return Nothing


@op(
    ins={"_": In(Nothing)},
    out=Out(Nothing),
    config_schema={"webhook_url": str},
    description="Sends a Slack notification indicating pipeline completion.",
)
def slack(context, _: Nothing) -> Nothing:
    webhook_url = context.op_config["webhook_url"]
    slack: SlackResource = context.resources.slack
    # Override resource config if needed
    if webhook_url:
        slack.webhook_url = webhook_url
    slack.send_message("Mondo ETL pipeline completed successfully.")
    return Nothing


@job(
    resource_defs={
        "s3": ResourceDefinition.hardcoded_resource(S3Resource(bucket="my-bucket")),
        "slack": ResourceDefinition.hardcoded_resource(
            SlackResource(webhook_url="https://hooks.slack.com/services/T000/B000/XXXX")
        ),
        "spark": ResourceDefinition.hardcoded_resource(
            SparkResource(
                spark_jar="s3://my-bucket/jars/spark.jar",
                obo_parser_spark_jar="s3://my-bucket/jars/obo_parser.jar",
            )
        ),
    }
)
def mondo_etl_job():
    color = params_validate()
    obo_path = download_mondo_terms(color)
    normalized_path = normalized_mondo_terms(obo_path)
    index_name = index_mondo_terms(normalized_path)
    publish_mondo(index_name)
    slack(publish_mondo(index_name))


if __name__ == "__main__":
    result = mondo_etl_job.execute_in_process(
        run_config={
            "ops": {
                "params_validate": {"config": {"color": "blue"}},
                "download_mondo_terms": {
                    "config": {
                        "s3_bucket": "my-bucket",
                        "s3_key": "raw/mondo.obo",
                        "github_repo": "monarch-initiative/mondo",
                        "github_api_url": "https://api.github.com/repos/monarch-initiative/mondo/releases/latest",
                    }
                },
                "slack": {"config": {"webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX"}},
            },
            "resources": {
                "s3": {"config": {"bucket": "my-bucket"}},
                "slack": {"config": {"webhook_url": "https://hooks.slack.com/services/T000/B000/XXXX"}},
                "spark": {
                    "config": {
                        "spark_jar": "s3://my-bucket/jars/spark.jar",
                        "obo_parser_spark_jar": "s3://my-bucket/jars/obo_parser.jar",
                    }
                },
            },
        }
    )
    if result.success:
        logging.info("Mondo ETL job completed successfully.")
    else:
        logging.error("Mondo ETL job failed.")