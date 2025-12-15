from __future__ import annotations

import hashlib
import logging
import os
import random
import tempfile
from dataclasses import dataclass
from typing import List

from dagster import (
    ConfigurableResource,
    InitResourceContext,
    In,
    Nothing,
    Out,
    OpExecutionContext,
    ResourceDefinition,
    job,
    op,
)


logger = logging.getLogger(__name__)


@dataclass
class S3Config:
    bucket: str
    landing_prefix: str = "raw/landing/ensembl/"


class S3Resource(ConfigurableResource):
    """Minimal stub for S3 interactions."""

    config: S3Config

    def upload_file(self, local_path: str, s3_key: str) -> None:
        logger.info("Uploading %s to s3://%s/%s", local_path, self.config.bucket, s3_key)
        # In a real implementation, use boto3 or another SDK.
        # Here we just simulate the upload.
        pass

    def get_latest_version(self, data_type: str) -> str:
        """Return a simulated latest version string for a data type."""
        # In reality, this would query S3 for the latest version.
        return "v1.0"


class SparkResource(ConfigurableResource):
    """Minimal stub for Spark job execution."""

    def submit_job(self, files: List[str]) -> None:
        logger.info("Submitting Spark job for files: %s", files)
        # In a real implementation, submit to a Spark cluster.
        # Here we just simulate the execution.
        pass


class SlackResource(ConfigurableResource):
    """Minimal stub for Slack notifications."""

    webhook_url: str

    def send_message(self, text: str) -> None:
        logger.info("Sending Slack message: %s", text)
        # In a real implementation, post to Slack via webhook.
        pass


@op(
    out=Out(List[str]),
    required_resource_keys={"s3", "slack"},
    description="Check Ensembl FTP for new versions, download, validate, and upload to S3.",
)
def file(context: OpExecutionContext) -> List[str]:
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    ftp_host = "ftp.ensembl.org"
    updated: List[str] = []

    logger = context.log
    logger.info("Connecting to FTP host %s", ftp_host)

    # Simulate version checking: randomly decide if a new version exists.
    for dt in data_types:
        has_new_version = random.choice([True, False])
        if has_new_version:
            logger.info("New version detected for %s", dt)
            # Simulate download to a temporary file.
            with tempfile.NamedTemporaryFile(delete=False, suffix=".tsv") as tmp_file:
                tmp_path = tmp_file.name
                tmp_file.write(b"simulated,content\n")
            logger.info("Downloaded %s to %s", dt, tmp_path)

            # Simulate MD5 checksum validation.
            md5_hash = hashlib.md5()
            with open(tmp_path, "rb") as f:
                for chunk in iter(lambda: f.read(8192), b""):
                    md5_hash.update(chunk)
            checksum = md5_hash.hexdigest()
            logger.info("MD5 checksum for %s: %s", dt, checksum)

            # Upload to S3.
            s3_key = f"{context.resources.s3.config.landing_prefix}{dt}.tsv"
            context.resources.s3.upload_file(tmp_path, s3_key)
            updated.append(dt)

            # Clean up temporary file.
            os.remove(tmp_path)
        else:
            logger.info("No new version for %s", dt)

    if not updated:
        msg = "No updates found for any data type; skipping downstream processing."
        logger.info(msg)
        context.resources.slack.send_message(msg)

    return updated


@op(
    ins={"updated_data_types": In(List[str])},
    required_resource_keys={"spark", "slack"},
    description="Process newly landed Ensembl files with Spark and load into tables.",
)
def table(context: OpExecutionContext, updated_data_types: List[str]) -> Nothing:
    logger = context.log
    if not updated_data_types:
        logger.info("No updated files to process; exiting table step.")
        return Nothing

    logger.info("Processing data types: %s", ", ".join(updated_data_types))
    # Build list of S3 paths for Spark job.
    s3_paths = [
        f"s3://{context.resources.s3.config.bucket}/{context.resources.s3.config.landing_prefix}{dt}.tsv"
        for dt in updated_data_types
    ]
    context.resources.spark.submit_job(s3_paths)
    context.resources.slack.send_message(
        f"Ensembl mapping table refreshed for: {', '.join(updated_data_types)}"
    )
    return Nothing


@job(
    description="ETL pipeline that syncs Ensembl mapping files to S3 and processes them with Spark.",
    resource_defs={
        "s3": ResourceDefinition.hardcoded_resource(
            S3Resource(
                config=S3Config(bucket="my-data-lake")
            )
        ),
        "spark": ResourceDefinition.hardcoded_resource(SparkResource()),
        "slack": ResourceDefinition.hardcoded_resource(
            SlackResource(webhook_url="https://hooks.slack.com/services/XXX/YYY/ZZZ")
        ),
    },
)
def ensembl_etl_job():
    updated = file()
    table(updated_data_types=updated)


if __name__ == "__main__":
    result = ensembl_etl_job.execute_in_process()
    if result.success:
        logger.info("Pipeline completed successfully.")
    else:
        logger.error("Pipeline failed.")