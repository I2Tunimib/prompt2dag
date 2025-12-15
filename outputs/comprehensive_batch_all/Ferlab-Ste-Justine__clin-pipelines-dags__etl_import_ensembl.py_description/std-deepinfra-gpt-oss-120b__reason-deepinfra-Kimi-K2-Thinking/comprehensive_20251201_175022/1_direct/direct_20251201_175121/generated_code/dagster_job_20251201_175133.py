from dataclasses import dataclass
from typing import List, Dict

import ftplib
import logging

from dagster import (
    Config,
    ConfigurableResource,
    In,
    Nothing,
    Out,
    OpExecutionContext,
    ResourceDefinition,
    op,
    job,
)


@dataclass
class EnsemblFileConfig(Config):
    ftp_host: str = "ftp.ensembl.org"
    data_types: List[str] = (
        "canonical",
        "ena",
        "entrez",
        "refseq",
        "uniprot",
    )
    s3_bucket: str = "my-data-lake"
    s3_prefix: str = "raw/landing/ensembl/"


class S3Resource(ConfigurableResource):
    """Minimal stub for S3 interactions."""

    bucket: str
    prefix: str

    def get_latest_version(self, data_type: str) -> str:
        """Retrieve the latest version stored in S3 for a given data type.

        In a real implementation this would read a metadata file or list objects.
        Here we return a fixed placeholder.
        """
        return "v0"

    def upload_file(self, local_path: str, remote_path: str) -> None:
        """Upload a file to S3. Stub implementation."""
        logging.getLogger(__name__).info(
            "Uploading %s to s3://%s/%s", local_path, self.bucket, remote_path
        )


class SparkResource(ConfigurableResource):
    """Stub for Spark job submission."""

    def submit_job(self, job_name: str, args: Dict[str, str]) -> None:
        """Submit a Spark job. Stub implementation."""
        logging.getLogger(__name__).info(
            "Submitting Spark job %s with args %s", job_name, args
        )


@op(
    out={"has_updates": Out(bool)},
    required_resource_keys={"s3"},
    config_schema=EnsemblFileConfig,
    description="Check Ensembl FTP for new versions, download, validate, and upload to S3.",
)
def check_and_download_files(context: OpExecutionContext) -> bool:
    cfg: EnsemblFileConfig = context.op_config
    s3: S3Resource = context.resources.s3

    logger = context.log
    has_updates = False

    try:
        ftp = ftplib.FTP(cfg.ftp_host)
        ftp.login()
        logger.info("Connected to FTP host %s", cfg.ftp_host)
    except Exception as exc:
        raise RuntimeError(f"Failed to connect to FTP: {exc}") from exc

    for data_type in cfg.data_types:
        # Example path: /pub/current_fasta/homo_sapiens/...
        # For simplicity we assume a file naming convention: {data_type}_mapping_v{N}.tsv
        remote_dir = f"/pub/current_tsv/homo_sapiens/{data_type}"
        try:
            ftp.cwd(remote_dir)
            files = ftp.nlst()
        except ftplib.error_perm:
            logger.warning("Directory %s does not exist on FTP, skipping.", remote_dir)
            continue

        # Find the file with the highest version number
        latest_file = None
        latest_version = -1
        for fname in files:
            if not fname.endswith(".tsv"):
                continue
            # Expected pattern: {data_type}_mapping_v{N}.tsv
            parts = fname.rstrip(".tsv").split("_v")
            if len(parts) != 2:
                continue
            try:
                version = int(parts[1])
                if version > latest_version:
                    latest_version = version
                    latest_file = fname
            except ValueError:
                continue

        if latest_file is None:
            logger.info("No TSV files found for %s", data_type)
            continue

        s3_version = s3.get_latest_version(data_type)
        logger.info(
            "Latest FTP version for %s: v%d, S3 version: %s", data_type, latest_version, s3_version
        )
        if f"v{latest_version}" != s3_version:
            # New version detected
            has_updates = True
            local_path = f"/tmp/{latest_file}"
            with open(local_path, "wb") as lf:
                ftp.retrbinary(f"RETR {latest_file}", lf.write)
            logger.info("Downloaded %s to %s", latest_file, local_path)

            # Simple MD5 checksum validation could be added here

            remote_path = f"{cfg.s3_prefix}{data_type}/{latest_file}"
            s3.upload_file(local_path, remote_path)
            logger.info("Uploaded %s to S3 at %s", latest_file, remote_path)
        else:
            logger.info("No new version for %s", data_type)

    ftp.quit()
    if not has_updates:
        logger.info("No updates found for any data type; downstream processing will be skipped.")
    return has_updates


@op(
    ins={"has_updates": In(bool)},
    required_resource_keys={"spark"},
    description="Run Spark job to process newly landed Ensembl files.",
)
def process_with_spark(context: OpExecutionContext, has_updates: bool) -> Nothing:
    if not has_updates:
        context.log.info("Skipping Spark processing because no new data was found.")
        return Nothing

    spark: SparkResource = context.resources.spark
    job_name = "ensembl_mapping_spark_job"
    args = {
        "input_path": "s3://my-data-lake/raw/landing/ensembl/",
        "output_table": "ensembl_mapping",
    }
    context.log.info("Starting Spark job to process Ensembl data.")
    spark.submit_job(job_name, args)
    context.log.info("Spark job completed successfully.")
    return Nothing


@job(
    resource_defs={
        "s3": ResourceDefinition.hardcoded_resource(
            S3Resource(bucket="my-data-lake", prefix="raw/landing/ensembl/")
        ),
        "spark": ResourceDefinition.hardcoded_resource(SparkResource()),
    },
)
def ensembl_etl_job():
    has_updates = check_and_download_files()
    process_with_spark(has_updates)


if __name__ == "__main__":
    result = ensembl_etl_job.execute_in_process()
    if result.success:
        print("Ensembl ETL job completed successfully.")
    else:
        print("Ensembl ETL job failed.")