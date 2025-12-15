import logging
from pathlib import Path
from typing import Dict, List

import ftplib
import hashlib

from dagster import (
    Config,
    Field,
    In,
    Out,
    OpExecutionContext,
    Output,
    ResourceDefinition,
    String,
    job,
    op,
    resource,
)

# ----------------------------------------------------------------------
# Resources (stubs for illustration)
# ----------------------------------------------------------------------


@resource(config_schema={"bucket_name": String})
def s3_resource(init_context) -> Dict:
    """A minimal stub for an S3 client.

    In a real deployment this would return a boto3 client configured with
    appropriate credentials.
    """
    bucket_name = init_context.resource_config["bucket_name"]
    init_context.log.info(f"Initialized S3 resource for bucket: {bucket_name}")
    return {"bucket_name": bucket_name}


@resource(config_schema={"spark_master": String, "app_name": String})
def spark_resource(init_context) -> Dict:
    """A minimal stub for a Spark session.

    In production this would create a SparkSession configured for the
    target cluster.
    """
    master = init_context.resource_config["spark_master"]
    app_name = init_context.resource_config["app_name"]
    init_context.log.info(f"Initialized Spark resource: master={master}, app_name={app_name}")
    return {"master": master, "app_name": app_name}


# ----------------------------------------------------------------------
# Configurations
# ----------------------------------------------------------------------


class FileCheckConfig(Config):
    ftp_host: str = Field(default="ftp.ensembl.org", description="Ensembl FTP host")
    data_types: List[str] = Field(
        default_factory=lambda: ["canonical", "ena", "entrez", "refseq", "uniprot"],
        description="Data types to check for updates",
    )
    s3_prefix: str = Field(default="raw/landing/ensembl/", description="S3 landing prefix")
    version_file: str = Field(
        default="latest_versions.json",
        description="JSON file in S3 that stores the last processed version per data type",
    )


# ----------------------------------------------------------------------
# Ops
# ----------------------------------------------------------------------


@op(
    config_schema=FileCheckConfig,
    required_resource_keys={"s3"},
    out={"has_updates": Out(bool)},
    description="Check Ensembl FTP for newer files, download, validate, and upload to S3.",
)
def check_and_download_files(context: OpExecutionContext) -> Output[bool]:
    cfg: FileCheckConfig = context.op_config
    ftp_host = cfg.ftp_host
    data_types = cfg.data_types
    s3_prefix = cfg.s3_prefix.rstrip("/") + "/"
    s3 = context.resources.s3

    # In a real implementation we would read the JSON version file from S3.
    # Here we simulate an empty dict meaning no previous versions.
    previous_versions: Dict[str, str] = {}

    has_updates = False

    try:
        with ftplib.FTP(ftp_host) as ftp:
            ftp.login()
            context.log.info(f"Connected to FTP host {ftp_host}")

            for data_type in data_types:
                remote_dir = f"/pub/current/{data_type}"
                try:
                    ftp.cwd(remote_dir)
                except ftplib.error_perm:
                    context.log.warning(f"Remote directory {remote_dir} does not exist; skipping.")
                    continue

                files = ftp.nlst()
                if not files:
                    context.log.info(f"No files found for {data_type} in {remote_dir}")
                    continue

                # Assume the latest file is the one with the highest lexical order.
                latest_file = sorted(files)[-1]
                latest_version = latest_file.split(".")[0]  # simplistic version extraction

                prev_version = previous_versions.get(data_type)
                if prev_version == latest_version:
                    context.log.info(f"No new version for {data_type} (still {prev_version})")
                    continue

                context.log.info(f"New version detected for {data_type}: {latest_version}")
                has_updates = True

                # Download the file to a temporary location.
                local_path = Path("/tmp") / latest_file
                with open(local_path, "wb") as lf:
                    ftp.retrbinary(f"RETR {latest_file}", lf.write)
                context.log.info(f"Downloaded {latest_file} to {local_path}")

                # Compute MD5 checksum.
                md5_hash = hashlib.md5()
                with open(local_path, "rb") as f:
                    for chunk in iter(lambda: f.read(8192), b""):
                        md5_hash.update(chunk)
                checksum = md5_hash.hexdigest()
                context.log.info(f"MD5 checksum for {latest_file}: {checksum}")

                # Upload to S3 (stubbed).
                s3_key = f"{s3_prefix}{data_type}/{latest_file}"
                context.log.info(f"Pretending to upload {local_path} to s3://{s3['bucket_name']}/{s3_key}")

                # Cleanup local file.
                local_path.unlink()
    except Exception as exc:
        context.log.error(f"Error during FTP processing: {exc}")
        raise

    return Output(has_updates, "has_updates")


@op(
    required_resource_keys={"spark"},
    ins={"has_updates": In(bool)},
    description="Run Spark job to process newly landed Ensembl files.",
)
def process_with_spark(context: OpExecutionContext, has_updates: bool) -> None:
    if not has_updates:
        context.log.info("No new data detected; skipping Spark processing.")
        return

    spark_cfg = context.resources.spark
    context.log.info(
        f"Launching Spark job with master={spark_cfg['master']} and app_name={spark_cfg['app_name']}"
    )
    # In a real implementation we would create a SparkSession and run the ETL logic.
    # Here we simply log the intended actions.
    context.log.info("Reading raw TSV files from S3, applying transformations, and writing to 'ensembl_mapping' table.")
    # Simulate job duration.
    import time

    time.sleep(2)
    context.log.info("Spark processing completed successfully.")


# ----------------------------------------------------------------------
# Job definition
# ----------------------------------------------------------------------


@job(
    resource_defs={"s3": s3_resource, "spark": spark_resource},
    description="ETL pipeline that syncs Ensembl mapping files to S3 and processes them with Spark.",
)
def ensembl_etl_job():
    has_updates = check_and_download_files()
    process_with_spark(has_updates)


# ----------------------------------------------------------------------
# Entry point for local execution
# ----------------------------------------------------------------------


if __name__ == "__main__":
    result = ensembl_etl_job.execute_in_process(
        run_config={
            "resources": {
                "s3": {"config": {"bucket_name": "my-data-lake"}},
                "spark": {"config": {"spark_master": "k8s://https://my-k8s-cluster", "app_name": "ensembl_etl"}},
            },
            "ops": {
                "check_and_download_files": {
                    "config": {
                        "ftp_host": "ftp.ensembl.org",
                        "data_types": ["canonical", "ena", "entrez", "refseq", "uniprot"],
                        "s3_prefix": "raw/landing/ensembl/",
                        "version_file": "latest_versions.json",
                    }
                }
            },
        }
    )
    if result.success:
        logging.info("Ensembl ETL job completed successfully.")
    else:
        logging.error("Ensembl ETL job failed.")