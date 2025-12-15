from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    ConfigurableResource,
    get_dagster_logger,
)
import hashlib
from typing import List, Dict, Any
from datetime import datetime

logger = get_dagster_logger()


class S3Resource(ConfigurableResource):
    """Resource for S3 operations."""
    bucket: str = "my-genomics-bucket"
    
    def list_objects(self, prefix: str) -> List[str]:
        return []
    
    def get_object_metadata(self, key: str) -> Dict[str, Any]:
        return {"etag": "mock-version"}
    
    def upload_file(self, local_path: str, key: str) -> None:
        logger.info(f"Uploaded {local_path} to s3://{self.bucket}/{key}")


class SlackResource(ConfigurableResource):
    """Resource for Slack notifications."""
    webhook_url: str = "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
    
    def post_message(self, message: str) -> None:
        logger.info(f"SLACK: {message}")


class FTPResource(ConfigurableResource):
    """Resource for FTP operations."""
    host: str = "ftp.ensembl.org"
    
    def download_file(self, remote_path: str, local_path: str) -> None:
        logger.info(f"Downloaded {remote_path} to {local_path}")


class EnsemblFileConfig(Config):
    """Configuration for Ensembl file processing."""
    data_types: List[str] = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    ftp_base_path: str = "/pub/current_tsv/homo_sapiens"
    s3_landing_prefix: str = "raw/landing/ensembl/"


class SparkProcessingConfig(Config):
    """Configuration for Spark processing."""
    spark_app_name: str = "ensembl_mapping_processor"
    output_table: str = "ensembl_mapping"
    s3_processed_prefix: str = "processed/ensembl/"


def validate_md5_checksum(file_path: str, expected_md5: str) -> bool:
    """Validate MD5 checksum of a file."""
    try:
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest() == expected_md5
    except FileNotFoundError:
        logger.warning(f"File not found for MD5 validation: {file_path}")
        return False


@op
def check_and_download_ensembl_files(
    context: OpExecutionContext,
    s3: S3Resource,
    ftp: FTPResource,
    slack: SlackResource,
    config: EnsemblFileConfig,
) -> List[str]:
    """
    Check for new Ensembl data versions and download updated files.
    
    Returns list of S3 paths for updated files. Empty list means no updates.
    """
    slack.post_message(
        f"Ensembl pipeline started at {datetime.now().isoformat()}. "
        f"Checking for updates in {', '.join(config.data_types)}"
    )
    
    updated_files = []
    
    for data_type in config.data_types:
        context.log.info(f"Checking data type: {data_type}")
        
        current_version = s3.get_object_metadata(
            f"{config.s3_landing_prefix}{data_type}.txt.gz"
        ).get("etag", "")
        
        has_update = current_version != f"new-version-{data_type}"
        
        if has_update:
            context.log.info(f"New version found for {data_type}")
            
            local_file = f"/tmp/{data_type}.txt.gz"
            ftp.download_file(
                f"{config.ftp_base_path}/{data_type}.txt.gz",
                local_file
            )
            
            if validate_md5_checksum(local_file, f"mock-md5-{data_type}"):
                context.log.info(f"MD5 validation passed for {data_type}")
                
                s3_key = f"{config.s3_landing_prefix}{data_type}.txt.gz"
                s3.upload_file(local_file, s3_key)
                updated_files.append(f"s3://{s3.bucket}/{s3_key}")
                
                slack.post_message(f"✅ Updated {data_type}")
            else:
                error_msg = f"MD5 validation failed for {data_type}"
                context.log.error(error_msg)
                slack.post_message(f"❌ {error_msg}")
                raise RuntimeError(error_msg)
        else:
            context.log.info(f"No update needed for {data_type}")
    
    if not updated_files:
        context.log.info("No updates found for any data type. Skipping Spark processing.")
        slack.post_message("ℹ️ No updates found. Skipping Spark processing.")
    
    return updated_files


@op
def process_ensembl_files_with_spark(
    context: OpExecutionContext,
    s3: S3Resource,
    slack: SlackResource,
    updated_files: List[str],
    config: SparkProcessingConfig,
) -> None:
    """
    Process updated Ensembl files using Spark.
    
    Only runs if updated_files list is not empty.
    """
    if not updated_files:
        context.log.info("No files to process. Skipping Spark job.")
        return
    
    slack.post_message(
        f"Starting Spark processing for {len(updated_files)} updated files"
    )
    
    try:
        context.log.info(f"Spark application: {config.spark_app_name}")
        context.log.info(f"Input files: {updated_files}")
        context.log.info(f"Output table: {config.output_table}")
        context.log.info(f"Output S3 prefix: {config.s3_processed_prefix}")
        
        for file_path in updated_files:
            context.log.info(f"Processing {file_path} with Spark")
        
        slack.post_message(
            f"✅ Spark processing completed. Data loaded to {config.output_table}"
        )
        
    except Exception as e:
        error_msg = f"Spark processing failed: {e}"
        context.log.error(error_msg)
        slack.post_message(f"❌ {error_msg}")
        raise


@job
def ensembl_etl_pipeline():
    """
    ETL pipeline for importing and processing Ensembl genomic mapping data.
    
    Linear workflow: check for updates -> process with Spark.
    """
    updated_files = check_and_download_ensembl_files()
    process_ensembl_files_with_spark(updated_files)


if __name__ == "__main__":
    result = ensembl_etl_pipeline.execute_in_process(
        run_config={
            "ops": {
                "check_and_download_ensembl_files": {
                    "config": {
                        "data_types": ["canonical", "ena", "entrez", "refseq", "uniprot"],
                        "ftp_base_path": "/pub/current_tsv/homo_sapiens",
                        "s3_landing_prefix": "raw/landing/ensembl/",
                    }
                },
                "process_ensembl_files_with_spark": {
                    "config": {
                        "spark_app_name": "ensembl_mapping_processor",
                        "output_table": "ensembl_mapping",
                        "s3_processed_prefix": "processed/ensembl/",
                    }
                },
            }
        }
    )
    
    if result.success:
        logger.info("Pipeline executed successfully!")
    else:
        logger.error("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                logger.error(f"Step failed: {event.step_key}")