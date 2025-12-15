import logging
import os
import random
import time
from typing import List

from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ENSEMBL_FTP_BASE = "ftp.ensembl.org"
S3_LANDING_PREFIX = "raw/landing/ensembl/"
DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]
SLACK_WEBHOOK_BLOCK_NAME = "slack-webhook"


@task(name="notify_slack_start")
def notify_slack_start() -> None:
    """Send Slack notification when pipeline starts."""
    try:
        slack_webhook = SlackWebhook.load(SLACK_WEBHOOK_BLOCK_NAME)
        slack_webhook.notify("🚀 Ensembl ETL pipeline started")
        logger.info("Slack start notification sent")
    except Exception as e:
        logger.warning(f"Failed to send Slack start notification: {e}")


@task(name="notify_slack_failure")
def notify_slack_failure(error_message: str) -> None:
    """Send Slack notification on pipeline failure."""
    try:
        slack_webhook = SlackWebhook.load(SLACK_WEBHOOK_BLOCK_NAME)
        slack_webhook.notify(f"❌ Ensembl ETL pipeline failed: {error_message}")
        logger.info("Slack failure notification sent")
    except Exception as e:
        logger.warning(f"Failed to send Slack failure notification: {e}")


@task(name="notify_slack_completion")
def notify_slack_completion() -> None:
    """Send Slack notification on pipeline completion."""
    try:
        slack_webhook = SlackWebhook.load(SLACK_WEBHOOK_BLOCK_NAME)
        slack_webhook.notify("✅ Ensembl ETL pipeline completed successfully")
        logger.info("Slack completion notification sent")
    except Exception as e:
        logger.warning(f"Failed to send Slack completion notification: {e}")


@task(name="check_and_upload_new_files", retries=3, retry_delay_seconds=60)
def check_and_upload_new_files() -> List[str]:
    """
    Check Ensembl FTP for new data versions and upload to S3 if updated.
    
    Returns:
        List[str]: List of data types that were updated and uploaded
    """
    logger.info("Starting Ensembl FTP check and S3 upload process...")
    
    updated_types = []
    
    for data_type in DATA_TYPES:
        logger.info(f"Checking data type: {data_type}")
        
        # Simulate FTP check and S3 version comparison
        # Production implementation would use ftplib and boto3
        if random.random() < 0.3:  # 30% chance of update for demonstration
            logger.info(f"New version found for {data_type}, downloading and uploading to S3...")
            updated_types.append(data_type)
        else:
            logger.info(f"No updates found for {data_type}")
    
    if updated_types:
        logger.info(f"Successfully uploaded new files for: {updated_types}")
    else:
        logger.info("No updates found for any data type.")
    
    return updated_types


@task(name="process_with_spark", retries=2, retry_delay_seconds=300)
def process_with_spark(updated_data_types: List[str]) -> None:
    """
    Process Ensembl data files from S3 using Spark.
    
    Args:
        updated_data_types: List of data types that were updated and need processing
    """
    if not updated_data_types:
        logger.info("No data types to process, skipping Spark job")
        return
    
    logger.info(f"Starting Spark processing for data types: {updated_data_types}")
    
    # Simulate Spark processing
    # Production implementation would initialize Spark session and process files
    logger.info("Initializing Spark session...")
    logger.info(f"Reading TSV files from S3 prefix: {S3_LANDING_PREFIX}")
    logger.info("Applying transformations...")
    logger.info("Loading data into ensembl_mapping table...")
    
    time.sleep(2)  # Simulate processing time
    
    logger.info("Spark processing completed successfully")


@flow(
    name="ensembl-etl-pipeline",
    description="ETL pipeline for Ensembl genomic mapping data to S3 and Spark processing"
)
def ensembl_etl_pipeline() -> None:
    """
    Main flow for Ensembl ETL pipeline.
    
    Linear execution:
    1. Check for new files on Ensembl FTP and upload to S3
    2. If updates found, process with Spark
    3. Send notifications at start, end, and on failure
    """
    notify_slack_start()
    
    try:
        updated_data_types = check_and_upload_new_files()
        
        if not updated_data_types:
            logger.info("No updates found for any data type. Skipping Spark processing.")
            notify_slack_completion()
            return
        
        process_with_spark(updated_data_types)
        notify_slack_completion()
        
    except Exception as error:
        error_msg = str(error)
        notify_slack_failure(error_msg)
        logger.error(f"Pipeline failed with error: {error_msg}")
        raise


if __name__ == "__main__":
    # Manual execution entry point
    # For deployment with Prefect, use: prefect deployment build/apply with schedule=None
    ensembl_etl_pipeline()