from prefect import flow, task
from prefect.logging import get_run_logger
import os
from typing import Optional, Dict, Any


@task
def params_validate(color: str) -> bool:
    """Validate the color parameter."""
    logger = get_run_logger()
    if not color or not isinstance(color, str):
        logger.error(f"Invalid color parameter: {color}")
        raise ValueError("Color parameter must be a non-empty string")
    logger.info(f"Color parameter validated: {color}")
    return True


@task
def download_mondo_terms(
    color: str,
    s3_bucket: str,
    obo_parser_spark_jar: str
) -> Optional[Dict[str, str]]:
    """Download latest Mondo OBO file and upload to S3 if newer version exists."""
    logger = get_run_logger()
    logger.info("Checking for new Mondo OBO version...")
    
    # Simulate version check against GitHub releases and S3
    # In production: implement actual version comparison logic
    new_version_available = True
    
    if not new_version_available:
        logger.info("Data is already up-to-date, skipping download")
        return None
    
    logger.info("New version found, downloading from monarch-initiative/mondo...")
    
    # Simulate download and S3 upload
    version = "2024.01.01"
    s3_key = f"raw/mondo_{version}.obo"
    
    logger.info(f"Uploaded to s3://{s3_bucket}/{s3_key}")
    
    return {"s3_key": s3_key, "version": version}


@task
def normalized_mondo_terms(
    download_result: Optional[Dict[str, str]],
    spark_jar: str,
    color: str,
    s3_bucket: str
) -> Optional[Dict[str, str]]:
    """Process Mondo OBO with Spark and store normalized terms in data lake."""
    logger = get_run_logger()
    
    if download_result is None:
        logger.info("No new data to process, skipping normalization")
        return None
    
    s3_key = download_result["s3_key"]
    version = download_result["version"]
    
    logger.info(f"Processing {s3_key} with Spark jar: {spark_jar}")
    
    # Simulate Spark processing job
    output_path = f"s3://{s3_bucket}/processed/mondo_normalized_{version}_{color}"
    logger.info(f"Normalized data stored at {output_path}")
    
    return {"normalized_path": output_path, "version": version}


@task
def index_mondo_terms(
    normalized_result: Optional[Dict[str, str]],
    spark_jar: str,
    elasticsearch_host: str
) -> Optional[str]:
    """Index normalized Mondo terms to Elasticsearch."""
    logger = get_run_logger()
    
    if normalized_result is None:
        logger.info("No normalized data to index, skipping indexing")
        return None
    
    normalized_path = normalized_result["normalized_path"]
    version = normalized_result["version"]
    
    logger.info(f"Indexing data from {normalized_path} to Elasticsearch at {elasticsearch_host}")
    
    # Simulate Spark indexing job
    index_name = f"mondo_terms_{version}"
    logger.info(f"Data indexed to Elasticsearch index: {index_name}")
    
    return index_name


@task
def publish_mondo(index_name: Optional[str], color: str) -> bool:
    """Publish the indexed Mondo data for consumption."""
    logger = get_run_logger()
    
    if index_name is None:
        logger.info("No index to publish, skipping publication")
        return False
    
    logger.info(f"Publishing Mondo data from index: {index_name}")
    
    # Simulate publication process
    publish_path = f"published/mondo_{color}"
    logger.info(f"Mondo data published to {publish_path}")
    
    return True


@task
def slack_notification(message: str, webhook_url: Optional[str] = None) -> None:
    """Send Slack notification."""
    logger = get_run_logger()
    logger.info(f"Slack notification: {message}")
    
    # In production: use slack_sdk or requests to send actual notification
    if webhook_url:
        logger.info("Notification sent to Slack webhook")
    else:
        logger.warning("No Slack webhook URL provided")


@flow(name="mondo-etl-pipeline")
def mondo_etl_pipeline(
    color: str,
    spark_jar: str = "spark-jobs.jar",
    obo_parser_spark_jar: str = "obo-parser.jar",
    s3_bucket: str = "mondo-data",
    elasticsearch_host: str = "localhost:9200",
    slack_webhook_url: Optional[str] = None
):
    """
    ETL pipeline for Mondo ontology data.
    
    Args:
        color: Validation parameter for the pipeline run
        spark_jar: Path to Spark job jar for normalization and indexing
        obo_parser_spark_jar: Path to OBO parser Spark jar
        s3_bucket: S3 bucket for raw and processed data storage
        elasticsearch_host: Elasticsearch cluster host for indexing
        slack_webhook_url: Slack webhook URL for notifications
    """
    logger = get_run_logger()
    
    try:
        # Step 1: Validate parameters
        params_validate(color)
        
        # Step 2: Download Mondo terms
        download_result = download_mondo_terms(color, s3_bucket, obo_parser_spark_jar)
        
        # Step 3: Normalize terms
        normalized_result = normalized_mondo_terms(
            download_result, spark_jar, color, s3_bucket
        )
        
        # Step 4: Index to Elasticsearch
        index_name = index_mondo_terms(normalized_result, spark_jar, elasticsearch_host)
        
        # Step 5: Publish data
        published = publish_mondo(index_name, color)
        
        # Determine completion status for notification
        if download_result is None:
            status = "completed with no new data (all steps skipped)"
        elif normalized_result is None:
            status = "completed but normalization was skipped"
        elif index_name is None:
            status = "completed but indexing was skipped"
        else:
            status = "completed successfully"
            
        # Step 6: Send success notification
        slack_notification(
            f"Mondo ETL pipeline {status} for color: {color}",
            slack_webhook_url
        )
        
    except Exception as exc:
        logger.error(f"Pipeline failed: {exc}", exc_info=True)
        
        # Send failure notification
        slack_notification(
            f"Mondo ETL pipeline failed for color: {color}. Error: {str(exc)}",
            slack_webhook_url
        )
        raise


if __name__ == "__main__":
    # Local execution example
    # For production deployment:
    # - Schedule: None (manual execution only)
    # - Concurrency: Configure 1 active run limit via deployment/work pool settings
    # - Infrastructure: Requires Spark cluster, AWS S3 access, Elasticsearch access
    
    mondo_etl_pipeline(
        color="production",
        spark_jar="s3://my-bucket/jobs/spark-normalization-1.0.jar",
        obo_parser_spark_jar="s3://my-bucket/jobs/obo-parser-1.0.jar",
        s3_bucket="mondo-ontology-data",
        elasticsearch_host="es.mydomain.com:9200",
        slack_webhook_url=os.getenv("SLACK_WEBHOOK_URL")
    )