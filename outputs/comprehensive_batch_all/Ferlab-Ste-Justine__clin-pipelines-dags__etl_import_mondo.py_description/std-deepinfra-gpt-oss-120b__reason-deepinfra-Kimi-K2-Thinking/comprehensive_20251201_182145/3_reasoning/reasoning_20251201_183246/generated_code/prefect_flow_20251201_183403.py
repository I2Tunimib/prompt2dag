from prefect import flow, task
from prefect.context import get_run_context
from typing import Optional, Dict, Any
import logging
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task(retries=2, retry_delay_seconds=30)
def params_validate(color: str) -> bool:
    """
    Validates the color parameter provided to the pipeline.
    
    Args:
        color: Color parameter to validate
        
    Returns:
        bool: True if valid
        
    Raises:
        ValueError: If color is invalid
    """
    logger.info(f"Validating color parameter: {color}")
    
    valid_colors = {"blue", "green", "red", "yellow", "purple"}
    if color.lower() not in valid_colors:
        raise ValueError(
            f"Invalid color '{color}'. Must be one of: {', '.join(valid_colors)}"
        )
    
    logger.info(f"Color validation passed: {color}")
    return True


@task(retries=3, retry_delay_seconds=60)
def download_mondo_terms(
    color: str,
    s3_bucket: str = "mondo-data-lake"
) -> Optional[Dict[str, Any]]:
    """
    Downloads the latest Mondo OBO file from GitHub releases,
    checks if a newer version exists, and uploads to S3 if available.
    
    Args:
        color: Validated color parameter
        s3_bucket: Target S3 bucket name
        
    Returns:
        Dict with version info if new version downloaded, None if skipped
    """
    logger.info("Checking for latest Mondo OBO release")
    
    # Simulate version check - in real implementation, query GitHub API and S3
    import hashlib
    import time
    
    # Mock version determination
    latest_version = f"2024.01.{hashlib.md5(str(time.time()).encode()).hexdigest()[:8]}"
    existing_version = "2024.01.abc123"  # Would be fetched from S3 in reality
    
    if latest_version == existing_version:
        logger.info("Mondo data is already up-to-date, skipping download")
        return None
    
    logger.info(f"New version {latest_version} found, downloading from GitHub")
    
    # Simulate download and S3 upload
    s3_key = f"raw/mondo/{latest_version}/mondo.obo"
    logger.info(f"Uploaded new OBO file to s3://{s3_bucket}/{s3_key}")
    
    return {
        "version": latest_version,
        "s3_bucket": s3_bucket,
        "s3_key": s3_key,
        "color": color
    }


@task(retries=2, retry_delay_seconds=120)
def normalized_mondo_terms(
    version_info: Optional[Dict[str, Any]],
    spark_jar: str,
    obo_parser_spark_jar: str,
    s3_bucket: str = "mondo-data-lake"
) -> Optional[Dict[str, Any]]:
    """
    Processes the Mondo OBO file using Spark to normalize and transform
    ontology terms, storing results in the data lake.
    
    Args:
        version_info: Version metadata from download step
        spark_jar: Path to Spark job JAR
        obo_parser_spark_jar: Path to OBO parser JAR
        s3_bucket: Target S3 bucket for processed data
        
    Returns:
        Dict with processing metadata or None if skipped
    """
    if version_info is None:
        logger.info("No new version to process, skipping normalization")
        return None
    
    logger.info(f"Normalizing Mondo terms version {version_info['version']}")
    
    # Simulate Spark job execution
    if not os.path.exists(spark_jar):
        logger.warning(f"Spark JAR not found at {spark_jar}, using mock processing")
    
    processed_key = f"processed/mondo/{version_info['version']}/normalized_terms.parquet"
    logger.info(f"Spark normalization complete, results stored at s3://{s3_bucket}/{processed_key}")
    
    return {
        "version": version_info["version"],
        "s3_bucket": s3_bucket,
        "processed_key": processed_key,
        "color": version_info["color"],
        "record_count": 45000  # Mock record count
    }


@task(retries=2, retry_delay_seconds=90)
def index_mondo_terms(
    processed_metadata: Optional[Dict[str, Any]],
    spark_jar: str,
    es_host: str = "elasticsearch.mondo.internal:9200"
) -> Optional[Dict[str, Any]]:
    """
    Indexes the normalized Mondo terms to Elasticsearch using Spark.
    
    Args:
        processed_metadata: Metadata from normalization step
        spark_jar: Path to Spark job JAR
        es_host: Elasticsearch host endpoint
        
    Returns:
        Dict with indexing metadata or None if skipped
    """
    if processed_metadata is None:
        logger.info("No processed data to index, skipping Elasticsearch indexing")
        return None
    
    logger.info(f"Indexing Mondo terms to Elasticsearch: {es_host}")
    
    # Simulate Elasticsearch indexing Spark job
    index_name = f"mondo-terms-{processed_metadata['version'].replace('.', '-')}"
    
    logger.info(f"Successfully indexed {processed_metadata['record_count']} terms to index '{index_name}'")
    
    return {
        "version": processed_metadata["version"],
        "index_name": index_name,
        "es_host": es_host,
        "color": processed_metadata["color"],
        "indexed_count": processed_metadata["record_count"]
    }


@task(retries=2, retry_delay_seconds=60)
def publish_mondo(
    index_metadata: Optional[Dict[str, Any]],
    s3_bucket: str = "mondo-data-lake"
) -> bool:
    """
    Publishes the indexed Mondo data to make it available for consumption.
    
    Args:
        index_metadata: Metadata from indexing step
        s3_bucket: S3 bucket for publishing manifest
        
    Returns:
        bool: True if publication succeeded
    """
    if index_metadata is None:
        logger.info("No new indexed data to publish")
        return False
    
    logger.info(f"Publishing Mondo data version {index_metadata['version']}")
    
    # Simulate publishing by creating a manifest
    manifest_key = f"published/mondo/{index_metadata['version']}/manifest.json"
    logger.info(f"Publication manifest created at s3://{s3_bucket}/{manifest_key}")
    
    return True


@task(retries=2, retry_delay_seconds=30)
def slack(
    message: str,
    status: str = "success",
    webhook_url: Optional[str] = None
) -> None:
    """
    Sends a Slack notification.
    
    Args:
        message: Notification message
        status: Status level (success, failure, warning)
        webhook_url: Slack webhook URL (optional, can use env var)
    """
    # In production, use prefect-slack or direct API call
    webhook = webhook_url or os.getenv("SLACK_WEBHOOK_URL")
    
    if not webhook:
        logger.warning("No Slack webhook configured, skipping notification")
        return
    
    logger.info(f"Sending Slack notification [{status}]: {message}")
    
    # Simulate Slack API call
    import json
    
    payload = {
        "text": f"[Mondo ETL] {status.upper()}: {message}",
        "username": "Mondo ETL Pipeline",
        "icon_emoji": ":dna:" if status == "success" else ":warning:"
    }
    
    logger.debug(f"Slack payload: {json.dumps(payload)}")


@flow(
    name="mondo-etl-pipeline",
    description="ETL pipeline for Mondo ontology data from Monarch Initiative"
)
def mondo_etl_pipeline(
    color: str = "blue",
    spark_jar: str = "/opt/spark/jars/mondo-etl-1.0.0.jar",
    obo_parser_spark_jar: str = "/opt/spark/jars/obo-parser-2.1.0.jar",
    s3_bucket: str = "mondo-data-lake",
    es_host: str = "elasticsearch.mondo.internal:9200",
    slack_webhook: Optional[str] = None
) -> None:
    """
    Main ETL flow for Mondo ontology processing.
    
    Args:
        color: Color parameter for pipeline execution
        spark_jar: Path to main Spark job JAR
        obo_parser_spark_jar: Path to OBO parser Spark JAR
        s3_bucket: S3 bucket for data storage
        es_host: Elasticsearch host endpoint
        slack_webhook: Slack webhook URL for notifications
    """
    # Get flow run context for metadata
    context = get_run_context()
    flow_run_id = context.flow_run.id if context and context.flow_run else "local"
    
    logger.info(f"Starting Mondo ETL pipeline (run_id: {flow_run_id})")
    
    try:
        # Step 1: Validate parameters
        params_validate(color)
        
        # Step 2: Download Mondo terms (may return None if up-to-date)
        version_info = download_mondo_terms(
            color=color,
            s3_bucket=s3_bucket
        )
        
        # Step 3: Normalize terms (skipped if no new version)
        processed_metadata = normalized_mondo_terms(
            version_info=version_info,
            spark_jar=spark_jar,
            obo_parser_spark_jar=obo_parser_spark_jar,
            s3_bucket=s3_bucket
        )
        
        # Step 4: Index to Elasticsearch (skipped if no processed data)
        index_metadata = index_mondo_terms(
            processed_metadata=processed_metadata,
            spark_jar=spark_jar,
            es_host=es_host
        )
        
        # Step 5: Publish the data
        published = publish_mondo(
            index_metadata=index_metadata,
            s3_bucket=s3_bucket
        )
        
        # Step 6: Send completion notification
        if published and index_metadata:
            message = (
                f"Pipeline completed successfully. "
                f"Version {index_metadata['version']} published to Elasticsearch "
                f"index '{index_metadata['index_name']}' ({index_metadata['indexed_count']} terms)"
            )
        elif version_info is None:
            message = "Pipeline completed. No new Mondo version available, all tasks skipped."
        else:
            message = "Pipeline completed with partial processing."
            
        slack(
            message=message,
            status="success",
            webhook_url=slack_webhook
        )
        
        logger.info("Mondo ETL pipeline completed successfully")
        
    except Exception as e:
        # Send failure notification
        error_message = f"Pipeline failed: {str(e)}"
        logger.error(error_message, exc_info=True)
        
        slack(
            message=error_message,
            status="failure",
            webhook_url=slack_webhook
        )
        
        raise


if __name__ == "__main__":
    # For local execution and testing
    # In production, deploy with:
    # prefect deployment build mondo_etl_pipeline:mondo_etl_pipeline \
    #   --name "mondo-etl" --pool "default-agent-pool" --work-queue "mondo" \
    #   --storage-block "s3/mondo-storage" --apply
    
    # Manual execution only - no schedule
    mondo_etl_pipeline(
        color="blue",
        spark_jar="/opt/spark/jars/mondo-etl-1.0.0.jar",
        obo_parser_spark_jar="/opt/spark/jars/obo-parser-2.1.0.jar",
        s3_bucket="mondo-data-lake",
        es_host="elasticsearch.mondo.internal:9200",
        slack_webhook=os.getenv("SLACK_WEBHOOK_URL")
    )