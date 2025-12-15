from prefect import flow, task
from prefect.task_runners import SequentialTaskRunner
from typing import Optional, Dict, Any
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@task
def params_validate(color: str) -> str:
    """
    Validates the color parameter provided to the pipeline.
    
    Args:
        color: The color parameter to validate
        
    Returns:
        The validated color string
        
    Raises:
        ValueError: If the color is not in the allowed list
    """
    logger.info(f"Validating color parameter: {color}")
    
    valid_colors = ["red", "green", "blue", "yellow", "purple", "orange", "pink", "black", "white"]
    if color.lower() not in valid_colors:
        raise ValueError(f"Invalid color: {color}. Must be one of {valid_colors}")
    
    logger.info(f"Color parameter '{color}' is valid")
    return color


@task
def download_mondo_terms(color: str) -> Optional[Dict[str, Any]]:
    """
    Downloads the latest Mondo OBO file from GitHub releases,
    checks if a newer version exists, and uploads to S3 if available.
    
    Args:
        color: Validated color parameter for tracking
        
    Returns:
        Dictionary with version info and S3 path if new version downloaded,
        None if no new version available or version cannot be determined
    """
    logger.info("Checking for latest Mondo OBO version from monarch-initiative/mondo")
    
    # Simulate version check against existing S3 data
    # In production, implement actual GitHub API and S3 version comparison
    import hashlib
    import time
    
    try:
        # Simulate determining current version
        current_version = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
        s3_path = f"s3://mondo-data/raw/mondo_{current_version}.obo"
        
        # Simulate version comparison logic
        # For demo, assume new version is available
        is_new_version = True
        
        if not is_new_version:
            logger.info("Source version is not newer than imported version. Skipping download.")
            return None
        
        logger.info(f"New version {current_version} available. Downloading and uploading to {s3_path}")
        
        # Actual implementation would:
        # 1. Download from GitHub releases
        # 2. Upload to S3
        # 3. Return version metadata
        
        return {
            "version": current_version,
            "s3_path": s3_path,
            "color": color
        }
        
    except Exception as e:
        logger.warning(f"Could not determine version or download file: {e}")
        return None


@task
def normalized_mondo_terms(
    download_result: Optional[Dict[str, Any]], 
    spark_jar: str, 
    obo_parser_spark_jar: str
) -> Optional[Dict[str, Any]]:
    """
    Processes the Mondo OBO file using Spark to normalize and transform
    the ontology terms, storing results in the data lake.
    
    Args:
        download_result: Result from download task containing version and S3 path
        spark_jar: Path to Spark jar file for processing
        obo_parser_spark_jar: Path to OBO parser Spark jar
        
    Returns:
        Dictionary with processed data info, or None if skipped
    """
    if download_result is None:
        logger.info("No download result available. Skipping normalization.")
        return None
    
    logger.info(f"Normalizing Mondo terms from {download_result['s3_path']}")
    
    # Simulate Spark processing job
    # Command would be: spark-submit --jars {spark_jar},{obo_parser_spark_jar} ...
    
    processed_s3_path = f"s3://mondo-data/processed/mondo_{download_result['version']}_normalized.parquet"
    
    logger.info(f"Normalized data stored at {processed_s3_path}")
    
    return {
        "version": download_result["version"],
        "raw_s3_path": download_result["s3_path"],
        "processed_s3_path": processed_s3_path,
        "color": download_result["color"]
    }


@task
def index_mondo_terms(
    normalize_result: Optional[Dict[str, Any]], 
    spark_jar: str
) -> Optional[Dict[str, Any]]:
    """
    Indexes the normalized Mondo terms to Elasticsearch using a Spark job
    with specific index configuration.
    
    Args:
        normalize_result: Result from normalization task
        spark_jar: Path to Spark jar file
        
    Returns:
        Dictionary with index info, or None if skipped
    """
    if normalize_result is None:
        logger.info("No normalized data available. Skipping indexing.")
        return None
    
    logger.info(f"Indexing data from {normalize_result['processed_s3_path']} to Elasticsearch")
    
    # Simulate Elasticsearch indexing with Spark
    # Command would be: spark-submit --jars {spark_jar} ...
    
    es_index = f"mondo_terms_{normalize_result['version']}"
    
    logger.info(f"Data indexed to Elasticsearch index: {es_index}")
    
    return {
        "version": normalize_result["version"],
        "es_index": es_index,
        "color": normalize_result["color"]
    }


@task
def publish_mondo(index_result: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Publishes the indexed Mondo data to make it available for consumption.
    
    Args:
        index_result: Result from indexing task
        
    Returns:
        Dictionary with publication info, or None if skipped
    """
    if index_result is None:
        logger.info("No indexed data available. Skipping publication.")
        return None
    
    logger.info(f"Publishing Mondo data from Elasticsearch index: {index_result['es_index']}")
    
    # Simulate publishing logic
    # - Update API endpoints
    # - Update data catalog
    # - Invalidate caches
    
    logger.info(f"Mondo version {index_result['version']} published successfully")
    
    return {
        "version": index_result["version"],
        "status": "published",
        "color": index_result["color"]
    }


@task
def slack(publish_result: Optional[Dict[str, Any]], notification_type: str = "completion") -> bool:
    """
    Sends a Slack notification indicating pipeline completion or failure.
    
    Args:
        publish_result: Result from publish task (None if earlier step failed)
        notification_type: Type of notification ('completion' or 'failure')
        
    Returns:
        True if notification sent successfully
    """
    logger.info(f"Preparing Slack notification: {notification_type}")
    
    if notification_type == "completion":
        if publish_result:
            message = (
                f"✅ Mondo ETL pipeline completed successfully!\n"
                f"Version: {publish_result['version']}\n"
                f"Color: {publish_result['color']}\n"
                f"Status: {publish_result['status']}"
            )
        else:
            message = "✅ Mondo ETL pipeline completed (no new data to process)"
    else:
        message = "❌ Mondo ETL pipeline failed! Please check the logs."
    
    # Actual implementation would send to Slack webhook
    # requests.post(slack_webhook_url, json={"text": message})
    
    logger.info(f"Slack notification sent: {message}")
    return True


@flow(
    name="mondo-etl-pipeline",
    description="ETL pipeline for importing and processing Mondo ontology data",
    task_runner=SequentialTaskRunner(),
    # Deployment configuration notes:
    # - schedule=None for manual execution only
    # - For concurrency limits (1 active task, 1 active run):
    #   prefect concurrency-limit create mondo-etl-limit 1
    #   Then apply to deployment: prefect deployment create ... --limit mondo-etl-limit
)
def mondo_etl_pipeline(
    color: str = "blue",
    spark_jar: str = "/opt/spark/jars/spark-utils.jar",
    obo_parser_spark_jar: str = "/opt/spark/jars/obo-parser.jar"
) -> None:
    """
    Main Mondo ETL pipeline flow with strictly linear execution.
    
    Args:
        color: Color parameter for validation and tracking
        spark_jar: Path to Spark jar file for processing jobs
        obo_parser_spark_jar: Path to OBO parser Spark jar
    """
    logger.info("=== Starting Mondo ETL Pipeline ===")
    
    try:
        # Step 1: Validate parameters
        validated_color = params_validate(color)
        
        # Step 2: Download Mondo terms (returns None if no new version)
        download_result = download_mondo_terms(validated_color)
        
        # Step 3: Normalize terms (skipped if download_result is None)
        normalize_result = normalized_mondo_terms(
            download_result, 
            spark_jar, 
            obo_parser_spark_jar
        )
        
        # Step 4: Index to Elasticsearch (skipped if normalize_result is None)
        index_result = index_mondo_terms(normalize_result, spark_jar)
        
        # Step 5: Publish data (skipped if index_result is None)
        publish_result = publish_mondo(index_result)
        
        # Step 6: Send completion notification
        slack(publish_result, notification_type="completion")
        
        logger.info("=== Mondo ETL Pipeline Finished Successfully ===")
        
    except Exception as e:
        logger.error(f"Pipeline failed with error: {str(e)}", exc_info=True)
        # Send failure notification
        slack(None, notification_type="failure")
        raise


if __name__ == "__main__":
    # Local execution example
    mondo_etl_pipeline(
        color="green",
        spark_jar="/opt/spark/jars/spark-utils.jar",
        obo_parser_spark_jar="/opt/spark/jars/obo-parser.jar"
    )