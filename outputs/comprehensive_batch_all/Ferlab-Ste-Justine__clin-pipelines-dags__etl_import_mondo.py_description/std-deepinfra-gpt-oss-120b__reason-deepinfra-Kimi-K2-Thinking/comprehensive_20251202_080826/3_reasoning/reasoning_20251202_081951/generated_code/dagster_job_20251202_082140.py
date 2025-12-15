from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    Out,
    Output,
    Nothing,
    ResourceParam,
    Definitions,
    In,
)
import requests
import os
from typing import Optional, Dict, Any


# Minimal resource stubs
class S3Resource:
    """Stub S3 resource for file operations."""
    
    def __init__(self, bucket: str = "mondo-data-lake"):
        self.bucket = bucket
    
    def upload_file(self, local_path: str, s3_key: str) -> str:
        # Stub implementation
        return f"s3://{self.bucket}/{s3_key}"
    
    def download_file(self, s3_key: str, local_path: str) -> None:
        # Stub implementation
        pass
    
    def file_exists(self, s3_key: str) -> bool:
        # Stub implementation
        return False


class SparkResource:
    """Stub Spark resource for data processing."""
    
    def __init__(self, spark_jar: str = ""):
        self.spark_jar = spark_jar
    
    def run_job(self, main_class: str, args: list) -> bool:
        # Stub implementation
        print(f"Running Spark job: {main_class} with args: {args}")
        return True


class ElasticsearchResource:
    """Stub Elasticsearch resource for indexing."""
    
    def __init__(self, host: str = "localhost", port: int = 9200):
        self.host = host
        self.port = port
    
    def index_data(self, index_name: str, data_path: str) -> bool:
        # Stub implementation
        print(f"Indexing data from {data_path} to {index_name}")
        return True
    
    def publish_index(self, index_name: str) -> bool:
        # Stub implementation
        print(f"Publishing index: {index_name}")
        return True


class SlackResource:
    """Stub Slack resource for notifications."""
    
    def __init__(self, webhook_url: str = ""):
        self.webhook_url = webhook_url
    
    def send_message(self, message: str) -> None:
        # Stub implementation
        print(f"Slack notification: {message}")


# Config schema
class MondoPipelineConfig(Config):
    color: str
    spark_jar: str = "mondo-spark-job.jar"
    obo_parser_spark_jar: str = "obo-parser.jar"
    s3_bucket: str = "mondo-data-lake"
    es_host: str = "localhost"
    es_port: int = 9200
    slack_webhook: str = ""


@op(
    out={"validation_result": Out(dict)},
    description="Validates the color parameter provided to the pipeline"
)
def params_validate(context: OpExecutionContext, config: MondoPipelineConfig) -> Dict[str, Any]:
    """Validates color parameter and returns validation result."""
    context.log.info(f"Validating color parameter: {config.color}")
    
    # Simple validation - color should be non-empty string
    if not config.color or not isinstance(config.color, str):
        raise ValueError("Color parameter must be a non-empty string")
    
    return {"color": config.color, "valid": True}


@op(
    out={"download_metadata": Out(Optional[Dict[str, Any]], is_required=False)},
    description="Downloads Mondo OBO file, checks version, uploads to S3"
)
def download_mondo_terms(
    context: OpExecutionContext, 
    config: MondoPipelineConfig,
    validation_result: Dict[str, Any],
    s3: ResourceParam[S3Resource]
) -> Optional[Dict[str, Any]]:
    """Downloads latest Mondo OBO file if newer version available."""
    try:
        context.log.info("Checking for latest Mondo OBO file")
        
        # GitHub API to get latest release
        repo_url = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
        response = requests.get(repo_url, timeout=30)
        response.raise_for_status()
        
        release_data = response.json()
        latest_version = release_data["tag_name"]
        
        # Check if already processed (stub version check)
        version_key = f"versions/mondo_{latest_version}.txt"
        if s3.file_exists(version_key):
            context.log.info(f"Version {latest_version} already processed, skipping download")
            return None
        
        # Download OBO file
        obo_assets = [asset for asset in release_data["assets"] 
                     if asset["name"].endswith(".obo")]
        if not obo_assets:
            raise ValueError("No OBO file found in latest release")
        
        obo_url = obo_assets[0]["browser_download_url"]
        obo_filename = obo_assets[0]["name"]
        
        context.log.info(f"Downloading {obo_filename} from {obo_url}")
        
        # Download to temp location
        local_path = f"/tmp/{obo_filename}"
        obo_response = requests.get(obo_url, timeout=60)
        obo_response.raise_for_status()
        
        with open(local_path, "wb") as f:
            f.write(obo_response.content)
        
        # Upload to S3
        s3_key = f"raw/mondo/{latest_version}/{obo_filename}"
        s3_path = s3.upload_file(local_path, s3_key)
        
        # Cleanup
        os.remove(local_path)
        
        context.log.info(f"Successfully uploaded to {s3_path}")
        
        return {
            "version": latest_version,
            "s3_path": s3_path,
            "filename": obo_filename
        }
        
    except Exception as e:
        context.log.error(f"Failed to download Mondo terms: {e}")
        raise


@op(
    out={"processed_path": Out(Optional[str], is_required=False)},
    description="Normalizes Mondo terms using Spark"
)
def normalized_mondo_terms(
    context: OpExecutionContext,
    config: MondoPipelineConfig,
    download_metadata: Optional[Dict[str, Any]],
    spark: ResourceParam[SparkResource],
    s3: ResourceParam[S3Resource]
) -> Optional[str]:
    """Processes OBO file with Spark to normalize ontology terms."""
    if download_metadata is None:
        context.log.info("No new data to process, skipping normalization")
        return None
    
    try:
        context.log.info(f"Normalizing Mondo terms version {download_metadata['version']}")
        
        # Spark processing job
        input_path = download_metadata["s3_path"]
        output_path = f"s3://{config.s3_bucket}/processed/mondo/{download_metadata['version']}/normalized/"
        
        # Run Spark normalization job
        success = spark.run_job(
            "org.mondo.NormalizeTermsJob",
            [
                "--input", input_path,
                "--output", output_path,
                "--color", config.color
            ]
        )
        
        if not success:
            raise RuntimeError("Spark normalization job failed")
        
        context.log.info(f"Successfully normalized terms to {output_path}")
        
        return output_path
        
    except Exception as e:
        context.log.error(f"Failed to normalize Mondo terms: {e}")
        raise


@op(
    out={"index_name": Out(Optional[str], is_required=False)},
    description="Indexes normalized Mondo terms to Elasticsearch"
)
def index_mondo_terms(
    context: OpExecutionContext,
    config: MondoPipelineConfig,
    processed_path: Optional[str],
    spark: ResourceParam[SparkResource],
    es: ResourceParam[ElasticsearchResource]
) -> Optional[str]:
    """Indexes normalized data to Elasticsearch using Spark."""
    if processed_path is None:
        context.log.info("No processed data to index, skipping")
        return None
    
    try:
        version = processed_path.split("/")[-2]  # Extract version from path
        index_name = f"mondo_terms_{version}"
        
        context.log.info(f"Indexing to {index_name}")
        
        # Run Spark indexing job
        success = spark.run_job(
            "org.mondo.IndexToElasticsearchJob",
            [
                "--input", processed_path,
                "--index", index_name,
                "--es-host", config.es_host,
                "--es-port", str(config.es_port),
                "--jar", config.spark_jar
            ]
        )
        
        if not success:
            raise RuntimeError("Spark indexing job failed")
        
        # Also index via direct ES client for metadata
        es.index_data(index_name, processed_path)
        
        context.log.info(f"Successfully indexed to {index_name}")
        
        return index_name
        
    except Exception as e:
        context.log.error(f"Failed to index Mondo terms: {e}")
        raise


@op(
    out={"publication_status": Out(Optional[Dict[str, Any]], is_required=False)},
    description="Publishes the indexed Mondo data"
)
def publish_mondo(
    context: OpExecutionContext,
    index_name: Optional[str],
    es: ResourceParam[ElasticsearchResource]
) -> Optional[Dict[str, Any]]:
    """Publishes indexed data for consumption."""
    if index_name is None:
        context.log.info("No index to publish, skipping")
        return None
    
    try:
        context.log.info(f"Publishing index {index_name}")
        
        success = es.publish_index(index_name)
        
        if not success:
            raise RuntimeError("Failed to publish index")
        
        status = {
            "index": index_name,
            "published": True,
            "timestamp": "2024-01-01T00:00:00Z"  # Stub timestamp
        }
        
        context.log.info(f"Successfully published {index_name}")
        
        return status
        
    except Exception as e:
        context.log.error(f"Failed to publish Mondo data: {e}")
        raise


@op(
    ins={
        "publication_status": In(Optional[Dict[str, Any]]),
        "validation_result": In(Dict[str, Any])
    },
    description="Sends Slack notification upon pipeline completion"
)
def slack_notify(
    context: OpExecutionContext,
    config: MondoPipelineConfig,
    publication_status: Optional[Dict[str, Any]],
    validation_result: Dict[str, Any],
    slack: ResourceParam[SlackResource]
) -> None:
    """Sends Slack notification based on pipeline results."""
    try:
        if publication_status:
            message = (
                f"✅ Mondo pipeline completed successfully!\n"
                f"Color: {validation_result['color']}\n"
                f"Published index: {publication_status['index']}"
            )
        else:
            message = (
                f"ℹ️ Mondo pipeline completed - no new data to process.\n"
                f"Color: {validation_result['color']}"
            )
        
        slack.send_message(message)
        context.log.info("Slack notification sent")
        
    except Exception as e:
        context.log.error(f"Failed to send Slack notification: {e}")
        # Don't fail the pipeline for notification issues


@job(
    description="ETL pipeline for Mondo ontology data from Monarch Initiative"
)
def mondo_pipeline_job():
    """Defines the linear Mondo pipeline job."""
    # Validate parameters first
    validation_result = params_validate()
    
    # Download step depends on validation
    download_metadata = download_mondo_terms(validation_result)
    
    # Normalization depends on download
    processed_path = normalized_mondo_terms(download_metadata)
    
    # Indexing depends on normalization
    index_name = index_mondo_terms(processed_path)
    
    # Publishing depends on indexing
    publication_status = publish_mondo(index_name)
    
    # Slack notification depends on publishing and validation
    slack_notify(publication_status, validation_result)


# Definitions for resources and job
defs = Definitions(
    jobs=[mondo_pipeline_job],
    resources={
        "s3": S3Resource(),
        "spark": SparkResource(),
        "es": ElasticsearchResource(),
        "slack": SlackResource()
    }
)


if __name__ == "__main__":
    # Example execution with config
    result = mondo_pipeline_job.execute_in_process(
        run_config={
            "ops": {
                "params_validate": {
                    "config": {
                        "color": "blue",
                        "spark_jar": "mondo-spark-job.jar",
                        "obo_parser_spark_jar": "obo-parser.jar",
                        "s3_bucket": "mondo-data-lake",
                        "es_host": "localhost",
                        "es_port": 9200,
                        "slack_webhook": ""
                    }
                }
            }
        }
    )
    
    if result.success:
        print("Pipeline executed successfully!")
    else:
        print(f"Pipeline failed: {result.failure_data}")