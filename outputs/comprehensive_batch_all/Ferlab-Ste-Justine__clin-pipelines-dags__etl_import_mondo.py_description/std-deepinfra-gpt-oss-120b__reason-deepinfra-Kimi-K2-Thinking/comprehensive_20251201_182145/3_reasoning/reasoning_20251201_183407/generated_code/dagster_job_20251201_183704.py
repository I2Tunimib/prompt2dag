from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    Nothing,
    Out,
    Output,
    In,
    resource,
    Field,
)
from typing import Dict, Any, Optional
import logging

# Resources (minimal stubs)
@resource
def s3_client(init_context):
    """Stub for S3 client."""
    # In production, configure with real credentials
    return {"client": "s3_client_stub"}

@resource
def spark_session(init_context):
    """Stub for Spark session."""
    # In production, configure with real Spark session
    return {"session": "spark_session_stub"}

@resource
def elasticsearch_client(init_context):
    """Stub for Elasticsearch client."""
    # In production, configure with real ES client
    return {"client": "elasticsearch_client_stub"}

@resource
def slack_client(init_context):
    """Stub for Slack client."""
    # In production, configure with real Slack webhook/token
    return {"webhook_url": "https://hooks.slack.com/services/T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX"}

@resource
def github_client(init_context):
    """Stub for GitHub client."""
    # In production, configure with real GitHub API client
    return {"client": "github_client_stub"}

# Config
class MondoPipelineConfig(Config):
    color: str = "default_color"
    spark_jar: str = "path/to/spark/jar"
    obo_parser_spark_jar: str = "path/to/obo/parser/spark/jar"

# Ops
@op(
    out={"validation_result": Out(dict)},
    description="Validates the color parameter provided to the pipeline"
)
def params_validate(context: OpExecutionContext, config: MondoPipelineConfig):
    """Validates the color parameter."""
    context.log.info(f"Validating color parameter: {config.color}")
    
    # Minimal validation
    if not config.color or len(config.color.strip()) == 0:
        raise ValueError("Color parameter cannot be empty")
    
    return {"color": config.color, "is_valid": True}

@op(
    ins={"validation_result": In(Nothing)},
    out={"download_status": Out(dict, is_required=False)},
    required_resource_keys={"s3_client", "github_client"},
    description="Downloads the latest Mondo OBO file from GitHub releases"
)
def download_mondo_terms(context: OpExecutionContext, config: MondoPipelineConfig):
    """Downloads Mondo OBO file, checks version, uploads to S3."""
    context.log.info("Checking for latest Mondo OBO release")
    
    # Simulate version check and download
    try:
        # In production: check GitHub API for latest release
        # github = context.resources.github_client["client"]
        # latest_version = get_latest_release(github)
        
        # Simulate version comparison
        latest_version = "2024.01.01"
        existing_version = "2024.01.01"  # Would check S3
        
        if latest_version == existing_version:
            context.log.info("Data is already up-to-date, skipping downstream tasks")
            # Return output that signals skip
            yield Output(
                {"should_skip": True, "version": latest_version},
                output_name="download_status"
            )
            return
        
        context.log.info(f"New version {latest_version} found, downloading...")
        
        # Simulate download and S3 upload
        s3 = context.resources.s3_client["client"]
        # download_and_upload_to_s3(s3, latest_version)
        
        yield Output(
            {"should_skip": False, "version": latest_version, "s3_path": f"s3://bucket/mondo/{latest_version}.obo"},
            output_name="download_status"
        )
        
    except Exception as e:
        context.log.error(f"Failed to download Mondo terms: {e}")
        # Return skip status to allow downstream to handle
        yield Output(
            {"should_skip": True, "version": None, "error": str(e)},
            output_name="download_status"
        )

@op(
    ins={"download_status": In(dict)},
    out={"normalization_status": Out(dict, is_required=False)},
    required_resource_keys={"spark_session", "s3_client"},
    description="Processes the Mondo OBO file using Spark to normalize terms"
)
def normalized_mondo_terms(context: OpExecutionContext, download_status: Dict[str, Any], config: MondoPipelineConfig):
    """Normalizes Mondo terms using Spark."""
    
    if download_status.get("should_skip"):
        context.log.info("Skipping normalization due to upstream skip")
        yield Output({"should_skip": True}, output_name="normalization_status")
        return
    
    version = download_status.get("version")
    context.log.info(f"Normalizing Mondo terms for version {version}")
    
    try:
        spark = context.resources.spark_session["session"]
        s3_path = download_status.get("s3_path")
        
        # In production: Spark job to normalize OBO file
        # normalized_df = spark.read.text(s3_path)
        # ... normalization logic ...
        # Write to data lake
        
        output_path = f"s3://bucket/processed/mondo/{version}_normalized"
        context.log.info(f"Normalized data stored at {output_path}")
        
        yield Output(
            {"should_skip": False, "version": version, "normalized_path": output_path},
            output_name="normalization_status"
        )
        
    except Exception as e:
        context.log.error(f"Normalization failed: {e}")
        raise  # Let Dagster handle failure

@op(
    ins={"normalization_status": In(dict)},
    out={"indexing_status": Out(dict, is_required=False)},
    required_resource_keys={"spark_session", "elasticsearch_client"},
    description="Indexes the normalized Mondo terms to Elasticsearch"
)
def index_mondo_terms(context: OpExecutionContext, normalization_status: Dict[str, Any], config: MondoPipelineConfig):
    """Indexes normalized terms to Elasticsearch."""
    
    if normalization_status.get("should_skip"):
        context.log.info("Skipping indexing due to upstream skip")
        yield Output({"should_skip": True}, output_name="indexing_status")
        return
    
    version = normalization_status.get("version")
    context.log.info(f"Indexing Mondo terms for version {version}")
    
    try:
        spark = context.resources.spark_session["session"]
        es = context.resources.elasticsearch_client["client"]
        normalized_path = normalization_status.get("normalized_path")
        
        # In production: Spark job to index to Elasticsearch
        # df = spark.read.parquet(normalized_path)
        # ... indexing logic with specific config ...
        
        index_name = f"mondo_terms_{version}"
        context.log.info(f"Data indexed to Elasticsearch index: {index_name}")
        
        yield Output(
            {"should_skip": False, "version": version, "index_name": index_name},
            output_name="indexing_status"
        )
        
    except Exception as e:
        context.log.error(f"Indexing failed: {e}")
        raise

@op(
    ins={"indexing_status": In(dict)},
    out={"publish_status": Out(dict, is_required=False)},
    required_resource_keys={"elasticsearch_client"},
    description="Publishes the indexed Mondo data"
)
def publish_mondo(context: OpExecutionContext, indexing_status: Dict[str, Any]):
    """Publishes indexed data for consumption."""
    
    if indexing_status.get("should_skip"):
        context.log.info("Skipping publish due to upstream skip")
        yield Output({"should_skip": True}, output_name="publish_status")
        return
    
    version = indexing_status.get("version")
    index_name = indexing_status.get("index_name")
    context.log.info(f"Publishing Mondo data for version {version}")
    
    try:
        es = context.resources.elasticsearch_client["client"]
        
        # In production: Update alias, make index searchable, etc.
        # es.indices.update_aliases(...)
        
        context.log.info(f"Published index {index_name} for consumption")
        
        yield Output(
            {"should_skip": False, "version": version, "published": True},
            output_name="publish_status"
        )
        
    except Exception as e:
        context.log.error(f"Publish failed: {e}")
        raise

@op(
    ins={"publish_status": In(dict)},
    required_resource_keys={"slack_client"},
    description="Sends a Slack notification indicating pipeline completion"
)
def slack_notify(context: OpExecutionContext, publish_status: Dict[str, Any], config: MondoPipelineConfig):
    """Sends Slack notification on pipeline completion."""
    
    should_skip = publish_status.get("should_skip", False)
    version = publish_status.get("version", "unknown")
    
    if should_skip:
        message = f":information_source: Mondo pipeline skipped - data already up-to-date (version: {version})"
    else:
        message = f":white_check_mark: Mondo pipeline completed successfully (version: {version})"
    
    context.log.info(f"Sending Slack notification: {message}")
    
    # In production: Send to Slack
    # slack = context.resources.slack_client
    # requests.post(slack["webhook_url"], json={"text": message})

# Job
@job(
    description="ETL pipeline for Mondo ontology data from Monarch Initiative",
    resource_defs={
        "s3_client": s3_client,
        "spark_session": spark_session,
        "elasticsearch_client": elasticsearch_client,
        "slack_client": slack_client,
        "github_client": github_client,
    },
    # In production, set max_concurrent_runs=1 to limit concurrency
)
def mondo_etl_pipeline():
    """Linear ETL pipeline for Mondo ontology data."""
    
    # Linear dependency chain
    validation_result = params_validate()
    download_status = download_mondo_terms(validation_result)
    normalization_status = normalized_mondo_terms(download_status)
    indexing_status = index_mondo_terms(normalization_status)
    publish_status = publish_mondo(indexing_status)
    slack_notify(publish_status)

# Launch pattern
if __name__ == "__main__":
    # Example execution with config
    result = mondo_etl_pipeline.execute_in_process(
        run_config={
            "ops": {
                "params_validate": {
                    "config": {
                        "color": "blue",
                        "spark_jar": "/opt/spark/jars/mondo-spark-job.jar",
                        "obo_parser_spark_jar": "/opt/spark/jars/obo-parser.jar"
                    }
                }
            }
        }
    )
    
    if result.success:
        print("Pipeline executed successfully")
    else:
        print("Pipeline execution failed")