from dagster import op, job, resource, RetryPolicy, Failure, Field, String, Int, get_dagster_logger
from dagster.utils import file_relative_path
import requests
import boto3
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import slack_sdk

log = get_dagster_logger()

# Resources
@resource(config_schema={"slack_token": Field(String, is_required=True)})
def slack_resource(context):
    return slack_sdk.WebClient(token=context.resource_config["slack_token"])

@resource(config_schema={"aws_access_key_id": String, "aws_secret_access_key": String})
def s3_resource(context):
    return boto3.client(
        "s3",
        aws_access_key_id=context.resource_config["aws_access_key_id"],
        aws_secret_access_key=context.resource_config["aws_secret_access_key"],
    )

@resource(config_schema={"es_host": String, "es_port": Int})
def elasticsearch_resource(context):
    return Elasticsearch([{"host": context.resource_config["es_host"], "port": context.resource_config["es_port"]}])

# Ops
@op(required_resource_keys={"slack"})
def params_validate(context, color: str):
    """Validates the color parameter provided to the pipeline."""
    if color not in ["red", "blue", "green"]:
        raise ValueError("Invalid color parameter. Must be one of 'red', 'blue', or 'green'.")
    log.info(f"Color parameter validated: {color}")

@op(required_resource_keys={"s3"})
def download_mondo_terms(context):
    """Downloads the latest Mondo OBO file from GitHub releases and uploads it to S3 if available."""
    url = "https://github.com/monarch-initiative/mondo/releases/latest/download/mondo.obo"
    response = requests.get(url)
    response.raise_for_status()
    
    s3 = context.resources.s3
    bucket_name = "mondo-ontology-bucket"
    key = "latest/mondo.obo"
    
    s3.upload_fileobj(response.raw, bucket_name, key)
    log.info(f"Downloaded and uploaded Mondo OBO file to S3: s3://{bucket_name}/{key}")

@op(required_resource_keys={"spark"})
def normalized_mondo_terms(context):
    """Processes the Mondo OBO file using Spark to normalize and transform the ontology terms."""
    spark = context.resources.spark
    s3_path = "s3a://mondo-ontology-bucket/latest/mondo.obo"
    df = spark.read.format("obo").load(s3_path)
    
    # Example normalization (this is a placeholder, actual normalization logic will vary)
    normalized_df = df.withColumn("normalized_term", df["term"].lower())
    normalized_df.write.parquet("s3a://mondo-ontology-bucket/processed/mondo_normalized.parquet")
    log.info("Normalized Mondo terms and stored in the data lake")

@op(required_resource_keys={"elasticsearch"})
def index_mondo_terms(context):
    """Indexes the normalized Mondo terms to Elasticsearch using a Spark job with specific index configuration."""
    spark = context.resources.spark
    es = context.resources.elasticsearch
    es_index = "mondo_terms"
    
    df = spark.read.parquet("s3a://mondo-ontology-bucket/processed/mondo_normalized.parquet")
    df.write.format("es").option("es.resource", es_index).save()
    log.info(f"Indexed Mondo terms to Elasticsearch: {es_index}")

@op
def publish_mondo(context):
    """Publishes the indexed Mondo data to make it available for consumption."""
    log.info("Published Mondo data for consumption")

@op(required_resource_keys={"slack"})
def slack(context, message: str):
    """Sends a Slack notification indicating pipeline completion or failure."""
    client = context.resources.slack
    client.chat_postMessage(channel="#mondo-pipeline", text=message)
    log.info(f"Sent Slack notification: {message}")

# Job
@job(
    resource_defs={
        "slack": slack_resource,
        "s3": s3_resource,
        "elasticsearch": elasticsearch_resource,
        "spark": SparkSession.builder.appName("MondoETL").getOrCreate(),
    },
    tags={"pipeline": "mondo-etl"},
    description="ETL pipeline for importing and processing Mondo ontology data.",
)
def mondo_etl_pipeline():
    color = "red"  # Example color parameter
    validated_color = params_validate(color)
    downloaded_file = download_mondo_terms()
    normalized_terms = normalized_mondo_terms(start_after=[downloaded_file])
    indexed_terms = index_mondo_terms(start_after=[normalized_terms])
    published_data = publish_mondo(start_after=[indexed_terms])
    slack(start_after=[published_data], message="Mondo ETL pipeline completed successfully")

# Execution
if __name__ == "__main__":
    result = mondo_etl_pipeline.execute_in_process()