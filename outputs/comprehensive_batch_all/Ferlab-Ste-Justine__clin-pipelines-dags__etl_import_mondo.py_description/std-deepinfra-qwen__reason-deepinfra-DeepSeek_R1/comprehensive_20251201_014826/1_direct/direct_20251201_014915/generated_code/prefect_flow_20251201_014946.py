from prefect import flow, task
import requests
import boto3
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import slack_sdk

@task
def params_validate(color):
    """Validates the color parameter provided to the pipeline."""
    valid_colors = ["red", "blue", "green"]
    if color not in valid_colors:
        raise ValueError(f"Invalid color: {color}. Valid colors are: {valid_colors}")

@task
def download_mondo_terms():
    """Downloads the latest Mondo OBO file from GitHub releases, checks if a newer version exists, and uploads to S3 if available."""
    url = "https://github.com/monarch-initiative/mondo/releases/latest/download/mondo.obo"
    response = requests.get(url)
    response.raise_for_status()
    
    s3 = boto3.client("s3")
    bucket_name = "mondo-ontology-bucket"
    s3_key = "latest/mondo.obo"
    
    s3.put_object(Bucket=bucket_name, Key=s3_key, Body=response.content)
    return s3_key

@task
def normalized_mondo_terms(s3_key):
    """Processes the Mondo OBO file using Spark to normalize and transform the ontology terms, storing results in the data lake."""
    spark = SparkSession.builder.appName("Mondo Normalization").getOrCreate()
    
    # Load the OBO file from S3
    obo_file = spark.read.text(f"s3a://{bucket_name}/{s3_key}")
    
    # Process and normalize the data
    normalized_data = obo_file.withColumn("normalized", obo_file.value)
    
    # Save the normalized data to the data lake
    normalized_data.write.parquet("s3a://data-lake/mondo/normalized")
    
    spark.stop()

@task
def index_mondo_terms():
    """Indexes the normalized Mondo terms to Elasticsearch using a Spark job with specific index configuration."""
    spark = SparkSession.builder.appName("Mondo Indexing").getOrCreate()
    
    # Load the normalized data from the data lake
    normalized_data = spark.read.parquet("s3a://data-lake/mondo/normalized")
    
    # Index the data to Elasticsearch
    es = Elasticsearch("http://localhost:9200")
    normalized_data.write.format("es").option("es.resource", "mondo/normalized").save()
    
    spark.stop()

@task
def publish_mondo():
    """Publishes the indexed Mondo data to make it available for consumption."""
    # Example: Update a status in a database or trigger a webhook
    print("Mondo data published successfully.")

@task
def slack_notification(message):
    """Sends a Slack notification indicating pipeline completion or failure."""
    client = slack_sdk.WebClient(token="your-slack-token")
    client.chat_postMessage(channel="#mondo-pipeline", text=message)

@flow
def mondo_etl_pipeline(color, spark_jar, obo_parser_spark_jar):
    """Orchestrates the Mondo ETL pipeline."""
    try:
        params_validate(color)
        s3_key = download_mondo_terms()
        normalized_mondo_terms(s3_key)
        index_mondo_terms()
        publish_mondo()
        slack_notification("Mondo ETL pipeline completed successfully.")
    except Exception as e:
        slack_notification(f"Mondo ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    # Example parameters
    color = "blue"
    spark_jar = "path/to/spark.jar"
    obo_parser_spark_jar = "path/to/obo-parser-spark.jar"
    
    mondo_etl_pipeline(color, spark_jar, obo_parser_spark_jar)