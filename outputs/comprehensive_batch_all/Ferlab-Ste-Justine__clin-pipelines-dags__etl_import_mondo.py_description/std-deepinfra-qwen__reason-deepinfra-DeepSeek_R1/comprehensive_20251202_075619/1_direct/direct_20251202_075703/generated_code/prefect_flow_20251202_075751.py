from prefect import flow, task
import requests
import boto3
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch
import slack_sdk

@task
def params_validate(color):
    """Validates the color parameter provided to the pipeline."""
    valid_colors = ["red", "green", "blue"]
    if color not in valid_colors:
        raise ValueError(f"Invalid color: {color}. Valid colors are: {valid_colors}")

@task
def download_mondo_terms():
    """Downloads the latest Mondo OBO file from GitHub releases, checks if a newer version exists, and uploads to S3 if available."""
    url = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
    response = requests.get(url)
    response.raise_for_status()
    latest_release = response.json()
    latest_version = latest_release["tag_name"]
    
    s3 = boto3.client("s3")
    bucket_name = "mondo-ontology-bucket"
    object_key = f"mondo/{latest_version}/mondo.obo"
    
    try:
        s3.head_object(Bucket=bucket_name, Key=object_key)
        print("Mondo OBO file is already up-to-date.")
        return None
    except s3.exceptions.ClientError:
        download_url = next(asset for asset in latest_release["assets"] if asset["name"] == "mondo.obo")["browser_download_url"]
        response = requests.get(download_url)
        response.raise_for_status()
        
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=response.content)
        print(f"Downloaded and uploaded Mondo OBO file version {latest_version} to S3.")
        return object_key

@task
def normalized_mondo_terms(s3_key, spark_jar, obo_parser_spark_jar):
    """Processes the Mondo OBO file using Spark to normalize and transform the ontology terms, storing results in the data lake."""
    spark = SparkSession.builder.appName("Mondo Normalization").getOrCreate()
    
    # Load the Mondo OBO file from S3
    mondo_rdd = spark.sparkContext.textFile(f"s3a://{s3_key}")
    
    # Process and normalize the data using the provided Spark JARs
    normalized_data = mondo_rdd.map(lambda line: line.split("\t")).filter(lambda x: len(x) > 1)
    
    # Save the normalized data to the data lake
    normalized_data.saveAsTextFile("s3a://data-lake/mondo/normalized")
    
    spark.stop()
    return "s3a://data-lake/mondo/normalized"

@task
def index_mondo_terms(normalized_data_path, spark_jar, obo_parser_spark_jar):
    """Indexes the normalized Mondo terms to Elasticsearch using a Spark job with specific index configuration."""
    spark = SparkSession.builder.appName("Mondo Indexing").getOrCreate()
    
    # Load the normalized data from the data lake
    normalized_data = spark.read.text(normalized_data_path)
    
    # Index the data to Elasticsearch
    es = Elasticsearch()
    normalized_data.write.format("es").option("es.resource", "mondo/terms").save()
    
    spark.stop()
    return "mondo/terms"

@task
def publish_mondo(index_name):
    """Publishes the indexed Mondo data to make it available for consumption."""
    es = Elasticsearch()
    es.indices.refresh(index=index_name)
    print(f"Published Mondo data to index: {index_name}")

@task
def slack_notification(message):
    """Sends a Slack notification indicating pipeline completion."""
    client = slack_sdk.WebClient(token="your-slack-token")
    response = client.chat_postMessage(channel="#notifications", text=message)
    if not response["ok"]:
        raise RuntimeError(f"Failed to send Slack notification: {response['error']}")

@flow
def mondo_etl_pipeline(color, spark_jar, obo_parser_spark_jar):
    """Orchestrates the Mondo ETL pipeline."""
    params_validate(color)
    s3_key = download_mondo_terms()
    if s3_key:
        normalized_data_path = normalized_mondo_terms(s3_key, spark_jar, obo_parser_spark_jar)
        index_name = index_mondo_terms(normalized_data_path, spark_jar, obo_parser_spark_jar)
        publish_mondo(index_name)
        slack_notification("Mondo ETL pipeline completed successfully.")
    else:
        slack_notification("Mondo ETL pipeline skipped as data is up-to-date.")

if __name__ == "__main__":
    # Example parameters
    color = "red"
    spark_jar = "path/to/spark.jar"
    obo_parser_spark_jar = "path/to/obo-parser-spark.jar"
    
    # Run the flow
    mondo_etl_pipeline(color, spark_jar, obo_parser_spark_jar)