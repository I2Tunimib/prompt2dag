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
        raise ValueError(f"Invalid color: {color}. Valid colors are {valid_colors}")

@task
def download_mondo_terms():
    """Downloads the latest Mondo OBO file from GitHub releases, checks if a newer version exists, and uploads to S3 if available."""
    github_url = "https://api.github.com/repos/monarch-initiative/mondo/releases/latest"
    response = requests.get(github_url)
    response.raise_for_status()
    latest_release = response.json()
    latest_version = latest_release["tag_name"]
    obo_url = next(asset["browser_download_url"] for asset in latest_release["assets"] if asset["name"].endswith(".obo"))
    
    s3 = boto3.client("s3")
    bucket_name = "mondo-ontology-bucket"
    s3_key = f"mondo/{latest_version}/mondo.obo"
    
    try:
        s3.head_object(Bucket=bucket_name, Key=s3_key)
        print("Mondo OBO file is already up-to-date.")
        return None
    except s3.exceptions.ClientError:
        response = requests.get(obo_url)
        response.raise_for_status()
        s3.put_object(Bucket=bucket_name, Key=s3_key, Body=response.content)
        print(f"Downloaded and uploaded Mondo OBO file version {latest_version} to S3.")
        return s3_key

@task
def normalized_mondo_terms(s3_key, spark_jar, obo_parser_spark_jar):
    """Processes the Mondo OBO file using Spark to normalize and transform the ontology terms, storing results in the data lake."""
    if not s3_key:
        print("No new Mondo OBO file to process.")
        return None
    
    spark = SparkSession.builder.appName("Mondo Normalization").getOrCreate()
    spark.sparkContext.addPyFile(obo_parser_spark_jar)
    
    # Example processing logic (this is a placeholder)
    df = spark.read.text(f"s3a://{s3_key}")
    normalized_df = df.withColumn("normalized_term", df["value"].lower())
    normalized_df.write.parquet("s3a://data-lake/mondo/normalized")
    
    print("Mondo terms normalized and stored in the data lake.")
    return "s3a://data-lake/mondo/normalized"

@task
def index_mondo_terms(normalized_data_path, spark_jar):
    """Indexes the normalized Mondo terms to Elasticsearch using a Spark job with specific index configuration."""
    if not normalized_data_path:
        print("No normalized Mondo data to index.")
        return None
    
    spark = SparkSession.builder.appName("Mondo Indexing").getOrCreate()
    
    # Example indexing logic (this is a placeholder)
    df = spark.read.parquet(normalized_data_path)
    es = Elasticsearch()
    df.write.format("org.elasticsearch.spark.sql").option("es.resource", "mondo/terms").save()
    
    print("Mondo terms indexed to Elasticsearch.")
    return "mondo/terms"

@task
def publish_mondo(index_path):
    """Publishes the indexed Mondo data to make it available for consumption."""
    if not index_path:
        print("No indexed Mondo data to publish.")
        return None
    
    # Example publishing logic (this is a placeholder)
    print(f"Published indexed Mondo data from {index_path}.")
    return True

@task
def slack(message):
    """Sends a Slack notification indicating pipeline completion."""
    client = slack_sdk.WebClient(token="your-slack-token")
    response = client.chat_postMessage(channel="#mondo-pipeline", text=message)
    response.raise_for_status()
    print(f"Slack notification sent: {message}")

@flow
def mondo_etl_pipeline(color, spark_jar, obo_parser_spark_jar):
    """Orchestrates the Mondo ETL pipeline."""
    params_validate(color)
    s3_key = download_mondo_terms()
    normalized_data_path = normalized_mondo_terms(s3_key, spark_jar, obo_parser_spark_jar)
    index_path = index_mondo_terms(normalized_data_path, spark_jar)
    publish_mondo(index_path)
    slack("Mondo ETL pipeline completed successfully.")

if __name__ == "__main__":
    # Example parameters
    color = "blue"
    spark_jar = "path/to/spark.jar"
    obo_parser_spark_jar = "path/to/obo-parser-spark.jar"
    
    mondo_etl_pipeline(color, spark_jar, obo_parser_spark_jar)