from dagster import job, op, resource, RetryPolicy, Failure, Success, get_dagster_logger
import requests
import boto3
import hashlib
from pyspark.sql import SparkSession

logger = get_dagster_logger()

# Resource definitions
@resource(config_schema={"s3_bucket": str, "s3_prefix": str, "ftp_base_url": str})
def s3_resource(context):
    return boto3.client('s3', aws_access_key_id=context.s3_access_key, aws_secret_access_key=context.s3_secret_key)

@resource(config_schema={"spark_master": str, "spark_app_name": str})
def spark_resource(context):
    return SparkSession.builder.master(context.spark_master).appName(context.spark_app_name).getOrCreate()

# Op definitions
@op(required_resource_keys={"s3_resource"})
def check_and_download_files(context):
    s3_client = context.resources.s3_resource
    ftp_base_url = context.resources.s3_resource.config.ftp_base_url
    s3_bucket = context.resources.s3_resource.config.s3_bucket
    s3_prefix = context.resources.s3_resource.config.s3_prefix
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    updated = False

    for data_type in data_types:
        ftp_url = f"{ftp_base_url}/{data_type}.tsv.gz"
        s3_key = f"{s3_prefix}/{data_type}.tsv.gz"
        md5_url = f"{ftp_url}.md5"

        # Check if file exists in S3
        try:
            s3_client.head_object(Bucket=s3_bucket, Key=s3_key)
            current_version = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)["Metadata"]["version"]
        except s3_client.exceptions.ClientError:
            current_version = None

        # Get latest version from FTP
        response = requests.get(md5_url)
        if response.status_code == 200:
            latest_version = response.text.split()[0]
        else:
            logger.error(f"Failed to fetch MD5 for {data_type}")
            continue

        # Compare versions
        if current_version != latest_version:
            logger.info(f"New version found for {data_type}. Downloading and uploading to S3.")
            file_content = requests.get(ftp_url).content
            md5_checksum = hashlib.md5(file_content).hexdigest()

            if md5_checksum == latest_version:
                s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=file_content, Metadata={"version": latest_version})
                updated = True
            else:
                logger.error(f"MD5 checksum mismatch for {data_type}. Skipping.")
                continue
        else:
            logger.info(f"No new version found for {data_type}.")

    if not updated:
        raise Failure("No new data versions found. Pipeline skipped.")

@op(required_resource_keys={"spark_resource"})
def process_files(context):
    spark = context.resources.spark_resource
    s3_bucket = context.resources.s3_resource.config.s3_bucket
    s3_prefix = context.resources.s3_resource.config.s3_prefix
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]

    for data_type in data_types:
        s3_key = f"{s3_prefix}/{data_type}.tsv.gz"
        df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").option("sep", "\t").load(f"s3a://{s3_bucket}/{s3_key}")

        # Apply transformations
        df = df.withColumn("processed", True)  # Example transformation

        # Write to structured table
        df.write.format("parquet").mode("overwrite").save(f"s3a://{s3_bucket}/processed/ensembl_mapping/{data_type}")

@job(resource_defs={"s3_resource": s3_resource, "spark_resource": spark_resource})
def ensembl_pipeline():
    process_files(check_and_download_files())

if __name__ == '__main__':
    result = ensembl_pipeline.execute_in_process()