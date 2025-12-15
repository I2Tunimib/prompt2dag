from prefect import flow, task
import requests
import boto3
import hashlib
from pyspark.sql import SparkSession

# Task: Check for new data versions and download if necessary
@task
def check_and_download_files():
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    base_url = "ftp://ftp.ensembl.org/pub/current_mapping/"
    s3_client = boto3.client("s3")
    bucket_name = "your-s3-bucket"
    prefix = "raw/landing/ensembl/"

    for data_type in data_types:
        remote_file = f"{data_type}.tsv.gz"
        remote_url = f"{base_url}/{remote_file}"
        local_file = f"/tmp/{remote_file}"
        s3_key = f"{prefix}/{remote_file}"

        # Check if file exists in S3
        try:
            s3_client.head_object(Bucket=bucket_name, Key=s3_key)
            s3_exists = True
        except:
            s3_exists = False

        # Download file if it doesn't exist or if a new version is available
        if not s3_exists or is_new_version(remote_url, s3_key, s3_client, bucket_name):
            download_file(remote_url, local_file)
            validate_md5(local_file, remote_url)
            upload_to_s3(local_file, s3_key, s3_client, bucket_name)

# Helper function to check if a new version is available
def is_new_version(remote_url, s3_key, s3_client, bucket_name):
    remote_md5 = get_remote_md5(remote_url)
    try:
        s3_md5 = s3_client.head_object(Bucket=bucket_name, Key=s3_key)["Metadata"]["md5"]
    except:
        s3_md5 = None
    return remote_md5 != s3_md5

# Helper function to get remote MD5 checksum
def get_remote_md5(url):
    response = requests.get(url + ".md5")
    return response.text.split()[0]

# Helper function to download file
def download_file(url, local_file):
    response = requests.get(url)
    with open(local_file, "wb") as f:
        f.write(response.content)

# Helper function to validate MD5 checksum
def validate_md5(local_file, remote_url):
    with open(local_file, "rb") as f:
        local_md5 = hashlib.md5(f.read()).hexdigest()
    remote_md5 = get_remote_md5(remote_url)
    assert local_md5 == remote_md5, "MD5 checksum validation failed"

# Helper function to upload file to S3
def upload_to_s3(local_file, s3_key, s3_client, bucket_name):
    s3_client.upload_file(local_file, bucket_name, s3_key, ExtraArgs={"Metadata": {"md5": get_remote_md5(local_file)}})

# Task: Process the newly landed Ensembl data files using Spark
@task
def process_files_with_spark():
    spark = SparkSession.builder.appName("Ensembl Mapping ETL").getOrCreate()
    s3_bucket = "your-s3-bucket"
    s3_prefix = "raw/landing/ensembl/"
    s3_client = boto3.client("s3")

    # List all files in the S3 prefix
    response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
    files = [content["Key"] for content in response.get("Contents", [])]

    for file in files:
        df = spark.read.csv(f"s3a://{s3_bucket}/{file}", header=True, sep="\t")
        df.write.mode("overwrite").saveAsTable("ensembl_mapping")

# Flow: Orchestrate the pipeline
@flow
def ensembl_etl_pipeline():
    check_and_download_files()
    process_files_with_spark()

if __name__ == "__main__":
    ensembl_etl_pipeline()