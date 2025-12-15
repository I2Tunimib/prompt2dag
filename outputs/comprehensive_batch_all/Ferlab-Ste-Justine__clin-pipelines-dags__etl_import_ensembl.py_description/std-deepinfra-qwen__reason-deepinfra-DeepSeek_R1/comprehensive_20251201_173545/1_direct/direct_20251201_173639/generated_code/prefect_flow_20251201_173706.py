from prefect import flow, task
import requests
import boto3
import hashlib
from pyspark.sql import SparkSession

# Task to check for new data versions and download if necessary
@task
def check_and_download_files():
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    base_url = "ftp://ftp.ensembl.org/pub/current_mapping/homo_sapiens/"
    s3_client = boto3.client("s3")
    bucket_name = "your-s3-bucket"
    prefix = "raw/landing/ensembl/"

    for data_type in data_types:
        remote_file = f"{data_type}.tsv.gz"
        remote_md5_file = f"{data_type}.tsv.gz.md5"
        remote_url = f"{base_url}{remote_file}"
        remote_md5_url = f"{base_url}{remote_md5_file}"
        local_file = f"/tmp/{remote_file}"
        local_md5_file = f"/tmp/{remote_md5_file}"

        # Download MD5 file
        response = requests.get(remote_md5_url)
        with open(local_md5_file, "wb") as f:
            f.write(response.content)

        # Read MD5 checksum
        with open(local_md5_file, "r") as f:
            remote_md5 = f.read().split()[0]

        # Check if file exists in S3
        try:
            s3_client.head_object(Bucket=bucket_name, Key=f"{prefix}{remote_file}")
            s3_md5 = s3_client.get_object(Bucket=bucket_name, Key=f"{prefix}{remote_file}")["ETag"].strip('"')
        except s3_client.exceptions.ClientError:
            s3_md5 = None

        # Download and upload if new version is found
        if s3_md5 != remote_md5:
            response = requests.get(remote_url)
            with open(local_file, "wb") as f:
                f.write(response.content)

            # Validate MD5 checksum
            with open(local_file, "rb") as f:
                local_md5 = hashlib.md5(f.read()).hexdigest()

            if local_md5 == remote_md5:
                s3_client.upload_file(local_file, bucket_name, f"{prefix}{remote_file}")
            else:
                raise ValueError("MD5 checksum validation failed for file: {remote_file}")
        else:
            print(f"No new version found for {data_type}")

# Task to process the newly landed Ensembl data files using Spark
@task
def process_files_with_spark():
    spark = SparkSession.builder.appName("Ensembl Mapping ETL").getOrCreate()
    s3_bucket = "your-s3-bucket"
    s3_prefix = "raw/landing/ensembl/"
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]

    for data_type in data_types:
        file_path = f"s3a://{s3_bucket}/{s3_prefix}{data_type}.tsv.gz"
        df = spark.read.csv(file_path, header=True, sep="\t", inferSchema=True)

        # Apply transformations (example: select specific columns)
        transformed_df = df.select("column1", "column2", "column3")

        # Load into structured table
        transformed_df.write.mode("overwrite").parquet(f"s3a://{s3_bucket}/processed/ensembl/{data_type}")

    spark.stop()

# Flow to orchestrate the pipeline
@flow
def ensembl_etl_pipeline():
    check_and_download_files()
    process_files_with_spark()

# Entry point for local execution
if __name__ == "__main__":
    ensembl_etl_pipeline()