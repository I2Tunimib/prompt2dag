from prefect import flow, task
import requests
import boto3
import hashlib
from pyspark.sql import SparkSession

# Task to check for new data versions and download/upload if necessary
@task
def check_and_update_files():
    data_types = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    base_url = "ftp://ftp.ensembl.org/pub/current_mapping/"
    s3_client = boto3.client("s3")
    bucket_name = "your-s3-bucket"
    prefix = "raw/landing/ensembl/"

    for data_type in data_types:
        remote_file = f"{data_type}.tsv"
        remote_md5 = f"{data_type}.md5"
        remote_file_url = f"{base_url}/{remote_file}"
        remote_md5_url = f"{base_url}/{remote_md5}"

        # Fetch remote file and MD5 checksum
        remote_file_response = requests.get(remote_file_url)
        remote_md5_response = requests.get(remote_md5_url)

        if remote_file_response.status_code == 200 and remote_md5_response.status_code == 200:
            remote_file_content = remote_file_response.content
            remote_md5_content = remote_md5_response.text.split()[0]

            # Calculate local MD5 checksum
            local_md5 = hashlib.md5(remote_file_content).hexdigest()

            # Check if the file needs to be updated
            s3_key = f"{prefix}/{remote_file}"
            try:
                s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
                local_md5_response = s3_object["Body"].read().decode("utf-8").split()[0]
            except s3_client.exceptions.NoSuchKey:
                local_md5_response = ""

            if local_md5_response != remote_md5_content:
                # Upload the new file to S3
                s3_client.put_object(Bucket=bucket_name, Key=s3_key, Body=remote_file_content)
                print(f"Updated {data_type} file in S3.")
            else:
                print(f"No update needed for {data_type} file.")
        else:
            print(f"Failed to fetch {data_type} file or MD5 checksum.")

# Task to process the newly landed Ensembl data files using Spark
@task
def process_ensembl_data():
    spark = SparkSession.builder.appName("EnsemblDataProcessing").getOrCreate()
    s3_bucket = "your-s3-bucket"
    s3_prefix = "raw/landing/ensembl/"
    s3_output_prefix = "processed/ensembl/"

    for data_type in ["canonical", "ena", "entrez", "refseq", "uniprot"]:
        s3_input_path = f"s3a://{s3_bucket}/{s3_prefix}/{data_type}.tsv"
        s3_output_path = f"s3a://{s3_bucket}/{s3_output_prefix}/{data_type}.parquet"

        df = spark.read.csv(s3_input_path, header=True, sep="\t")
        df = df.withColumnRenamed("column1", "new_column1")  # Example transformation
        df.write.parquet(s3_output_path, mode="overwrite")

    spark.stop()

# Flow to orchestrate the pipeline
@flow
def ensembl_etl_pipeline():
    check_and_update_files()
    process_ensembl_data()

if __name__ == "__main__":
    ensembl_etl_pipeline()