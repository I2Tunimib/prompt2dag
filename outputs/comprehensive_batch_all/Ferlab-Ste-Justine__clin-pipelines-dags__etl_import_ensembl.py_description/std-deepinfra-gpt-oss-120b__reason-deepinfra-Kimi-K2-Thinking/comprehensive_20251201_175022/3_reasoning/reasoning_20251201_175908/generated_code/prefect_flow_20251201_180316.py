import os
import hashlib
import ftplib
from typing import Dict, List, Optional
from pathlib import Path

from prefect import flow, task, get_run_logger
from prefect_slack import SlackWebhook
from prefect_slack.notifications import send_slack_notification

import boto3
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit


# Configuration
ENSEMBL_FTP_HOST = os.getenv("ENSEMBL_FTP_HOST", "ftp.ensembl.org")
ENSEMBL_FTP_PATH = os.getenv("ENSEMBL_FTP_PATH", "/pub/current_tsv/homo_sapiens")
S3_BUCKET = os.getenv("S3_BUCKET", "genomics-data-lake")
S3_LANDING_PREFIX = os.getenv("S3_LANDING_PREFIX", "raw/landing/ensembl")
S3_TABLE_PREFIX = os.getenv("S3_TABLE_PREFIX", "processed/tables")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")

DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]


@task(retries=2, retry_delay_seconds=30)
def check_and_upload_data_type(data_type: str) -> Optional[str]:
    """
    Check for new version of a specific data type on Ensembl FTP,
    download if newer, validate MD5, and upload to S3.
    Returns the version string if updated, None otherwise.
    """
    logger = get_run_logger()
    logger.info(f"Checking for updates for data type: {data_type}")
    
    s3_client = boto3.client("s3")
    
    file_name = f"homo_sapiens_{data_type}.tsv.gz"
    md5_name = f"{file_name}.md5"
    local_path = Path(f"/tmp/{file_name}")
    md5_path = Path(f"/tmp/{md5_name}")
    
    try:
        ftp = ftplib.FTP(ENSEMBL_FTP_HOST)
        ftp.login()
        ftp.cwd(ENSEMBL_FTP_PATH)
        
        with open(md5_path, "wb") as f:
            ftp.retrbinary(f"RETR {md5_name}", f.write)
        
        with open(md5_path, "r") as f:
            remote_md5_line = f.read().strip()
            remote_md5 = remote_md5_line.split()[0]
            remote_version = remote_md5
        
        s3_key = f"{S3_LANDING_PREFIX}/{data_type}/{file_name}"
        version_key = f"{S3_LANDING_PREFIX}/{data_type}/version.txt"
        
        current_version = None
        try:
            version_obj = s3_client.get_object(Bucket=S3_BUCKET, Key=version_key)
            current_version = version_obj["Body"].read().decode("utf-8").strip()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info(f"No existing version found for {data_type}")
            else:
                raise
        
        if current_version == remote_version:
            logger.info(f"No update needed for {data_type}")
            return None
        
        logger.info(f"Downloading new version for {data_type}")
        with open(local_path, "wb") as f:
            ftp.retrbinary(f"RETR {file_name}", f.write)
        
        ftp.quit()
        
        logger.info(f"Validating MD5 for {data_type}")
        md5_hash = hashlib.md5()
        with open(local_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                md5_hash.update(chunk)
        
        local_md5 = md5_hash.hexdigest()
        if local_md5 != remote_md5:
            raise ValueError(f"MD5 mismatch: expected {remote_md5}, got {local_md5}")
        
        logger.info(f"Uploading {data_type} to S3")
        s3_client.upload_file(str(local_path), S3_BUCKET, s3_key)
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=version_key,
            Body=remote_version.encode("utf-8")
        )
        
        local_path.unlink()
        md5_path.unlink()
        
        logger.info(f"Successfully updated {data_type} to version {remote_version}")
        return remote_version
        
    except Exception as e:
        logger.error(f"Failed to process {data_type}: {e}")
        raise


@task
def file_task() -> Dict[str, str]:
    """
    Check all data types for updates and upload to S3.
    Returns dict of updated data types and their versions.
    """
    logger = get_run_logger()
    logger.info("Starting file task")
    
    futures = {
        data_type: check_and_upload_data_type.submit(data_type)
        for data_type in DATA_TYPES
    }
    
    updated_types = {}
    for data_type, future in futures.items():
        version = future.result()
        if version:
            updated_types[data_type] = version
    
    if not updated_types:
        logger.info("No updates found. Pipeline will skip processing.")
    
    return updated_types


@task(retries=1, retry_delay_seconds=60)
def table_task(updated_types: Dict[str, str]) -> None:
    """
    Process updated Ensembl data files using Spark.
    """
    if not updated_types:
        return
    
    logger = get_run_logger()
    logger.info(f"Processing {len(updated_types)} updated types")
    
    spark = SparkSession.builder \
        .appName("EnsemblETL") \
        .config("spark.kubernetes.namespace", os.getenv("SPARK_K8S_NAMESPACE", "spark")) \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    try:
        for data_type, version in updated_types.items():
            logger.info(f"Processing {data_type}")
            
            input_path = f"s3a://{S3_BUCKET}/{S3_LANDING_PREFIX}/{data_type}/*.tsv.gz"
            df = spark.read.csv(input_path, sep="\t", header=True, inferSchema=True)
            
            transformed_df = df \