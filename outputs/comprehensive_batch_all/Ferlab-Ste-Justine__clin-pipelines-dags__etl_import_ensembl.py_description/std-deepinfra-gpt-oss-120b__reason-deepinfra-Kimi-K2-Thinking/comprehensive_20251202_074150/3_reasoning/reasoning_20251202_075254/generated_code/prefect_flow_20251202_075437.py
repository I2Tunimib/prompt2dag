import os
import hashlib
import ftplib
from typing import List, Dict, Optional, Tuple
import boto3
from prefect import flow, task
from prefect.blocks.notifications import SlackWebhook
from prefect.context import get_run_context

# Configuration
class Config:
    ENSEMBL_FTP_HOST = "ftp.ensembl.org"
    ENSEMBL_BASE_PATH = "/pub/current_tsv/homo_sapiens"
    S3_BUCKET = os.getenv("S3_BUCKET", "my-genomics-bucket")
    S3_LANDING_PREFIX = "raw/landing/ensembl/"
    SPARK_APP_NAME = "EnsemblMappingProcessor"
    SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")
    DATA_TYPES = ["canonical", "ena", "entrez", "refseq", "uniprot"]

# Helper functions
def get_s3_client():
    return boto3.client('s3')

def get_ftp_connection():
    ftp = ftplib.FTP(Config.ENSEMBL_FTP_HOST)
    ftp.login()
    return ftp

def calculate_md5(file_path):
    hash_md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

# Tasks
@task(name="check_and_download_ensembl_data")
def check_and_download_ensembl_data() -> Dict[str, str]:
    """
    Checks for new Ensembl data versions and downloads/uploads to S3 if updated.
    Returns a dictionary mapping data types to their S3 paths.
    """
    s3_client = get_s3_client()
    downloaded_files = {}
    
    # Notification: pipeline start
    if Config.SLACK_WEBHOOK_URL:
        try:
            slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
            slack.notify("🚀 Starting Ensembl mapping data check and download...")
        except Exception:
            pass  # Fail silently for notifications
    
    try:
        ftp = get_ftp_connection()
        
        for data_type in Config.DATA_TYPES:
            # Construct file paths
            filename = f"homo_sapiens.{data_type}.tsv.gz"
            md5_filename = f"{filename}.md5"
            ftp_path = f"{Config.ENSEMBL_BASE_PATH}/{filename}"
            md5_ftp_path = f"{Config.ENSEMBL_BASE_PATH}/{md5_filename}"
            
            s3_key = f"{Config.S3_LANDING_PREFIX}{filename}"
            
            # Get latest version from FTP (using file timestamp or version in filename)
            # For simplicity, we'll check if file exists and get its size/timestamp
            try:
                ftp_size = ftp.size(ftp_path)
                ftp.sendcmd(f"MDTM {ftp_path}")
            except Exception:
                print(f"File {filename} not found on FTP, skipping...")
                continue
            
            # Check current version in S3
            try:
                s3_head = s3_client.head_object(Bucket=Config.S3_BUCKET, Key=s3_key)
                s3_size = s3_head['ContentLength']
                
                # If sizes match, assume same version (simplified logic)
                if s3_size == ftp_size:
                    print(f"No update needed for {data_type}, S3 version matches FTP")
                    continue
            except s3_client.exceptions.NoSuchKey:
                # File doesn't exist in S3, need to download
                pass
            
            # Download file from FTP
            print(f"Downloading {filename} from Ensembl FTP...")
            local_path = f"/tmp/{filename}"
            with open(local_path, 'wb') as f:
                ftp.retrbinary(f'RETR {ftp_path}', f.write)
            
            # Download MD5 checksum
            md5_local_path = f"/tmp/{md5_filename}"
            try:
                with open(md5_local_path, 'w') as f:
                    ftp.retrlines(f'RETR {md5_ftp_path}', f.write)
                
                # Read expected MD5
                with open(md5_local_path, 'r') as f:
                    expected_md5 = f.read().strip().split()[0]  # First part of MD5 file
            except Exception:
                print(f"MD5 file not found for {filename}, skipping validation")
                expected_md5 = None
            
            # Validate MD5 if available
            if expected_md5:
                actual_md5 = calculate_md5(local_path)
                if actual_md5 != expected_md5:
                    raise ValueError(f"MD5 mismatch for {filename}: expected {expected_md5}, got {actual_md5}")
                print(f"MD5 validation passed for {filename}")
            
            # Upload to S3
            print(f"Uploading {filename} to S3...")
            s3_client.upload_file(local_path, Config.S3_BUCKET, s3_key)
            
            # Store the S3 path for downstream processing
            downloaded_files[data_type] = f"s3://{Config.S3_BUCKET}/{s3_key}"
            
            # Clean up local files
            os.remove(local_path)
            if os.path.exists(md5_local_path):
                os.remove(md5_local_path)
        
        ftp.quit()
        
        # If no files were downloaded, we should skip the pipeline
        if not downloaded_files:
            print("No new data found. Pipeline will be skipped.")
            return {}
        
        return downloaded_files
        
    except Exception as e:
        # Task failure notification
        if Config.SLACK_WEBHOOK_URL:
            try:
                slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
                slack.notify(f"❌ Task failed: check_and_download_ensembl_data - {str(e)}")
            except Exception:
                pass
        raise

@task(name="process_ensembl_data_with_spark")
def process_ensembl_data_with_spark(s3_paths: Dict[str, str]) -> None:
    """
    Processes downloaded Ensembl TSV files using Spark and loads into structured table.
    """
    if not s3_paths:
        print("No files to process. Skipping Spark job.")
        return
    
    # Notification: Spark job starting
    if Config.SLACK_WEBHOOK_URL:
        try:
            slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
            slack.notify("🔥 Starting Spark processing of Ensembl data...")
        except Exception:
            pass
    
    try:
        # Initialize Spark session
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, current_timestamp
        
        spark = SparkSession.builder \
            .appName(Config.SPARK_APP_NAME) \
            .getOrCreate()
        
        # Process each data type
        dataframes = []
        for data_type, s3_path in s3_paths.items():
            print(f"Reading {data_type} data from {s3_path}...")
            
            # Read TSV file
            df = spark.read.csv(
                s3_path,
                sep="\t",
                header=True,
                inferSchema=True
            )
            
            # Add metadata columns
            df = df.withColumn("data_type", lit(data_type)) \
                   .withColumn("processed_at", current_timestamp())
            
            dataframes.append(df)
        
        # Union all dataframes
        if dataframes:
            combined_df = dataframes[0]
            for df in dataframes[1:]:
                combined_df = combined_df.unionByName(df, allowMissingColumns=True)
            
            # Write to structured table
            output_path = f"s3://{Config.S3_BUCKET}/processed/ensembl_mapping/"
            print(f"Writing processed data to {output_path}...")
            
            combined_df.write \
                .mode("overwrite") \
                .parquet(output_path)
            
            print("Spark processing completed successfully.")
        
        spark.stop()
        
    except Exception as e:
        # Task failure notification
        if Config.SLACK_WEBHOOK_URL:
            try:
                slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
                slack.notify(f"❌ Task failed: process_ensembl_data_with_spark - {str(e)}")
            except Exception:
                pass
        raise

@flow(name="ensembl-mapping-pipeline")
def ensembl_mapping_pipeline():
    """
    Main flow that orchestrates the Ensembl data ETL pipeline.
    """
    # Flow start notification
    if Config.SLACK_WEBHOOK_URL:
        try:
            slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
            context = get_run_context()
            flow_run_name = context.flow_run.name
            slack.notify(f"🚀 Ensembl mapping pipeline started: {flow_run_name}")
        except Exception:
            pass
    
    try:
        # Step 1: Check for new data and download to S3
        downloaded_files = check_and_download_ensembl_data()
        
        # If no files were downloaded, skip the rest of the pipeline
        if not downloaded_files:
            print("No new data available. Pipeline completed without processing.")
            if Config.SLACK_WEBHOOK_URL:
                try:
                    slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
                    slack.notify("✅ Pipeline completed: No new data to process.")
                except Exception:
                    pass
            return
        
        # Step 2: Process the data with Spark
        process_ensembl_data_with_spark(downloaded_files)
        
        # Flow completion notification
        if Config.SLACK_WEBHOOK_URL:
            try:
                slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
                slack.notify("✅ Ensembl mapping pipeline completed successfully!")
            except Exception:
                pass
        
    except Exception as e:
        # Flow failure notification
        if Config.SLACK_WEBHOOK_URL:
            try:
                slack = SlackWebhook(url=Config.SLACK_WEBHOOK_URL)
                slack.notify(f"❌ Pipeline failed: {str(e)}")
            except Exception:
            pass
        raise

if __name__ == "__main__":
    # Manual execution
    ensembl_mapping_pipeline()