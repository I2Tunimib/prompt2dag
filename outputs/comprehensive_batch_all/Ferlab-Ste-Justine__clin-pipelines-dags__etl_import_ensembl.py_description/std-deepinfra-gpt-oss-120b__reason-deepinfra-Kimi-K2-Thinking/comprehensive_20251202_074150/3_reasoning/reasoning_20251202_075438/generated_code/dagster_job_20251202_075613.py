from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    ResourceParam,
    get_dagster_logger,
    In,
    Out,
    Nothing,
)
import boto3
from typing import List, Dict, Optional
import ftplib
import hashlib
import os
import json

logger = get_dagster_logger()

# Minimal config classes
class EnsemblConfig(Config):
    ftp_host: str = "ftp.ensembl.org"
    ftp_path: str = "/pub/current_tsv/homo_sapiens/"
    data_types: List[str] = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    s3_bucket: str = "my-genomics-bucket"
    s3_prefix: str = "raw/landing/ensembl/"

class SparkConfig(Config):
    kubernetes_namespace: str = "spark-jobs"
    spark_image: str = "my-spark-image:latest"
    main_class: str = "com.example.EnsemblProcessing"
    s3_output_path: str = "s3a://my-genomics-bucket/processed/ensembl_mapping"

# Minimal resource stubs
class S3Resource:
    def __init__(self, bucket: str):
        self.bucket = bucket
        self.client = boto3.client('s3')
    
    def get_object(self, key: str):
        try:
            return self.client.get_object(Bucket=self.bucket, Key=key)
        except:
            return None
    
    def put_object(self, key: str, body: bytes):
        self.client.put_object(Bucket=self.bucket, Key=key, Body=body)
    
    def list_objects(self, prefix: str):
        resp = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return resp.get('Contents', [])

class SlackResource:
    def __init__(self, webhook_url: str):
        self.webhook_url = webhook_url
    
    def notify(self, message: str):
        # In real implementation, would post to Slack
        logger.info(f"SLACK NOTIFICATION: {message}")

# Op 1: Check for new versions and upload to S3
@op(
    out={"updated_files": Out(List[str], description="List of data types that were updated")}
)
def check_and_upload_ensembl_files(
    context: OpExecutionContext,
    config: EnsemblConfig,
    s3: ResourceParam[S3Resource],
    slack: ResourceParam[SlackResource]
) -> List[str]:
    """Check Ensembl FTP for new data versions and upload to S3 if updated."""
    
    slack.notify("Starting Ensembl ETL pipeline: checking for new data versions")
    
    updated_data_types = []
    
    for data_type in config.data_types:
        context.log.info(f"Checking data type: {data_type}")
        
        # Simulate version check - in real implementation would query FTP
        # For demo, we'll check if a version file exists in S3
        version_key = f"{config.s3_prefix}{data_type}/version.txt"
        existing_version = None
        
        try:
            obj = s3.get_object(version_key)
            if obj:
                existing_version = obj['Body'].read().decode('utf-8').strip()
        except:
            pass
        
        # Simulate FTP check - would connect to Ensembl FTP and get latest version
        # For this example, we'll just generate a mock version
        latest_version = f"2024.1-{data_type}"
        
        if existing_version != latest_version:
            context.log.info(f"New version found for {data_type}: {latest_version}")
            
            # Simulate download and MD5 validation
            # In real implementation: download file, check MD5 checksum
            try:
                # Mock file content
                file_content = f"ensembl_data_{data_type}\n".encode('utf-8')
                md5_hash = hashlib.md5(file_content).hexdigest()
                
                # Upload to S3
                file_key = f"{config.s3_prefix}{data_type}/data.tsv"
                s3.put_object(file_key, file_content)
                
                # Upload version file
                s3.put_object(version_key, latest_version.encode('utf-8'))
                
                updated_data_types.append(data_type)
                slack.notify(f"✅ Updated {data_type} to version {latest_version}")
                
            except Exception as e:
                context.log.error(f"Failed to process {data_type}: {e}")
                slack.notify(f"❌ Failed to process {data_type}: {e}")
                raise
        else:
            context.log.info(f"No update needed for {data_type}")
    
    if not updated_data_types:
        slack.notify("⚠️ No new Ensembl data versions found. Pipeline will skip processing.")
    else:
        slack.notify(f"📥 Downloaded {len(updated_data_types)} updated data types: {', '.join(updated_data_types)}")
    
    return updated_data_types

# Op 2: Process with Spark
@op(
    ins={"updated_files": In(List[str], description="List of data types to process")},
    out={"success": Out(bool, description="Whether Spark processing succeeded")}
)
def process_with_spark(
    context: OpExecutionContext,
    config: SparkConfig,
    updated_files: List[str],
    s3: ResourceParam[S3Resource],
    slack: ResourceParam[SlackResource]
) -> bool:
    """Process uploaded Ensembl files with Spark and load into structured table."""
    
    if not updated_files:
        context.log.info("No files to process, skipping Spark job")
        return True
    
    slack.notify(f"🔥 Starting Spark processing for: {', '.join(updated_files)}")
    
    try:
        # In real implementation, would submit Spark job to Kubernetes
        # For this example, we'll simulate the processing
        
        context.log.info(f"Submitting Spark job to Kubernetes namespace: {config.kubernetes_namespace}")
        context.log.info(f"Using Spark image: {config.spark_image}")
        context.log.info(f"Processing data types: {updated_files}")
        context.log.info(f"Output path: {config.s3_output_path}")
        
        # Simulate Spark processing
        # Would read from s3://bucket/raw/landing/ensembl/{data_type}/data.tsv
        # Apply transformations
        # Write to structured table at config.s3_output_path
        
        # Verify output was created (mock)
        context.log.info("Spark job completed successfully")
        slack.notify(f"✅ Spark processing complete. Data loaded to {config.s3_output_path}")
        
        return True
        
    except Exception as e:
        context.log.error(f"Spark processing failed: {e}")
        slack.notify(f"❌ Spark processing failed: {e}")
        raise

# Define the job
@job(
    description="ETL pipeline for Ensembl genomic mapping data from FTP to S3 and Spark processing"
)
def ensembl_etl_pipeline():
    updated_files = check_and_upload_ensembl_files()
    process_with_spark(updated_files)

# Resources configuration for execution
resources = {
    "s3": S3Resource(bucket="my-genomics-bucket"),
    "slack": SlackResource(webhook_url="https://hooks.slack.com/services/YOUR/WEBHOOK/URL")
}

# Launch pattern
if __name__ == '__main__':
    result = ensembl_etl_pipeline.execute_in_process(
        run_config={
            "ops": {
                "check_and_upload_ensembl_files": {
                    "config": {
                        "ftp_host": "ftp.ensembl.org",
                        "ftp_path": "/pub/current_tsv/homo_sapiens/",
                        "data_types": ["canonical", "ena", "entreze", "refseq", "uniprot"],
                        "s3_bucket": "my-genomics-bucket",
                        "s3_prefix": "raw/landing/ensembl/"
                    }
                },
                "process_with_spark": {
                    "config": {
                        "kubernetes_namespace": "spark-jobs",
                        "spark_image": "my-spark-image:latest",
                        "main_class": "com.example.EnsemblProcessing",
                        "s3_output_path": "s3a://my-genomics-bucket/processed/ensembl_mapping"
                    }
                }
            },
            "resources": {
                "s3": {
                    "config": {
                        "bucket": "my-genomics-bucket"
                    }
                },
                "slack": {
                    "config": {
                        "webhook_url": "https://hooks.slack.com/services/YOUR/WEBHOOK/URL"
                    }
                }
            }
        }
    )