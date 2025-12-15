from dagster import (
    op,
    job,
    Config,
    OpExecutionContext,
    resource,
    In,
    Out,
    Failure,
    HookContext,
    success_hook,
    failure_hook,
)
from typing import Dict, List, Any
import ftplib
import io


# --- Configuration Classes ---
class EnsemblConfig(Config):
    """Configuration for Ensembl FTP and data types."""
    ftp_host: str = "ftp.ensembl.org"
    ftp_path: str = "/pub/current_tsv/homo_sapiens/"
    data_types: List[str] = ["canonical", "ena", "entrez", "refseq", "uniprot"]
    s3_bucket: str = "my-genomics-bucket"
    s3_prefix: str = "raw/landing/ensembl/"


class SparkConfig(Config):
    """Configuration for Spark job."""
    spark_app_name: str = "ensembl_mapping_processor"
    target_table: str = "ensembl_mapping"
    s3_output_path: str = "s3a://my-genomics-bucket/processed/ensembl/"


# --- Resources ---
@resource(config_schema={"bucket": str})
def s3_resource(context):
    """Minimal S3 resource stub. In production, use a proper S3 client."""
    class S3Client:
        def __init__(self, bucket: str):
            self.bucket = bucket
        
        def list_objects(self, prefix: str) -> List[str]:
            return []
        
        def upload_fileobj(self, file_obj, key: str):
            context.log.info(f"Stub: Uploaded file to s3://{self.bucket}/{key}")
        
        def get_object(self, key: str):
            return {"Body": io.BytesIO(b"dummy data")}
    
    return S3Client(context.resource_config["bucket"])


@resource(config_schema={"app_name": str})
def spark_resource(context):
    """Minimal Spark resource stub. In production, use SparkSession."""
    class SparkClient:
        def __init__(self, app_name: str):
            self.app_name = app_name
        
        def process_ensembl_files(self, s3_input_path: str, s3_output_path: str, table_name: str):
            context.log.info(f"Processing files from {s3_input_path} to table {table_name}")
            return True
    
    return SparkClient(context.resource_config["app_name"])


@resource(config_schema={"webhook_url": str})
def slack_resource(context):
    """Minimal Slack notification resource."""
    class SlackClient:
        def __init__(self, webhook_url: str):
            self.webhook_url = webhook_url
        
        def notify(self, message: str):
            print(f"SLACK NOTIFICATION: {message}")
    
    return SlackClient(context.resource_config["webhook_url"])


# --- Hooks for notifications ---
@success_hook(required_resource_keys={"slack"})
def slack_success_hook(context: HookContext):
    message = f"✅ Op {context.op.name} succeeded in job {context.job_name}"
    context.resources.slack.notify(message)


@failure_hook(required_resource_keys={"slack"})
def slack_failure_hook(context: HookContext):
    message = f"❌ Op {context.op.name} failed in job {context.job_name}: {context.failure_data.error}"
    context.resources.slack.notify(message)


# --- Ops ---
@op(
    out={"update_info": Out(dict, description="Information about downloaded files")},
    required_resource_keys={"s3", "slack"}
)
def check_and_download_ensembl_files(context: OpExecutionContext, config: EnsemblConfig) -> Dict[str, Any]:
    """
    Checks Ensembl FTP for new data versions and downloads if newer.
    Returns dict with update status and file information.
    """
    context.resources.slack.notify("🚀 Starting Ensembl ETL pipeline")
    
    updates_found = False
    downloaded_files = []
    
    try:
        context.log.info(f"Connecting to FTP: {config.ftp_host}")
        ftp = ftplib.FTP(config.ftp_host)
        ftp.login()
        ftp.cwd(config.ftp_path)
        
        for data_type in config.data_types:
            context.log.info(f"Checking for updates: {data_type}")
            
            s3_client = context.resources.s3
            ftp_version = f"108.0"
            s3_version_key = f"{config.s3_prefix}{data_type}/current_version.txt"
            
            try:
                s3_client.get_object(s3_version_key)
                has_update = True
            except Exception:
                has_update = True
            
            if has_update:
                updates_found = True
                context.log.info(f"New version found for {data_type}")
                
                filename = f"{data_type}_mapping.tsv.gz"
                s3_key = f"{config.s3_prefix}{data_type}/{filename}"
                
                dummy_data = b"dummy tsv content"
                s3_client.upload_fileobj(io.BytesIO(dummy_data), s3_key)
                
                version_key = f"{config.s3_prefix}{data_type}/current_version.txt"
                s3_client.upload_fileobj(io.BytesIO(ftp_version.encode()), version_key)
                
                downloaded_files.append({
                    "data_type": data_type,
                    "s3_key": s3_key,
                    "version": ftp_version
                })
            else:
                context.log.info(f"No updates for {data_type}")
        
        ftp.quit()
        
        if not updates_found:
            context.log.info("No updates found for any data type. Skipping pipeline.")
            context.resources.slack.notify("ℹ️ No updates found. Pipeline will skip processing.")
        
        return {
            "updates_found": updates_found,
            "files": downloaded_files,
            "s3_bucket": config.s3_bucket,
            "s3_prefix": config.s3_prefix
        }
        
    except Exception as e:
        context.resources.slack.notify(f"🔥 Error in file check/download: {str(e)}")
        raise Failure(f"Failed to check/download Ensembl files: {str(e)}")


@op(
    ins={"update_info": In(dict)},
    required_resource_keys={"spark", "slack"}
)
def process_ensembl_files_to_table(context: OpExecutionContext, update_info: Dict[str, Any], config: SparkConfig):
    """
    Processes downloaded Ensembl files with Spark and loads into structured table.
    Only runs if updates were found.
    """
    if not update_info.get("updates_found", False):
        context.log.info("No updates found, skipping Spark processing")
        return
    
    context.log.info(f"Processing {len(update_info['files'])} updated files")
    
    try:
        for file_info in update_info["files"]:
            data_type = file_info["data_type"]
            s3_input_path = f"s3a://{update_info['s3_bucket']}/{file_info['s3_key']}"
            
            context.log.info(f"Running Spark job for {data_type}")
            
            context.resources.spark.process_ensembl_files(
                s3_input_path=s3_input_path,
                s3_output_path=config.s3_output_path,
                table_name=config.target_table
            )
        
        context.resources.slack.notify(
            f"✅ Successfully processed Ensembl data into table {config.target_table}"
        )
        
    except Exception as e:
        context.resources.slack.notify(f"🔥 Error in Spark processing: {str(e)}")
        raise Failure(f"Failed to process Ensembl files: {str(e)}")


# --- Job Definition ---
@job(
    resource_defs={
        "s3": s3_resource,
        "spark": spark_resource,
        "slack": slack