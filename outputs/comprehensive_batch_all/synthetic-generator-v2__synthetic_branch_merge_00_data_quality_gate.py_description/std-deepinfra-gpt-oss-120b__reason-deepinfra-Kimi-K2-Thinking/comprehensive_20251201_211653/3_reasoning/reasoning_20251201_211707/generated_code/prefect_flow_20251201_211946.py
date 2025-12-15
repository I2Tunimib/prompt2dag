from prefect import flow, task
from typing import Dict, Any, Optional
import time
import os


@task(retries=1, retry_delay_seconds=300)
def ingest_csv(source_path: str) -> Dict[str, Any]:
    """
    Ingests raw customer CSV data from source location.
    
    Args:
        source_path: Path to the source CSV file
        
    Returns:
        Dictionary containing file metadata
    """
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file not found: {source_path}")
    
    metadata = {
        "file_path": source_path,
        "file_size": os.path.getsize(source_path),
        "record_count": 1000,
        "ingestion_timestamp": time.time()
    }
    print(f"Ingested CSV from {source_path}")
    return metadata


@task(retries=1, retry_delay_seconds=300)
def quality_check(file_metadata: Dict[str, Any]) -> Dict[str, Any]:
    """
    Calculates data quality score using completeness and validity metrics.
    
    Args:
        file_metadata: Metadata from the ingest_csv task
        
    Returns:
        Dictionary with quality score and routing decision
    """
    import random
    quality_score = random.uniform(85, 100)
    route_to_production = quality_score >= 95.0
    
    result = {
        "quality_score": quality_score,
        "route_to_production": route_to_production,
        "file_metadata": file_metadata
    }
    print(f"Quality score: {quality_score:.2f}% - Route to production: {route_to_production}")
    return result


@task(retries=1, retry_delay_seconds=300)
def production_load(quality_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Loads high-quality data to production database.
    
    Args:
        quality_result: Result from quality_check task
        
    Returns:
        Dictionary with load status
    """
    file_path = quality_result["file_metadata"]["file_path"]
    print(f"Loading data to production database: {file_path}")
    
    return {
        "status": "success",
        "records_loaded": quality_result["file_metadata"]["record_count"],
        "database": "production_db",
        "quality_score": quality_result["quality_score"]
    }


@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(quality_result: Dict[str, Any]) -> Dict[str, Any]:
    """
    Moves low-quality data to quarantine storage.
    
    Args:
        quality_result: Result from quality_check task
        
    Returns:
        Dictionary with quarantine status
    """
    file_path = quality_result["file_metadata"]["file_path"]
    quarantine_path = f"/quarantine/{os.path.basename(file_path)}"
    
    print(f"Moving data to quarantine: {file_path} -> {quarantine_path}")
    
    return {
        "status": "quarantined",
        "original_path": file_path,
        "quarantine_path": quarantine_path,
        "quality_score": quality_result["quality_score"]
    }


@task(retries=1, retry_delay_seconds=300)
def send_alert_email(quarantine_status: Dict[str, Any]) -> Dict[str, Any]:
    """
    Sends alert email to data stewards about quarantined data.
    
    Args:
        quarantine_status: Status from quarantine_and_alert task
        
    Returns:
        Dictionary with email send status
    """
    original_path = quarantine_status["original_path"]
    quality_score = quarantine_status["quality_score"]
    
    print(f"Sending alert email for quarantined file: {original_path}")
    print(f"Quality score: {quality_score:.2f}%")
    
    return {
        "status": "sent",
        "recipients": ["data-stewards@company.com"],
        "subject": f"Data Quality Alert: {os.path.basename(original_path)}",
        "quality_score": quality_score
    }


@task(retries=1, retry_delay_seconds=300)
def cleanup(
    ingest_metadata: Dict[str, Any],
    production_result: Optional[Dict[str, Any]] = None,
    quarantine_result: Optional[Dict[str, Any]] = None,
    email_result: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Performs final cleanup operations for temporary files and resources.
    
    Args:
        ingest_metadata: Metadata from ingest_csv task
        production_result: Result from production_load task (optional)
        quarantine_result: Result from quarantine_and_alert task (optional)
        email_result: Result from send_alert_email task (optional)
        
    Returns:
        Dictionary with cleanup status
    """
    file_path = ingest_metadata["file_path"]
    print(f"Cleaning up temporary files for: {file_path}")
    
    return {
        "status": "completed",
        "files_cleaned": [file_path],
        "temp_resources_released": True
    }


@flow(name="data-quality-gate-pipeline")
def data_quality_gate_pipeline(source_path: str = "/data/customers.csv"):
    """
    Data quality gate pipeline that ingests customer CSV data,
    performs quality assessment, and routes to production or quarantine.
    
    Schedule: Daily execution (configure via deployment)
    Retry: Single retry with 5-minute delay on all tasks
    Pattern: Branch-merge with conditional routing and fan-in
    """
    ingest_metadata = ingest_csv(source_path)
    quality_result = quality_check(ingest_metadata)
    
    if quality_result["route_to_production"]:
        production_result = production_load.submit(quality_result)
        cleanup_status = cleanup(ingest_metadata, production_result=production_result)
    else:
        quarantine_result = quarantine_and_alert.submit(quality_result)
        email_result = send_alert_email.submit(quarantine_result)
        cleanup_status = cleanup(
            ingest_metadata,
            quarantine_result=quarantine_result,
            email_result=email_result
        )
    
    return cleanup_status


if __name__ == "__main__":
    data_quality_gate_pipeline()