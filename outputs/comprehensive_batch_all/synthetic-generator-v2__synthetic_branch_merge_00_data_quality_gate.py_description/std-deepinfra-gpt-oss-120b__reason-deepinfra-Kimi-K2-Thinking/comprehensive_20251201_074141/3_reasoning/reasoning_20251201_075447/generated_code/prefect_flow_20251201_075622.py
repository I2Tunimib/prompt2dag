from prefect import flow, task
from typing import Dict, Any
import os
import csv
import shutil
from pathlib import Path


@task(retries=1, retry_delay_seconds=300)
def ingest_csv(source_path: str) -> Dict[str, Any]:
    """Ingest raw customer CSV data and return file metadata."""
    if not os.path.exists(source_path):
        raise FileNotFoundError(f"Source file not found: {source_path}")
    
    file_stats = os.stat(source_path)
    metadata = {
        "source_path": source_path,
        "file_size": file_stats.st_size,
        "file_name": os.path.basename(source_path),
        "temp_dir": "./temp"
    }
    
    os.makedirs(metadata["temp_dir"], exist_ok=True)
    return metadata


@task(retries=1, retry_delay_seconds=300)
def quality_check(file_metadata: Dict[str, Any]) -> float:
    """Calculate data quality score based on completeness and validity."""
    source_path = file_metadata["source_path"]
    
    try:
        with open(source_path, 'r', newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            total_rows = 0
            complete_rows = 0
            
            for row in reader:
                total_rows += 1
                if all(value.strip() for value in row.values()):
                    complete_rows += 1
            
        if total_rows == 0:
            return 0.0
        
        return complete_rows / total_rows
        
    except Exception as e:
        print(f"Error during quality check: {e}")
        return 0.0


@task(retries=1, retry_delay_seconds=300)
def production_load(file_metadata: Dict[str, Any]) -> None:
    """Load high-quality data to production database."""
    print(f"Loading {file_metadata['file_name']} to production database...")
    print(f"File size: {file_metadata['file_size']} bytes")
    
    prod_marker = f"./prod_loaded_{file_metadata['file_name']}.marker"
    with open(prod_marker, 'w') as f:
        f.write("loaded")
    
    print("Production load completed successfully.")


@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(file_metadata: Dict[str, Any]) -> None:
    """Move low-quality data to quarantine storage."""
    source_path = file_metadata["source_path"]
    quarantine_dir = "./quarantine"
    
    os.makedirs(quarantine_dir, exist_ok=True)
    
    file_name = file_metadata["file_name"]
    quarantine_path = os.path.join(quarantine_dir, file_name)
    
    if os.path.exists(source_path):
        shutil.move(source_path, quarantine_path)
        print(f"Moved {file_name} to quarantine: {quarantine_path}")
    else:
        print(f"File {source_path} already moved or missing.")


@task(retries=1, retry_delay_seconds=300)
def send_alert_email(file_metadata: Dict[str, Any]) -> None:
    """Send email alert to data stewards about low-quality data."""
    file_name = file_metadata["file_name"]
    print(f"Sending alert email for {file_name}...")
    print("To: data-stewards@company.com")
    print("Subject: Data Quality Alert - Quarantined File")
    print(f"Body: File {file_name} has been quarantined due to low quality score.")
    print("Alert email sent successfully.")


@task(retries=1, retry_delay_seconds=300)
def cleanup(file_metadata: Dict[str, Any]) -> None:
    """Perform final cleanup of temporary files and resources."""
    temp_dir = file_metadata.get("temp_dir", "./temp")
    
    if os.path.exists(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"Cleaned up temporary directory: {temp_dir}")
    
    marker_files = [f"./prod_loaded_{file_metadata['file_name']}.marker"]
    for marker in marker_files:
        if os.path.exists(marker):
            os.remove(marker)
            print(f"Removed marker file: {marker}")
    
    print("Cleanup completed.")


@flow(name="data-quality-gate-pipeline")
def data_quality_gate_pipeline(source_path: str = "./data/customers.csv"):
    """
    Data quality gate pipeline that ingests customer CSV data,
    performs quality assessment, and routes data based on quality scores.
    
    Schedule: Daily execution recommended
    Retry policy: Single retry with 5-minute delay on failure
    """
    file_metadata = None
    try:
        file_metadata = ingest_csv(source_path)
        quality_score = quality_check(file_metadata)
        print(f"Quality score: {quality_score:.2%}")
        
        if quality_score >= 0.95:
            print("Quality threshold met. Routing to production.")
            load_task = production_load.submit(file_metadata)
            load_task.result()
        else:
            print("Quality threshold not met. Routing to quarantine.")
            quarantine_task = quarantine_and_alert.submit(file_metadata)
            quarantine_task.result()
            email_task = send_alert_email.submit(file_metadata)
            email_task.result()
    finally:
        if file_metadata:
            cleanup(file_metadata)


if __name__ == "__main__":
    os.makedirs("./data", exist_ok=True)
    
    sample_data = """customer_id,name,email,age
C001,John Doe,john@example.com,30
C002,Jane Smith,jane@example.com,25
C003,Bob Johnson,bob@example.com,
C004,Alice Brown,alice@example.com,35
C005,Charlie Wilson,charlie@example.com,40
"""
    
    with open("./data/customers.csv", "w") as f:
        f.write(sample_data)
    
    data_quality_gate_pipeline("./data/customers.csv")