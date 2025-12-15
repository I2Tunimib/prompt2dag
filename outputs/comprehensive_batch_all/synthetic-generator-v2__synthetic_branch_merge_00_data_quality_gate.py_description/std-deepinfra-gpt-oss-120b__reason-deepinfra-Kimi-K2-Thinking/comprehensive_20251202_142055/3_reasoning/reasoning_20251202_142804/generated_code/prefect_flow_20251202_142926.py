from prefect import flow, task
from typing import Dict, Any
import time

# Simulated integrations - replace with actual implementations
@task(retries=1, retry_delay_seconds=300)
def ingest_csv(source_path: str) -> Dict[str, Any]:
    """Ingests raw customer CSV data from source location."""
    print(f"Ingesting CSV from {source_path}")
    return {
        "file_path": source_path,
        "row_count": 1000,
        "ingestion_time": time.time()
    }

@task(retries=1, retry_delay_seconds=300)
def quality_check(file_metadata: Dict[str, Any]) -> float:
    """Calculates data quality score using completeness and validity metrics."""
    print(f"Calculating quality score for {file_metadata['file_path']}")
    # Simulate quality score calculation - in production implement actual checks
    return 96.5  # Example: ≥95% threshold triggers production path

@task(retries=1, retry_delay_seconds=300)
def production_load(file_metadata: Dict[str, Any]) -> str:
    """Loads high-quality data to production database."""
    print(f"Loading data to production database: {file_metadata['file_path']}")
    return "production_load_success"

@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(file_metadata: Dict[str, Any]) -> str:
    """Moves low-quality data to quarantine storage."""
    print(f"Moving data to quarantine: {file_metadata['file_path']}")
    return "quarantine_success"

@task(retries=1, retry_delay_seconds=300)
def send_alert_email(file_metadata: Dict[str, Any], quarantine_result: str) -> str:
    """Sends alert email to data stewards about quarantined data."""
    print(f"Sending alert email for quarantined file: {file_metadata['file_path']}")
    return "email_sent"

@task(retries=1, retry_delay_seconds=300)
def cleanup(file_metadata: Dict[str, Any], upstream_result: Any) -> None:
    """
    Performs final cleanup operations for temporary files and resources.
    The upstream_result parameter ensures this task waits for the branch to complete.
    """
    print(f"Cleaning up temporary files for: {file_metadata['file_path']}")

@flow(name="data-quality-gate-pipeline")
def data_quality_gate_pipeline(source_path: str = "./data/customers.csv"):
    """
    Data quality gate pipeline that ingests customer CSV data,
    performs quality assessment, and routes data based on quality scores.
    
    Schedule: Daily execution (configure via deployment)
    Retry policy: Single retry with 5-minute delay on task failures
    """
    # Step 1: Ingest CSV data
    file_metadata = ingest_csv(source_path)
    
    # Step 2: Perform quality check
    quality_score = quality_check(file_metadata)
    
    # Step 3: Conditional branch with fan-out pattern
    if quality_score >= 95.0:
        # High quality path: production load
        branch_result = production_load.submit(file_metadata)
    else:
        # Low quality path: quarantine and alert
        quarantine_result = quarantine_and_alert.submit(file_metadata)
        branch_result = send_alert_email.submit(file_metadata, quarantine_result)
    
    # Step 4: Cleanup with fan-in from both paths
    cleanup(file_metadata, branch_result)

if __name__ == "__main__":
    # Local execution entry point
    data_quality_gate_pipeline()