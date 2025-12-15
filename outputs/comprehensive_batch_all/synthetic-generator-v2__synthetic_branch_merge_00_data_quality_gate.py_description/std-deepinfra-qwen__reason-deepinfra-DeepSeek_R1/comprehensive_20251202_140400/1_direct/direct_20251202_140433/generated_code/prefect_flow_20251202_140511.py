from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd
import os
import smtplib
from email.message import EmailMessage

@task(retries=1, retry_delay_seconds=300, cache_key_fn=task_input_hash)
def ingest_csv(file_path: str) -> dict:
    """Ingests raw customer CSV data from source location and returns file metadata."""
    df = pd.read_csv(file_path)
    metadata = {
        "file_name": os.path.basename(file_path),
        "file_size": os.path.getsize(file_path),
        "row_count": len(df),
        "column_count": len(df.columns)
    }
    return metadata

@task(retries=1, retry_delay_seconds=300)
def quality_check(metadata: dict, df: pd.DataFrame) -> float:
    """Calculates data quality score using completeness and validity metrics."""
    completeness = df.notna().mean().mean()
    validity = df.apply(lambda col: col.astype(str).str.match(r'^\S+$').mean()).mean()
    quality_score = (completeness + validity) / 2
    return quality_score

@task(retries=1, retry_delay_seconds=300)
def production_load(df: pd.DataFrame):
    """Loads data to production database."""
    # Placeholder for production database load logic
    logger = get_run_logger()
    logger.info("Loading data to production database...")

@task(retries=1, retry_delay_seconds=300)
def quarantine_and_alert(df: pd.DataFrame, metadata: dict):
    """Moves data to quarantine and triggers alert."""
    # Placeholder for quarantine logic
    logger = get_run_logger()
    logger.info("Quarantining data...")
    send_alert_email(metadata)

@task(retries=1, retry_delay_seconds=300)
def send_alert_email(metadata: dict):
    """Sends an alert email to data stewards."""
    logger = get_run_logger()
    logger.info("Sending alert email...")
    msg = EmailMessage()
    msg.set_content(f"Data quality check failed for file: {metadata['file_name']}")
    msg['Subject'] = "Data Quality Alert"
    msg['From'] = "data.steward@example.com"
    msg['To'] = "data.steward@example.com"
    with smtplib.SMTP('localhost') as s:
        s.send_message(msg)

@task(retries=1, retry_delay_seconds=300)
def cleanup(file_path: str):
    """Performs final cleanup operations for temporary files and resources."""
    logger = get_run_logger()
    logger.info("Cleaning up temporary files...")
    os.remove(file_path)

@flow(retries=1, retry_delay_seconds=300)
def data_quality_gate_pipeline(file_path: str):
    """Orchestrates the data quality gate pipeline."""
    metadata = ingest_csv(file_path)
    df = pd.read_csv(file_path)
    quality_score = quality_check(metadata, df)
    
    if quality_score >= 0.95:
        production_load.submit(df)
    else:
        quarantine_and_alert.submit(df, metadata)
        send_alert_email.submit(metadata)
    
    cleanup(file_path)

if __name__ == '__main__':
    # Example file path for local execution
    file_path = "path/to/customer_data.csv"
    data_quality_gate_pipeline(file_path)

# Deployment/schedule configuration (optional)
# To schedule this flow to run daily, you can use Prefect's deployment and schedule features.
# Example:
# from prefect.deployments import Deployment
# from prefect.orion.schemas.schedules import IntervalSchedule
# from datetime import timedelta
# deployment = Deployment.build_from_flow(
#     flow=data_quality_gate_pipeline,
#     name="daily-data-quality-gate",
#     schedule=IntervalSchedule(interval=timedelta(days=1)),
#     work_queue_name="default"
# )
# deployment.apply()