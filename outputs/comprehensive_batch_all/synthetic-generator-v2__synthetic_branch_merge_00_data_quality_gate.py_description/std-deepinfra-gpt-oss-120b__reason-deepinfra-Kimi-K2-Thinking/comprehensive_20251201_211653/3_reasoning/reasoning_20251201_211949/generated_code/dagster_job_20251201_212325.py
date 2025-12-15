from dagster import op, job, RetryPolicy, resource, Out, Output
import pandas as pd
from typing import Dict, Any

# Stub resources
@resource
def email_resource(_):
    # In real usage, this would configure SMTP credentials
    return {"smtp_host": "smtp.example.com", "from_email": "alerts@example.com"}

@resource
def production_db_resource(_):
    # In real usage, this would return a database connection
    return {"connection_string": "postgresql://user:pass@host:5432/prod"}

@resource
def quarantine_storage_resource(_):
    # In real usage, this would return a storage client
    return {"path": "/data/quarantine"}

# Ops
@op(
    out={"file_metadata": Out(Dict[str, Any])},
    retry_policy=RetryPolicy(max_retries=1, delay=300)  # 5 minute delay
)
def ingest_csv(context):
    """Ingests raw customer CSV data from source location and returns file metadata."""
    # For demonstration, create a sample CSV
    import tempfile
    import os
    
    # Create sample data
    data = {
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "David", "Eve"],
        "email": ["alice@example.com", "bob@example.com", "charlie@example.com", 
                  "david@example.com", "eve@example.com"]
    }
    df = pd.DataFrame(data)
    
    # Write to temp file
    temp_dir = tempfile.gettempdir()
    file_path = os.path.join(temp_dir, "customers.csv")
    df.to_csv(file_path, index=False)
    
    metadata = {
        "file_path": file_path,
        "row_count": len(df),
        "column_count": len(df.columns),
        "size_bytes": os.path.getsize(file_path)
    }
    
    context.log.info(f"Ingested CSV: {metadata}")
    return metadata

@op(
    out={"quality_result": Out(Dict[str, Any])},
    retry_policy=RetryPolicy(max_retries=1, delay=300)
)
def quality_check(context, file_metadata):
    """Calculates data quality score using completeness and validity metrics."""
    import pandas as pd
    
    # Read the CSV
    df = pd.read_csv(file_metadata["file_path"])
    
    # Calculate completeness (no nulls)
    completeness = (1 - df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
    
    # Calculate validity (simple email validation for demo)
    email_pattern = r'^[\w\.-]+@[\w\.-]+\.\w+$'
    valid_emails = df["email"].str.match(email_pattern).sum()
    validity = (valid_emails / len(df)) * 100
    
    # Overall quality score
    quality_score = (completeness + validity) / 2
    
    result = {
        "file_metadata": file_metadata,
        "quality_score": quality_score,
        "completeness": completeness,
        "validity": validity,
        "passed": quality_score >= 95
    }
    
    context.log.info(f"Quality score: {quality_score:.2f}% (threshold: 95%)")
    return result

@op(retry_policy=RetryPolicy(max_retries=1, delay=300))
def production_load(context, quality_result):
    """Loads data to production database if quality score is high enough."""
    # Only execute if quality passed
    if not quality_result["passed"]:
        context.log.info("Quality check failed, skipping production load")
        return {"status": "skipped", "records_loaded": 0}
    
    # In real usage, would load to production DB
    db = context.resources.production_db
    context.log.info(f"Loading {quality_result['file_metadata']['row_count']} records to production database")
    
    # Simulate load
    return {
        "status": "loaded",
        "records_loaded": quality_result["file_metadata"]["row_count"]
    }

@op(retry_policy=RetryPolicy(max_retries=1, delay=300))
def quarantine_and_alert(context, quality_result):
    """Moves data to quarantine if quality score is too low."""
    # Only execute if quality failed
    if quality_result["passed"]:
        context.log.info("Quality check passed, skipping quarantine")
        return {"status": "skipped", "quarantine_path": None}
    
    # In real usage, would move to quarantine storage
    storage = context.resources.quarantine_storage
    file_path = quality_result["file_metadata"]["file_path"]
    
    import shutil
    import os
    
    quarantine_dir = storage["path"]
    os.makedirs(quarantine_dir, exist_ok=True)
    quarantine_path = os.path.join(quarantine_dir, "quarantined_customers.csv")
    shutil.move(file_path, quarantine_path)
    
    context.log.info(f"Moved file to quarantine: {quarantine_path}")
    
    return {
        "status": "quarantined",
        "quarantine_path": quarantine_path,
        "quality_score": quality_result["quality_score"]
    }

@op(
    required_resource_keys={"email"},
    retry_policy=RetryPolicy(max_retries=1, delay=300)
)
def send_alert_email(context, quarantine_result):
    """Sends alert email to data stewards if data was quarantined."""
    # Only send if quarantine actually happened
    if quarantine_result["status"] != "quarantined":
        context.log.info("No quarantine needed, skipping alert")
        return {"status": "skipped", "emails_sent": 0}
    
    # In real usage, would send actual email
    email_config = context.resources.email
    context.log.info(
        f"Sending alert email via {email_config['smtp_host']} from {email_config['from_email']}"
    )
    context.log.info(
        f"Alert: Data quality score {quarantine_result['quality_score']:.2f}% below threshold. "
        f"File moved to {quarantine_result['quarantine_path']}"
    )
    
    return {
        "status": "sent",
        "emails_sent": 1,
        "quality_score": quarantine_result["quality_score"]
    }

@op(retry_policy=RetryPolicy(max_retries=1, delay=300))
def cleanup(context, production_result, email_result):
    """Performs final cleanup operations for temporary files and resources."""
    # Clean up temp files if they still exist
    import os
    
    # In real usage, would clean up based on actual file paths
    temp_dir = "/tmp"
    csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
    
    for f in csv_files:
        try:
            os.remove(os.path.join(temp_dir, f))
            context.log.info(f"Cleaned up file: {f}")
        except FileNotFoundError:
            pass
    
    context.log.info("Cleanup completed")
    return {
        "status": "cleaned",
        "production_status": production_result["status"],
        "alert_status": email_result["status"]
    }

# Job
@job(
    resource_defs={
        "email": email_resource,
        "production_db": production_db_resource,
        "quarantine_storage": quarantine_storage_resource
    }
)
def data_quality_gate_pipeline():
    """Data quality gate pipeline that routes data based on quality scores."""
    
    # Ingest data
    file_metadata = ingest_csv()
    
    # Check quality
    quality_result = quality_check(file_metadata)
    
    # Branch based on quality
    production_result = production_load(quality_result)
    quarantine_result = quarantine_and_alert(quality_result)
    
    # Send alert if quarantined
    email_result = send_alert_email(quarantine_result)
    
    # Cleanup after both paths complete
    cleanup(production_result, email_result)

# Launch pattern
if __name__ == "__main__":
    result = data_quality_gate_pipeline.execute_in_process()
    assert result.success