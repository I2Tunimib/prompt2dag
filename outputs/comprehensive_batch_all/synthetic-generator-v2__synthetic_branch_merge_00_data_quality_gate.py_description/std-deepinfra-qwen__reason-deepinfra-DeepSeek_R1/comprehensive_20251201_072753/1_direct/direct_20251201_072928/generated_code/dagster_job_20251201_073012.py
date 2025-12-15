from dagster import op, job, RetryPolicy, Field, String, Int

# Resources and Config
class EmailResource:
    def send_email(self, recipient: str, subject: str, body: str):
        # Simplified email sending logic
        print(f"Sending email to {recipient}: {subject} - {body}")

class DatabaseResource:
    def load_data(self, data: str):
        # Simplified data loading logic
        print(f"Loading data: {data}")

class StorageResource:
    def move_to_quarantine(self, file_path: str):
        # Simplified quarantine logic
        print(f"Moving file to quarantine: {file_path}")

    def cleanup(self, file_path: str):
        # Simplified cleanup logic
        print(f"Cleaning up file: {file_path}")

# Ops
@op
def ingest_csv(context) -> dict:
    """Ingests raw customer CSV data from source location and returns file metadata."""
    file_path = "path/to/customer_data.csv"
    metadata = {"file_path": file_path, "file_size": 1024}
    context.log.info(f"Ingested file: {file_path}")
    return metadata

@op
def quality_check(context, metadata: dict) -> float:
    """Calculates data quality score using completeness and validity metrics."""
    file_path = metadata["file_path"]
    # Simplified quality check logic
    quality_score = 0.96  # Example score
    context.log.info(f"Quality score for {file_path}: {quality_score}")
    return quality_score

@op
def production_load(context, metadata: dict):
    """Loads data to production database."""
    file_path = metadata["file_path"]
    context.log.info(f"Loading data from {file_path} to production database")
    # Use DatabaseResource to load data
    context.resources.db.load_data(file_path)

@op
def quarantine_and_alert(context, metadata: dict):
    """Moves data to quarantine and sends an alert email."""
    file_path = metadata["file_path"]
    context.log.info(f"Moving data from {file_path} to quarantine")
    # Use StorageResource to move data to quarantine
    context.resources.storage.move_to_quarantine(file_path)
    context.log.info(f"Sending alert email for {file_path}")
    # Use EmailResource to send alert email
    context.resources.email.send_email(
        recipient="data-stewards@example.com",
        subject="Data Quality Alert",
        body=f"Data from {file_path} has been quarantined due to low quality score."
    )

@op
def send_alert_email(context, metadata: dict):
    """Sends an alert email to notify data stewards."""
    file_path = metadata["file_path"]
    context.log.info(f"Sending alert email for {file_path}")
    # Use EmailResource to send alert email
    context.resources.email.send_email(
        recipient="data-stewards@example.com",
        subject="Data Quality Alert",
        body=f"Data from {file_path} has been quarantined due to low quality score."
    )

@op
def cleanup(context, metadata: dict):
    """Performs final cleanup operations for temporary files and resources."""
    file_path = metadata["file_path"]
    context.log.info(f"Cleaning up file: {file_path}")
    # Use StorageResource to clean up
    context.resources.storage.cleanup(file_path)

# Job
@job(
    resource_defs={
        "email": EmailResource,
        "db": DatabaseResource,
        "storage": StorageResource
    },
    retry_policy=RetryPolicy(max_retries=1, delay=300)
)
def data_quality_gate():
    metadata = ingest_csv()
    quality_score = quality_check(metadata)
    
    production_load_op = production_load(metadata).with_hooks({quality_score >= 0.95})
    quarantine_and_alert_op = quarantine_and_alert(metadata).with_hooks({quality_score < 0.95})
    send_alert_email_op = send_alert_email(metadata).with_hooks({quality_score < 0.95})
    
    cleanup_op = cleanup(metadata)
    production_load_op >> cleanup_op
    quarantine_and_alert_op >> send_alert_email_op >> cleanup_op

if __name__ == '__main__':
    result = data_quality_gate.execute_in_process()