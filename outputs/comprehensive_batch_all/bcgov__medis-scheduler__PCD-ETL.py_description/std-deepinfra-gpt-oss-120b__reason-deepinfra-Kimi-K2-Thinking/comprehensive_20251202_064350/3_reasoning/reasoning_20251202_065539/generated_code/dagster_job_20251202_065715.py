from dagster import op, job, In, Out, Nothing, Config, OpExecutionContext
import time
import random

# Simulated Kubernetes job execution
def execute_kubernetes_job(job_name: str, context: OpExecutionContext):
    context.log.info(f"Executing Kubernetes job: {job_name}")
    # Simulate job execution
    time.sleep(2)
    # Randomly succeed or fail for demonstration
    if random.random() > 0.9:
        raise Exception(f"Kubernetes job {job_name} failed")
    return f"{job_name}_result"

# Simulated HTTP API call
def call_http_api(api_name: str, context: OpExecutionContext):
    context.log.info(f"Calling HTTP API: {api_name}")
    time.sleep(1)
    # Simulate API call
    if random.random() > 0.95:
        raise Exception(f"API call {api_name} failed")
    return f"{api_name}_data"

# Op definitions
@op
def check_pcd_sftp_folder(context: OpExecutionContext):
    """Check PCD SFTP folder for files."""
    return execute_kubernetes_job("check_pcd_sftp_folder", context)

@op
def check_pcd_shared_folder(context: OpExecutionContext, _sftp_result):
    """Check PCD shared folder for files."""
    return execute_kubernetes_job("check_pcd_shared_folder", context)

@op
def start_pcd_extract_1(context: OpExecutionContext, _shared_result):
    """Synchronization point for starting parallel extraction."""
    context.log.info("Starting parallel extraction phase 1")
    return "extract_1_started"

# Parallel extraction ops
@op
def status_tracker(context: OpExecutionContext, _start_signal):
    """Extract status tracker data."""
    return call_http_api("status_tracker", context)

@op
def financial_expense(context: OpExecutionContext, _start_signal):
    """Extract financial expense data."""
    return call_http_api("financial_expense", context)

# ... (all 18 extraction ops)

@op
def start_pcd_extract_2(context: OpExecutionContext, _status_tracker_result):
    """Synchronization point for extraction phase 2."""
    context.log.info("Starting extraction phase 2")
    return "extract_2_started"

@op
def pcd_file_upload(context: OpExecutionContext, **kwargs):
    """Upload consolidated files."""
    # This op depends on all extraction results
    context.log.info("Consolidating extraction results for upload")
    return execute_kubernetes_job("pcd_file_upload", context)

@op
def etl_notification(context: OpExecutionContext, upload_result):
    """Send ETL notification email."""
    context.log.info(f"Sending ETL notification for result: {upload_result}")
    # Simulate email sending
    return "notification_sent"

# Job definition
@job
def pcd_etl_pipeline():
    # Folder checks
    sftp_result = check_pcd_sftp_folder()
    shared_result = check_pcd_shared_folder(sftp_result)
    
    # Start extraction phase 1
    start_1 = start_pcd_extract_1(shared_result)
    
    # Parallel extraction tasks
    status_result = status_tracker(start_1)
    financial_result = financial_expense(start_1)
    # ... all other extraction ops
    
    # Start extraction phase 2
    start_2 = start_pcd_extract_2(status_result)
    
    # File upload depends on all extraction tasks
    upload_inputs = {
        "status_tracker_result": status_result,
        "financial_expense_result": financial_result,
        # ... all other results
        "start_pcd_extract_2_result": start_2,
    }
    upload_result = pcd_file_upload(**upload_inputs)
    
    # Notification depends on upload
    etl_notification(upload_result)

# Launch pattern
if __name__ == "__main__":
    result = pcd_etl_pipeline.execute_in_process()