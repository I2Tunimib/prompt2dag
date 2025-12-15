from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import time
import random

# Simulate Kubernetes job tasks
@task
def check_pcd_sftp_folder():
    # Simulate checking SFTP folder
    pass

@task
def check_pcd_shared_folder():
    # Simulate checking shared folder
    pass

# Synchronization point tasks
@task
def start_pcd_extract_1():
    # Just a marker task
    pass

@task
def start_pcd_extract_2():
    # Just a marker task
    pass

# HTTP API extraction tasks
@task
def status_tracker():
    pass

@task
def financial_expense():
    pass

# ... (all 18 extraction tasks)

@task
def pcd_file_upload():
    # Simulate Kubernetes job for file upload
    pass

@task
def etl_notification():
    # Send email notification
    pass

@flow(
    name="PCD ETL Pipeline",
    timeout_seconds=3600,  # 60 minutes
    # schedule can be configured at deployment time
)
def pcd_etl_flow():
    # Step 1: Check SFTP folder
    sftp_check = check_pcd_sftp_folder.submit()
    
    # Step 2: Check shared folder (depends on SFTP check)
    shared_check = check_pcd_shared_folder.submit(wait_for=[sftp_check])
    
    # Step 3: Synchronization point
    sync_point_1 = start_pcd_extract_1.submit(wait_for=[shared_check])
    
    # Step 4: Parallel HTTP API extraction tasks
    # All triggered by Start_PCD_Extract_1
    extraction_tasks = []
    
    # Start all extraction tasks in parallel
    status_task = status_tracker.submit(wait_for=[sync_point_1])
    extraction_tasks.append(status_task)
    
    financial_task = financial_expense.submit(wait_for=[sync_point_1])
    extraction_tasks.append(financial_task)
    
    # ... (all other extraction tasks)
    
    # Step 5: Start_PCD_Extract_2 (triggered by Status_Tracker)
    sync_point_2 = start_pcd_extract_2.submit(wait_for=[status_task])
    extraction_tasks.append(sync_point_2)
    
    # Step 6: PCD file upload (depends on all extraction tasks)
    upload_task = pcd_file_upload.submit(wait_for=extraction_tasks)
    
    # Step 7: ETL Notification (runs after all upstream tasks complete)
    # Use return_state=True to ensure it runs regardless of upstream failures
    notification_task = etl_notification.submit(wait_for=[upload_task])
    
    return notification_task

if __name__ == '__main__':
    pcd_etl_flow()