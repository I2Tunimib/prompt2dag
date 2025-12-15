from prefect import flow, task

@task(name='check_pcd_sftp_folder', retries=3)
def check_pcd_sftp_folder():
    """Task: Check PCD SFTP Folder"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='check_pcd_shared_folder', retries=3)
def check_pcd_shared_folder():
    """Task: Check PCD Shared Folder"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='process_and_load_pcd', retries=2)
def process_and_load_pcd():
    """Task: Process and Load PCD Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='send_etl_notification', retries=1)
def send_etl_notification():
    """Task: ETL Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_pcd_api', retries=2)
def extract_pcd_api():
    """Task: Extract PCD API Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="check_pcd_sftp_folder_pipeline")
def check_pcd_sftp_folder_pipeline():
    """Sequential pipeline orchestrating PCD checks, extraction, processing, and notification."""
    # Step 1: Check SFTP folder
    sftp_check = check_pcd_sftp_folder()
    
    # Step 2: Check shared folder (depends on SFTP check)
    shared_check = check_pcd_shared_folder(wait_for=[sftp_check])
    
    # Step 3: Extract data from PCD API (depends on shared folder check)
    api_extract = extract_pcd_api(wait_for=[shared_check])
    
    # Step 4: Process and load PCD data (depends on API extraction)
    load_process = process_and_load_pcd(wait_for=[api_extract])
    
    # Step 5: Send ETL notification (depends on processing/loading)
    send_etl_notification(wait_for=[load_process])

# Entry point for local execution
if __name__ == "__main__":
    check_pcd_sftp_folder_pipeline()