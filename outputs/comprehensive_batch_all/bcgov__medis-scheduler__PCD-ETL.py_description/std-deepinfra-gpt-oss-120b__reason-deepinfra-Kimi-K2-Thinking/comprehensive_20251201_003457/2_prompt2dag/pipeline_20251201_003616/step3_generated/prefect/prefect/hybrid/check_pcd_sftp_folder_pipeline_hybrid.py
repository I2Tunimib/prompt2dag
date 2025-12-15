from prefect import flow, task

@task(name='check_pcd_sftp_folder', retries=0)
def check_pcd_sftp_folder():
    """Task: Check PCD SFTP Folder"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='check_pcd_shared_folder', retries=0)
def check_pcd_shared_folder():
    """Task: Check PCD Shared Folder"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_pcd_api', retries=3)
def extract_pcd_api():
    """Task: Extract PCD API Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='process_and_load_pcd_data', retries=2)
def process_and_load_pcd_data():
    """Task: Process and Load PCD Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='send_etl_notification', retries=1)
def send_etl_notification():
    """Task: Send ETL Notification"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="check_pcd_sftp_folder_pipeline")
def check_pcd_sftp_folder_pipeline():
    # Entry point
    sftp_result = check_pcd_sftp_folder()
    
    # Sequential dependencies
    shared_result = check_pcd_shared_folder(wait_for=[sftp_result])
    api_result = extract_pcd_api(wait_for=[shared_result])
    process_result = process_and_load_pcd_data(wait_for=[api_result])
    send_etl_notification(wait_for=[process_result])

if __name__ == "__main__":
    check_pcd_sftp_folder_pipeline()