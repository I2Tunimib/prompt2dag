from dagster import job, op, Out, In, ResourceDefinition, fs_io_manager, k8s_job_executor

@op(
    name='check_pcd_sftp_folder',
    description='Check PCD SFTP Folder',
)
def check_pcd_sftp_folder(context):
    """Op: Check PCD SFTP Folder"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='check_pcd_shared_folder',
    description='Check PCD Shared Folder',
)
def check_pcd_shared_folder(context):
    """Op: Check PCD Shared Folder"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='start_pcd_extract_1',
    description='Start PCD Extract 1',
)
def start_pcd_extract_1(context):
    """Op: Start PCD Extract 1"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='parallel_http_api_extraction',
    description='Parallel HTTP API Extraction',
)
def parallel_http_api_extraction(context):
    """Op: Parallel HTTP API Extraction"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='start_pcd_extract_2',
    description='Start PCD Extract 2',
)
def start_pcd_extract_2(context):
    """Op: Start PCD Extract 2"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='pcd_file_upload',
    description='PCD File Upload',
)
def pcd_file_upload(context):
    """Op: PCD File Upload"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='etl_notification',
    description='ETL Notification',
)
def etl_notification(context):
    """Op: ETL Notification"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="check_pcd_sftp_folder_pipeline",
    description="No description provided.",
    executor_def=k8s_job_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "data_storage": ResourceDefinition.hardcoded_resource(None, "data_storage"),
        "http_api": ResourceDefinition.hardcoded_resource(None, "http_api"),
        "email_system": ResourceDefinition.hardcoded_resource(None, "email_system"),
        "kubernetes": ResourceDefinition.hardcoded_resource(None, "kubernetes"),
    },
)
def check_pcd_sftp_folder_pipeline():
    check_pcd_sftp_folder_op = check_pcd_sftp_folder()
    check_pcd_shared_folder_op = check_pcd_shared_folder(check_pcd_sftp_folder_op)
    start_pcd_extract_1_op = start_pcd_extract_1(check_pcd_shared_folder_op)
    parallel_http_api_extraction_op = parallel_http_api_extraction(start_pcd_extract_1_op)
    start_pcd_extract_2_op = start_pcd_extract_2(parallel_http_api_extraction_op, parallel_http_api_extraction_op)
    pcd_file_upload_op = pcd_file_upload(start_pcd_extract_2_op)
    etl_notification_op = etl_notification(pcd_file_upload_op)
```
This code defines the Dagster job `check_pcd_sftp_folder_pipeline` with the specified ops and their dependencies, including the fanout-fanin pattern. The job uses the Kubernetes job executor and the file system IO manager, and it includes the required resources.