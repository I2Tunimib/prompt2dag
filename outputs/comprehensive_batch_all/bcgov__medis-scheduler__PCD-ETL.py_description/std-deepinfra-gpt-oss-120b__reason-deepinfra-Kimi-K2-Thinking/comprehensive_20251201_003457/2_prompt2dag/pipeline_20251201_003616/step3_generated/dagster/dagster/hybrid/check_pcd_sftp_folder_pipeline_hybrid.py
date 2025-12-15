from dagster import op, job, ResourceDefinition, fs_io_manager


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
    name='extract_pcd_api',
    description='Extract PCD API Data',
)
def extract_pcd_api(context):
    """Op: Extract PCD API Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='process_and_load_pcd_data',
    description='Process and Load PCD Data',
)
def process_and_load_pcd_data(context):
    """Op: Process and Load PCD Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='send_etl_notification',
    description='Send ETL Notification',
)
def send_etl_notification(context):
    """Op: Send ETL Notification"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="check_pcd_sftp_folder_pipeline",
    description="Staged ETL pipeline for Primary Care Data (PCD) processing with folder validation, parallel API extraction, Kubernetes‑based processing and email notification.",
    resource_defs={
        "io_manager": fs_io_manager,
        "shared_folder_conn": ResourceDefinition.hardcoded_resource(None),
        "pcd_api_conn": ResourceDefinition.hardcoded_resource(None),
        "email_service_conn": ResourceDefinition.hardcoded_resource(None),
        "sftp_conn": ResourceDefinition.hardcoded_resource(None),
        "k8s_job_conn": ResourceDefinition.hardcoded_resource(None),
        "object_storage_conn": ResourceDefinition.hardcoded_resource(None),
    },
)
def check_pcd_sftp_folder_pipeline():
    (
        check_pcd_sftp_folder()
        >> check_pcd_shared_folder()
        >> extract_pcd_api()
        >> process_and_load_pcd_data()
        >> send_etl_notification()
    )