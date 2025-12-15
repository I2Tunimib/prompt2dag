from dagster import op, job, ResourceDefinition, fs_io_manager
from dagster_k8s import k8s_job_executor


@op(
    name="check_pcd_sftp_folder",
    description="Check PCD SFTP Folder",
)
def check_pcd_sftp_folder(context):
    """Op: Check PCD SFTP Folder"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="check_pcd_shared_folder",
    description="Check PCD Shared Folder",
)
def check_pcd_shared_folder(context):
    """Op: Check PCD Shared Folder"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="extract_pcd_api",
    description="Extract PCD API Data",
)
def extract_pcd_api(context):
    """Op: Extract PCD API Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="process_and_load_pcd",
    description="Process and Load PCD Data",
)
def process_and_load_pcd(context):
    """Op: Process and Load PCD Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="send_etl_notification",
    description="ETL Notification",
)
def send_etl_notification(context):
    """Op: ETL Notification"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="check_pcd_sftp_folder_pipeline",
    description="No description provided.",
    executor_def=k8s_job_executor,
    resource_defs={
        "object_storage_conn": ResourceDefinition.hardcoded_resource(None),
        "sftp_conn": ResourceDefinition.hardcoded_resource(None),
        "shared_folder_conn": ResourceDefinition.hardcoded_resource(None),
        "email_conn": ResourceDefinition.hardcoded_resource(None),
        "pcd_api_conn": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def check_pcd_sftp_folder_pipeline():
    sftp = check_pcd_sftp_folder()
    shared = check_pcd_shared_folder()
    api = extract_pcd_api()
    process = process_and_load_pcd()
    notify = send_etl_notification()

    sftp >> shared >> api >> process >> notify