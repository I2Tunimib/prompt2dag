from dagster import job, op, In, Out, ResourceDefinition, fs_io_manager, in_process_executor

@op(
    name='check_and_download_ensembl_files',
    description='Check and Download Ensembl Files',
)
def check_and_download_ensembl_files(context):
    """Op: Check and Download Ensembl Files"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='process_ensembl_files_with_spark',
    description='Process Ensembl Files with Spark',
)
def process_ensembl_files_with_spark(context):
    """Op: Process Ensembl Files with Spark"""
    # Docker execution
    # Image: python:3.9
    pass

@job(
    name="etl_import_ensembl",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={"io_manager": fs_io_manager, "config_s3_conn_id": ResourceDefinition.hardcoded_resource("s3_conn_id")},
)
def etl_import_ensembl():
    process_ensembl_files_with_spark(check_and_download_ensembl_files())