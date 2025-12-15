from dagster import op, job, in_process_executor, fs_io_manager, ResourceDefinition


@op(
    name='extract_ensembl_files',
    description='Extract Ensembl Mapping Files',
)
def extract_ensembl_files(context):
    """Op: Extract Ensembl Mapping Files"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='transform_ensembl_mapping',
    description='Transform Ensembl Mapping with Spark',
)
def transform_ensembl_mapping(context):
    """Op: Transform Ensembl Mapping with Spark"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="etl_import_ensembl",
    description="Comprehensive Pipeline Description",
    executor_def=in_process_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "ftp_ensembl_conn": ResourceDefinition.hardcoded_resource(None),
        "config_s3_conn_id": ResourceDefinition.hardcoded_resource(None),
    },
)
def etl_import_ensembl():
    # Sequential execution: extract then transform
    extract_ensembl_files()
    transform_ensembl_mapping()