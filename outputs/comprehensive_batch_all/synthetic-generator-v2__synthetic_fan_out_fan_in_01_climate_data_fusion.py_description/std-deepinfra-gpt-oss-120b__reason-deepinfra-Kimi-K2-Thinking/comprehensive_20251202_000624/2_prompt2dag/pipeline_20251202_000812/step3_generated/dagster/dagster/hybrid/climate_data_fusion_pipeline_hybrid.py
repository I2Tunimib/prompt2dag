from dagster import op, job, ResourceDefinition, fs_io_manager, multiprocess_executor


@op(
    name='download_agency_data',
    description='Download Agency Weather Data',
)
def download_agency_data(context):
    """Op: Download Agency Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='normalize_agency_data',
    description='Normalize Agency Weather Data',
)
def normalize_agency_data(context):
    """Op: Normalize Agency Weather Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='merge_climate_data',
    description='Merge Climate Datasets',
)
def merge_climate_data(context):
    """Op: Merge Climate Datasets"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="climate_data_fusion_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "conn_agency_endpoint": ResourceDefinition.hardcoded_resource(None),
        "conn_local_fs": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def climate_data_fusion_pipeline():
    download = download_agency_data()
    normalize = normalize_agency_data()
    merge = merge_climate_data()

    # Define fan‑in / ordering dependencies
    download >> normalize >> merge