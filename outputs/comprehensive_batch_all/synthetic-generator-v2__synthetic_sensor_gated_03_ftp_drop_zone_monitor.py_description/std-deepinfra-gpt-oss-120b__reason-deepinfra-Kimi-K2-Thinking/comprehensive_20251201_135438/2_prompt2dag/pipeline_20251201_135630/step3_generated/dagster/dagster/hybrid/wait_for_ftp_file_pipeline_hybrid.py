from dagster import op, job, in_process_executor, fs_io_manager, resource


@resource
def internal_db_resource(_):
    """Placeholder resource for internal database connection."""
    return {}


@resource
def ftp_conn_resource(_):
    """Placeholder resource for FTP connection."""
    return {}


@op(
    name="wait_for_ftp_file",
    description="Wait for FTP File",
)
def wait_for_ftp_file(context):
    """Op: Wait for FTP File"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="download_vendor_file",
    description="Download Vendor File",
)
def download_vendor_file(context):
    """Op: Download Vendor File"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="cleanse_vendor_data",
    description="Cleanse Vendor Data",
)
def cleanse_vendor_data(context):
    """Op: Cleanse Vendor Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="merge_with_internal_inventory",
    description="Merge with Internal Inventory",
)
def merge_with_internal_inventory(context):
    """Op: Merge with Internal Inventory"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="wait_for_ftp_file_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "internal_db": internal_db_resource,
        "ftp_conn": ftp_conn_resource,
        "io_manager": fs_io_manager,
    },
)
def wait_for_ftp_file_pipeline():
    (
        wait_for_ftp_file()
        .then(download_vendor_file)
        .then(cleanse_vendor_data)
        .then(merge_with_internal_inventory)
    )