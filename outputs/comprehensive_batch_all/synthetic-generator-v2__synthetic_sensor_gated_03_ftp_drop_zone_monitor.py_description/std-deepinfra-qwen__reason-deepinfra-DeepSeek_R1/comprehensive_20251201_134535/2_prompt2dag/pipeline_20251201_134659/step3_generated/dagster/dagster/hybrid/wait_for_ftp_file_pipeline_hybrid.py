from dagster import job, op, in_process_executor, fs_io_manager, resource

# Task Definitions
@op(
    name='wait_for_ftp_file',
    description='Wait for FTP File',
)
def wait_for_ftp_file(context):
    """Op: Wait for FTP File"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='download_vendor_file',
    description='Download Vendor File',
)
def download_vendor_file(context):
    """Op: Download Vendor File"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='cleanse_vendor_data',
    description='Cleanse Vendor Data',
)
def cleanse_vendor_data(context):
    """Op: Cleanse Vendor Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='merge_with_internal_inventory',
    description='Merge with Internal Inventory',
)
def merge_with_internal_inventory(context):
    """Op: Merge with Internal Inventory"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def ftp_server(context):
    return context.resources.ftp_server

@resource
def internal_inventory(context):
    return context.resources.internal_inventory

# Job Definition
@job(
    name="wait_for_ftp_file_pipeline",
    description="No description provided.",
    executor_def=in_process_executor,
    resource_defs={
        "ftp_server": ftp_server,
        "internal_inventory": internal_inventory,
    },
    io_manager_def=fs_io_manager,
)
def wait_for_ftp_file_pipeline():
    merge_with_internal_inventory(
        cleanse_vendor_data(
            download_vendor_file(
                wait_for_ftp_file()
            )
        )
    )