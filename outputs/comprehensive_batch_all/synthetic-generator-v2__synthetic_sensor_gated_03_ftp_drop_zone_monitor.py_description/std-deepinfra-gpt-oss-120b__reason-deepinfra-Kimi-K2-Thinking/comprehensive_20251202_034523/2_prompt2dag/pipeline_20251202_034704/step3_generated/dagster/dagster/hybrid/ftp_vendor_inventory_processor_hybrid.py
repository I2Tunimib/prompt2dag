from dagster import op, job, ResourceDefinition, fs_io_manager, in_process_executor


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


@job(
    name='ftp_vendor_inventory_processor',
    description='No description provided.',
    resource_defs={
        'internal_inventory_conn': ResourceDefinition.hardcoded_resource(None),
        'ftp_conn': ResourceDefinition.hardcoded_resource(None),
        'io_manager': fs_io_manager,
    },
    executor_def=in_process_executor,
)
def ftp_vendor_inventory_processor():
    wait_for_ftp_file() >> download_vendor_file() >> cleanse_vendor_data() >> merge_with_internal_inventory()