from dagster import op, job, ResourceDefinition, fs_io_manager, multiprocess_executor

# Pre-generated task definitions

@op(
    name='cleanse_and_normalize_shipments',
    description='Cleanse and Normalize Shipment Data',
)
def cleanse_and_normalize_shipments(context):
    """Op: Cleanse and Normalize Shipment Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_shipment_data',
    description='Load Cleansed Shipments to Inventory Database',
)
def load_shipment_data(context):
    """Op: Load Cleansed Shipments to Inventory Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='send_summary_email',
    description='Send ETL Summary Email',
)
def send_summary_email(context):
    """Op: Send ETL Summary Email"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='extract_vendor_shipments',
    description='Extract Vendor Shipment CSVs',
)
def extract_vendor_shipments(context):
    """Op: Extract Vendor Shipment CSVs"""
    # Docker execution
    # Image: python:3.9
    pass


# Additional placeholder for missing dependency
@op(
    name='extract_join',
    description='Placeholder for extract_join dependency',
)
def extract_join(context):
    """Placeholder op for extract_join dependency."""
    pass


@job(
    name="extract_vendor_shipments_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "fs_vendor_a": ResourceDefinition.hardcoded_resource(None),
        "ref_location": ResourceDefinition.hardcoded_resource(None),
        "fs_vendor_c": ResourceDefinition.hardcoded_resource(None),
        "email_smtp": ResourceDefinition.hardcoded_resource(None),
        "fs_vendor_b": ResourceDefinition.hardcoded_resource(None),
        "postgres_inventory": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def extract_vendor_shipments_pipeline():
    # Define the sequential flow respecting dependencies
    join = extract_join()
    vendor = extract_vendor_shipments().after(join)
    cleansed = cleanse_and_normalize_shipments().after(vendor)
    loaded = load_shipment_data().after(cleansed)
    send_summary_email().after(loaded)