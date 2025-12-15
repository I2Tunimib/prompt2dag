from dagster import job, op, multiprocess_executor, fs_io_manager, resource

# Task Definitions
@op(
    name='ingest_vendor_a',
    description='Ingest Vendor A Data',
)
def ingest_vendor_a(context):
    """Op: Ingest Vendor A Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='ingest_vendor_b',
    description='Ingest Vendor B Data',
)
def ingest_vendor_b(context):
    """Op: Ingest Vendor B Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='ingest_vendor_c',
    description='Ingest Vendor C Data',
)
def ingest_vendor_c(context):
    """Op: Ingest Vendor C Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='cleanse_data',
    description='Cleanse and Normalize Data',
)
def cleanse_data(context):
    """Op: Cleanse and Normalize Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='load_to_db',
    description='Load to Database',
)
def load_to_db(context):
    """Op: Load to Database"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='send_summary_email',
    description='Send Summary Email',
)
def send_summary_email(context):
    """Op: Send Summary Email"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def inventory_db():
    pass

@resource
def vendor_c_filesystem():
    pass

@resource
def email_system():
    pass

@resource
def location_reference_tables():
    pass

@resource
def vendor_b_filesystem():
    pass

@resource
def vendor_a_filesystem():
    pass

# Job Definition
@job(
    name="ingest_vendor_a_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "inventory_db": inventory_db,
        "vendor_c_filesystem": vendor_c_filesystem,
        "email_system": email_system,
        "location_reference_tables": location_reference_tables,
        "vendor_b_filesystem": vendor_b_filesystem,
        "vendor_a_filesystem": vendor_a_filesystem,
        "io_manager": fs_io_manager,
    },
)
def ingest_vendor_a_pipeline():
    ingest_a = ingest_vendor_a()
    ingest_b = ingest_vendor_b()
    ingest_c = ingest_vendor_c()
    cleanse = cleanse_data(ingest_a, ingest_b, ingest_c)
    load = load_to_db(cleanse)
    send_summary_email(load)