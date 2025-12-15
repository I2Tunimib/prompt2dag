from dagster import op, job, ResourceDefinition, multiprocess_executor, fs_io_manager

# ----------------------------------------------------------------------
# Provided task definitions (used exactly as given)
# ----------------------------------------------------------------------
@op(
    name='extract_vendor_b',
    description='Extract Vendor B Shipments',
)
def extract_vendor_b(context):
    """Op: Extract Vendor B Shipments"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_shipment_to_inventory',
    description='Load Shipment Data to Inventory Database',
)
def load_shipment_to_inventory(context):
    """Op: Load Shipment Data to Inventory Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='send_etl_summary_email',
    description='Send ETL Summary Email',
)
def send_etl_summary_email(context):
    """Op: Send ETL Summary Email"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='extract_vendor_a',
    description='Extract Vendor A Shipments',
)
def extract_vendor_a(context):
    """Op: Extract Vendor A Shipments"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='extract_vendor_c',
    description='Extract Vendor C Shipments',
)
def extract_vendor_c(context):
    """Op: Extract Vendor C Shipments"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='cleanse_shipment_data',
    description='Cleanse and Enrich Shipment Data',
)
def cleanse_shipment_data(context):
    """Op: Cleanse and Enrich Shipment Data"""
    # Docker execution
    # Image: python:3.9
    pass


# ----------------------------------------------------------------------
# Additional op needed for fan‑out (extract_parallel)
# ----------------------------------------------------------------------
@op(
    name='extract_parallel',
    description='Parallel extraction starter for fan‑out',
)
def extract_parallel(context):
    """Op: Parallel extraction starter (no actual work)"""
    pass


# ----------------------------------------------------------------------
# Job definition with fan‑out / fan‑in wiring
# ----------------------------------------------------------------------
@job(
    name="extract_vendor_a_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "db_location_ref": ResourceDefinition.hardcoded_resource(None),
        "email_smtp": ResourceDefinition.hardcoded_resource(None),
        "fs_vendor_b": ResourceDefinition.hardcoded_resource(None),
        "postgres_inventory": ResourceDefinition.hardcoded_resource(None),
        "fs_vendor_c": ResourceDefinition.hardcoded_resource(None),
        "fs_vendor_a": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def extract_vendor_a_pipeline():
    # Fan‑out: start with a single parallel op
    parallel = extract_parallel()

    # Vendor extraction ops (fan‑out)
    vendor_a = extract_vendor_a()
    vendor_a.after(parallel)

    vendor_b = extract_vendor_b()
    vendor_b.after(parallel)

    vendor_c = extract_vendor_c()
    vendor_c.after(parallel)

    # Fan‑in: cleanse step runs after all vendor extracts complete
    cleanse = cleanse_shipment_data()
    cleanse.after(vendor_a, vendor_b, vendor_c)

    # Load and notification steps
    load = load_shipment_to_inventory()
    load.after(cleanse)

    email = send_etl_summary_email()
    email.after(load)