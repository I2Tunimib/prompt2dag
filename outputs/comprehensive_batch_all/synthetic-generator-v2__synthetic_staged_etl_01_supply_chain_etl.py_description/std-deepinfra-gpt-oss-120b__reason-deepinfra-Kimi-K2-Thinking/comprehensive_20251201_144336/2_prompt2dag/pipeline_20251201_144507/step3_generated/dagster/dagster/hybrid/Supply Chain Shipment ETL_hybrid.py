from dagster import op, job, ResourceDefinition, multiprocess_executor, fs_io_manager

@op(
    name='join_extraction',
    description='Join Extraction',
)
def join_extraction(context):
    """Op: Join Extraction"""
    # Placeholder for join extraction logic
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
    name='cleanse_and_normalize_shipments',
    description='Cleanse and Normalize Shipments',
)
def cleanse_and_normalize_shipments(context):
    """Op: Cleanse and Normalize Shipments"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_shipments_to_inventory',
    description='Load Shipments to Inventory Database',
)
def load_shipments_to_inventory(context):
    """Op: Load Shipments to Inventory Database"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name='supply_chain_shipment_etl',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        "ref_location_data": ResourceDefinition(lambda _: None),
        "email_smtp": ResourceDefinition(lambda _: None),
        "postgres_inventory": ResourceDefinition(lambda _: None),
        "io_manager": fs_io_manager,
    },
)
def supply_chain_shipment_etl():
    # Sequential execution respecting dependencies
    join_extraction()
    cleanse_and_normalize_shipments()
    load_shipments_to_inventory()
    send_etl_summary_email()