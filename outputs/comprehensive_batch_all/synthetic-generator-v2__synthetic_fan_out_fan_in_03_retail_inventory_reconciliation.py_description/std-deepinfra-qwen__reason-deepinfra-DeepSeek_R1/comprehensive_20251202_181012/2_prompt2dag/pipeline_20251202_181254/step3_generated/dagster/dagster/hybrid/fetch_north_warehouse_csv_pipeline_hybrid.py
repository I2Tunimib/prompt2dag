from dagster import job, op, multiprocess_executor, fs_io_manager, resource

# Task Definitions
@op(
    name='fetch_east_warehouse_csv',
    description='Fetch East Warehouse CSV',
)
def fetch_east_warehouse_csv(context):
    """Op: Fetch East Warehouse CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='fetch_north_warehouse_csv',
    description='Fetch North Warehouse CSV',
)
def fetch_north_warehouse_csv(context):
    """Op: Fetch North Warehouse CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='fetch_south_warehouse_csv',
    description='Fetch South Warehouse CSV',
)
def fetch_south_warehouse_csv(context):
    """Op: Fetch South Warehouse CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='fetch_west_warehouse_csv',
    description='Fetch West Warehouse CSV',
)
def fetch_west_warehouse_csv(context):
    """Op: Fetch West Warehouse CSV"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_east_skus',
    description='Normalize East SKUs',
)
def normalize_east_skus(context):
    """Op: Normalize East SKUs"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_north_skus',
    description='Normalize North SKUs',
)
def normalize_north_skus(context):
    """Op: Normalize North SKUs"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_south_skus',
    description='Normalize South SKUs',
)
def normalize_south_skus(context):
    """Op: Normalize South SKUs"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='normalize_west_skus',
    description='Normalize West SKUs',
)
def normalize_west_skus(context):
    """Op: Normalize West SKUs"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='reconcile_all_inventories',
    description='Reconcile All Inventories',
)
def reconcile_all_inventories(context):
    """Op: Reconcile All Inventories"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='generate_final_report',
    description='Generate Final Report',
)
def generate_final_report(context):
    """Op: Generate Final Report"""
    # Docker execution
    # Image: python:3.9
    pass

# Resources
@resource
def east_warehouse_management_system():
    pass

@resource
def west_warehouse_management_system():
    pass

@resource
def north_warehouse_management_system():
    pass

@resource
def south_warehouse_management_system():
    pass

# Job Definition
@job(
    name='fetch_north_warehouse_csv_pipeline',
    description='No description provided.',
    executor_def=multiprocess_executor,
    resource_defs={
        'east_warehouse_management_system': east_warehouse_management_system,
        'west_warehouse_management_system': west_warehouse_management_system,
        'north_warehouse_management_system': north_warehouse_management_system,
        'south_warehouse_management_system': south_warehouse_management_system,
        'io_manager': fs_io_manager,
    },
)
def fetch_north_warehouse_csv_pipeline():
    north_csv = fetch_north_warehouse_csv()
    south_csv = fetch_south_warehouse_csv()
    east_csv = fetch_east_warehouse_csv()
    west_csv = fetch_west_warehouse_csv()

    north_skus = normalize_north_skus(north_csv)
    south_skus = normalize_south_skus(south_csv)
    east_skus = normalize_east_skus(east_csv)
    west_skus = normalize_west_skus(west_csv)

    reconciled_inventories = reconcile_all_inventories(north_skus, south_skus, east_skus, west_skus)
    generate_final_report(reconciled_inventories)