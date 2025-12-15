from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition

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

@job(
    name="retail_inventory_reconciliation",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "io_manager": fs_io_manager,
        "warehouse_system_north": ResourceDefinition.hardcoded_resource(None),
        "warehouse_system_east": ResourceDefinition.hardcoded_resource(None),
        "warehouse_system_west": ResourceDefinition.hardcoded_resource(None),
        "warehouse_system_south": ResourceDefinition.hardcoded_resource(None),
    },
)
def retail_inventory_reconciliation():
    # Entry point fetch ops
    fetch_north = fetch_north_warehouse_csv()
    fetch_south = fetch_south_warehouse_csv()
    fetch_east = fetch_east_warehouse_csv()
    fetch_west = fetch_west_warehouse_csv()

    # Normalization ops
    norm_north = normalize_north_skus()
    norm_south = normalize_south_skus()
    norm_east = normalize_east_skus()
    norm_west = normalize_west_skus()

    # Set fan‑in dependencies for normalization
    fetch_north >> norm_north
    fetch_south >> norm_south
    fetch_east >> norm_east
    fetch_west >> norm_west

    # Reconciliation op (fanin from all normalizations)
    reconcile = reconcile_all_inventories()
    norm_north >> reconcile
    norm_south >> reconcile
    norm_east >> reconcile
    norm_west >> reconcile

    # Final report generation
    final_report = generate_final_report()
    reconcile >> final_report