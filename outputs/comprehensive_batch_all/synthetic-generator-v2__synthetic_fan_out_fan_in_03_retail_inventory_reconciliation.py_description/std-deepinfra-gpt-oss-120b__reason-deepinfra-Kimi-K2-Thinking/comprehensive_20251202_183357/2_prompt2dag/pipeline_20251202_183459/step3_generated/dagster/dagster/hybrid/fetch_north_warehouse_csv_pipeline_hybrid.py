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
    name="fetch_north_warehouse_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "warehouse_south_api": ResourceDefinition.hardcoded_resource(None),
        "warehouse_west_api": ResourceDefinition.hardcoded_resource(None),
        "warehouse_east_api": ResourceDefinition.hardcoded_resource(None),
        "warehouse_north_api": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def fetch_north_warehouse_csv_pipeline():
    # Entry point fetch ops
    north_fetch = fetch_north_warehouse_csv()
    south_fetch = fetch_south_warehouse_csv()
    east_fetch = fetch_east_warehouse_csv()
    west_fetch = fetch_west_warehouse_csv()

    # Normalization ops
    north_norm = normalize_north_skus()
    south_norm = normalize_south_skus()
    east_norm = normalize_east_skus()
    west_norm = normalize_west_skus()

    # Set fan‑in dependencies from fetch to normalize
    north_fetch >> north_norm
    south_fetch >> south_norm
    east_fetch >> east_norm
    west_fetch >> west_norm

    # Reconciliation op
    reconcile = reconcile_all_inventories()

    # Fan‑in dependencies from all normalizations to reconciliation
    north_norm >> reconcile
    south_norm >> reconcile
    east_norm >> reconcile
    west_norm >> reconcile

    # Final report op
    final_report = generate_final_report()
    reconcile >> final_report