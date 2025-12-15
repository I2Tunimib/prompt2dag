from prefect import flow, task

@task(name='fetch_east_warehouse_csv', retries=2)
def fetch_east_warehouse_csv():
    """Task: Fetch East Warehouse CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='fetch_north_warehouse_csv', retries=2)
def fetch_north_warehouse_csv():
    """Task: Fetch North Warehouse CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='fetch_south_warehouse_csv', retries=2)
def fetch_south_warehouse_csv():
    """Task: Fetch South Warehouse CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='fetch_west_warehouse_csv', retries=2)
def fetch_west_warehouse_csv():
    """Task: Fetch West Warehouse CSV"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_east_skus', retries=2)
def normalize_east_skus():
    """Task: Normalize East SKUs"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_north_skus', retries=2)
def normalize_north_skus():
    """Task: Normalize North SKUs"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_south_skus', retries=2)
def normalize_south_skus():
    """Task: Normalize South SKUs"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='normalize_west_skus', retries=2)
def normalize_west_skus():
    """Task: Normalize West SKUs"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='reconcile_all_inventories', retries=2)
def reconcile_all_inventories():
    """Task: Reconcile All Inventories"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='generate_final_report', retries=2)
def generate_final_report():
    """Task: Generate Final Report"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name='retail_inventory_reconciliation')
def retail_inventory_reconciliation():
    # Entry point fetch tasks (fan-out)
    east_fetch = fetch_east_warehouse_csv()
    north_fetch = fetch_north_warehouse_csv()
    south_fetch = fetch_south_warehouse_csv()
    west_fetch = fetch_west_warehouse_csv()

    # Normalization tasks (each depends on its fetch)
    east_norm = normalize_east_skus(wait_for=[east_fetch])
    north_norm = normalize_north_skus(wait_for=[north_fetch])
    south_norm = normalize_south_skus(wait_for=[south_fetch])
    west_norm = normalize_west_skus(wait_for=[west_fetch])

    # Reconciliation (fan-in)
    reconcile = reconcile_all_inventories(
        wait_for=[east_norm, north_norm, south_norm, west_norm]
    )

    # Final report generation
    generate_final_report(wait_for=[reconcile])

if __name__ == "__main__":
    retail_inventory_reconciliation()