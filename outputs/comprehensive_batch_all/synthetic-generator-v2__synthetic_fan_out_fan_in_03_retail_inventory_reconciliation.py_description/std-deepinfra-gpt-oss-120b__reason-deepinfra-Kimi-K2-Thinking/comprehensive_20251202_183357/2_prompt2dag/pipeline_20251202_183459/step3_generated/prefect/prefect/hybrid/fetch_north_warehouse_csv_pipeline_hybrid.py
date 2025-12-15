from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


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


@flow(name='fetch_north_warehouse_csv_pipeline', task_runner=ConcurrentTaskRunner())
def fetch_north_warehouse_csv_pipeline():
    # Entry point fetch tasks
    north_fetch = fetch_north_warehouse_csv()
    south_fetch = fetch_south_warehouse_csv()
    east_fetch = fetch_east_warehouse_csv()
    west_fetch = fetch_west_warehouse_csv()

    # Normalization tasks with fan‑in dependencies
    norm_north = normalize_north_skus(wait_for=[north_fetch])
    norm_south = normalize_south_skus(wait_for=[south_fetch])
    norm_east = normalize_east_skus(wait_for=[east_fetch])
    norm_west = normalize_west_skus(wait_for=[west_fetch])

    # Reconcile after all normalizations complete
    reconciliation = reconcile_all_inventories(
        wait_for=[norm_north, norm_south, norm_east, norm_west]
    )

    # Final report generation
    final_report = generate_final_report(wait_for=[reconciliation])

    return final_report


if __name__ == "__main__":
    fetch_north_warehouse_csv_pipeline()