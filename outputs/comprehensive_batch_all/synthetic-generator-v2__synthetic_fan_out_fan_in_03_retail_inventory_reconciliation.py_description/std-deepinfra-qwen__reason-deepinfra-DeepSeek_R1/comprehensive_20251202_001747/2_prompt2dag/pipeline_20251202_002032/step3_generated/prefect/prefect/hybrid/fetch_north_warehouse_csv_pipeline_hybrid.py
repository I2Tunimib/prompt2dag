from prefect import flow, task, get_run_logger
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import CronSchedule
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

@flow(name="fetch_north_warehouse_csv_pipeline", task_runner=ConcurrentTaskRunner)
def fetch_north_warehouse_csv_pipeline():
    logger = get_run_logger()
    logger.info("Starting the fetch_north_warehouse_csv_pipeline")

    # Fetch CSVs from warehouses
    north_csv = fetch_north_warehouse_csv.submit()
    south_csv = fetch_south_warehouse_csv.submit()
    east_csv = fetch_east_warehouse_csv.submit()
    west_csv = fetch_west_warehouse_csv.submit()

    # Normalize SKUs
    north_skus = normalize_north_skus.submit(wait_for=[north_csv])
    south_skus = normalize_south_skus.submit(wait_for=[south_csv])
    east_skus = normalize_east_skus.submit(wait_for=[east_csv])
    west_skus = normalize_west_skus.submit(wait_for=[west_csv])

    # Reconcile all inventories
    reconciled_inventories = reconcile_all_inventories.submit(wait_for=[north_skus, south_skus, east_skus, west_skus])

    # Generate final report
    final_report = generate_final_report.submit(wait_for=[reconciled_inventories])

    logger.info("fetch_north_warehouse_csv_pipeline completed successfully")

# Deployment configuration
deployment = Deployment.build_from_flow(
    flow=fetch_north_warehouse_csv_pipeline,
    name="fetch_north_warehouse_csv_pipeline_deployment",
    work_pool_name="default-agent-pool",
    schedule=CronSchedule(cron="0 0 * * *"),  # @daily
)

if __name__ == "__main__":
    deployment.apply()
```
```python