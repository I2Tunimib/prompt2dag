from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name="extract_join", retries=2)
def extract_join():
    """Placeholder task for extract_join"""
    # Implementation would go here
    pass


@task(name="extract_vendor_shipments", retries=2)
def extract_vendor_shipments():
    """Task: Extract Vendor Shipment CSVs"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="cleanse_and_normalize_shipments", retries=2)
def cleanse_and_normalize_shipments():
    """Task: Cleanse and Normalize Shipment Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="load_shipment_data", retries=2)
def load_shipment_data():
    """Task: Load Cleansed Shipments to Inventory Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="send_summary_email", retries=2)
def send_summary_email():
    """Task: Send ETL Summary Email"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="extract_vendor_shipments_pipeline",
    task_runner=ConcurrentTaskRunner(),
)
def extract_vendor_shipments_pipeline():
    # Sequential execution respecting dependencies
    extract_join()
    extract_vendor_shipments()
    cleanse_and_normalize_shipments()
    load_shipment_data()
    send_summary_email()


if __name__ == "__main__":
    extract_vendor_shipments_pipeline()