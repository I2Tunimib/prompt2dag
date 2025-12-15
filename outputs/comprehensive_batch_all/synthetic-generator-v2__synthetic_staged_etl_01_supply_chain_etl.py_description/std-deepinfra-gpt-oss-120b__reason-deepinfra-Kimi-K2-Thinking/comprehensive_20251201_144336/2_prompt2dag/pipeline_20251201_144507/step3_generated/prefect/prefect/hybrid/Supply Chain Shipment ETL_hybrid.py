from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner


@task(name="join_extraction", retries=2)
def join_extraction():
    """Placeholder task for joining extraction steps."""
    # In a real pipeline this would combine raw extraction data.
    pass


@task(name="cleanse_and_normalize_shipments", retries=2)
def cleanse_and_normalize_shipments():
    """Task: Cleanse and Normalize Shipments"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="load_shipments_to_inventory", retries=2)
def load_shipments_to_inventory():
    """Task: Load Shipments to Inventory Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@task(name="send_etl_summary_email", retries=2)
def send_etl_summary_email():
    """Task: Send ETL Summary Email"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass


@flow(
    name="supply_chain_shipment_etl",
    task_runner=ConcurrentTaskRunner(),
)
def supply_chain_shipment_etl():
    """Sequential ETL flow for supply chain shipments."""
    # Step 1: Join extraction results
    join_extraction()

    # Step 2: Cleanse and normalize shipments (depends on join_extraction)
    cleanse_and_normalize_shipments()

    # Step 3: Load shipments into inventory (depends on cleansing)
    load_shipments_to_inventory()

    # Step 4: Send summary email (depends on loading)
    send_etl_summary_email()


if __name__ == "__main__":
    supply_chain_shipment_etl()