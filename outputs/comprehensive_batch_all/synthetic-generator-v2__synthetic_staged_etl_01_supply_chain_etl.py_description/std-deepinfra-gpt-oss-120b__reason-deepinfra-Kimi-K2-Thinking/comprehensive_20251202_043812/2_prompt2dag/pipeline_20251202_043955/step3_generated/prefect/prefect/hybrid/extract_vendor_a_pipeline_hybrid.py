from prefect import flow, task

@task(name='cleanse_shipment_data', retries=2)
def cleanse_shipment_data():
    """Task: Cleanse and Enrich Shipment Data"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='load_shipment_to_inventory', retries=2)
def load_shipment_to_inventory():
    """Task: Load Shipment Data to Inventory Database"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='send_etl_summary_email', retries=2)
def send_etl_summary_email():
    """Task: Send ETL Summary Email"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_vendor_a', retries=2)
def extract_vendor_a():
    """Task: Extract Vendor A Shipments"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_vendor_c', retries=2)
def extract_vendor_c():
    """Task: Extract Vendor C Shipments"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@task(name='extract_vendor_b', retries=2)
def extract_vendor_b():
    """Task: Extract Vendor B Shipments"""
    # Docker execution via infrastructure
    # Image: python:3.9
    pass

@flow(name="extract_vendor_a_pipeline")
def extract_vendor_a_pipeline():
    """
    Prefect flow implementing the fanout_fanin pattern:
    - Fanout: extract_vendor_a, extract_vendor_b, extract_vendor_c run in parallel.
    - Fanin: cleanse_shipment_data runs after all extracts.
    - Then load_shipment_to_inventory.
    - Finally send_etl_summary_email.
    """
    # Fanout: start all extract tasks (they can run concurrently)
    vendor_a = extract_vendor_a()
    vendor_b = extract_vendor_b()
    vendor_c = extract_vendor_c()

    # Fanin: wait for all extracts to complete before cleansing
    cleanse = cleanse_shipment_data()

    # Load to inventory after cleansing
    load = load_shipment_to_inventory()

    # Send summary email after load
    email = send_etl_summary_email()

    return email

# If this script is executed directly, run the flow
if __name__ == "__main__":
    extract_vendor_a_pipeline()