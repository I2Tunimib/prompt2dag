from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
import pandas as pd

# Task to ingest data from Vendor A
@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def ingest_vendor_a():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor A")
    # Simulate data ingestion
    data_a = pd.DataFrame({
        'SKU': ['A123', 'A456', 'A789'],
        'ShipmentDate': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Quantity': [100, 150, 200]
    })
    return data_a

# Task to ingest data from Vendor B
@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def ingest_vendor_b():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor B")
    # Simulate data ingestion
    data_b = pd.DataFrame({
        'SKU': ['B123', 'B456', 'B789'],
        'ShipmentDate': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Quantity': [120, 170, 220]
    })
    return data_b

# Task to ingest data from Vendor C
@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def ingest_vendor_c():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor C")
    # Simulate data ingestion
    data_c = pd.DataFrame({
        'SKU': ['C123', 'C456', 'C789'],
        'ShipmentDate': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'Quantity': [130, 180, 230]
    })
    return data_c

# Task to cleanse and normalize data
@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def cleanse_data(data_a, data_b, data_c):
    logger = get_run_logger()
    logger.info("Cleansing and normalizing data")
    # Combine data from all vendors
    combined_data = pd.concat([data_a, data_b, data_c])
    # Normalize SKU formats
    combined_data['SKU'] = combined_data['SKU'].str.upper()
    # Validate and filter invalid shipment dates
    combined_data['ShipmentDate'] = pd.to_datetime(combined_data['ShipmentDate'], errors='coerce')
    combined_data = combined_data.dropna(subset=['ShipmentDate'])
    # Enrich data with location information
    combined_data['Location'] = 'Warehouse 1'
    return combined_data

# Task to load data to the database
@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def load_to_db(cleaned_data):
    logger = get_run_logger()
    logger.info("Loading data to PostgreSQL inventory database")
    # Simulate database load
    # Assuming a function `upsert_to_db` is defined elsewhere
    # upsert_to_db(cleaned_data, 'inventory_shipments')
    return cleaned_data

# Task to send summary email
@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def send_summary_email(cleaned_data):
    logger = get_run_logger()
    logger.info("Sending summary email")
    # Simulate sending email
    # Assuming a function `send_email` is defined elsewhere
    # send_email(to='supply-chain-team@company.com', subject='Shipment Data Summary', body=generate_email_body(cleaned_data))
    return cleaned_data

# Main flow to orchestrate the pipeline
@flow(name="Supply Chain Shipment Data Pipeline", retries=2, retry_delay_seconds=300)
def supply_chain_pipeline():
    logger = get_run_logger()
    logger.info("Starting Supply Chain Shipment Data Pipeline")

    # Stage 1: Extract - Parallel vendor data ingestion
    data_a = ingest_vendor_a.submit()
    data_b = ingest_vendor_b.submit()
    data_c = ingest_vendor_c.submit()

    # Stage 2: Transform - Data cleansing and normalization
    cleaned_data = cleanse_data.submit(data_a, data_b, data_c)

    # Stage 3: Load - Database loading and notification
    loaded_data = load_to_db.submit(cleaned_data)
    send_summary_email.submit(loaded_data)

if __name__ == '__main__':
    supply_chain_pipeline()

# Deployment/Schedule Configuration (optional)
# Schedule: Daily execution
# Start Date: January 1, 2024
# No catchup processing
# Retry configuration: 2 retries with 5-minute delay between attempts
# Failure email notifications disabled
# Task execution follows strict sequential stages with parallel extraction
# Uses TaskGroups for logical stage grouping