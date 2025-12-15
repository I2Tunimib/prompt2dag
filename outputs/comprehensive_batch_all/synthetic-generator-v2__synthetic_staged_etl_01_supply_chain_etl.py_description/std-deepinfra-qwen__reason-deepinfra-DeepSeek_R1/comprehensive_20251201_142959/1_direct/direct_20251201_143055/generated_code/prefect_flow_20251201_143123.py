from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.text import MIMEText

# Task definitions
@task(cache_key_fn=task_input_hash)
def ingest_vendor_a():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor A")
    # Simulate data ingestion
    data = pd.DataFrame({
        'sku': ['A123', 'A456', 'A789'],
        'quantity': [100, 200, 300],
        'date': ['2023-10-01', '2023-10-02', '2023-10-03']
    })
    return data

@task(cache_key_fn=task_input_hash)
def ingest_vendor_b():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor B")
    # Simulate data ingestion
    data = pd.DataFrame({
        'sku': ['B123', 'B456', 'B789'],
        'quantity': [150, 250, 350],
        'date': ['2023-10-01', '2023-10-02', '2023-10-03']
    })
    return data

@task(cache_key_fn=task_input_hash)
def ingest_vendor_c():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor C")
    # Simulate data ingestion
    data = pd.DataFrame({
        'sku': ['C123', 'C456', 'C789'],
        'quantity': [120, 220, 320],
        'date': ['2023-10-01', '2023-10-02', '2023-10-03']
    })
    return data

@task(cache_key_fn=task_input_hash)
def cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data):
    logger = get_run_logger()
    logger.info("Cleansing and normalizing data")
    # Combine data from all vendors
    combined_data = pd.concat([vendor_a_data, vendor_b_data, vendor_c_data])
    # Normalize SKU formats
    combined_data['sku'] = combined_data['sku'].str.upper()
    # Validate and filter invalid records
    combined_data = combined_data[pd.to_datetime(combined_data['date'], errors='coerce').notnull()]
    # Enrich data with location information
    combined_data['location'] = 'Warehouse 1'
    return combined_data

@task(cache_key_fn=task_input_hash)
def load_to_db(cleaned_data):
    logger = get_run_logger()
    logger.info("Loading data to PostgreSQL inventory database")
    # Simulate database load
    # In a real scenario, use a database connection to upsert the data
    return cleaned_data

@task(cache_key_fn=task_input_hash)
def send_summary_email(cleaned_data):
    logger = get_run_logger()
    logger.info("Sending summary email")
    # Simulate sending an email
    record_count = len(cleaned_data)
    processing_date = datetime.now().strftime('%Y-%m-%d')
    message = MIMEText(f"Processing Date: {processing_date}\nRecord Count: {record_count}")
    message['Subject'] = 'Supply Chain Shipment Data Processing Summary'
    message['From'] = 'no-reply@company.com'
    message['To'] = 'supply-chain-team@company.com'
    # In a real scenario, use an SMTP server to send the email
    with smtplib.SMTP('localhost') as server:
        server.sendmail('no-reply@company.com', 'supply-chain-team@company.com', message.as_string())

# Flow definition
@flow(retries=2, retry_delay_seconds=300)
def supply_chain_etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting supply chain ETL pipeline")

    # Stage 1: Extract
    vendor_a_data = ingest_vendor_a.submit()
    vendor_b_data = ingest_vendor_b.submit()
    vendor_c_data = ingest_vendor_c.submit()

    # Stage 2: Transform
    cleaned_data = cleanse_data.submit(vendor_a_data, vendor_b_data, vendor_c_data)

    # Stage 3: Load
    loaded_data = load_to_db.submit(cleaned_data)
    send_summary_email.submit(loaded_data)

if __name__ == '__main__':
    supply_chain_etl_pipeline()

# Deployment/schedule configuration (optional)
# Schedule: Daily execution
# Start Date: January 1, 2024
# No catchup processing
# Retry configuration: 2 retries with 5-minute delay between attempts
# Failure email notifications disabled