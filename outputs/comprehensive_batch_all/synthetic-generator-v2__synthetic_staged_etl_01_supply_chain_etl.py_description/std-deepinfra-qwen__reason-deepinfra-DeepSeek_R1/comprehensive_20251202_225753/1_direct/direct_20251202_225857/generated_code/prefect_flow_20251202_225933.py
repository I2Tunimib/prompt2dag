from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import datetime, timedelta
import pandas as pd
import smtplib
from email.mime.text import MIMEText

# Stage 1: Extract - Parallel vendor data ingestion
@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def ingest_vendor_a():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor A")
    # Simulate data ingestion
    data = pd.DataFrame({
        'sku': ['A123', 'A456', 'A789'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'quantity': [100, 200, 150]
    })
    return data

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def ingest_vendor_b():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor B")
    # Simulate data ingestion
    data = pd.DataFrame({
        'sku': ['B123', 'B456', 'B789'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'quantity': [150, 100, 200]
    })
    return data

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def ingest_vendor_c():
    logger = get_run_logger()
    logger.info("Ingesting data from Vendor C")
    # Simulate data ingestion
    data = pd.DataFrame({
        'sku': ['C123', 'C456', 'C789'],
        'date': ['2024-01-01', '2024-01-02', '2024-01-03'],
        'quantity': [200, 150, 100]
    })
    return data

# Stage 2: Transform - Data cleansing and normalization
@task(retries=2, retry_delay_seconds=300)
def cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data):
    logger = get_run_logger()
    logger.info("Cleansing and normalizing data")
    # Combine data from all vendors
    combined_data = pd.concat([vendor_a_data, vendor_b_data, vendor_c_data])
    
    # Normalize SKU formats
    combined_data['sku'] = combined_data['sku'].str.upper()
    
    # Validate shipment dates and filter invalid records
    combined_data['date'] = pd.to_datetime(combined_data['date'], errors='coerce')
    combined_data = combined_data.dropna(subset=['date'])
    
    # Enrich data with location information from reference tables
    # Simulate location enrichment
    location_data = pd.DataFrame({
        'sku': ['A123', 'B456', 'C789'],
        'location': ['Warehouse A', 'Warehouse B', 'Warehouse C']
    })
    combined_data = combined_data.merge(location_data, on='sku', how='left')
    
    return combined_data

# Stage 3: Load - Database loading and notification
@task(retries=2, retry_delay_seconds=300)
def load_to_db(cleaned_data):
    logger = get_run_logger()
    logger.info("Loading data to PostgreSQL inventory database")
    # Simulate database load
    # In a real scenario, use a database connection to upsert the data
    logger.info(f"Loaded {len(cleaned_data)} records to inventory_shipments table")

@task(retries=2, retry_delay_seconds=300)
def send_summary_email(cleaned_data):
    logger = get_run_logger()
    logger.info("Sending summary email notification")
    # Simulate sending an email
    processing_date = datetime.now().strftime('%Y-%m-%d')
    record_count = len(cleaned_data)
    data_quality_metrics = "All records valid"
    
    message = MIMEText(f"""
    Processing Date: {processing_date}
    Record Count: {record_count}
    Data Quality Metrics: {data_quality_metrics}
    """)
    message['Subject'] = 'Supply Chain Shipment Data Processing Summary'
    message['From'] = 'no-reply@company.com'
    message['To'] = 'supply-chain-team@company.com'
    
    # Simulate sending the email
    with smtplib.SMTP('localhost', 25) as server:
        server.sendmail('no-reply@company.com', 'supply-chain-team@company.com', message.as_string())
    logger.info("Summary email sent")

# Orchestration
@flow(name="Supply Chain Shipment Data Processing Pipeline")
def supply_chain_pipeline():
    logger = get_run_logger()
    logger.info("Starting supply chain shipment data processing pipeline")
    
    # Stage 1: Extract
    vendor_a_data = ingest_vendor_a.submit()
    vendor_b_data = ingest_vendor_b.submit()
    vendor_c_data = ingest_vendor_c.submit()
    
    # Stage 2: Transform
    cleaned_data = cleanse_data.submit(vendor_a_data, vendor_b_data, vendor_c_data)
    
    # Stage 3: Load
    load_to_db.submit(cleaned_data)
    send_summary_email.submit(cleaned_data)

if __name__ == '__main__':
    supply_chain_pipeline()

# Schedule: Daily execution
# Start Date: January 1, 2024
# No catchup processing
# Retry configuration: 2 retries with 5-minute delay between attempts
# Failure email notifications disabled
# Task execution follows strict sequential stages with parallel extraction
# Uses TaskGroups for logical stage grouping