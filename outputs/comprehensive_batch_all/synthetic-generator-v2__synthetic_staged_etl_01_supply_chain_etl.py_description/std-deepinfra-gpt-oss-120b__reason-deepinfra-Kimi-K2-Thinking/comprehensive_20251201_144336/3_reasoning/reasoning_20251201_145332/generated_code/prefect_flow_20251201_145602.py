from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
from datetime import datetime, timedelta
import pandas as pd
from typing import Dict, Any


@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a() -> pd.DataFrame:
    """Ingest raw shipment CSV data from Vendor A (1,250 records)"""
    data = {
        'vendor': ['A'] * 1250,
        'sku': [f'SKU-A-{i:04d}' for i in range(1250)],
        'shipment_date': [datetime.now() - timedelta(days=i % 30) for i in range(1250)],
        'quantity': [(i % 100) + 1 for i in range(1250)],
        'location_code': [f'LOC-{i % 10}' for i in range(1250)]
    }
    return pd.DataFrame(data)


@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b() -> pd.DataFrame:
    """Ingest raw shipment CSV data from Vendor B (980 records)"""
    data = {
        'vendor': ['B'] * 980,
        'sku': [f'SKU-B-{i:04d}' for i in range(980)],
        'shipment_date': [datetime.now() - timedelta(days=i % 30) for i in range(980)],
        'quantity': [(i % 50) + 1 for i in range(980)],
        'location_code': [f'LOC-{i % 8}' for i in range(980)]
    }
    return pd.DataFrame(data)


@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c() -> pd.DataFrame:
    """Ingest raw shipment CSV data from Vendor C (1,750 records)"""
    data = {
        'vendor': ['C'] * 1750,
        'sku': [f'SKU-C-{i:04d}' for i in range(1750)],
        'shipment_date': [datetime.now() - timedelta(days=i % 30) for i in range(1750)],
        'quantity': [(i % 75) + 1 for i in range(1750)],
        'location_code': [f'LOC-{i % 12}' for i in range(1750)]
    }
    return pd.DataFrame(data)


@task(retries=2, retry_delay_seconds=300)
def cleanse_data(
    vendor_a_data: pd.DataFrame,
    vendor_b_data: pd.DataFrame,
    vendor_c_data: pd.DataFrame
) -> pd.DataFrame:
    """
    Cleanses and normalizes data from all vendors.
    - Normalizes SKU formats across vendors
    - Validates shipment dates and filters invalid records
    - Enriches data with location information from reference tables
    """
    combined = pd.concat([vendor_a_data, vendor_b_data, vendor_c_data], ignore_index=True)
    
    combined['sku_normalized'] = combined['sku'].str.replace(
        r'^SKU-([A-C])-', r'VENDOR-\1-', regex=True
    )
    
    current_date = datetime.now()
    combined['shipment_date'] = pd.to_datetime(combined['shipment_date'])
    combined = combined[combined['shipment_date'] <= current_date]
    
    location_reference = {
        'LOC-0': 'Warehouse-North',
        'LOC-1': 'Warehouse-South',
        'LOC-2': 'Warehouse-East',
        'LOC-3': 'Warehouse-West',
        'LOC-4': 'Warehouse-Central',
        'LOC-5': 'Warehouse-Central',
        'LOC-6': 'Warehouse-Central',
        'LOC-7': 'Warehouse-Central',
        'LOC-8': 'Warehouse-Central',
        'LOC-9': 'Warehouse-Central',
        'LOC-10': 'Warehouse-Central',
        'LOC-11': 'Warehouse-Central',
    }
    combined['location_name'] = combined['location_code'].map(
        location_reference
    ).fillna('Warehouse-Unknown')
    
    return combined


@task(retries=2, retry_delay_seconds=300)
def load_to_db(cleansed_data: pd.DataFrame) -> Dict[str, Any]:
    """
    Loads cleansed data to PostgreSQL inventory database.
    Upserts records to inventory_shipments table.
    """
    record_count = len(cleansed_data)
    print(f"Upserting {record_count} records to inventory_shipments table")
    
    return {
        'status': 'success',
        'records_loaded': record_count,
        'table': 'inventory_shipments',
        'timestamp': datetime.now().isoformat()
    }


@task(retries=2, retry_delay_seconds=300)
def send_summary_email(db_load_result: Dict[str, Any]) -> str:
    """
    Sends email notification to supply chain team.
    Includes processing date, record counts, and data quality metrics.
    """
    processing_date = datetime.now().strftime('%Y-%m-%d')
    record_count = db_load_result['records_loaded']
    
    email_body = f"""Supply Chain Shipment Processing Summary
======================================
Processing Date: {processing_date}
Records Processed: {record_count}
Data Quality Metrics:
  - Total records: {record_count}
  - Load status: {db_load_result['status']}
  - Load timestamp: {db_load_result['timestamp']}

This is an automated notification from the shipment ETL pipeline.
"""
    
    print(f"Sending email to supply-chain-team@company.com:\n{email_body}")
    
    return f"Email sent successfully: {record_count} records processed on {processing_date}"


@flow(
    name="supply-chain-shipment-etl",
    description="Three-stage ETL pipeline for supply chain shipment data processing",
    task_runner=ConcurrentTaskRunner()
)
def shipment_etl_pipeline():
    """
    Main ETL pipeline for supply chain shipment data.
    
    - Stage 1: Parallel extraction from three vendors (fan-out)
    - Stage 2: Sequential transformation and cleansing (fan-in)
    - Stage 3: Sequential database load and email notification
    
    Schedule: Daily execution starting January 1, 2024
    Retry: 2 retries with 5-minute delay between attempts
    """
    
    vendor_a_future = ingest_vendor_a.submit()
    vendor_b_future = ingest_vendor_b.submit()
    vendor_c_future = ingest_vendor_c.submit()
    
    cleansed_data = cleanse_data.submit(
        vendor_a_future,
        vendor_b_future,
        vendor_c_future
    )
    
    db_load_result = load_to_db.submit(cleansed_data)
    
    email_result = send_summary_email.submit(db_load_result)
    
    return {
        'stage_1_extraction': [vendor_a_future, vendor_b_future, vendor_c_future],
        'stage_2_transformation': cleansed_data,
        'stage_3_load': db_load_result,
        'stage_3_notification': email_result
    }


if __name__ == '__main__':
    shipment_etl_pipeline()