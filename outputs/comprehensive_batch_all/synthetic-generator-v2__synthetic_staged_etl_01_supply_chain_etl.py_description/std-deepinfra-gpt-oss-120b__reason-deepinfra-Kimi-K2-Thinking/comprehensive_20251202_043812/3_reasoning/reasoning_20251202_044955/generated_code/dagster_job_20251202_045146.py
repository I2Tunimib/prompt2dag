from dagster import (
    op,
    job,
    schedule,
    ScheduleDefinition,
    RetryPolicy,
    DefaultScheduleStatus,
    resource,
    In,
    Out,
    Nothing,
    graph,
    AssetMaterialization,
    Output,
)
from datetime import datetime
import pandas as pd
from typing import List, Dict, Any
import logging

# Simulated resources
@resource
def postgres_resource(context):
    """Stub for PostgreSQL connection resource."""
    # In production, this would return a real connection object
    return {
        "host": "localhost",
        "port": 5432,
        "database": "inventory_db",
        "user": "user",
        "password": "password"
    }

@resource
def email_resource(context):
    """Stub for email sending resource."""
    # In production, this would return an email client
    return {
        "smtp_server": "smtp.company.com",
        "smtp_port": 587,
        "from_email": "pipeline@company.com",
        "to_email": "supply-chain-team@company.com"
    }

# Ops
@op(
    out={"vendor_a_data": Out(List[Dict[str, Any]])},
    description="Ingests raw shipment CSV data from Vendor A (1,250 records)"
)
def ingest_vendor_a(context):
    """Simulate ingesting Vendor A data."""
    context.log.info("Ingesting Vendor A data...")
    # Simulate CSV data
    data = [
        {
            "shipment_id": f"A-{i}",
            "sku": f"SKU-A-{i:04d}",
            "shipment_date": "2024-01-01",
            "quantity": 10,
            "location_code": "LOC-A"
        }
        for i in range(1250)
    ]
    return data

@op(
    out={"vendor_b_data": Out(List[Dict[str, Any]])},
    description="Ingests raw shipment CSV data from Vendor B (980 records)"
)
def ingest_vendor_b(context):
    """Simulate ingesting Vendor B data."""
    context.log.info("Ingesting Vendor B data...")
    # Simulate CSV data
    data = [
        {
            "shipment_id": f"B-{i}",
            "sku": f"SKU-B-{i:04d}",
            "shipment_date": "2024-01-02",
            "quantity": 20,
            "location_code": "LOC-B"
        }
        for i in range(980)
    ]
    return data

@op(
    out={"vendor_c_data": Out(List[Dict[str, Any]])},
    description="Ingests raw shipment CSV data from Vendor C (1,750 records)"
)
def ingest_vendor_c(context):
    """Simulate ingesting Vendor C data."""
    context.log.info("Ingesting Vendor C data...")
    # Simulate CSV data
    data = [
        {
            "shipment_id": f"C-{i}",
            "sku": f"SKU-C-{i:04d}",
            "shipment_date": "2024-01-03",
            "quantity": 30,
            "location_code": "LOC-C"
        }
        for i in range(1750)
    ]
    return data

@op(
    ins={
        "vendor_a_data": In(List[Dict[str, Any]]),
        "vendor_b_data": In(List[Dict[str, Any]]),
        "vendor_c_data": In(List[Dict[str, Any]])
    },
    out={"cleansed_data": Out(List[Dict[str, Any]])},
    description="Processes all three vendor datasets together, normalizes SKU formats, validates shipment dates, filters invalid records, and enriches data with location information"
)
def cleanse_data(context, vendor_a_data, vendor_b_data, vendor_c_data):
    """Cleanse and normalize data from all vendors."""
    context.log.info(f"Starting data cleansing. Input records: A={len(vendor_a_data)}, B={len(vendor_b_data)}, C={len(vendor_c_data)}")
    
    # Combine all data
    combined_data = vendor_a_data + vendor_b_data + vendor_c_data
    
    # Simulate cleansing: normalize SKU, validate dates, enrich location
    cleansed_records = []
    for record in combined_data:
        try:
            # Normalize SKU (convert to uppercase)
            record["sku_normalized"] = record["sku"].upper()
            
            # Validate date
            datetime.strptime(record["shipment_date"], "%Y-%m-%d")
            
            # Enrich with location info (simulate)
            record["location_name"] = f"Location-{record['location_code']}"
            
            cleansed_records.append(record)
        except (ValueError, KeyError):
            # Skip invalid records
            continue
    
    context.log.info(f"Data cleansing complete. Output records: {len(cleansed_records)}")
    
    # Yield both the data and a materialization event
    yield Output(cleansed_records, "cleansed_data")
    yield AssetMaterialization(
        asset_key="cleansed_shipments",
        description="Cleansed shipment data",
        metadata={"record_count": len(cleansed_records)}
    )

@op(
    ins={"cleansed_data": In(List[Dict[str, Any]])},
    out={"load_metrics": Out(Dict[str, Any])},
    required_resource_keys={"postgres"},
    description="Loads cleansed data to PostgreSQL inventory database, upserts 3,850 records to inventory_shipments table"
)
def load_to_db(context, cleansed_data):
    """Load cleansed data to PostgreSQL database."""
    context.log.info(f"Loading {len(cleansed_data)} records to PostgreSQL...")
    
    # Get database connection info from resource
    db_info = context.resources.postgres
    
    # Simulate database loading
    # In production, would use psycopg2 or SQLAlchemy to perform upsert
    upserted_count = len(cleansed_data)
    
    context.log.info(f"Successfully upserted {upserted_count} records to inventory_shipments table")
    
    # Return metrics for email notification
    return {
        "processing_date": datetime.now().strftime("%Y-%m-%d"),
        "total_records": len(cleansed_data),
        "upserted_records": upserted_count,
        "data_quality_score": 0.98  # Simulated metric
    }

@op(
    ins={"load_metrics": In(Dict[str, Any])},
    required_resource_keys={"email"},
    description="Sends email notification to supply chain team with processing date, record counts, and data quality metrics"
)
def send_summary_email(context, load_metrics):
    """Send summary email notification."""
    context.log.info("Sending summary email notification...")
    
    # Get email config from resource
    email_config = context.resources.email
    
    # Simulate email sending
    email_body = f"""
    Supply Chain Shipment Processing Summary
    
    Processing Date: {load_metrics['processing_date']}
    Total Records Processed: {load_metrics['total_records']}
    Records Upserted to DB: {load_metrics['upserted_records']}
    Data Quality Score: {load_metrics['data_quality_score']}
    
    Pipeline execution completed successfully.
    """
    
    context.log.info(f"Email would be sent to: {email_config['to_email']}")
    context.log.info(f"Email body:\n{email_body}")
    
    # In production, would use smtplib or email library to send actual email

# Job definition
@job(
    description="Three-stage ETL pipeline for supply chain shipment data processing",
    resource_defs={
        "postgres": postgres_resource,
        "email": email_resource
    },
    retry_policy=RetryPolicy(max_retries=2, delay=300)  # 2 retries, 5-minute delay
)
def supply_chain_etl_job():
    """
    ETL pipeline for supply chain shipment data.
    
    Stage 1: Extract - Parallel vendor data ingestion
    Stage 2: Transform - Data cleansing and normalization
    Stage 3: Load - Database loading and notification
    """
    # Stage 1: Parallel extraction
    vendor_a = ingest_vendor_a()
    vendor_b = ingest_vendor_b()
    vendor_c = ingest_vendor_c()
    
    # Stage 2: Transformation (fan-in from all extraction tasks)
    cleansed = cleanse_data(vendor_a, vendor_b, vendor_c)
    
    # Stage 3: Load and notify (sequential)
    metrics = load_to_db(cleansed)
    send_summary_email(metrics)

# Schedule definition
@schedule(
    cron_schedule="0 0 * * *",  # Daily at midnight
    job=supply_chain_etl_job,
    execution_timezone="UTC",
    default_status=DefaultScheduleStatus.RUNNING
)
def daily_supply_chain_schedule(context):
    """Daily schedule for supply chain ETL pipeline."""
    scheduled_date = context.scheduled_execution_time.strftime("%Y-%m-%d")
    return {
        "ops": {
            "ingest_vendor_a": {"config": {"execution_date": scheduled_date}},
            "ingest_vendor_b": {"config": {"execution_date": scheduled_date}},
            "ingest_vendor_c": {"config": {"execution_date": scheduled_date}}
        }
    }

# Launch pattern
if __name__ == "__main__":
    # Execute the job in-process for testing
    result = supply_chain_etl_job.execute_in_process()
    
    # Check if execution was successful
    if result.success:
        print("Pipeline execution completed successfully!")
    else:
        print("Pipeline execution failed!")
        for event in result.all_events:
            if event.event_type_value == "STEP_FAILURE":
                print(f"Step failed: {event.step_key}")