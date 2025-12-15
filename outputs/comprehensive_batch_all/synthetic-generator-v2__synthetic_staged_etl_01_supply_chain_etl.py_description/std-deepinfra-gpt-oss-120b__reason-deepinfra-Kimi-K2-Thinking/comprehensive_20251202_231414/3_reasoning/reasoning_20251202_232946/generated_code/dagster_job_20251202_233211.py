from dagster import (
    op,
    job,
    ScheduleDefinition,
    RetryPolicy,
    DefaultScheduleStatus,
    In,
    Out,
    Nothing,
    OpExecutionContext,
)
from typing import List, Dict, Any
import datetime

# Type alias for shipment records
ShipmentRecord = Dict[str, Any]


@op(
    description="Ingests raw shipment CSV data from Vendor A (1,250 records)",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out=Out(List[ShipmentRecord], description="Raw vendor A data"),
    tags={"stage": "extraction", "vendor": "A"}
)
def ingest_vendor_a(context: OpExecutionContext) -> List[ShipmentRecord]:
    """Simulate ingestion of Vendor A data from CSV."""
    context.log.info("Ingesting Vendor A data...")
    # Simulate CSV ingestion
    return [
        {
            "vendor": "A",
            "sku": f"SKU-A-{i:04d}",
            "shipment_date": "2024-01-01",
            "quantity": 100 + i,
            "warehouse": "WH-01"
        }
        for i in range(1250)
    ]


@op(
    description="Ingests raw shipment CSV data from Vendor B (980 records)",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out=Out(List[ShipmentRecord], description="Raw vendor B data"),
    tags={"stage": "extraction", "vendor": "B"}
)
def ingest_vendor_b(context: OpExecutionContext) -> List[ShipmentRecord]:
    """Simulate ingestion of Vendor B data from CSV."""
    context.log.info("Ingesting Vendor B data...")
    return [
        {
            "vendor": "B",
            "sku": f"SKU_B_{i}",
            "shipment_date": "2024-01-01",
            "quantity": 200 + i,
            "warehouse": "WH-02"
        }
        for i in range(980)
    ]


@op(
    description="Ingests raw shipment CSV data from Vendor C (1,750 records)",
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    out=Out(List[ShipmentRecord], description="Raw vendor C data"),
    tags={"stage": "extraction", "vendor": "C"}
)
def ingest_vendor_c(context: OpExecutionContext) -> List[ShipmentRecord]:
    """Simulate ingestion of Vendor C data from CSV."""
    context.log.info("Ingesting Vendor C data...")
    return [
        {
            "vendor": "C",
            "sku": f"SKU.C.{i}",
            "shipment_date": "2024-01-01",
            "quantity": 300 + i,
            "warehouse": "WH-03"
        }
        for i in range(1750)
    ]


@op(
    description="Processes all three vendor datasets together",
    ins={
        "vendor_a_data": In(List[ShipmentRecord], description="Raw data from Vendor A"),
        "vendor_b_data": In(List[ShipmentRecord], description="Raw data from Vendor B"),
        "vendor_c_data": In(List[ShipmentRecord], description="Raw data from Vendor C"),
    },
    out=Out(List[ShipmentRecord], description="Cleansed and normalized data"),
    tags={"stage": "transformation"}
)
def cleanse_data(
    context: OpExecutionContext,
    vendor_a_data: List[ShipmentRecord],
    vendor_b_data: List[ShipmentRecord],
    vendor_c_data: List[ShipmentRecord]
) -> List[ShipmentRecord]:
    """
    Normalizes SKU formats across vendors, validates shipment dates,
    filters invalid records, and enriches data with location information.
    """
    context.log.info("Cleansing and normalizing data...")
    
    # Combine all vendor data
    all_data = vendor_a_data + vendor_b_data + vendor_c_data
    
    # Simulate cleansing and enrichment
    cleansed_data = []
    for record in all_data:
        # Normalize SKU: convert to uppercase, replace separators with underscore
        normalized_sku = record["sku"].upper().replace("-", "_").replace(".", "_")
        
        # Validate shipment date (simple check)
        try:
            datetime.datetime.fromisoformat(record["shipment_date"])
            is_valid = True
        except ValueError:
            is_valid = False
        
        if is_valid:
            # Enrich with location information
            enriched_record = {
                "vendor": record["vendor"],
                "sku_normalized": normalized_sku,
                "shipment_date": record["shipment_date"],
                "quantity": record["quantity"],
                "warehouse": record["warehouse"],
                "location_zone": "NORTH_AMERICA",  # Simulated enrichment
                "processing_timestamp": datetime.datetime.utcnow().isoformat()
            }
            cleansed_data.append(enriched_record)
    
    context.log.info(f"Processed {len(cleansed_data)} records after cleansing")
    return cleansed_data


@op(
    description="Loads cleansed data to PostgreSQL inventory database",
    ins={"cleansed_data": In(List[ShipmentRecord], description="Cleansed data to load")},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"stage": "load"}
)
def load_to_db(context: OpExecutionContext, cleansed_data: List[ShipmentRecord]) -> None:
    """
    Upserts records to inventory_shipments table in PostgreSQL.
    """
    context.log.info(f"Loading {len(cleansed_data)} records to PostgreSQL...")
    
    # Simulate database upsert operation
    # In production, use a resource like dagster_postgres
    for record in cleansed_data:
        # Simulate upsert
        pass
    
    context.log.info("Successfully loaded data to inventory_shipments table")


@op(
    description="Sends email notification to supply chain team",
    ins={"_db_load_complete": In(Nothing, description="Trigger after DB load completes")},
    retry_policy=RetryPolicy(max_retries=2, delay=300),
    tags={"stage": "notification"}
)
def send_summary_email(context: OpExecutionContext) -> None:
    """
    Sends email notification with processing date, record counts,
    and data quality metrics to supply-chain-team@company.com.
    """
    context.log.info("Sending summary email to supply-chain-team@company.com...")
    
    # Simulate email sending
    email_content = {
        "to": "supply-chain-team@company.com",
        "subject": "Daily Supply Chain ETL Summary",
        "body": f"""
        Processing Date: {datetime.datetime.utcnow().date()}
        Records Processed: 3,850
        Data Quality: PASSED
        Status: SUCCESS
        """
    }
    
    context.log.info(f"Email sent: {email_content['subject']}")


@job(
    description="Three-stage ETL pipeline for supply chain shipment data processing",
    tags={"pipeline_type": "etl", "domain": "supply_chain"}
)
def supply_chain_etl_job():
    """
    ETL Job: Parallel vendor extraction → Consolidated transformation → Sequential load and notification
    """
    # Stage 1: Parallel extraction (fan-out)
    vendor_a_data = ingest_vendor_a()
    vendor_b_data = ingest_vendor_b()
    vendor_c_data = ingest_vendor_c()
    
    # Stage 2: Consolidated transformation (fan-in)
    cleansed_data = cleanse_data(vendor_a_data, vendor_b_data, vendor_c_data)
    
    # Stage 3: Load and notification (sequential)
    db_load_complete = load_to_db(cleansed_data)
    send_summary_email(db_load_complete)


# Schedule configuration
# Daily execution starting January 1, 2024, no catch