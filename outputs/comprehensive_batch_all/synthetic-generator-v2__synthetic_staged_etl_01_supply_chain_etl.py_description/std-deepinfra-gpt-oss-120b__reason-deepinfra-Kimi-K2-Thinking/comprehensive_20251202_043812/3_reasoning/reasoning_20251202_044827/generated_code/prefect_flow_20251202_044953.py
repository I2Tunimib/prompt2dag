from prefect import flow, task
from typing import List, Dict, Any
import datetime


@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_a() -> List[Dict[str, Any]]:
    """Ingest raw shipment CSV data from Vendor A (1,250 records)."""
    return [
        {"vendor": "A", "sku": f"SKU_A_{i}", "shipment_date": "2024-01-01", "quantity": 10}
        for i in range(1250)
    ]


@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_b() -> List[Dict[str, Any]]:
    """Ingest raw shipment CSV data from Vendor B (980 records)."""
    return [
        {"vendor": "B", "sku": f"SKU_B_{i}", "shipment_date": "2024-01-01", "quantity": 20}
        for i in range(980)
    ]


@task(retries=2, retry_delay_seconds=300)
def ingest_vendor_c() -> List[Dict[str, Any]]:
    """Ingest raw shipment CSV data from Vendor C (1,750 records)."""
    return [
        {"vendor": "C", "sku": f"SKU_C_{i}", "shipment_date": "2024-01-01", "quantity": 15}
        for i in range(1750)
    ]


@task(retries=2, retry_delay_seconds=300)
def cleanse_data(
    data_a: List[Dict[str, Any]],
    data_b: List[Dict[str, Any]],
    data_c: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Cleanse and normalize data from all vendors.
    - Normalizes SKU formats across vendors
    - Validates shipment dates and filters invalid records
    - Enriches data with location information
    """
    combined_data = data_a + data_b + data_c
    cleansed_records = []
    invalid_records = 0

    for record in combined_data:
        record["sku_normalized"] = record["sku"].upper().replace("_", "-")
        
        try:
            datetime.datetime.fromisoformat(record["shipment_date"])
            record["is_valid"] = True
        except (ValueError, TypeError):
            record["is_valid"] = False
            invalid_records += 1
        
        record["location"] = "WAREHOUSE_01"
        
        if record["is_valid"]:
            cleansed_records.append(record)

    return {
        "records": cleansed_records,
        "metrics": {
            "total_input": len(combined_data),
            "total_output": len(cleansed_records),
            "invalid_records": invalid_records,
            "processing_timestamp": datetime.datetime.now().isoformat()
        }
    }


@task(retries=2, retry_delay_seconds=300)
def load_to_db(cleansed_data: Dict[str, Any]) -> Dict[str, Any]:
    """Load cleansed data to PostgreSQL inventory database."""
    records = cleansed_data["records"]
    metrics = cleansed_data["metrics"]
    
    load_metrics = {
        "records_loaded": len(records),
        "table": "inventory_shipments",
        "load_timestamp": datetime.datetime.now().isoformat(),
        "data_quality": metrics
    }
    
    return load_metrics


@task(retries=2, retry_delay_seconds=300)
def send_summary_email(load_metrics: Dict[str, Any]) -> None:
    """Send email notification to supply chain team with processing details."""
    print("--- Email Notification ---")
    print(f"To: supply-chain-team@company.com")
    print(f"Subject: Supply Chain ETL Pipeline Summary")
    print(f"Processing Date: {load_metrics['data_quality']['processing_timestamp']}")
    print(f"Records Loaded: {load_metrics['records_loaded']}")
    print(f"Target Table: {load_metrics['table']}")
    print(f"Data Quality - Total Input: {load_metrics['data_quality']['total_input']}")
    print(f"Data Quality - Invalid Records: {load_metrics['data_quality']['invalid_records']}")
    print("-------------------------")


@flow(
    name="supply-chain-etl-pipeline",
    description="Three-stage ETL pipeline for supply chain shipment data processing",
    retries=2,
    retry_delay_seconds=300
)
def supply_chain_etl_pipeline():
    """Main flow orchestrating the supply chain ETL pipeline."""
    # Stage 1: Extract - Parallel vendor data ingestion
    vendor_a_future = ingest_vendor_a.submit()
    vendor_b_future = ingest_vendor_b.submit()
    vendor_c_future = ingest_vendor_c.submit()
    
    # Stage 2: Transform - Data cleansing and normalization
    cleansed_data = cleanse_data(
        vendor_a_future.result(),
        vendor_b_future.result(),
        vendor_c_future.result()
    )
    
    # Stage 3: Load - Database loading and notification
    load_metrics = load_to_db(cleansed_data)
    send_summary_email(load_metrics)


if __name__ == "__main__":
    supply_chain_etl_pipeline()
    
    # Deployment with daily schedule (no catchup, failure emails disabled):
    # prefect deployment build supply_chain_etl_pipeline.py:supply_chain_etl_pipeline \
    #   --name "daily-supply-chain-etl" \
    #   --cron "0 2 * * *" \
    #   --start-date "2024-01-01T00:00:00" \
    #   --timezone "UTC" \
    #   --skip-catchup \
    #   --apply
    # Configure notifications separately to disable failure emails if needed