from prefect import flow, task
from prefect.logging import get_run_logger
from typing import Dict, Any, List
from datetime import datetime
import csv


# Simulated exchange rates - in production, fetch from a reliable API
EXCHANGE_RATES = {
    "EUR": 1.08,
    "JPY": 0.0067,
}

# Sample data mapping for demonstration purposes
SAMPLE_SALES_DATA = {
    "US-East": {"amount": 150000, "currency": "USD"},
    "US-West": {"amount": 120000, "currency": "USD"},
    "EU": {"amount": 90000, "currency": "EUR"},
    "APAC": {"amount": 15000000, "currency": "JPY"},
}


@task(retries=3, retry_delay_seconds=300)
def start_pipeline() -> None:
    """Mark pipeline initiation."""
    logger = get_run_logger()
    logger.info("Starting multi-region ecommerce analytics pipeline")


@task(retries=3, retry_delay_seconds=300)
def ingest_region_sales(region: str) -> Dict[str, Any]:
    """Ingest sales data for a specific region from CSV source."""
    logger = get_run_logger()
    logger.info(f"Ingesting {region} region sales data")
    
    # Simulate CSV ingestion - replace with actual file I/O in production
    data = SAMPLE_SALES_DATA[region].copy()
    data["region"] = region
    data["ingestion_timestamp"] = datetime.now().isoformat()
    
    logger.info(f"{region} data ingested: {data}")
    return data


@task(retries=3, retry_delay_seconds=300)
def convert_to_usd(data: Dict[str, Any]) -> Dict[str, Any]:
    """Convert regional data to USD using daily exchange rates."""
    logger = get_run_logger()
    region = data["region"]
    currency = data["currency"]
    amount = data["amount"]
    
    if currency == "USD":
        logger.info(f"{region} data already in USD, no conversion needed")
        converted_amount = amount
        exchange_rate = 1.0
    else:
        logger.info(f"Converting {region} data from {currency} to USD")
        exchange_rate = EXCHANGE_RATES[currency]
        converted_amount = amount * exchange_rate
    
    result = {
        "region": region,
        "original_currency": currency,
        "original_amount": amount,
        "exchange_rate": exchange_rate,
        "converted_amount_usd": converted_amount,
    }
    
    logger.info(f"{region} conversion complete: {result}")
    return result


@task(retries=3, retry_delay_seconds=300)
def aggregate_global_revenue(regional_data: List[Dict[str, Any]]) -> str:
    """Aggregate all converted regional data and generate global revenue report CSV."""
    logger = get_run_logger()
    logger.info("Aggregating regional data and generating global revenue report")
    
    # Calculate global total
    total_revenue_usd = sum(data["converted_amount_usd"] for data in regional_data)
    
    # Generate report content
    report_lines = []
    report_lines.append("=== Global Revenue Report ===")
    report_lines.append(f"Report Date: {datetime.now().strftime('%Y-%m-%d')}")
    report_lines.append(f"Generated At: {datetime.now().isoformat()}")
    report_lines.append("")
    report_lines.append("Regional Breakdown:")
    
    for data in regional_data:
        region = data["region"]
        orig_amount = data["original_amount"]
        currency = data["original_currency"]
        usd_amount = data["converted_amount_usd"]
        
        if currency == "USD":
            report_lines.append(f"  {region}: ${usd_amount:,.2f} USD")
        else:
            rate = data["exchange_rate"]
            report_lines.append(
                f"  {region}: ${usd_amount:,.2f} USD "
                f"(converted from {orig_amount:,.2f} {currency} @ {rate})"
            )
    
    report_lines.append("")
    report_lines.append(f"Global Total Revenue: ${total_revenue_usd:,.2f} USD")
    report_lines.append("=" * 40)
    
    # Write CSV report
    report_content = "\n".join(report_lines)
    filename = f"global_revenue_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["Report Type", "Global Revenue Report"])
        writer.writerow(["Date", datetime.now().strftime("%Y-%m-%d")])
        writer.writerow([])
        writer.writerow(["Region", "Original Amount", "Currency", "Exchange Rate", "USD Amount"])
        
        for data in regional_data:
            writer.writerow([
                data["region"],
                data["original_amount"],
                data["original_currency"],
                data["exchange_rate"],
                data["converted_amount_usd"],
            ])
        
        writer.writerow([])
        writer.writerow(["Global Total", "", "", "", total_revenue_usd])
    
    logger.info(f"Report saved to: {filename}")
    logger.info(f"Summary:\n{report_content}")
    
    return filename


@task(retries=3, retry_delay_seconds=300)
def end_pipeline() -> None:
    """Mark pipeline completion."""
    logger = get_run_logger()
    logger.info("Multi-region ecommerce analytics pipeline completed successfully")


@flow(
    name="multi-region-ecommerce-analytics",
    description="Daily multi-region ecommerce analytics pipeline with fan-out/fan-in pattern"
)
def ecommerce_analytics_pipeline():
    """
    Orchestrate the multi-region ecommerce analytics pipeline.
    
    Execution flow:
    1. Start pipeline
    2. Fan-out: Ingest data from 4 regions in parallel
    3. Fan-out: Convert currencies for 4 regions in parallel
    4. Fan-in: Aggregate all data and generate report
    5. End pipeline
    """
    # Pipeline start
    start_pipeline()
    
    # Define regions to process
    regions = ["US-East", "US-West", "EU", "APAC"]
    
    # Fan-out phase 1: Parallel ingestion tasks
    ingestion_futures = [
        ingest_region_sales.submit(region)
        for region in regions
    ]
    
    # Fan-out phase 2: Parallel conversion tasks
    # Each conversion depends on its respective ingestion task
    conversion_futures = [
        convert_to_usd.submit(ingestion_future)
        for ingestion_future in ingestion_futures
    ]
    
    # Fan-in phase: Single aggregation task
    # Prefect automatically waits for all conversion futures to complete
    report_file = aggregate_global_revenue(conversion_futures)
    
    # Pipeline end
    end_pipeline()
    
    return report_file


if __name__ == "__main__":
    # Execute flow locally
    # Deployment configuration (run these commands to deploy):
    # prefect deployment build ecommerce_analytics_pipeline:ecommerce_analytics_pipeline \
    #   --name "daily-ecommerce-analytics" \
    #   --schedule "0 6 * * *" \
    #   --no-catchup \
    #   --tag "production"
    # Configure email notifications through Prefect UI or automation
    ecommerce_analytics_pipeline()