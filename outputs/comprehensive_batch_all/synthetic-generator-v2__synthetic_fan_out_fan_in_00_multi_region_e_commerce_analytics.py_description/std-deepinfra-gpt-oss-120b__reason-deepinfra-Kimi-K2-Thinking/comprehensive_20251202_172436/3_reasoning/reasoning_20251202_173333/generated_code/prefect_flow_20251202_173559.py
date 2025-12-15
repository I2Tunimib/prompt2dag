from prefect import flow, task
from prefect.task_runners import ConcurrentTaskRunner
import pandas as pd
from datetime import datetime
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Task configurations
RETRY_DELAY = 300  # 5 minutes in seconds

@task(retries=3, retry_delay_seconds=RETRY_DELAY)
def start_pipeline():
    """Mark pipeline initiation."""
    logger.info("Starting multi-region ecommerce analytics pipeline")
    return "Pipeline started"

@task(retries=3, retry_delay_seconds=RETRY_DELAY)
def ingest_region_data(region: str, date: str) -> pd.DataFrame:
    """Ingest regional sales data from CSV source."""
    # Simulate reading from CSV - in real scenario, read actual file
    logger.info(f"Ingesting data for {region} region")
    
    # Mock data for demonstration
    data = {
        'date': [date] * 10,
        'product_id': range(1, 11),
        'sales_amount': [100.0 + i * 10 for i in range(10)],
        'currency': ['USD'] * 10
    }
    
    if region == 'EU':
        data['currency'] = ['EUR'] * 10
        data['sales_amount'] = [90.0 + i * 9 for i in range(10)]
    elif region == 'APAC':
        data['currency'] = ['JPY'] * 10
        data['sales_amount'] = [10000.0 + i * 1000 for i in range(10)]
    
    df = pd.DataFrame(data)
    logger.info(f"Successfully ingested {len(df)} records for {region}")
    return df

@task(retries=3, retry_delay_seconds=RETRY_DELAY)
def convert_currency(data: pd.DataFrame, region: str, date: str) -> pd.DataFrame:
    """Convert regional currency to USD."""
    logger.info(f"Converting currency for {region} region")
    
    # Create a copy to avoid modifying original
    converted_data = data.copy()
    
    if region == 'EU':
        # Mock exchange rate - in real scenario, fetch from API
        eur_to_usd = 1.08
        converted_data['sales_amount_usd'] = converted_data['sales_amount'] * eur_to_usd
        logger.info(f"Converted EUR to USD at rate {eur_to_usd}")
    elif region == 'APAC':
        # Mock exchange rate - in real scenario, fetch from API
        jpy_to_usd = 0.0067
        converted_data['sales_amount_usd'] = converted_data['sales_amount'] * jpy_to_usd
        logger.info(f"Converted JPY to USD at rate {jpy_to_usd}")
    else:
        # US regions already in USD
        converted_data['sales_amount_usd'] = converted_data['sales_amount']
        logger.info(f"No conversion needed for {region}")
    
    return converted_data

@task(retries=3, retry_delay_seconds=RETRY_DELAY)
def aggregate_global_revenue(
    us_east_data: pd.DataFrame,
    us_west_data: pd.DataFrame,
    eu_data: pd.DataFrame,
    apac_data: pd.DataFrame,
    date: str
) -> str:
    """Aggregate all regional data and generate global revenue report."""
    logger.info("Aggregating global revenue data")
    
    # Calculate regional revenues
    us_east_revenue = us_east_data['sales_amount_usd'].sum()
    us_west_revenue = us_west_data['sales_amount_usd'].sum()
    eu_revenue = eu_data['sales_amount_usd'].sum()
    apac_revenue = apac_data['sales_amount_usd'].sum()
    
    # Calculate global total
    global_revenue = us_east_revenue + us_west_revenue + eu_revenue + apac_revenue
    
    # Create report
    report_data = {
        'date': [date],
        'us_east_revenue_usd': [us_east_revenue],
        'us_west_revenue_usd': [us_west_revenue],
        'eu_revenue_usd': [eu_revenue],
        'apac_revenue_usd': [apac_revenue],
        'global_revenue_usd': [global_revenue]
    }
    
    report_df = pd.DataFrame(report_data)
    
    # Save to CSV
    output_path = f"global_revenue_report_{date}.csv"
    report_df.to_csv(output_path, index=False)
    
    logger.info(f"Global revenue report generated: {output_path}")
    logger.info(f"Total global revenue: ${global_revenue:,.2f}")
    
    return output_path

@task(retries=3, retry_delay_seconds=RETRY_DELAY)
def end_pipeline():
    """Mark pipeline completion."""
    logger.info("Multi-region ecommerce analytics pipeline completed successfully")
    return "Pipeline completed"

@flow(
    name="multi-region-ecommerce-analytics",
    description="Daily pipeline for processing multi-region sales data and generating global revenue reports",
    # For deployment, add schedule:
    # schedule={"interval": 86400, "anchor_date": "2023-01-01T00:00:00", "timezone": "UTC"},
    # Note: Prefect 2.x schedules are configured during deployment
    task_runner=ConcurrentTaskRunner()
)
def ecommerce_analytics_pipeline(date: str = None):
    """
    Main flow for multi-region ecommerce analytics.
    
    Args:
        date: Date string in YYYY-MM-DD format. If None, uses current date.
    """
    if date is None:
        date = datetime.now().strftime("%Y-%m-%d")
    
    # Start pipeline
    start_result = start_pipeline()
    
    # Fan-out phase 1: Parallel ingestion
    us_east_ingestion = ingest_region_data.submit("US-East", date)
    us_west_ingestion = ingest_region_data.submit("US-West", date)
    eu_ingestion = ingest_region_data.submit("EU", date)
    apac_ingestion = ingest_region_data.submit("APAC", date)
    
    # Fan-out phase 2: Parallel currency conversion
    # Wait for ingestion to complete and pass results to conversion
    us_east_converted = convert_currency.submit(
        us_east_ingestion.result(), "US-East", date
    )
    us_west_converted = convert_currency.submit(
        us_west_ingestion.result(), "US-West", date
    )
    eu_converted = convert_currency.submit(
        eu_ingestion.result(), "EU", date
    )
    apac_converted = convert_currency.submit(
        apac_ingestion.result(), "APAC", date
    )
    
    # Fan-in phase: Single aggregation
    # Wait for all conversions to complete
    report_path = aggregate_global_revenue(
        us_east_converted.result(),
        us_west_converted.result(),
        eu_converted.result(),
        apac_converted.result(),
        date
    )
    
    # End pipeline
    end_pipeline()
    
    return report_path

if __name__ == '__main__':
    # For local testing
    ecommerce_analytics_pipeline(date="2023-12-01")