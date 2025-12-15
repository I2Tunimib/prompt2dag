from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def ingest_sales_data(region: str) -> pd.DataFrame:
    """Ingest sales data from CSV for a given region."""
    logger = get_run_logger()
    logger.info(f"Ingesting sales data for {region} region.")
    # Simulate data ingestion from CSV
    data = pd.DataFrame({
        'date': ['2023-01-01', '2023-01-02'],
        'sales': [1000, 1500],
        'currency': ['USD', 'USD']
    })
    return data

@task(retries=3, retry_delay_seconds=300)
def convert_currency(data: pd.DataFrame, region: str) -> pd.DataFrame:
    """Convert currency to USD for a given region."""
    logger = get_run_logger()
    logger.info(f"Converting currency for {region} region.")
    if region in ['US-East', 'US-West']:
        return data
    elif region == 'EU':
        data['sales'] = data['sales'] * 1.1  # Example conversion rate EUR to USD
        data['currency'] = 'USD'
    elif region == 'APAC':
        data['sales'] = data['sales'] * 0.007  # Example conversion rate JPY to USD
        data['currency'] = 'USD'
    return data

@task(retries=3, retry_delay_seconds=300)
def aggregate_data(data_list: list[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate data from all regions and generate a global revenue report."""
    logger = get_run_logger()
    logger.info("Aggregating data from all regions.")
    combined_data = pd.concat(data_list)
    combined_data['revenue'] = combined_data['sales']
    global_revenue = combined_data.groupby('date')['revenue'].sum().reset_index()
    global_revenue.to_csv('global_revenue_report.csv', index=False)
    return global_revenue

@flow(name="Multi-Region Ecommerce Analytics Pipeline")
def ecommerce_pipeline():
    """Orchestrates the multi-region ecommerce analytics pipeline."""
    logger = get_run_logger()
    logger.info("Starting the pipeline.")

    # Fan-out phase 1: Ingest sales data from four regions
    us_east_data = ingest_sales_data.submit('US-East')
    us_west_data = ingest_sales_data.submit('US-West')
    eu_data = ingest_sales_data.submit('EU')
    apac_data = ingest_sales_data.submit('APAC')

    # Fan-out phase 2: Convert currencies
    us_east_converted = convert_currency.submit(us_east_data, 'US-East')
    us_west_converted = convert_currency.submit(us_west_data, 'US-West')
    eu_converted = convert_currency.submit(eu_data, 'EU')
    apac_converted = convert_currency.submit(apac_data, 'APAC')

    # Fan-in phase: Aggregate data
    aggregated_data = aggregate_data([us_east_converted, us_west_converted, eu_converted, apac_converted])

    logger.info("Pipeline completed successfully.")
    return aggregated_data

if __name__ == '__main__':
    ecommerce_pipeline()

# Optional: Deployment/schedule configuration
# Deploy this flow with a daily schedule and no catch-up for missed runs.
# Use task retries with 5-minute delays on failure.
# Set up email notifications on task failures.