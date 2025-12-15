from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def ingest_sales_data(region: str) -> pd.DataFrame:
    """Ingest sales data from CSV for a specific region."""
    logger = get_run_logger()
    logger.info(f"Ingesting sales data for {region} region.")
    # Simulate data ingestion from CSV
    data = {
        'US-East': pd.DataFrame({'sales': [100, 200, 300], 'currency': ['USD', 'USD', 'USD']}),
        'US-West': pd.DataFrame({'sales': [150, 250, 350], 'currency': ['USD', 'USD', 'USD']}),
        'EU': pd.DataFrame({'sales': [120, 220, 320], 'currency': ['EUR', 'EUR', 'EUR']}),
        'APAC': pd.DataFrame({'sales': [180, 280, 380], 'currency': ['JPY', 'JPY', 'JPY']})
    }
    return data[region]

@task(retries=3, retry_delay_seconds=300)
def convert_currency(data: pd.DataFrame, region: str) -> pd.DataFrame:
    """Convert sales data to USD for a specific region."""
    logger = get_run_logger()
    logger.info(f"Converting currency for {region} region.")
    exchange_rates = {
        'USD': 1.0,
        'EUR': 1.1,
        'JPY': 0.007
    }
    if region in ['US-East', 'US-West']:
        return data
    else:
        data['sales'] = data['sales'] * exchange_rates[data['currency'][0]]
        data['currency'] = 'USD'
        return data

@task(retries=3, retry_delay_seconds=300)
def aggregate_data(data: list[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate all regional data into a single global revenue report."""
    logger = get_run_logger()
    logger.info("Aggregating regional data.")
    combined_data = pd.concat(data)
    combined_data['revenue'] = combined_data['sales']
    total_revenue = combined_data['revenue'].sum()
    logger.info(f"Total global revenue: {total_revenue} USD")
    return combined_data

@flow(name="Multi-Region Ecommerce Analytics Pipeline")
def ecommerce_pipeline():
    """Orchestrates the multi-region ecommerce analytics pipeline."""
    logger = get_run_logger()
    logger.info("Starting the pipeline.")

    # Fan-out phase 1: Ingest sales data
    us_east_data = ingest_sales_data.submit("US-East")
    us_west_data = ingest_sales_data.submit("US-West")
    eu_data = ingest_sales_data.submit("EU")
    apac_data = ingest_sales_data.submit("APAC")

    # Fan-out phase 2: Convert currencies
    us_east_converted = convert_currency.submit(us_east_data, "US-East")
    us_west_converted = convert_currency.submit(us_west_data, "US-West")
    eu_converted = convert_currency.submit(eu_data, "EU")
    apac_converted = convert_currency.submit(apac_data, "APAC")

    # Fan-in phase: Aggregate data
    all_converted_data = aggregate_data.submit([us_east_converted, us_west_converted, eu_converted, apac_converted])

    logger.info("Pipeline completed successfully.")

if __name__ == '__main__':
    ecommerce_pipeline()

# Optional: Deployment/schedule configuration
# Schedule the flow to run daily with no catch-up
# Deployment configuration can be added using Prefect's deployment API or YAML files