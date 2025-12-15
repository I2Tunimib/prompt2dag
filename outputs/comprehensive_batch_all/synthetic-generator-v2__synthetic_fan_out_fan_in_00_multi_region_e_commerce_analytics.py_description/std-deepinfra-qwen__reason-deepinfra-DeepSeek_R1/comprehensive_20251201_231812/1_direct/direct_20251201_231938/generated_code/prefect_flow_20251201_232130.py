from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
import pandas as pd

@task(retries=3, retry_delay_seconds=300, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def ingest_sales_data(region: str) -> pd.DataFrame:
    """Ingest sales data from CSV for the specified region."""
    logger = get_run_logger()
    logger.info(f"Ingesting sales data for {region} region.")
    # Simulate data ingestion from CSV
    data = {
        "date": ["2023-01-01", "2023-01-02", "2023-01-03"],
        "sales": [1000, 1500, 1200],
        "currency": ["USD", "USD", "USD"]
    }
    if region == "EU":
        data["currency"] = ["EUR", "EUR", "EUR"]
    elif region == "APAC":
        data["currency"] = ["JPY", "JPY", "JPY"]
    return pd.DataFrame(data)

@task(retries=3, retry_delay_seconds=300)
def convert_currency(data: pd.DataFrame, region: str) -> pd.DataFrame:
    """Convert sales data currency to USD for the specified region."""
    logger = get_run_logger()
    logger.info(f"Converting currency for {region} region.")
    if region in ["US-East", "US-West"]:
        return data
    exchange_rates = {
        "EUR": 1.10,  # Example exchange rate
        "JPY": 0.0075  # Example exchange rate
    }
    data["sales"] = data.apply(lambda row: row["sales"] * exchange_rates[row["currency"]], axis=1)
    data["currency"] = "USD"
    return data

@task(retries=3, retry_delay_seconds=300)
def aggregate_data(dataframes: list[pd.DataFrame]) -> pd.DataFrame:
    """Aggregate sales data from all regions and generate the global revenue report."""
    logger = get_run_logger()
    logger.info("Aggregating data from all regions.")
    combined_data = pd.concat(dataframes)
    combined_data["date"] = pd.to_datetime(combined_data["date"])
    combined_data = combined_data.groupby(combined_data["date"]).agg({"sales": "sum"}).reset_index()
    combined_data.to_csv("global_revenue_report.csv", index=False)
    return combined_data

@flow(name="Multi-Region Ecommerce Analytics Pipeline")
def ecommerce_pipeline():
    """Orchestrates the multi-region ecommerce analytics pipeline."""
    logger = get_run_logger()
    logger.info("Starting the pipeline.")
    
    # Fan-out phase 1: Ingest sales data from all regions
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
    all_converted_data = [us_east_converted, us_west_converted, eu_converted, apac_converted]
    aggregated_data = aggregate_data(all_converted_data)
    
    logger.info("Pipeline completed successfully.")
    logger.info(f"Global revenue report generated: {aggregated_data}")

if __name__ == "__main__":
    ecommerce_pipeline()

# Optional: Deployment/schedule configuration
# To schedule this flow to run daily, you can use Prefect's deployment and schedule features.
# Example:
# from prefect.deployments import Deployment
# from prefect.orion.schemas.schedules import CronSchedule
# deployment = Deployment.build_from_flow(
#     flow=ecommerce_pipeline,
#     name="daily-ecommerce-pipeline",
#     schedule=CronSchedule(cron="0 0 * * *", timezone="UTC"),
#     work_queue_name="default"
# )
# deployment.apply()