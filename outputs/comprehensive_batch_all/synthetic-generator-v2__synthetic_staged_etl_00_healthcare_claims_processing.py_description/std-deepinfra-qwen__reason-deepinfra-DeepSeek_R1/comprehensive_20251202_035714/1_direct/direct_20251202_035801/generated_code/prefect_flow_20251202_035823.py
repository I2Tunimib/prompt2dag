from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect.orion.schemas.schedules import IntervalSchedule
import pandas as pd

# Task to extract patient claims data from CSV
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract_claims():
    logger = get_run_logger()
    logger.info("Extracting patient claims data...")
    claims_df = pd.read_csv("path/to/claims.csv")
    return claims_df

# Task to extract provider data from CSV
@task(cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=1))
def extract_providers():
    logger = get_run_logger()
    logger.info("Extracting provider data...")
    providers_df = pd.read_csv("path/to/providers.csv")
    return providers_df

# Task to join and transform claims and provider data
@task
def transform_join(claims_df, providers_df):
    logger = get_run_logger()
    logger.info("Joining and transforming data...")
    merged_df = pd.merge(claims_df, providers_df, on="provider_id")
    merged_df["patient_id"] = merged_df["patient_id"].apply(lambda x: hash(x))
    merged_df["risk_score"] = merged_df["procedure_code"].apply(lambda x: len(x)) + merged_df["amount"] / 1000
    return merged_df

# Task to load transformed data to the analytics warehouse
@task
def load_warehouse(merged_df):
    logger = get_run_logger()
    logger.info("Loading data to analytics warehouse...")
    # Assuming a Postgres connection
    merged_df.to_sql("claims_fact", con="postgresql://user:password@host:port/dbname", if_exists="replace", index=False)

# Task to refresh BI dashboard tools
@task
def refresh_bi():
    logger = get_run_logger()
    logger.info("Refreshing BI dashboards...")
    # Trigger Power BI and Tableau refresh
    # This is a placeholder for actual API calls or commands
    pass

# Main flow to orchestrate the pipeline
@flow(name="Healthcare Claims ETL Pipeline", retries=2, retry_delay_seconds=300)
def healthcare_claims_etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting Healthcare Claims ETL Pipeline...")

    # Extract stage with parallel execution
    claims_df = extract_claims.submit()
    providers_df = extract_providers.submit()

    # Transform stage
    merged_df = transform_join(claims_df.result(), providers_df.result())

    # Load stage with parallel execution
    load_warehouse.submit(merged_df)
    refresh_bi.submit()

if __name__ == "__main__":
    # Schedule: Daily execution starting January 1, 2024
    # Note: To deploy with a schedule, use Prefect's deployment and schedule configuration
    healthcare_claims_etl_pipeline()