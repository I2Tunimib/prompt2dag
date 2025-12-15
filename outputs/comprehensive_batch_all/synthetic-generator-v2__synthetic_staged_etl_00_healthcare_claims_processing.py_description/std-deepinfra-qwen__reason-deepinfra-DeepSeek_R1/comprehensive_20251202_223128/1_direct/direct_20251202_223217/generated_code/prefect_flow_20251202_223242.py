from prefect import flow, task, get_run_logger
from datetime import datetime, timedelta
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule


@task
def extract_claims():
    logger = get_run_logger()
    logger.info("Extracting patient claims data from CSV file.")
    # Simulate data extraction
    claims_data = [{"claim_id": 1, "patient_id": 101, "provider_id": 201, "amount": 500.0}]
    return claims_data


@task
def extract_providers():
    logger = get_run_logger()
    logger.info("Extracting provider data from CSV file.")
    # Simulate data extraction
    providers_data = [{"provider_id": 201, "name": "Dr. Smith", "specialty": "Cardiology"}]
    return providers_data


@task
def transform_join(claims_data, providers_data):
    logger = get_run_logger()
    logger.info("Joining claims and provider data, anonymizing PII, and calculating risk scores.")
    # Simulate data transformation
    transformed_data = []
    for claim in claims_data:
        for provider in providers_data:
            if claim["provider_id"] == provider["provider_id"]:
                transformed_claim = {
                    "claim_id": claim["claim_id"],
                    "hashed_patient_id": hash(claim["patient_id"]),
                    "provider_name": provider["name"],
                    "specialty": provider["specialty"],
                    "amount": claim["amount"],
                    "risk_score": claim["amount"] * 0.01  # Example risk score calculation
                }
                transformed_data.append(transformed_claim)
    return transformed_data


@task
def load_warehouse(transformed_data):
    logger = get_run_logger()
    logger.info("Loading transformed data to healthcare analytics warehouse.")
    # Simulate data loading
    for record in transformed_data:
        logger.info(f"Loaded record: {record}")


@task
def refresh_bi():
    logger = get_run_logger()
    logger.info("Refreshing BI dashboard tools including Power BI and Tableau.")
    # Simulate BI refresh
    logger.info("BI dashboards refreshed.")


@flow(retries=2, retry_delay_seconds=300)
def healthcare_etl_pipeline():
    logger = get_run_logger()
    logger.info("Starting healthcare claims processing ETL pipeline.")

    # Extract stage
    claims_data = extract_claims.submit()
    providers_data = extract_providers.submit()

    # Transform stage
    transformed_data = transform_join.submit(claims_data, providers_data)

    # Load stage
    load_warehouse.submit(transformed_data)
    refresh_bi.submit()


if __name__ == "__main__":
    # Schedule: Daily execution starting January 1, 2024
    schedule = IntervalSchedule(interval=timedelta(days=1), start_date=datetime(2024, 1, 1))
    deployment = Deployment.build_from_flow(
        flow=healthcare_etl_pipeline,
        name="Healthcare ETL Pipeline",
        schedule=schedule,
    )
    deployment.apply()

    # Run the flow locally for testing
    healthcare_etl_pipeline()