from prefect import flow, task, get_run_logger
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect.deployments import Deployment
from prefect.orion.schemas.schedules import IntervalSchedule

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def extract_claims():
    logger = get_run_logger()
    logger.info("Extracting patient claims data from CSV file.")
    # Simulate data extraction
    claims_data = [{"claim_id": 1, "patient_id": 101, "procedure_code": "A123", "amount": 500.00},
                   {"claim_id": 2, "patient_id": 102, "procedure_code": "B456", "amount": 750.00}]
    return claims_data

@task(cache_key_fn=task_input_hash, retries=2, retry_delay_seconds=300)
def extract_providers():
    logger = get_run_logger()
    logger.info("Extracting provider data from CSV file.")
    # Simulate data extraction
    providers_data = [{"provider_id": 1, "name": "Dr. Smith", "specialty": "Cardiology"},
                      {"provider_id": 2, "name": "Dr. Jones", "specialty": "Orthopedics"}]
    return providers_data

@task(retries=2, retry_delay_seconds=300)
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
                    "patient_id_hash": hash(claim["patient_id"]),
                    "provider_id": provider["provider_id"],
                    "provider_name": provider["name"],
                    "procedure_code": claim["procedure_code"],
                    "amount": claim["amount"],
                    "risk_score": calculate_risk_score(claim["procedure_code"], claim["amount"])
                }
                transformed_data.append(transformed_claim)
    return transformed_data

def calculate_risk_score(procedure_code, amount):
    # Simulate risk score calculation
    base_score = 100
    if procedure_code.startswith("A"):
        base_score += 50
    elif procedure_code.startswith("B"):
        base_score += 75
    return base_score + (amount / 100)

@task(retries=2, retry_delay_seconds=300)
def load_warehouse(transformed_data):
    logger = get_run_logger()
    logger.info("Loading transformed data to healthcare analytics warehouse.")
    # Simulate data loading
    for record in transformed_data:
        logger.info(f"Loaded record: {record}")

@task(retries=2, retry_delay_seconds=300)
def refresh_bi():
    logger = get_run_logger()
    logger.info("Refreshing BI dashboard tools including Power BI and Tableau.")
    # Simulate BI refresh
    logger.info("BI dashboards refreshed successfully.")

@flow(retries=2, retry_delay_seconds=300)
def healthcare_claims_etl():
    logger = get_run_logger()
    logger.info("Starting healthcare claims ETL pipeline.")

    # Extract stage
    claims_data = extract_claims.submit()
    providers_data = extract_providers.submit()

    # Transform stage
    transformed_data = transform_join.submit(claims_data, providers_data)

    # Load stage
    load_warehouse.submit(transformed_data)
    refresh_bi.submit()

if __name__ == '__main__':
    # Schedule: Daily execution
    # Start Date: January 1, 2024
    # Retry Policy: 2 retries with 5-minute delays
    # Deployment configuration (optional)
    deployment = Deployment.build_from_flow(
        flow=healthcare_claims_etl,
        name="healthcare_claims_etl",
        schedule=IntervalSchedule(interval=timedelta(days=1), start_date="2024-01-01T00:00:00Z"),
        work_queue_name="default",
    )
    deployment.apply()

    # Run the flow locally
    healthcare_claims_etl()