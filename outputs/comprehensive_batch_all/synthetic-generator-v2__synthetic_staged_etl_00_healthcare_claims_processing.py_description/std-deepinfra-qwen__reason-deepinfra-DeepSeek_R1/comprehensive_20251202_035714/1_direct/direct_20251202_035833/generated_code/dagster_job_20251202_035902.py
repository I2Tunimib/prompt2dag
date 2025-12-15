from dagster import op, job, In, Out, RetryPolicy, daily_schedule, graph

# Resources and Configs
# Example resource for CSV file handling
# @resource(config_schema={"base_dir": str})
# def csv_resource(context):
#     return {"base_dir": context.resource_config["base_dir"]}

# Example resource for Postgres database
# @resource(config_schema={"db_url": str})
# def postgres_resource(context):
#     return {"db_url": context.resource_config["db_url"]}

# Example resource for BI tools
# @resource(config_schema={"bi_tools": [str]})
# def bi_tool_resource(context):
#     return {"bi_tools": context.resource_config["bi_tools"]}

# Extract Stage
@op(out={"claims_data": Out()})
def extract_claims(context):
    """Extract patient claims data from CSV file."""
    # Example: Read claims data from CSV
    claims_data = [{"claim_id": 1, "patient_id": 101, "procedure_code": "A123", "amount": 500.0}]
    return claims_data

@op(out={"providers_data": Out()})
def extract_providers(context):
    """Extract provider data from CSV file."""
    # Example: Read providers data from CSV
    providers_data = [{"provider_id": 1, "name": "Dr. Smith", "specialty": "Cardiology"}]
    return providers_data

# Transform Stage
@op(ins={"claims_data": In(), "providers_data": In()}, out={"transformed_data": Out()})
def transform_join(context, claims_data, providers_data):
    """Join claims and provider data, anonymize PII, and calculate risk scores."""
    transformed_data = []
    for claim in claims_data:
        for provider in providers_data:
            if claim["provider_id"] == provider["provider_id"]:
                # Anonymize patient_id
                anonymized_patient_id = hash(claim["patient_id"])
                # Calculate risk score
                risk_score = len(claim["procedure_code"]) * claim["amount"] / 100
                transformed_data.append({
                    "claim_id": claim["claim_id"],
                    "anonymized_patient_id": anonymized_patient_id,
                    "provider_id": provider["provider_id"],
                    "procedure_code": claim["procedure_code"],
                    "amount": claim["amount"],
                    "risk_score": risk_score
                })
    return transformed_data

# Load Stage
@op(ins={"transformed_data": In()})
def load_warehouse(context, transformed_data):
    """Load transformed data to healthcare analytics warehouse tables."""
    # Example: Load data to Postgres
    for row in transformed_data:
        context.log.info(f"Loading row: {row}")

@op(ins={"transformed_data": In()})
def refresh_bi(context, transformed_data):
    """Trigger refresh of BI dashboard tools including Power BI and Tableau."""
    # Example: Refresh BI tools
    for tool in ["Power BI", "Tableau"]:
        context.log.info(f"Refreshing {tool} dashboard")

# Job Definition
@graph
def healthcare_etl():
    claims_data = extract_claims()
    providers_data = extract_providers()
    transformed_data = transform_join(claims_data, providers_data)
    load_warehouse(transformed_data)
    refresh_bi(transformed_data)

# Job with Retry Policy
healthcare_etl_job = healthcare_etl.to_job(
    name="healthcare_etl_job",
    description="Healthcare claims processing ETL pipeline",
    resource_defs={
        # "csv_resource": csv_resource,
        # "postgres_resource": postgres_resource,
        # "bi_tool_resource": bi_tool_resource
    },
    tags={"dagster/priority": "1"},
    executor_def=daily_schedule(
        name="daily_healthcare_etl",
        cron_schedule="0 0 * * *",
        start_date="2024-01-01",
        execution_timezone="UTC",
    ),
    op_retry_policy=RetryPolicy(max_retries=2, delay=300)
)

# Launch Pattern
if __name__ == "__main__":
    result = healthcare_etl_job.execute_in_process()