from dagster import op, job, In, Out, RetryPolicy, graph, daily_schedule, ResourceDefinition, Field, String

# Resource stubs
csv_resource = ResourceDefinition.hardcoded_resource("csv_resource")
warehouse_resource = ResourceDefinition.hardcoded_resource("warehouse_resource")
bi_tool_resource = ResourceDefinition.hardcoded_resource("bi_tool_resource")

# Extract Stage
@op(required_resource_keys={"csv_resource"}, out={"claims_data": Out(), "providers_data": Out()})
def extract_claims(context):
    """Extract patient claims data from CSV file."""
    claims_data = context.resources.csv_resource.read("claims.csv")
    return {"claims_data": claims_data, "providers_data": None}

@op(required_resource_keys={"csv_resource"}, out={"claims_data": Out(), "providers_data": Out()})
def extract_providers(context):
    """Extract provider data from CSV file."""
    providers_data = context.resources.csv_resource.read("providers.csv")
    return {"claims_data": None, "providers_data": providers_data}

# Transform Stage
@op(ins={"claims_data": In(), "providers_data": In()}, out=Out())
def transform_join(context, claims_data, providers_data):
    """Join claims and provider data, anonymize PII, and calculate risk scores."""
    joined_data = claims_data.join(providers_data, on="provider_id")
    anonymized_data = joined_data.hash("patient_id")
    risk_scored_data = anonymized_data.calculate_risk_scores()
    return risk_scored_data

# Load Stage
@op(required_resource_keys={"warehouse_resource"})
def load_warehouse(context, transformed_data):
    """Load transformed data to healthcare analytics warehouse tables."""
    context.resources.warehouse_resource.load(transformed_data, "claims_fact")

@op(required_resource_keys={"bi_tool_resource"})
def refresh_bi(context):
    """Trigger refresh of BI dashboard tools including Power BI and Tableau."""
    context.resources.bi_tool_resource.refresh()

# Job Definition
@graph
def healthcare_etl():
    claims_data, _ = extract_claims()
    _, providers_data = extract_providers()
    transformed_data = transform_join(claims_data, providers_data)
    load_warehouse(transformed_data)
    refresh_bi()

# Job with Retry Policy
healthcare_etl_job = healthcare_etl.to_job(
    resource_defs={
        "csv_resource": csv_resource,
        "warehouse_resource": warehouse_resource,
        "bi_tool_resource": bi_tool_resource
    },
    config={
        "ops": {
            "extract_claims": {"config": {"file_path": "claims.csv"}},
            "extract_providers": {"config": {"file_path": "providers.csv"}},
        }
    },
    tags={"dagster/priority": "1"},
    description="Healthcare claims processing ETL pipeline",
    retry_policy=RetryPolicy(max_retries=2, delay=300)
)

# Schedule Definition
@daily_schedule(
    pipeline_name="healthcare_etl_job",
    start_date="2024-01-01",
    execution_time="00:00",
    execution_timezone="UTC",
)
def healthcare_etl_daily_schedule(date):
    return {
        "ops": {
            "extract_claims": {"config": {"file_path": "claims.csv"}},
            "extract_providers": {"config": {"file_path": "providers.csv"}},
        }
    }

# Launch Pattern
if __name__ == "__main__":
    result = healthcare_etl_job.execute_in_process()