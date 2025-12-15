from dagster import op, job, In, Out, RetryPolicy, graph, daily_schedule, ResourceDefinition, Field, String

# Resource stubs
csv_resource = ResourceDefinition.config_field(
    Field(
        {"base_dir": Field(String, default_value="data", description="Base directory for CSV files")},
        description="CSV file resource configuration",
    )
)

warehouse_resource = ResourceDefinition.config_field(
    Field(
        {"connection_string": Field(String, default_value="postgresql://user:pass@localhost:5432/warehouse")},
        description="Postgres warehouse resource configuration",
    )
)

bi_tool_resource = ResourceDefinition.config_field(
    Field(
        {"base_url": Field(String, default_value="http://localhost:8080")},
        description="BI tool resource configuration",
    )
)

# Ops
@op(required_resource_keys={"csv_resource"})
def extract_claims(context):
    """Extract patient claims data from CSV file."""
    base_dir = context.resources.csv_resource.config.base_dir
    # Simulate data extraction
    claims_data = f"Claims data from {base_dir}/claims.csv"
    return claims_data

@op(required_resource_keys={"csv_resource"})
def extract_providers(context):
    """Extract provider data from CSV file."""
    base_dir = context.resources.csv_resource.config.base_dir
    # Simulate data extraction
    providers_data = f"Providers data from {base_dir}/providers.csv"
    return providers_data

@op(ins={"claims_data": In(), "providers_data": In()}, out=Out())
def transform_join(context, claims_data, providers_data):
    """Join claims and provider data, anonymize PII, and calculate risk scores."""
    # Simulate data transformation
    transformed_data = f"Transformed data from {claims_data} and {providers_data}"
    return transformed_data

@op(required_resource_keys={"warehouse_resource"})
def load_warehouse(context, transformed_data):
    """Load transformed data to healthcare analytics warehouse tables."""
    connection_string = context.resources.warehouse_resource.config.connection_string
    # Simulate data loading
    context.log.info(f"Loading data to {connection_string}")

@op(required_resource_keys={"bi_tool_resource"})
def refresh_bi(context):
    """Trigger refresh of BI dashboard tools including Power BI and Tableau."""
    base_url = context.resources.bi_tool_resource.config.base_url
    # Simulate BI tool refresh
    context.log.info(f"Refreshing BI tools at {base_url}")

# Job
@graph
def healthcare_etl():
    claims_data = extract_claims()
    providers_data = extract_providers()
    transformed_data = transform_join(claims_data, providers_data)
    load_warehouse(transformed_data)
    refresh_bi()

# Job with retry policy
healthcare_etl_job = healthcare_etl.to_job(
    name="healthcare_etl",
    resource_defs={
        "csv_resource": csv_resource,
        "warehouse_resource": warehouse_resource,
        "bi_tool_resource": bi_tool_resource,
    },
    description="Healthcare claims processing ETL pipeline",
    tags={"pipeline": "healthcare_etl"},
    executor_def=RetryPolicy(max_retries=2, delay=300),
)

# Schedule
@daily_schedule(
    pipeline_name="healthcare_etl",
    start_date="2024-01-01",
    execution_time="00:00",
    name="healthcare_etl_daily",
)
def healthcare_etl_daily(_):
    return {}

# Launch pattern
if __name__ == "__main__":
    result = healthcare_etl_job.execute_in_process()