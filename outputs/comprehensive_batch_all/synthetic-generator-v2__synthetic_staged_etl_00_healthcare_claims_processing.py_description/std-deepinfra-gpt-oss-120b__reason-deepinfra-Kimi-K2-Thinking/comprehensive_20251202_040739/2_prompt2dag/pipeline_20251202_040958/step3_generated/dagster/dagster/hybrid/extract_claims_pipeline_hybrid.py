from dagster import (
    op,
    job,
    multiprocess_executor,
    fs_io_manager,
    ResourceDefinition,
    ScheduleDefinition,
)

# ----------------------------------------------------------------------
# Task Definitions (provided exactly as given)
# ----------------------------------------------------------------------


@op(
    name="extract_claims",
    description="Extract Claims CSV",
)
def extract_claims(context):
    """Op: Extract Claims CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="extract_providers",
    description="Extract Providers CSV",
)
def extract_providers(context):
    """Op: Extract Providers CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="transform_join",
    description="Join Claims and Providers, Anonymize PII, Calculate Risk Scores",
)
def transform_join(context):
    """Op: Join Claims and Providers, Anonymize PII, Calculate Risk Scores"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="load_warehouse",
    description="Load Transformed Data to Data Warehouse",
)
def load_warehouse(context):
    """Op: Load Transformed Data to Data Warehouse"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="refresh_bi",
    description="Refresh BI Dashboards",
)
def refresh_bi(context):
    """Op: Refresh BI Dashboards"""
    # Docker execution
    # Image: python:3.9
    pass


# ----------------------------------------------------------------------
# Job Definition (fanout_fanin pattern)
# ----------------------------------------------------------------------


@job(
    name="extract_claims_pipeline",
    description=(
        "Healthcare claims processing ETL pipeline implementing a staged ETL pattern "
        "with parallel extraction, transformation, and parallel loading stages."
    ),
    executor_def=multiprocess_executor,
    resource_defs={
        "bi_tools": ResourceDefinition.hardcoded_resource(None),
        "local_fs": ResourceDefinition.hardcoded_resource(None),
        "postgres_warehouse": ResourceDefinition.hardcoded_resource(None),
        "io_manager": fs_io_manager,
    },
)
def extract_claims_pipeline():
    # Entry point ops (fan‑out)
    extract_claims_op = extract_claims()
    extract_providers_op = extract_providers()

    # Fan‑in transformation
    transform_join_op = transform_join()

    # Parallel downstream ops (fan‑out)
    load_warehouse_op = load_warehouse()
    refresh_bi_op = refresh_bi()

    # Define dependencies
    extract_claims_op >> transform_join_op
    extract_providers_op >> transform_join_op
    transform_join_op >> load_warehouse_op
    transform_join_op >> refresh_bi_op


# ----------------------------------------------------------------------
# Schedule (daily)
# ----------------------------------------------------------------------


daily_schedule = ScheduleDefinition(
    job=extract_claims_pipeline,
    cron_schedule="@daily",
    name="extract_claims_pipeline_schedule",
)