from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition, resource


@op(
    name='start_pipeline',
    description='Start Pipeline',
)
def start_pipeline(context):
    """Op: Start Pipeline"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='ingest_apac',
    description='Ingest APAC Sales Data',
)
def ingest_apac(context):
    """Op: Ingest APAC Sales Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='ingest_eu',
    description='Ingest EU Sales Data',
)
def ingest_eu(context):
    """Op: Ingest EU Sales Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='ingest_us_east',
    description='Ingest US‑East Sales Data',
)
def ingest_us_east(context):
    """Op: Ingest US‑East Sales Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='ingest_us_west',
    description='Ingest US‑West Sales Data',
)
def ingest_us_west(context):
    """Op: Ingest US‑West Sales Data"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='convert_currency_apac',
    description='Convert APAC Currency to USD',
)
def convert_currency_apac(context):
    """Op: Convert APAC Currency to USD"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='convert_currency_eu',
    description='Convert EU Currency to USD',
)
def convert_currency_eu(context):
    """Op: Convert EU Currency to USD"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='convert_currency_us_east',
    description='Convert US‑East Currency to USD',
)
def convert_currency_us_east(context):
    """Op: Convert US‑East Currency to USD"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='convert_currency_us_west',
    description='Convert US‑West Currency to USD',
)
def convert_currency_us_west(context):
    """Op: Convert US‑West Currency to USD"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='aggregate_global_revenue',
    description='Aggregate Global Revenue',
)
def aggregate_global_revenue(context):
    """Op: Aggregate Global Revenue"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='end_pipeline',
    description='End Pipeline',
)
def end_pipeline(context):
    """Op: End Pipeline"""
    # Docker execution
    # Image: python:3.9
    pass


# Dummy resource definition for regional_csv_storage
@resource
def regional_csv_storage(_):
    return None


@job(
    name="start_pipeline_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "regional_csv_storage": regional_csv_storage,
        "io_manager": fs_io_manager,
    },
)
def start_pipeline_pipeline():
    # Entry point
    start = start_pipeline()

    # US East branch
    ingest_us_east_op = ingest_us_east()
    start >> ingest_us_east_op
    convert_currency_us_east_op = convert_currency_us_east()
    ingest_us_east_op >> convert_currency_us_east_op

    # US West branch
    ingest_us_west_op = ingest_us_west()
    start >> ingest_us_west_op
    convert_currency_us_west_op = convert_currency_us_west()
    ingest_us_west_op >> convert_currency_us_west_op

    # EU branch
    ingest_eu_op = ingest_eu()
    start >> ingest_eu_op
    convert_currency_eu_op = convert_currency_eu()
    ingest_eu_op >> convert_currency_eu_op

    # APAC branch
    ingest_apac_op = ingest_apac()
    start >> ingest_apac_op
    convert_currency_apac_op = convert_currency_apac()
    ingest_apac_op >> convert_currency_apac_op

    # Aggregation (fan‑in)
    aggregate_op = aggregate_global_revenue()
    convert_currency_us_east_op >> aggregate_op
    convert_currency_us_west_op >> aggregate_op
    convert_currency_eu_op >> aggregate_op
    convert_currency_apac_op >> aggregate_op

    # End (fan‑out)
    end_op = end_pipeline()
    aggregate_op >> end_op