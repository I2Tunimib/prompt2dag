from dagster import job, op, Out, In, execute_job, multiprocess_executor, fs_io_manager

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
    description='Ingest US-East Sales Data',
)
def ingest_us_east(context):
    """Op: Ingest US-East Sales Data"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='ingest_us_west',
    description='Ingest US-West Sales Data',
)
def ingest_us_west(context):
    """Op: Ingest US-West Sales Data"""
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
    description='Convert US-East Currency to USD',
)
def convert_currency_us_east(context):
    """Op: Convert US-East Currency to USD"""
    # Docker execution
    # Image: python:3.9
    pass

@op(
    name='convert_currency_us_west',
    description='Convert US-West Currency to USD',
)
def convert_currency_us_west(context):
    """Op: Convert US-West Currency to USD"""
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

@job(
    name="start_pipeline_pipeline",
    description="Comprehensive Pipeline Description: This pipeline performs multi-region ecommerce analytics by ingesting sales data from four geographic regions in parallel, converting regional currencies to USD, and aggregating the results into a global revenue report.",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": fs_io_manager},
)
def start_pipeline_pipeline():
    start = start_pipeline()
    ingest_us_east(start)
    ingest_us_west(start)
    ingest_eu(start)
    ingest_apac(start)
    
    convert_us_east = convert_currency_us_east(ingest_us_east(start))
    convert_us_west = convert_currency_us_west(ingest_us_west(start))
    convert_eu = convert_currency_eu(ingest_eu(start))
    convert_apac = convert_currency_apac(ingest_apac(start))
    
    aggregate = aggregate_global_revenue(convert_us_east, convert_us_west, convert_eu, convert_apac)
    end_pipeline(aggregate)