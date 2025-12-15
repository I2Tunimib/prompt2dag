from dagster import op, job, multiprocess_executor, fs_io_manager, ScheduleDefinition


@op(
    name='dump_production_csv',
    description='Dump Production Database to CSV',
)
def dump_production_csv(context):
    """Op: Dump Production Database to CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_dev_database',
    description='Load CSV Snapshot into Development Database',
)
def load_dev_database(context):
    """Op: Load CSV Snapshot into Development Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_qa_database',
    description='Load CSV Snapshot into QA Database',
)
def load_qa_database(context):
    """Op: Load CSV Snapshot into QA Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_staging_database',
    description='Load CSV Snapshot into Staging Database',
)
def load_staging_database(context):
    """Op: Load CSV Snapshot into Staging Database"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="dump_production_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={"io_manager": fs_io_manager},
)
def dump_production_csv_pipeline():
    dump = dump_production_csv()
    load_dev_database(dump)
    load_qa_database(dump)
    load_staging_database(dump)


daily_schedule = ScheduleDefinition(
    job=dump_production_csv_pipeline,
    cron_schedule="@daily",
)