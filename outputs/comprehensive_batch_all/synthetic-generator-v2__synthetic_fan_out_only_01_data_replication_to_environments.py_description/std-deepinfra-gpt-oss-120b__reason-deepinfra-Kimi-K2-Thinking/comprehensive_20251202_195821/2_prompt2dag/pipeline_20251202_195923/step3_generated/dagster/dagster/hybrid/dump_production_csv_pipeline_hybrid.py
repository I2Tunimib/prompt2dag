from dagster import op, job, multiprocess_executor, fs_io_manager, ResourceDefinition


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
    description='Load CSV into Development Database',
)
def load_dev_database(context):
    """Op: Load CSV into Development Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_qa_database',
    description='Load CSV into QA Database',
)
def load_qa_database(context):
    """Op: Load CSV into QA Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name='load_staging_database',
    description='Load CSV into Staging Database',
)
def load_staging_database(context):
    """Op: Load CSV into Staging Database"""
    # Docker execution
    # Image: python:3.9
    pass


@job(
    name="dump_production_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "local_fs": ResourceDefinition.hardcoded_resource("/tmp"),
        "dev_db_conn": ResourceDefinition.hardcoded_resource(None),
        "staging_db_conn": ResourceDefinition.hardcoded_resource(None),
        "prod_db_conn": ResourceDefinition.hardcoded_resource(None),
        "qa_db_conn": ResourceDefinition.hardcoded_resource(None),
    },
    io_manager_def=fs_io_manager,
)
def dump_production_csv_pipeline():
    # Entry point
    dump = dump_production_csv()
    # Fan‑out to the three load ops
    load_dev_database()
    load_staging_database()
    load_qa_database()