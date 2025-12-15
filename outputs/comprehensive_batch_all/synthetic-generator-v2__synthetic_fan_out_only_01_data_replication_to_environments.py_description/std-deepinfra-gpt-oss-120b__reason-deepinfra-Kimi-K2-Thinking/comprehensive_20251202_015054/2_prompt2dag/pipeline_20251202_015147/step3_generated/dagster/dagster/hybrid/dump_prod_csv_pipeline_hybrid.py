from dagster import op, job, multiprocess_executor, fs_io_manager, resource


@op(
    name="dump_prod_csv",
    description="Dump Production Database to CSV",
)
def dump_prod_csv(context):
    """Op: Dump Production Database to CSV"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="copy_dev",
    description="Load CSV Snapshot into Development Database",
)
def copy_dev(context):
    """Op: Load CSV Snapshot into Development Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="copy_qa",
    description="Load CSV Snapshot into QA Database",
)
def copy_qa(context):
    """Op: Load CSV Snapshot into QA Database"""
    # Docker execution
    # Image: python:3.9
    pass


@op(
    name="copy_staging",
    description="Load CSV Snapshot into Staging Database",
)
def copy_staging(context):
    """Op: Load CSV Snapshot into Staging Database"""
    # Docker execution
    # Image: python:3.9
    pass


@resource
def staging_database(_):
    """Placeholder resource for staging database."""
    return None


@resource
def dev_database(_):
    """Placeholder resource for development database."""
    return None


@resource
def qa_database(_):
    """Placeholder resource for QA database."""
    return None


@resource
def local_filesystem(_):
    """Placeholder resource for local filesystem."""
    return None


@job(
    name="dump_prod_csv_pipeline",
    description="No description provided.",
    executor_def=multiprocess_executor,
    resource_defs={
        "staging_database": staging_database,
        "dev_database": dev_database,
        "qa_database": qa_database,
        "local_filesystem": local_filesystem,
    },
    io_manager_def=fs_io_manager,
)
def dump_prod_csv_pipeline():
    # Entry point
    dump = dump_prod_csv()
    # Fan‑out to the three copy ops
    copy_dev().after(dump)
    copy_qa().after(dump)
    copy_staging().after(dump)